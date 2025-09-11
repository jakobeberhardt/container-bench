package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"container-bench/internal/collectors"
	"container-bench/internal/config"
	"container-bench/internal/database"
	"container-bench/internal/dataframe"
	"container-bench/internal/logging"
	"container-bench/internal/scheduler"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const Version = "1.0.0"

// isPrivateRegistryImage checks if an image belongs to a private registry
func isPrivateRegistryImage(image string, registryHost string) bool {
	if registryHost == "" {
		return false
	}
	
	// If image starts with the registry host, it's from the private registry
	return strings.HasPrefix(image, registryHost)
}

// createRegistryAuth creates a base64-encoded auth string for Docker registry
func createRegistryAuth(registryConfig *config.RegistryConfig) (string, error) {
	if registryConfig == nil {
		return "", nil
	}
	
	authConfig := registry.AuthConfig{
		Username:      registryConfig.Username,
		Password:      registryConfig.Password,
		ServerAddress: registryConfig.Host,
	}
	
	authJSON, err := json.Marshal(authConfig)
	if err != nil {
		return "", fmt.Errorf("failed to marshal auth config: %w", err)
	}
	
	return base64.URLEncoding.EncodeToString(authJSON), nil
}

type ContainerBench struct {
	config        *config.BenchmarkConfig
	configContent string
	dockerClient  *client.Client
	dbClient      *database.InfluxDBClient
	dataframes    *dataframe.DataFrames
	scheduler     scheduler.Scheduler
	collectors    []*collectors.ContainerCollector
	benchmarkID   int
	startTime     time.Time
	endTime       time.Time
}

func (cb *ContainerBench) cleanup() {
	// Emergency cleanup - only stop collectors
	// Containers should only be stopped in finalizeBenchmark() for proper orchestration
	for _, collector := range cb.collectors {
		if collector != nil {
			collector.Stop()
		}
	}
}

func (cb *ContainerBench) cleanupContainers(ctx context.Context) {
	for _, collector := range cb.collectors {
		if collector != nil && collector.ContainerID != "" {
			cb.stopAndRemoveContainer(ctx, collector.ContainerID)
		}
	}
}

func (cb *ContainerBench) stopAndRemoveContainer(ctx context.Context, containerID string) {
	logger := logging.GetLogger()
	
	// Check if container exists and get its state
	containerInfo, err := cb.dockerClient.ContainerInspect(ctx, containerID)
	if err != nil {
		if client.IsErrNotFound(err) {
			logger.WithField("container_id", containerID[:12]).Warn("Container not found - may have been removed unexpectedly")
			return
		}
		logger.WithField("container_id", containerID[:12]).WithError(err).Error("Failed to inspect container")
		return
	}
	
	// Check container state and warn if unexpectedly stopped
	if !containerInfo.State.Running {
		if containerInfo.State.Status == "exited" {
			logger.WithFields(logrus.Fields{
				"container_id": containerID[:12],
				"exit_code":    containerInfo.State.ExitCode,
				"status":       containerInfo.State.Status,
			}).Warn("Container exited unexpectedly before benchmark completion")
		} else {
			logger.WithFields(logrus.Fields{
				"container_id": containerID[:12],
				"status":       containerInfo.State.Status,
			}).Warn("Container in unexpected state")
		}
	} else {
		// Container is running as expected - stop it
		logger.WithField("container_id", containerID[:12]).Info("Stopping container")
		
		timeout := 10 // 10 seconds
		stopOptions := container.StopOptions{Timeout: &timeout}
		if err := cb.dockerClient.ContainerStop(ctx, containerID, stopOptions); err != nil {
			if !client.IsErrNotFound(err) {
				logger.WithField("container_id", containerID[:12]).WithError(err).Error("Failed to stop container")
			}
		} else {
			logger.WithField("container_id", containerID[:12]).Info("Container stopped")
		}
	}
	
	// Always try to remove container for cleanup
	if err := cb.dockerClient.ContainerRemove(ctx, containerID, types.ContainerRemoveOptions{Force: true}); err != nil {
		if !client.IsErrNotFound(err) {
			logger.WithField("container_id", containerID[:12]).WithError(err).Error("Failed to remove container")
		}
	} else {
		logger.WithField("container_id", containerID[:12]).Info("Container removed")
	}
}

func loadEnvironment() {
	logger := logging.GetLogger()
	
	// Try to load .env file from current directory
	envFile := ".env"
	if _, err := os.Stat(envFile); err == nil {
		if err := godotenv.Load(envFile); err != nil {
			logger.WithField("file", envFile).WithError(err).Warn("Error loading .env file")
		} else {
			logger.WithField("file", envFile).Info("Loaded environment variables")
		}
	} else {
		// Try to load from the application directory
		if execPath, err := os.Executable(); err == nil {
			appDir := filepath.Dir(execPath)
			envFile = filepath.Join(appDir, ".env")
			if _, err := os.Stat(envFile); err == nil {
				if err := godotenv.Load(envFile); err != nil {
					logger.WithField("file", envFile).WithError(err).Warn("Error loading .env file")
				} else {
					logger.WithField("file", envFile).Info("Loaded environment variables")
				}
			}
		}
	}
}

func validateEnvironment() error {
	logger := logging.GetLogger()
	
	requiredVars := []string{
		"INFLUXDB_HOST",
		"INFLUXDB_USER", 
		"INFLUXDB_TOKEN",
		"INFLUXDB_ORG",
		"INFLUXDB_BUCKET",
	}
	
	var missing []string
	for _, varName := range requiredVars {
		if os.Getenv(varName) == "" {
			missing = append(missing, varName)
		}
	}
	
	if len(missing) > 0 {
		logger.WithField("missing_vars", missing).Error("Missing required environment variables")
		return fmt.Errorf("missing required environment variables: %v. Please ensure your .env file contains these variables", missing)
	}
	
	logger.Debug("All required environment variables are present")
	return nil
}

func main() {
	// Initialize logging
	logger := logging.GetLogger()
	
	// Load environment variables from .env file if it exists
	loadEnvironment()

	var configFile string

	rootCmd := &cobra.Command{
		Use:   "container-bench",
		Short: "Container performance benchmarking tool",
		Long:  "A configurable tool for profiling Docker containers with perf, docker stats, and Intel RDT",
	}

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Run a benchmark",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate environment variables
			if err := validateEnvironment(); err != nil {
				return err
			}
			return runBenchmark(configFile)
		},
	}

	validateCmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate a benchmark configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			return validateConfig(configFile)
		},
	}

	runCmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to benchmark configuration file")
	runCmd.MarkFlagRequired("config")

	validateCmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to benchmark configuration file")
	validateCmd.MarkFlagRequired("config")

	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(validateCmd)

	if err := rootCmd.Execute(); err != nil {
		logger.WithError(err).Fatal("Command execution failed")
	}
}

func validateConfig(configFile string) error {
	logger := logging.GetLogger()
	
	_, err := config.LoadConfig(configFile)
	if err != nil {
		logger.WithField("config_file", configFile).WithError(err).Error("Configuration validation failed")
		return err
	}
	logger.WithField("config_file", configFile).Info("Configuration is valid")
	return nil
}

func runBenchmark(configFile string) error {
	logger := logging.GetLogger()
	
	bench := &ContainerBench{
		dataframes: dataframe.NewDataFrames(),
	}

	// Load configuration
	var err error
	bench.config, bench.configContent, err = config.LoadConfigWithContent(configFile)
	if err != nil {
		logger.WithField("config_file", configFile).WithError(err).Error("Failed to load configuration")
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Set log level from configuration
	if err := logging.SetLogLevel(bench.config.Benchmark.LogLevel); err != nil {
		logger.WithField("log_level", bench.config.Benchmark.LogLevel).WithError(err).Warn("Invalid log level in config, using INFO")
		logging.SetLogLevel("info")
	} else {
		logger.WithField("log_level", bench.config.Benchmark.LogLevel).Debug("Log level set from configuration")
	}

	// Initialize Docker client
	bench.dockerClient, err = client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		logger.WithError(err).Error("Failed to create Docker client")
		return fmt.Errorf("failed to create Docker client: %w", err)
	}
	defer bench.dockerClient.Close()

	// Initialize database client
	bench.dbClient, err = database.NewInfluxDBClient(bench.config.Benchmark.Data.DB)
	if err != nil {
		logger.WithError(err).Error("Failed to create database client")
		return fmt.Errorf("failed to create database client: %w", err)
	}
	defer bench.dbClient.Close()

	// Get next benchmark ID
	lastID, err := bench.dbClient.GetLastBenchmarkID()
	if err != nil {
		logger.WithError(err).Error("Failed to get last benchmark ID")
		return fmt.Errorf("failed to get last benchmark ID: %w", err)
	}
	bench.benchmarkID = lastID + 1

	// Initialize scheduler
	bench.scheduler = scheduler.NewDefaultScheduler()
	
	// Set scheduler log level if specified in config
	if bench.config.Benchmark.Scheduler.LogLevel != "" {
		if err := bench.scheduler.SetLogLevel(bench.config.Benchmark.Scheduler.LogLevel); err != nil {
			logger.WithField("scheduler_log_level", bench.config.Benchmark.Scheduler.LogLevel).WithError(err).Warn("Invalid scheduler log level, using default")
		} else {
			logger.WithField("scheduler_log_level", bench.config.Benchmark.Scheduler.LogLevel).Debug("Scheduler log level set from configuration")
		}
	}
	
	if err := bench.scheduler.Initialize(); err != nil {
		logger.WithError(err).Error("Failed to initialize scheduler")
		return fmt.Errorf("failed to initialize scheduler: %w", err)
	}
	defer bench.scheduler.Shutdown()

	logger.WithFields(logrus.Fields{
		"benchmark_id": bench.benchmarkID,
		"name":        bench.config.Benchmark.Name,
	}).Info("Starting benchmark")

	// Setup containers
	if err := bench.setupContainers(); err != nil {
		logger.WithError(err).Error("Failed to setup containers")
		return fmt.Errorf("failed to setup containers: %w", err)
	}
	// Ensure cleanup happens regardless of how the function exits
	defer bench.cleanup()

	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Received interrupt signal, shutting down")
		cancel()
	}()

	// Start benchmark
	bench.startTime = time.Now()
	if err := bench.startBenchmark(ctx); err != nil {
		logger.WithError(err).Error("Benchmark failed")
		return fmt.Errorf("benchmark failed: %w", err)
	}

	logger.Info("Benchmark completed successfully")
	return nil
}

func (cb *ContainerBench) setupContainers() error {
	logger := logging.GetLogger()
	containers := cb.config.GetContainersSorted()

	for _, containerConfig := range containers {
		logger.WithFields(logrus.Fields{
			"index": containerConfig.Index,
			"image": containerConfig.Image,
		}).Info("Setting up container")

		// Pull image
		ctx := context.Background()
		logger.WithField("image", containerConfig.Image).Debug("Pulling image")
		
		// Prepare pull options with authentication if needed
		pullOptions := types.ImagePullOptions{}
		
		// Check if this image requires private registry authentication
		registryConfig := cb.config.GetRegistryConfig()
		if registryConfig != nil && isPrivateRegistryImage(containerConfig.Image, registryConfig.Host) {
			authString, err := createRegistryAuth(registryConfig)
			if err != nil {
				logger.WithField("image", containerConfig.Image).WithError(err).Error("Failed to create registry auth")
				return fmt.Errorf("failed to create registry auth for %s: %w", containerConfig.Image, err)
			}
			pullOptions.RegistryAuth = authString
			logger.WithFields(logrus.Fields{
				"image":    containerConfig.Image,
				"registry": registryConfig.Host,
			}).Debug("Using private registry authentication")
		}
		
		pullResp, err := cb.dockerClient.ImagePull(ctx, containerConfig.Image, pullOptions)
		if err != nil {
			logger.WithField("image", containerConfig.Image).WithError(err).Error("Failed to pull image")
			return fmt.Errorf("failed to pull image %s: %w", containerConfig.Image, err)
		}
		defer pullResp.Close()
		
		// Read the pull response to completion (required for pull to finish)
		_, err = io.Copy(io.Discard, pullResp)
		if err != nil {
			logger.WithField("image", containerConfig.Image).WithError(err).Error("Failed to complete image pull")
			return fmt.Errorf("failed to complete image pull for %s: %w", containerConfig.Image, err)
		}
		logger.WithField("image", containerConfig.Image).Info("Image pulled successfully")

		// Create container
		containerName := containerConfig.GetContainerName(cb.benchmarkID)
		
		config := &container.Config{
			Image: containerConfig.Image,
		}

		if containerConfig.Command != "" {
			config.Cmd = []string{"sh", "-c", containerConfig.Command}
		}

		hostConfig := &container.HostConfig{}

		if containerConfig.Port != "" {
			// Parse port mapping (format: host:container, e.g., "8080:80" or "8080:8080")
			parts := strings.Split(containerConfig.Port, ":")
			if len(parts) != 2 {
				logger.WithField("port", containerConfig.Port).Error("Invalid port format")
				return fmt.Errorf("invalid port format %s, expected format: host:container", containerConfig.Port)
			}
			
			hostPort := parts[0]
			containerPort := parts[1]
			
			// Create port binding
			port, err := nat.NewPort("tcp", containerPort)
			if err != nil {
				logger.WithField("container_port", containerPort).WithError(err).Error("Invalid container port")
				return fmt.Errorf("invalid container port %s: %w", containerPort, err)
			}
			
			hostConfig.PortBindings = nat.PortMap{
				port: []nat.PortBinding{
					{
						HostIP:   "0.0.0.0",
						HostPort: hostPort,
					},
				},
			}
			
			// Also expose the port
			if config.ExposedPorts == nil {
				config.ExposedPorts = make(nat.PortSet)
			}
			config.ExposedPorts[port] = struct{}{}
		}

		// Set CPU affinity
		hostConfig.CpusetCpus = strconv.Itoa(containerConfig.Core)

		resp, err := cb.dockerClient.ContainerCreate(ctx, config, hostConfig, nil, nil, containerName)
		if err != nil {
			logger.WithField("container_name", containerName).WithError(err).Error("Failed to create container")
			return fmt.Errorf("failed to create container %s: %w", containerName, err)
		}

		// Setup data frame for this container
		containerDF := cb.dataframes.AddContainer(containerConfig.Index)

		// Create collector
		collectorConfig := collectors.CollectorConfig{
			Frequency:    time.Duration(containerConfig.Data.Frequency) * time.Millisecond,
			EnablePerf:   containerConfig.Data.Perf,
			EnableDocker: containerConfig.Data.Docker,
			EnableRDT:    containerConfig.Data.RDT,
		}

		collector := collectors.NewContainerCollector(containerConfig.Index, resp.ID, collectorConfig, containerDF)
		cb.collectors = append(cb.collectors, collector)

		logger.WithFields(logrus.Fields{
			"index":        containerConfig.Index,
			"container_id": resp.ID[:12],
		}).Info("Container created")
	}

	return nil
}

func (cb *ContainerBench) startBenchmark(ctx context.Context) error {
	logger := logging.GetLogger()
	
	// Start all containers and get their PIDs
	for i, collector := range cb.collectors {
		containerConfig := cb.config.GetContainersSorted()[i]
		
		// Start container
		if err := cb.dockerClient.ContainerStart(ctx, collector.ContainerID, container.StartOptions{}); err != nil {
			logger.WithFields(logrus.Fields{
				"index":        containerConfig.Index,
				"container_id": collector.ContainerID,
			}).WithError(err).Error("Failed to start container")
			return fmt.Errorf("failed to start container %d: %w", containerConfig.Index, err)
		}

		// Get container info
		info, err := cb.dockerClient.ContainerInspect(ctx, collector.ContainerID)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"index":        containerConfig.Index,
				"container_id": collector.ContainerID,
			}).WithError(err).Error("Failed to inspect container")
			return fmt.Errorf("failed to inspect container %d: %w", containerConfig.Index, err)
		}

		pid := info.State.Pid
		// Fix cgroup path - use the correct systemd cgroup structure
		cgroupPath := fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope", info.ID)

		collector.SetContainerInfo(pid, cgroupPath, containerConfig.Core)

		// Start collector
		if err := collector.Start(ctx); err != nil {
			logger.WithFields(logrus.Fields{
				"index":        containerConfig.Index,
				"container_id": collector.ContainerID,
			}).WithError(err).Error("Failed to start collector")
			return fmt.Errorf("failed to start collector for container %d: %w", containerConfig.Index, err)
		}

		logger.WithFields(logrus.Fields{
			"index": containerConfig.Index,
			"pid":   pid,
		}).Info("Container started")
	}

	// Start scheduler updates
	schedulerTicker := time.NewTicker(1 * time.Second)
	defer schedulerTicker.Stop()

	// Set up timeout
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, cb.config.GetMaxDuration())
	defer timeoutCancel()

	logger.WithField("duration", cb.config.GetMaxDuration()).Info("Benchmark running")

	// Main benchmark loop
	for {
		select {
		case <-timeoutCtx.Done():
			if timeoutCtx.Err() == context.DeadlineExceeded {
				logger.Info("Benchmark timeout reached")
			} else {
				logger.Info("Benchmark interrupted")
			}
			cb.endTime = time.Now()
			return cb.finalizeBenchmark()
		case <-schedulerTicker.C:
			// Update scheduler with current data
			if err := cb.scheduler.ProcessDataFrames(cb.dataframes); err != nil {
				logger.WithError(err).Warn("Scheduler error")
			}
		}
	}
}

func (cb *ContainerBench) finalizeBenchmark() error {
	logger := logging.GetLogger()
	logger.Info("Finalizing benchmark")

	// Step 1: Stop all collectors first
	logger.Info("Stopping profilers/collectors")
	for _, collector := range cb.collectors {
		if err := collector.Stop(); err != nil {
			logger.WithError(err).Warn("Error stopping collector")
		}
	}

	// Step 2: Stop and remove all containers (single orchestration point)
	logger.Info("Stopping and removing containers")
	ctx := context.Background()
	cb.cleanupContainers(ctx)

	// Export data to database
	logger.Info("Exporting data to database")
	if err := cb.dbClient.WriteDataFrames(cb.benchmarkID, cb.config, cb.dataframes, cb.startTime, cb.endTime); err != nil {
		logger.WithError(err).Error("Failed to export data")
		return fmt.Errorf("failed to export data: %w", err)
	}

	// Collect and export metadata
	logger.Info("Collecting and exporting metadata")
	metadata, err := database.CollectBenchmarkMetadata(cb.benchmarkID, cb.config, cb.configContent, cb.dataframes, cb.startTime, cb.endTime, Version)
	if err != nil {
		logger.WithError(err).Error("Failed to collect metadata")
		return fmt.Errorf("failed to collect metadata: %w", err)
	}

	if err := cb.dbClient.WriteMetadata(metadata); err != nil {
		logger.WithError(err).Error("Failed to export metadata")
		return fmt.Errorf("failed to export metadata: %w", err)
	}

	duration := cb.endTime.Sub(cb.startTime)
	logger.WithFields(logrus.Fields{
		"benchmark_id": cb.benchmarkID,
		"duration":     duration,
	}).Info("Benchmark completed")

	return nil
}
