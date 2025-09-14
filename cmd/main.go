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
	"container-bench/internal/datahandeling"
	"container-bench/internal/logging"
	"container-bench/internal/scheduler"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/intel/goresctrl/pkg/rdt"
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
	dbClient      database.DatabaseClient
	dataframes    *dataframe.DataFrames
	dataHandler   datahandeling.DataHandler
	scheduler     scheduler.Scheduler
	collectors    []*collectors.ContainerCollector
	benchmarkID   int
	startTime     time.Time
	endTime       time.Time
	
	// Container tracking for clean orchestration
	containerIDs  map[int]string // container index -> container ID
	containerPIDs map[int]int    // container index -> process PID
}

func (cb *ContainerBench) cleanup() {
	// Emergency cleanup - only stop collectors
	// Containers cleanup is handled properly in cleanupContainers()
	for _, collector := range cb.collectors {
		if collector != nil {
			collector.Stop()
		}
	}
}

func (cb *ContainerBench) cleanupContainers(ctx context.Context) {
	logger := logging.GetLogger()
	logger.Info("Stopping and cleaning up containers")
	
	for containerIndex, containerID := range cb.containerIDs {
		if containerID != "" {
			logger.WithFields(logrus.Fields{
				"index":        containerIndex,
				"container_id": containerID[:12],
			}).Info("Stopping and removing container")
			cb.stopAndRemoveContainer(ctx, containerID)
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
		dataframes:    dataframe.NewDataFrames(),
		containerIDs:  make(map[int]string),
		containerPIDs: make(map[int]int),
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

	// Initialize data handler
	bench.dataHandler = datahandeling.NewDefaultDataHandler()

	// Get next benchmark ID
	lastID, err := bench.dbClient.GetLastBenchmarkID()
	if err != nil {
		logger.WithError(err).Error("Failed to get last benchmark ID")
		return fmt.Errorf("failed to get last benchmark ID: %w", err)
	}
	bench.benchmarkID = lastID + 1

	// Initialize RDT if any container has RDT monitoring enabled OR if scheduler needs RDT access
	rdtNeeded := bench.config.Benchmark.Scheduler.RDT
	if !rdtNeeded {
		// Check if any container has RDT monitoring enabled
		for _, containerConfig := range bench.config.GetContainersSorted() {
			if containerConfig.Data.RDT {
				rdtNeeded = true
				break
			}
		}
	}
	
	if rdtNeeded {
		logger.Info("Initializing Intel RDT (required for monitoring or scheduler)")
		if err := rdt.Initialize(""); err != nil {
			logger.WithError(err).Warn("Failed to initialize RDT, RDT features will be disabled")
		} else {
			logger.Info("Intel RDT initialized successfully")
		}
	}

	// Initialize scheduler based on configuration
	schedulerImpl := bench.config.Benchmark.Scheduler.Implementation
	if schedulerImpl == "" {
		schedulerImpl = "default" // Default to default scheduler
	}
	
	switch schedulerImpl {
	case "cache-aware":
		logger.Info("Using cache-aware scheduler")
		bench.scheduler = scheduler.NewCacheAwareSchedulerWithRDT(bench.config.Benchmark.Scheduler.RDT)
	case "default":
		logger.Info("Using default scheduler")
		bench.scheduler = scheduler.NewDefaultScheduler()
	default:
		logger.WithField("implementation", schedulerImpl).Warn("Unknown scheduler implementation, using default")
		bench.scheduler = scheduler.NewDefaultScheduler()
	}
	
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

	logger.WithFields(logrus.Fields{
		"benchmark_id": bench.benchmarkID,
		"name":        bench.config.Benchmark.Name,
	}).Info("Starting benchmark")

	// Execute benchmark with clean orchestration
	bench.startTime = time.Now()
	if err := bench.executeBenchmark(); err != nil {
		logger.WithError(err).Error("Benchmark failed")
		return fmt.Errorf("benchmark failed: %w", err)
	}

	logger.Info("Benchmark completed successfully")
	return nil
}

// executeBenchmark orchestrates the benchmark execution in proper order
func (cb *ContainerBench) executeBenchmark() error {
	logger := logging.GetLogger()
	
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
	
	// Pull all container images
	logger.Info("Pulling container images")
	if err := cb.pullImages(ctx); err != nil {
		return fmt.Errorf("failed to pull images: %w", err)
	}
	
	// Create all containers (but don't start them)
	logger.Info("Creating containers")
	if err := cb.createContainers(ctx); err != nil {
		return fmt.Errorf("failed to create containers: %w", err)
	}
	
	// Start all containers
	logger.Info("Starting containers")
	if err := cb.startContainers(ctx); err != nil {
		return fmt.Errorf("failed to start containers: %w", err)
	}
	
	// Start all collectors
	logger.Info("Starting collectors")
	if err := cb.startCollectors(ctx); err != nil {
		return fmt.Errorf("failed to start collectors: %w", err)
	}
	
	// Start scheduler
	logger.Info("Starting scheduler")
	if err := cb.startScheduler(); err != nil {
		return fmt.Errorf("failed to start scheduler: %w", err)
	}
	
	// Run benchmark (main execution loop)
	logger.Info("Running benchmark")
	if err := cb.runBenchmarkLoop(ctx); err != nil {
		// Even if benchmark fails, we need to clean up properly
		cb.cleanupInOrder(ctx)
		return fmt.Errorf("benchmark execution failed: %w", err)
	}
	
	// Normal cleanup and data writing in correct order
	return cb.cleanupInOrder(ctx)
}

// cleanupInOrder executes cleanup in the correct sequence
func (cb *ContainerBench) cleanupInOrder(ctx context.Context) error {
	// Stop scheduler
	cb.stopScheduler()
	
	// Stop collectors
	cb.stopCollectors()
	
	// RDT cleanup happens automatically when collectors stop
	
	// Stop and cleanup containers
	cb.cleanupContainers(ctx)
	
	// Write data to database (last step)
	return cb.writeDatabaseData()
}

// Pull all container images
func (cb *ContainerBench) pullImages(ctx context.Context) error {
	logger := logging.GetLogger()
	containers := cb.config.GetContainersSorted()

	for _, containerConfig := range containers {
		logger.WithFields(logrus.Fields{
			"index": containerConfig.Index,
			"image": containerConfig.Image,
		}).Info("Pulling container image")

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
	}

	return nil
}

// Create all containers (but don't start them)
func (cb *ContainerBench) createContainers(ctx context.Context) error {
	logger := logging.GetLogger()
	containers := cb.config.GetContainersSorted()

	for _, containerConfig := range containers {
		logger.WithFields(logrus.Fields{
			"index": containerConfig.Index,
			"image": containerConfig.Image,
		}).Info("Creating container")

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

		// Store container ID for later use
		cb.containerIDs[containerConfig.Index] = resp.ID

		logger.WithFields(logrus.Fields{
			"index":        containerConfig.Index,
			"container_id": resp.ID[:12],
		}).Info("Container created")
	}

	return nil
}

// Start all containers and get their PIDs
func (cb *ContainerBench) startContainers(ctx context.Context) error {
	logger := logging.GetLogger()
	containers := cb.config.GetContainersSorted()

	for _, containerConfig := range containers {
		containerID := cb.containerIDs[containerConfig.Index]
		
		// Start container
		if err := cb.dockerClient.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
			logger.WithFields(logrus.Fields{
				"index":        containerConfig.Index,
				"container_id": containerID[:12],
			}).WithError(err).Error("Failed to start container")
			return fmt.Errorf("failed to start container %d: %w", containerConfig.Index, err)
		}

		// Get container info to obtain PID
		info, err := cb.dockerClient.ContainerInspect(ctx, containerID)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"index":        containerConfig.Index,
				"container_id": containerID[:12],
			}).WithError(err).Error("Failed to inspect container")
			return fmt.Errorf("failed to inspect container %d: %w", containerConfig.Index, err)
		}

		pid := info.State.Pid
		cb.containerPIDs[containerConfig.Index] = pid

		logger.WithFields(logrus.Fields{
			"index": containerConfig.Index,
			"pid":   pid,
		}).Info("Container started")
	}

	return nil
}

// Start all collectors
func (cb *ContainerBench) startCollectors(ctx context.Context) error {
	logger := logging.GetLogger()
	containers := cb.config.GetContainersSorted()

	for _, containerConfig := range containers {
		containerID := cb.containerIDs[containerConfig.Index]
		pid := cb.containerPIDs[containerConfig.Index]
		
		// Setup data frame for this container
		containerDF := cb.dataframes.AddContainer(containerConfig.Index)

		// Create collector
		collectorConfig := collectors.CollectorConfig{
			Frequency:    time.Duration(containerConfig.Data.Frequency) * time.Millisecond,
			EnablePerf:   containerConfig.Data.Perf,
			EnableDocker: containerConfig.Data.Docker,
			EnableRDT:    containerConfig.Data.RDT,
		}

		collector := collectors.NewContainerCollector(containerConfig.Index, containerID, collectorConfig, containerDF)
		
		// Fix cgroup path - use the correct systemd cgroup structure
		cgroupPath := fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope", containerID)
		collector.SetContainerInfo(pid, cgroupPath, containerConfig.Core)
		
		// Start collector
		if err := collector.Start(ctx); err != nil {
			logger.WithFields(logrus.Fields{
				"index":        containerConfig.Index,
				"container_id": containerID[:12],
			}).WithError(err).Error("Failed to start collector")
			return fmt.Errorf("failed to start collector for container %d: %w", containerConfig.Index, err)
		}

		cb.collectors = append(cb.collectors, collector)

		logger.WithFields(logrus.Fields{
			"index":        containerConfig.Index,
			"container_id": containerID[:12],
		}).Info("Collector started")
	}

	return nil
}

// Start scheduler
func (cb *ContainerBench) startScheduler() error {
	logger := logging.GetLogger()
	containers := cb.config.GetContainersSorted()

	// Notify scheduler of container PIDs for cache-aware scheduling
	if cacheAwareScheduler, ok := cb.scheduler.(*scheduler.CacheAwareScheduler); ok {
		for _, containerConfig := range containers {
			pid := cb.containerPIDs[containerConfig.Index]
			cacheAwareScheduler.SetContainerPID(containerConfig.Index, pid)
		}
	}

	logger.Info("Scheduler started and configured")
	return nil
}

// Run benchmark main loop
func (cb *ContainerBench) runBenchmarkLoop(ctx context.Context) error {
	logger := logging.GetLogger()
	
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
			return nil
		case <-schedulerTicker.C:
			// Update scheduler with current data
			if err := cb.scheduler.ProcessDataFrames(cb.dataframes); err != nil {
				logger.WithError(err).Warn("Scheduler error")
			}
		}
	}
}

// Stop scheduler
func (cb *ContainerBench) stopScheduler() {
	logger := logging.GetLogger()
	logger.Info("Stopping scheduler")
	if cb.scheduler != nil {
		cb.scheduler.Shutdown()
	}
}

// Stop collectors
func (cb *ContainerBench) stopCollectors() {
	logger := logging.GetLogger()
	logger.Info("Stopping collectors")
	for _, collector := range cb.collectors {
		if err := collector.Stop(); err != nil {
			logger.WithError(err).Warn("Error stopping collector")
		}
	}
}

// Clean up RDT (handled in finalizeBenchmark)
// Stop and cleanup containers handled by cleanupContainers

// Write data to database (happens after all cleanup)
func (cb *ContainerBench) writeDatabaseData() error {
	logger := logging.GetLogger()
	logger.Info("Writing data to database")

	// Process dataframes through data handler
	logger.Info("Processing dataframes through data handler")
	processedMetrics, err := cb.dataHandler.ProcessDataFrames(cb.benchmarkID, cb.config, cb.dataframes, cb.startTime, cb.endTime)
	if err != nil {
		logger.WithError(err).Error("Failed to process dataframes")
		return fmt.Errorf("failed to process dataframes: %w", err)
	}

	// Export processed data to database
	if err := cb.dbClient.WriteBenchmarkMetrics(cb.benchmarkID, cb.config, processedMetrics, cb.startTime, cb.endTime); err != nil {
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
