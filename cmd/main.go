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
	"strings"
	"sync"
	"syscall"
	"time"

	"container-bench/internal/collectors"
	"container-bench/internal/config"
	"container-bench/internal/database"
	"container-bench/internal/dataframe"
	"container-bench/internal/datahandeling"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/plot"
	"container-bench/internal/probe"
	"container-bench/internal/probe/kernels"
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

const Version = "1.0.1"

// checks if an image belongs to a private registry
func isPrivateRegistryImage(image string, registryHost string) bool {
	if registryHost == "" {
		return false
	}

	// If image starts with the registry host, its from the private registry
	return strings.HasPrefix(image, registryHost)
}

// creates a base64-encoded auth string for Docker registry
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
	hostConfig    *host.HostConfig
	collectors    []*collectors.ContainerCollector
	prober        *probe.Probe
	benchmarkID   int
	startTime     time.Time
	endTime       time.Time

	// Container tracking for clean orchestration
	containerIDs  map[int]string
	containerPIDs map[int]int
	networkID     string
}

func (cb *ContainerBench) cleanup() {

	for _, collector := range cb.collectors {
		if collector != nil {
			collector.Stop()
		}
	}
}

func (cb *ContainerBench) cleanupContainers(ctx context.Context) {
	logger := logging.GetLogger()
	logger.Info("Stopping and removing all containers")

	var wg sync.WaitGroup
	for containerIndex, containerID := range cb.containerIDs {
		if containerID != "" {
			wg.Add(1)
			go func(idx int, id string) {
				defer wg.Done()
				logger.WithFields(logrus.Fields{
					"index":        idx,
					"container_id": id[:12],
				}).Debug("Force removing container")
				cb.stopAndRemoveContainer(ctx, id)
			}(containerIndex, containerID)
		}
	}

	wg.Wait()
	logger.Info("All containers stopped and removed")
}

func (cb *ContainerBench) stopAndRemoveContainer(ctx context.Context, containerID string) {
	logger := logging.GetLogger()
	logger.WithField("container_id", containerID[:12]).Debug("Force removing container")

	removeOptions := types.ContainerRemoveOptions{
		Force:         true,
		RemoveVolumes: true,
	}

	if err := cb.dockerClient.ContainerRemove(ctx, containerID, removeOptions); err != nil {
		if !client.IsErrNotFound(err) {
			logger.WithField("container_id", containerID[:12]).WithError(err).Warn("Failed to force remove container")
		}
	} else {
		logger.WithField("container_id", containerID[:12]).Debug("Container force removed")
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
			logger.WithField("file", envFile).Debug("Loaded environment variables")
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
					logger.WithField("file", envFile).Debug("Loaded environment variables")
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

	loadEnvironment()

	var configFile string
	var benchmarkID int
	var xField, yField string
	var interval float64
	var minVal, maxVal float64
	var minSet, maxSet bool
	var probeIndices []int
	var onlyPlot, onlyWrapper bool
	var logLevel string

	rootCmd := &cobra.Command{
		Use:   "container-bench",
		Short: "Container performance benchmarking tool",
		Long:  "A configurable tool for profiling Docker containers with perf, docker stats, and Intel RDT",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if logLevel != "" {
				if err := logging.SetLogLevel(logLevel); err != nil {
					return fmt.Errorf("invalid log level: %w", err)
				}
			}
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "", "Set log level (trace, debug, info, warn, error)")

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

	plotCmd := &cobra.Command{
		Use:   "plot",
		Short: "Generate plots from benchmark data",
		Long:  "Generate LaTeX/TikZ plots from benchmark data stored in InfluxDB",
	}

	timeseriesCmd := &cobra.Command{
		Use:   "timeseries",
		Short: "Generate a timeseries plot",
		Long:  "Generate a timeseries plot for a specific benchmark and metric",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateEnvironment(); err != nil {
				return err
			}
			var minPtr, maxPtr *float64
			if minSet {
				minPtr = &minVal
			}
			if maxSet {
				maxPtr = &maxVal
			}
			return generateTimeseriesPlot(benchmarkID, xField, yField, interval, minPtr, maxPtr, onlyPlot, onlyWrapper)
		},
	}

	polarCmd := &cobra.Command{
		Use:   "polar",
		Short: "Generate a polar plot",
		Long:  "Generate a polar sensitivity plot from probe data",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateEnvironment(); err != nil {
				return err
			}
			return generatePolarPlot(probeIndices, onlyPlot, onlyWrapper)
		},
	}

	runCmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to benchmark configuration file")
	runCmd.MarkFlagRequired("config")

	validateCmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to benchmark configuration file")
	validateCmd.MarkFlagRequired("config")

	timeseriesCmd.Flags().IntVar(&benchmarkID, "benchmark-id", 0, "Benchmark ID to plot")
	timeseriesCmd.Flags().StringVar(&xField, "x", "relative_time", "X-axis field")
	timeseriesCmd.Flags().StringVar(&yField, "y", "", "Y-axis field")
	timeseriesCmd.Flags().Float64Var(&interval, "interval", 0, "Aggregation interval in seconds (0 = no aggregation)")
	timeseriesCmd.Flags().Float64Var(&minVal, "min", 0, "Minimum Y-axis value")
	timeseriesCmd.Flags().Float64Var(&maxVal, "max", 0, "Maximum Y-axis value")
	timeseriesCmd.Flags().BoolVar(&onlyPlot, "plot", false, "Print only the plot file (TikZ)")
	timeseriesCmd.Flags().BoolVar(&onlyWrapper, "wrapper", false, "Print only the wrapper file (LaTeX)")
	timeseriesCmd.MarkFlagRequired("benchmark-id")
	timeseriesCmd.MarkFlagRequired("y")

	timeseriesCmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		minSet = cmd.Flags().Changed("min")
		maxSet = cmd.Flags().Changed("max")
		return nil
	}

	polarCmd.Flags().IntSliceVar(&probeIndices, "probes", []int{}, "Comma-separated list of probe indices")
	polarCmd.Flags().BoolVar(&onlyPlot, "plot", false, "Print only the plot file (TikZ)")
	polarCmd.Flags().BoolVar(&onlyWrapper, "wrapper", false, "Print only the wrapper file (LaTeX)")
	polarCmd.MarkFlagRequired("probes")

	plotCmd.AddCommand(timeseriesCmd)
	plotCmd.AddCommand(polarCmd)

	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(validateCmd)
	rootCmd.AddCommand(plotCmd)

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

	// Load configuration first to determine RDT requirements
	bench := &ContainerBench{
		dataframes:    dataframe.NewDataFrames(),
		containerIDs:  make(map[int]string),
		containerPIDs: make(map[int]int),
	}

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

	// Check if RDT is needed for any container or scheduler
	rdtNeeded := false
	for _, container := range bench.config.Containers {
		rdtConfig := container.Data.GetRDTConfig()
		if rdtConfig != nil {
			rdtNeeded = true
			break
		}
	}
	if bench.config.Benchmark.Scheduler.RDT {
		rdtNeeded = true
	}

	// Initialize RDT early if needed
	if rdtNeeded {
		logger.Info("Initializing Intel RDT (required for monitoring or scheduler)")
		if err := rdt.Initialize(""); err != nil {
			logger.WithError(err).Warn("Failed to initialize RDT, RDT features will be disabled")
		} else {
			logger.Info("Intel RDT initialized successfully")
		}
	}

	// Initialize host configuration after RDT is initialized
	hostConfig, hostErr := host.GetHostConfig()
	if hostErr != nil {
		logger.WithError(hostErr).Error("Failed to initialize host configuration")
		return hostErr
	}
	bench.hostConfig = hostConfig

	logger.WithFields(logrus.Fields{
		"hostname":       hostConfig.Hostname,
		"cpu_model":      hostConfig.CPUModel,
		"physical_cores": hostConfig.Topology.PhysicalCores,
		"logical_cores":  hostConfig.Topology.LogicalCores,
		"l3_cache_mb":    hostConfig.L3Cache.SizeMB,
		"rdt_supported":  hostConfig.RDT.Supported,
	}).Info("Host configuration initialized")

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

	// Initialize scheduler based on configuration
	schedulerImpl := bench.config.Benchmark.Scheduler.Implementation
	if schedulerImpl == "" {
		schedulerImpl = "default" // Default to default scheduler
	}

	switch schedulerImpl {
	case "default":
		logger.Info("Using default scheduler")
		bench.scheduler = scheduler.NewDefaultScheduler()
	case "probe":
		logger.Info("Using probe scheduler")
		bench.scheduler = scheduler.NewProbeScheduler()
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

	// Set host config for scheduler
	bench.scheduler.SetHostConfig(bench.hostConfig)

	// Initialize Probe if configured
	if bench.config.Benchmark.Scheduler.Prober != nil {
		proberConfig := bench.config.Benchmark.Scheduler.Prober
		logger.WithField("prober_implementation", proberConfig.Implementation).Info("Initializing probe")

		// Determine probe image
		probeImage := proberConfig.ProbeImage
		if probeImage == "" {
			probeImage = "registry.jakob-eberhardt.de/rdt4nn/stress-ng" // Default probe image
		}

		// Create probe kernel
		var probeKernel kernels.ProbeKernel
		switch proberConfig.Implementation {
		case "default", "":
			probeKernel = kernels.NewDefaultProbeKernel()
		default:
			logger.WithField("implementation", proberConfig.Implementation).Warn("Unknown probe kernel, using default")
			probeKernel = kernels.NewDefaultProbeKernel()
		}

		// Create Probe singleton
		bench.prober = probe.NewProbe(
			bench.dockerClient,
			probeKernel,
			bench.hostConfig,
			bench.benchmarkID,
			bench.config.Benchmark.Name,
			probeImage,
		)

		// Inject probe into scheduler
		bench.scheduler.SetProbe(bench.prober)

		logger.WithFields(logrus.Fields{
			"kernel":      probeKernel.GetName(),
			"probe_image": probeImage,
		}).Info("Probe initialized and injected into scheduler")
	}

	// Scheduler Initialize() will be called after containers start to provide PIDs

	logger.WithFields(logrus.Fields{
		"benchmark_id": bench.benchmarkID,
		"name":         bench.config.Benchmark.Name,
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

// orchestrates the benchmark execution in proper order
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

	// Create Docker network for inter-container communication
	logger.Info("Creating Docker network for benchmark")
	if err := cb.createNetwork(ctx); err != nil {
		return fmt.Errorf("failed to create network: %w", err)
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

	// Initialize scheduler now that we have container PIDs
	logger.Info("Initializing scheduler with container PIDs")
	if err := cb.initializeSchedulerWithPIDs(); err != nil {
		return fmt.Errorf("failed to initialize scheduler: %w", err)
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

	// Execute commands in containers (after collectors/scheduler are ready)
	logger.Info("Executing container commands")
	if err := cb.executeContainerCommands(ctx); err != nil {
		return fmt.Errorf("failed to execute container commands: %w", err)
	}

	// Sync RDT PIDs after command execution (docker exec creates new PIDs that need monitoring)
	logger.Info("Syncing RDT monitoring PIDs after command execution")
	if err := cb.syncRDTPIDs(); err != nil {
		logger.WithError(err).Warn("Failed to sync RDT PIDs, RDT monitoring may be incomplete")
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

	// Cleanup Docker network
	cb.cleanupNetwork(ctx)

	// Write data to database (last step)
	return cb.writeDatabaseData()
}

// Pull all container images
func (cb *ContainerBench) pullImages(ctx context.Context) error {
	logger := logging.GetLogger()
	containers := cb.config.GetContainersSorted()
	registryConfig := cb.config.GetRegistryConfig()

	// Deduplicate images
	type imageInfo struct {
		image          string
		needsAuth      bool
		containerCount int
	}
	uniqueImages := make(map[string]*imageInfo)

	for _, containerConfig := range containers {
		if _, exists := uniqueImages[containerConfig.Image]; !exists {
			needsAuth := registryConfig != nil && isPrivateRegistryImage(containerConfig.Image, registryConfig.Host)
			uniqueImages[containerConfig.Image] = &imageInfo{
				image:          containerConfig.Image,
				needsAuth:      needsAuth,
				containerCount: 0,
			}
		}
		uniqueImages[containerConfig.Image].containerCount++
	}

	// Add probe image if prober is configured
	if cb.prober != nil {
		probeImage := cb.prober.GetProbeImage()
		if _, exists := uniqueImages[probeImage]; !exists {
			needsAuth := registryConfig != nil && isPrivateRegistryImage(probeImage, registryConfig.Host)
			uniqueImages[probeImage] = &imageInfo{
				image:          probeImage,
				needsAuth:      needsAuth,
				containerCount: 1,
			}
			logger.WithField("probe_image", probeImage).Debug("Added probe image to pull list")
		}
	}

	logger.WithFields(logrus.Fields{
		"total_containers": len(containers),
		"unique_images":    len(uniqueImages),
	}).Info("Preparing to pull images")

	var authString string
	var err error
	if registryConfig != nil {
		authString, err = createRegistryAuth(registryConfig)
		if err != nil {
			logger.WithError(err).Error("Failed to create registry auth")
			return fmt.Errorf("failed to create registry auth: %w", err)
		}
	}

	type pullResult struct {
		image string
		err   error
	}

	resultChan := make(chan pullResult, len(uniqueImages))
	var wg sync.WaitGroup

	for _, imgInfo := range uniqueImages {
		wg.Add(1)
		go func(info *imageInfo) {
			defer wg.Done()

			pullOptions := types.ImagePullOptions{}
			if info.needsAuth {
				pullOptions.RegistryAuth = authString
				logger.WithFields(logrus.Fields{
					"image":    info.image,
					"registry": registryConfig.Host,
				}).Debug("Using private registry authentication")
			}

			logger.WithFields(logrus.Fields{
				"image":      info.image,
				"containers": info.containerCount,
			}).Info("Pulling image")

			pullResp, err := cb.dockerClient.ImagePull(ctx, info.image, pullOptions)
			if err != nil {
				logger.WithField("image", info.image).WithError(err).Error("Failed to pull image")
				resultChan <- pullResult{image: info.image, err: fmt.Errorf("failed to pull image %s: %w", info.image, err)}
				return
			}
			defer pullResp.Close()

			// Read the pull response to completion
			_, err = io.Copy(io.Discard, pullResp)
			if err != nil {
				logger.WithField("image", info.image).WithError(err).Error("Failed to complete image pull")
				resultChan <- pullResult{image: info.image, err: fmt.Errorf("failed to complete image pull for %s: %w", info.image, err)}
				return
			}

			logger.WithFields(logrus.Fields{
				"image":      info.image,
				"containers": info.containerCount,
			}).Info("Image pulled successfully")
			resultChan <- pullResult{image: info.image, err: nil}
		}(imgInfo)
	}

	// Wait for all pulls to complete
	wg.Wait()
	close(resultChan)

	// Check for errors
	var pullErrors []error
	for result := range resultChan {
		if result.err != nil {
			pullErrors = append(pullErrors, result.err)
		}
	}

	if len(pullErrors) > 0 {
		logger.WithField("error_count", len(pullErrors)).Error("Failed to pull some images")
		return fmt.Errorf("failed to pull %d images: %v", len(pullErrors), pullErrors[0])
	}

	logger.Info("All images pulled successfully")
	return nil
}

// Create Docker network for inter-container communication
func (cb *ContainerBench) createNetwork(ctx context.Context) error {
	logger := logging.GetLogger()

	networkName := fmt.Sprintf("container-bench-%d", cb.benchmarkID)

	// Create a bridge network for the benchmark
	resp, err := cb.dockerClient.NetworkCreate(ctx, networkName, types.NetworkCreate{
		Driver:         "bridge",
		CheckDuplicate: true,
	})
	if err != nil {
		logger.WithField("network_name", networkName).WithError(err).Error("Failed to create Docker network")
		return fmt.Errorf("failed to create network %s: %w", networkName, err)
	}

	cb.networkID = resp.ID

	logger.WithFields(logrus.Fields{
		"network_name": networkName,
		"network_id":   resp.ID[:12],
	}).Info("Docker network created for benchmark")

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

		// Add environment variables if specified
		if len(containerConfig.Environment) > 0 {
			config.Env = make([]string, 0, len(containerConfig.Environment))
			for key, value := range containerConfig.Environment {
				config.Env = append(config.Env, fmt.Sprintf("%s=%s", key, value))
			}
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
		hostConfig.CpusetCpus = containerConfig.Core

		// Set privileged mode if specified
		if containerConfig.Privileged {
			hostConfig.Privileged = true
		}

		// Add volume mounts if specified
		if len(containerConfig.Volumes) > 0 {
			hostConfig.Binds = containerConfig.Volumes
		}

		// Attach container to the benchmark network
		networkName := fmt.Sprintf("container-bench-%d", cb.benchmarkID)
		hostConfig.NetworkMode = container.NetworkMode(networkName)

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

// Cleanup Docker network
func (cb *ContainerBench) cleanupNetwork(ctx context.Context) {
	logger := logging.GetLogger()

	if cb.networkID == "" {
		return
	}

	logger.WithField("network_id", cb.networkID[:12]).Info("Removing Docker network")

	if err := cb.dockerClient.NetworkRemove(ctx, cb.networkID); err != nil {
		logger.WithField("network_id", cb.networkID[:12]).WithError(err).Warn("Failed to remove Docker network")
	} else {
		logger.WithField("network_id", cb.networkID[:12]).Info("Docker network removed")
	}
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

// Execute commands in running containers (after collectors/scheduler are started)
func (cb *ContainerBench) executeContainerCommands(ctx context.Context) error {
	logger := logging.GetLogger()
	containers := cb.config.GetContainersSorted()

	for _, containerConfig := range containers {
		// Skip containers without a command
		if containerConfig.Command == "" {
			continue
		}

		containerID := cb.containerIDs[containerConfig.Index]

		logger.WithFields(logrus.Fields{
			"index":        containerConfig.Index,
			"container_id": containerID[:12],
			"command":      containerConfig.Command,
		}).Info("Executing command in container")

		// Create exec configuration
		execConfig := types.ExecConfig{
			Cmd:          []string{"sh", "-c", containerConfig.Command},
			AttachStdout: false,
			AttachStderr: false,
			Detach:       true, // Run in background
		}

		// Create exec instance
		execResp, err := cb.dockerClient.ContainerExecCreate(ctx, containerID, execConfig)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"index":        containerConfig.Index,
				"container_id": containerID[:12],
			}).WithError(err).Error("Failed to create exec instance")
			return fmt.Errorf("failed to create exec for container %d: %w", containerConfig.Index, err)
		}

		// Start exec
		if err := cb.dockerClient.ContainerExecStart(ctx, execResp.ID, types.ExecStartCheck{Detach: true}); err != nil {
			logger.WithFields(logrus.Fields{
				"index":        containerConfig.Index,
				"container_id": containerID[:12],
			}).WithError(err).Error("Failed to start exec")
			return fmt.Errorf("failed to start exec for container %d: %w", containerConfig.Index, err)
		}

		logger.WithFields(logrus.Fields{
			"index": containerConfig.Index,
			"pid":   cb.containerPIDs[containerConfig.Index],
		}).Info("Command executed in container")
	}

	return nil
}

// initializes the scheduler with container information including PIDs
func (cb *ContainerBench) initializeSchedulerWithPIDs() error {
	logger := logging.GetLogger()

	// Create container info list with PIDs
	containers := cb.config.GetContainersSorted()
	containerInfos := make([]scheduler.ContainerInfo, 0, len(containers))

	for _, containerConfig := range containers {
		pid, exists := cb.containerPIDs[containerConfig.Index]
		if !exists {
			return fmt.Errorf("PID not found for container %d", containerConfig.Index)
		}

		containerID, exists := cb.containerIDs[containerConfig.Index]
		if !exists {
			return fmt.Errorf("Container ID not found for container %d", containerConfig.Index)
		}

		containerInfos = append(containerInfos, scheduler.ContainerInfo{
			Index:       containerConfig.Index,
			Config:      &containerConfig,
			PID:         pid,
			ContainerID: containerID,
		})
	}

	// Create RDT allocator if supported
	var rdtAllocator scheduler.RDTAllocator
	if cb.hostConfig.RDT.Supported && cb.config.Benchmark.Scheduler.RDT {
		rdtAllocator = scheduler.NewDefaultRDTAllocator()
		logger.Debug("RDT allocator created for scheduler")
	} else {
		logger.Debug("RDT allocator not created (RDT not supported or not enabled)")
	}

	// Initialize scheduler with allocator and container information
	if err := cb.scheduler.Initialize(rdtAllocator, containerInfos, &cb.config.Benchmark.Scheduler); err != nil {
		return fmt.Errorf("failed to initialize scheduler: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"containers":  len(containerInfos),
		"rdt_enabled": rdtAllocator != nil,
	}).Info("Scheduler initialized with container PIDs")

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

		// Parse metric configs from YAML
		perfConfig := containerConfig.Data.GetPerfConfig()
		dockerConfig := containerConfig.Data.GetDockerConfig()
		rdtConfig := containerConfig.Data.GetRDTConfig()

		// Create collector
		collectorConfig := collectors.CollectorConfig{
			Frequency:    time.Duration(containerConfig.Data.Frequency) * time.Millisecond,
			PerfConfig:   perfConfig,
			DockerConfig: dockerConfig,
			RDTConfig:    rdtConfig,
		}

		collector := collectors.NewContainerCollector(containerConfig.Index, containerID, collectorConfig, containerDF)

		// Fix cgroup path and use the correct systemd cgroup structure
		cgroupPath := fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope", containerID)
		collector.SetContainerInfo(pid, cgroupPath, containerConfig.CPUCores)

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
	logger.Info("Scheduler started and configured")
	return nil
}

// Sync RDT PIDs after docker exec to ensure new processes are monitored
func (cb *ContainerBench) syncRDTPIDs() error {
	logger := logging.GetLogger()

	for _, collector := range cb.collectors {
		if collector != nil {
			if err := collector.SyncRDTPIDs(); err != nil {
				logger.WithError(err).Warn("Failed to sync RDT PIDs for collector")
			}
		}
	}

	logger.Debug("RDT PIDs synced for all collectors")
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

	// Export probe results if prober was used
	if cb.prober != nil {
		probeResults := cb.prober.GetResults()
		if len(probeResults) > 0 {
			logger.WithField("probe_count", len(probeResults)).Info("Exporting probe results")
			if err := cb.dbClient.WriteProbeResults(probeResults); err != nil {
				logger.WithError(err).Error("Failed to export probe results")
				return fmt.Errorf("failed to export probe results: %w", err)
			}
		}
	}

	duration := cb.endTime.Sub(cb.startTime)
	logger.WithFields(logrus.Fields{
		"benchmark_id": cb.benchmarkID,
		"duration":     duration,
	}).Info("Benchmark completed")

	return nil
}

func generateTimeseriesPlot(benchmarkID int, xField, yField string, interval float64, minPtr, maxPtr *float64, onlyPlot, onlyWrapper bool) error {
	logger := logging.GetLogger()
	logger.WithFields(logrus.Fields{
		"benchmark_id": benchmarkID,
		"x_field":      xField,
		"y_field":      yField,
		"interval":     interval,
	}).Debug("Generating timeseries plot")

	plotMgr, err := plot.NewPlotManager()
	if err != nil {
		logger.WithError(err).Error("Failed to create plot manager")
		return fmt.Errorf("failed to create plot manager: %w", err)
	}
	defer plotMgr.Close()

	plotTikz, wrapperTex, err := plotMgr.GenerateTimeseriesPlot(benchmarkID, xField, yField, interval, minPtr, maxPtr)
	if err != nil {
		logger.WithError(err).Error("Failed to generate plot")
		return fmt.Errorf("failed to generate plot: %w", err)
	}

	// Determine what to print
	showPlot := !onlyWrapper
	showWrapper := !onlyPlot

	if showPlot {
		fmt.Println(plotTikz)
		if showWrapper {
			fmt.Println()
		}
	}

	if showWrapper {
		fmt.Println(wrapperTex)
	}

	logger.Debug("Timeseries plot generated successfully")
	return nil
}

func generatePolarPlot(probeIndices []int, onlyPlot, onlyWrapper bool) error {
	logger := logging.GetLogger()
	logger.WithField("probe_indices", probeIndices).Debug("Generating polar plot")

	if len(probeIndices) == 0 {
		return fmt.Errorf("no probe indices specified")
	}

	plotMgr, err := plot.NewPlotManager()
	if err != nil {
		logger.WithError(err).Error("Failed to create plot manager")
		return fmt.Errorf("failed to create plot manager: %w", err)
	}
	defer plotMgr.Close()

	plotTikz, wrapperTex, err := plotMgr.GeneratePolarPlot(probeIndices)
	if err != nil {
		logger.WithError(err).Error("Failed to generate plot")
		return fmt.Errorf("failed to generate plot: %w", err)
	}

	// Determine what to print
	showPlot := !onlyWrapper
	showWrapper := !onlyPlot

	if showPlot {
		fmt.Println("=" + strings.Repeat("=", 78) + "=")
		fmt.Println("PLOT FILE: probe-sensitivity.tikz")
		fmt.Println("=" + strings.Repeat("=", 78) + "=")
		fmt.Println(plotTikz)
		if showWrapper {
			fmt.Println()
		}
	}

	if showWrapper {
		fmt.Println("=" + strings.Repeat("=", 78) + "=")
		fmt.Println("WRAPPER FILE: probe-sensitivity-wrapper.tex")
		fmt.Println("=" + strings.Repeat("=", 78) + "=")
		fmt.Println(wrapperTex)
	}

	logger.Debug("Polar plot generated successfully")
	return nil
}

