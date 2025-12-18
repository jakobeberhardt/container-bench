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

	"container-bench/internal/accounting"
	"container-bench/internal/allocation"
	"container-bench/internal/collectors"
	"container-bench/internal/config"
	"container-bench/internal/cpuallocator"
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
	collectors    map[int]*collectors.ContainerCollector
	prober        *probe.Probe
	benchmarkID   int
	startTime     time.Time
	endTime       time.Time

	// Runtime container configs (stable pointers, updated with scheduler assignments)
	containersSorted  []*config.ContainerConfig
	containersByIndex map[int]*config.ContainerConfig

	containerStartTimes        map[int]time.Time
	containerCommandStartTimes map[int]time.Time
	containerStopTimes         map[int]time.Time
	containerExitTimes         map[int]time.Time
	containerAborted           map[int]bool
	timingsMu                  sync.Mutex

	// Container tracking for clean orchestration
	containerIDs  map[int]string
	containerPIDs map[int]int
	networkID     string
}

type dockerCpusetApplier struct {
	client *client.Client
}

func (d dockerCpusetApplier) UpdateCpuset(ctx context.Context, containerID string, cpuset string) error {
	if d.client == nil {
		return fmt.Errorf("docker client is nil")
	}
	_, err := d.client.ContainerUpdate(ctx, containerID, container.UpdateConfig{
		Resources: container.Resources{CpusetCpus: cpuset},
	})
	return err
}

func (cb *ContainerBench) cleanup() {

	for _, collector := range cb.collectors {
		if collector != nil {
			collector.Stop()
		}
	}
}

func (cb *ContainerBench) syncRuntimeContainerConfig(containerConfig *config.ContainerConfig) {
	if cb == nil || cb.config == nil || containerConfig == nil {
		return
	}
	if containerConfig.KeyName == "" {
		return
	}
	cb.config.Containers[containerConfig.KeyName] = *containerConfig
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
				cb.stopAndRemoveContainer(ctx, idx, id)
			}(containerIndex, containerID)
		}
	}

	wg.Wait()
	logger.Info("All containers stopped and removed")
}

func (cb *ContainerBench) stopAndRemoveContainer(ctx context.Context, containerIndex int, containerID string) {
	logger := logging.GetLogger()
	logger.WithField("container_id", containerID[:12]).Debug("Force removing container")

	// Notify scheduler so any CPU reservations are released.
	if listener, ok := cb.scheduler.(scheduler.ContainerLifecycleListener); ok {
		_ = listener.OnContainerStop(containerIndex)
	}

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
	var probeMetric string
	var allocationProbeIndex int
	var allocationMetrics []string
	var onlyPlot, onlyWrapper bool
	var outputDir string
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
			return generatePolarPlot(probeIndices, probeMetric, onlyPlot, onlyWrapper)
		},
	}

	allocationCmd := &cobra.Command{
		Use:   "allocation",
		Short: "Generate an allocation probe plot",
		Long:  "Generate a plot showing performance metrics vs L3 cache ways for different memory bandwidth allocations",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateEnvironment(); err != nil {
				return err
			}
			return generateAllocationPlot(benchmarkID, allocationProbeIndex, allocationMetrics, onlyPlot, onlyWrapper, outputDir)
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
	polarCmd.Flags().StringVar(&probeMetric, "metric", "ipc", "Metric type to plot (ipc, scp)")
	polarCmd.Flags().BoolVar(&onlyPlot, "plot", false, "Print only the plot file (TikZ)")
	polarCmd.Flags().BoolVar(&onlyWrapper, "wrapper", false, "Print only the wrapper file (LaTeX)")
	polarCmd.MarkFlagRequired("probes")

	allocationCmd.Flags().IntVar(&benchmarkID, "benchmark", 0, "Benchmark ID")
	allocationCmd.Flags().IntVar(&allocationProbeIndex, "probe-id", 0, "Allocation probe index")
	allocationCmd.Flags().StringSliceVar(&allocationMetrics, "metrics", []string{"stalled_cycles"}, "Metrics to plot (comma-separated or 'all': ipc, ipc_efficiency, cache_miss_rate, stalled_cycles, stalls_l3_miss_percent, l3_occupancy, mem_bandwidth_used)")
	allocationCmd.Flags().BoolVar(&onlyPlot, "plot", false, "Print only the plot file (TikZ)")
	allocationCmd.Flags().BoolVar(&onlyWrapper, "wrapper", false, "Print only the wrapper file (LaTeX)")
	allocationCmd.Flags().StringVar(&outputDir, "files", "", "Output directory to save plot files (creates wrapper.tex and metric.tikz files)")
	allocationCmd.MarkFlagRequired("benchmark")
	allocationCmd.MarkFlagRequired("probe-id")

	plotCmd.AddCommand(timeseriesCmd)
	plotCmd.AddCommand(polarCmd)
	plotCmd.AddCommand(allocationCmd)

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
		dataframes:                 dataframe.NewDataFrames(),
		containerIDs:               make(map[int]string),
		containerPIDs:              make(map[int]int),
		collectors:                 make(map[int]*collectors.ContainerCollector),
		containerStartTimes:        make(map[int]time.Time),
		containerCommandStartTimes: make(map[int]time.Time),
		containerStopTimes:         make(map[int]time.Time),
		containerExitTimes:         make(map[int]time.Time),
		containerAborted:           make(map[int]bool),
	}

	var err error
	bench.config, bench.configContent, err = config.LoadConfigWithContent(configFile)
	if err != nil {
		logger.WithField("config_file", configFile).WithError(err).Error("Failed to load configuration")
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Build stable runtime container configs (so scheduler assignments are visible everywhere)
	bench.containersByIndex = make(map[int]*config.ContainerConfig)
	containersSorted := bench.config.GetContainersSorted()
	bench.containersSorted = make([]*config.ContainerConfig, 0, len(containersSorted))
	for _, c := range containersSorted {
		cfg := c
		bench.containersSorted = append(bench.containersSorted, &cfg)
		bench.containersByIndex[cfg.Index] = &cfg
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

	// Initialize data handler (needs host topology for derived metrics)
	bench.dataHandler = datahandeling.NewDefaultDataHandler(bench.hostConfig)

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
	case "least_loaded":
		logger.Info("Using least-loaded scheduler")
		bench.scheduler = scheduler.NewLeastLoadedScheduler()
	case "probe":
		logger.Info("Using probe scheduler")
		bench.scheduler = scheduler.NewProbeScheduler()
	case "allocation":
		logger.Info("Using allocation scheduler")
		bench.scheduler = scheduler.NewAllocationScheduler()
	case "probe_allocation":
		logger.Info("Using probe allocation scheduler")
		bench.scheduler = scheduler.NewProbeAllocationScheduler()
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

	// Inject CPU allocator (physical cores only) with live cpuset updates via Docker
	cpusetApplier := dockerCpusetApplier{client: bench.dockerClient}
	cpuAlloc, err := cpuallocator.NewPhysicalCoreAllocator(bench.hostConfig, cpusetApplier, logger)
	if err != nil {
		return fmt.Errorf("failed to initialize CPU allocator: %w", err)
	}
	bench.scheduler.SetCPUAllocator(cpuAlloc)

	// Set benchmark ID for scheduler
	bench.scheduler.SetBenchmarkID(bench.benchmarkID)

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

	// Pre-populate container PID map with 0 (not running yet)
	for _, containerConfig := range cb.containersSorted {
		if _, ok := cb.containerPIDs[containerConfig.Index]; !ok {
			cb.containerPIDs[containerConfig.Index] = 0
		}
	}

	// Initialize scheduler (containers may start later; PID==0 means not running)
	logger.Info("Initializing scheduler")
	if err := cb.initializeSchedulerWithPIDs(); err != nil {
		return fmt.Errorf("failed to initialize scheduler: %w", err)
	}

	// Start scheduler
	logger.Info("Starting scheduler")
	if err := cb.startScheduler(); err != nil {
		return fmt.Errorf("failed to start scheduler: %w", err)
	}

	// Run benchmark (main execution loop)
	logger.Info("Running benchmark")
	cb.startTime = time.Now()
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
	containers := cb.containersSorted
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
	containers := cb.containersSorted

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

		// CPU affinity is assigned by the scheduler at start time.
		hostConfig.CpusetCpus = ""

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
	containers := cb.containersSorted

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
	containers := cb.containersSorted

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
	containerInfos := make([]scheduler.ContainerInfo, 0, len(cb.containersSorted))

	for _, containerConfig := range cb.containersSorted {
		pid := cb.containerPIDs[containerConfig.Index] // defaults to 0 (not running)

		containerID, exists := cb.containerIDs[containerConfig.Index]
		if !exists {
			return fmt.Errorf("Container ID not found for container %d", containerConfig.Index)
		}

		containerInfos = append(containerInfos, scheduler.ContainerInfo{
			Index:       containerConfig.Index,
			Config:      containerConfig,
			PID:         pid,
			ContainerID: containerID,
		})
	}

	// Create RDT accountant if supported
	var rdtAccountant *accounting.RDTAccountant
	if cb.hostConfig.RDT.Supported && cb.config.Benchmark.Scheduler.RDT {
		rdtAllocator := allocation.NewDefaultRDTAllocator()
		logger.Info("RDT allocator created")

		// Initialize the RDT allocator
		if err := rdtAllocator.Initialize(); err != nil {
			logger.WithError(err).Warn("Failed to initialize RDT allocator")
		} else {
			// Create accountant wrapper
			var err error
			rdtAccountant, err = accounting.NewRDTAccountant(rdtAllocator, cb.hostConfig)
			if err != nil {
				logger.WithError(err).Warn("Failed to create RDT accountant, scheduler will have limited functionality")
				rdtAccountant = nil
			} else {
				logger.Info("RDT accountant created successfully")
			}
		}
	} else {
		logger.Debug("RDT accountant not created (RDT not supported or not enabled)")
	}

	// Initialize scheduler with accountant and container information
	if err := cb.scheduler.Initialize(rdtAccountant, containerInfos, &cb.config.Benchmark.Scheduler); err != nil {
		return fmt.Errorf("failed to initialize scheduler: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"containers":  len(containerInfos),
		"rdt_enabled": rdtAccountant != nil,
	}).Info("Scheduler initialized with container PIDs")

	return nil
}

func (cb *ContainerBench) startContainer(ctx context.Context, containerConfig *config.ContainerConfig) (int, error) {
	logger := logging.GetLogger()
	if containerConfig == nil {
		return 0, fmt.Errorf("containerConfig is nil")
	}
	containerID := cb.containerIDs[containerConfig.Index]

	// Ensure CPU affinity is assigned by the scheduler before starting the container.
	if _, err := cb.scheduler.AssignCPUCores(containerConfig.Index); err != nil {
		return 0, fmt.Errorf("failed to assign CPU cores for container %d: %w", containerConfig.Index, err)
	}
	cb.syncRuntimeContainerConfig(containerConfig)
	if containerConfig.Core != "" {
		_, err := cb.dockerClient.ContainerUpdate(ctx, containerID, container.UpdateConfig{
			Resources: container.Resources{CpusetCpus: containerConfig.Core},
		})
		if err != nil {
			return 0, fmt.Errorf("failed to apply cpuset for container %d: %w", containerConfig.Index, err)
		}
	}

	if err := cb.dockerClient.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		logger.WithFields(logrus.Fields{
			"index":        containerConfig.Index,
			"container_id": containerID[:12],
		}).WithError(err).Error("Failed to start container")
		return 0, fmt.Errorf("failed to start container %d: %w", containerConfig.Index, err)
	}

	info, err := cb.dockerClient.ContainerInspect(ctx, containerID)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"index":        containerConfig.Index,
			"container_id": containerID[:12],
		}).WithError(err).Error("Failed to inspect container")
		return 0, fmt.Errorf("failed to inspect container %d: %w", containerConfig.Index, err)
	}

	pid := info.State.Pid
	cb.containerPIDs[containerConfig.Index] = pid
	now := time.Now()
	cb.containerStartTimes[containerConfig.Index] = now
	cb.timingsMu.Lock()
	if _, exists := cb.containerCommandStartTimes[containerConfig.Index]; !exists {
		cb.containerCommandStartTimes[containerConfig.Index] = now
	}
	cb.timingsMu.Unlock()

	// Notify scheduler about the container start (optional)
	if listener, ok := cb.scheduler.(scheduler.ContainerLifecycleListener); ok {
		_ = listener.OnContainerStart(scheduler.ContainerInfo{
			Index:       containerConfig.Index,
			Config:      containerConfig,
			PID:         pid,
			ContainerID: containerID,
		})
	}

	logger.WithFields(logrus.Fields{
		"index": containerConfig.Index,
		"pid":   pid,
	}).Info("Container started")

	return pid, nil
}

type containerExitEvent struct {
	index     int
	exitedAt  time.Time
	status    int64
	waitError error
}

type containerExecEvent struct {
	index      int
	finishedAt time.Time
	exitCode   int
	err        error
}

func (cb *ContainerBench) startExitWatcher(ctx context.Context, containerConfig *config.ContainerConfig, exitEvents chan<- containerExitEvent) {
	if containerConfig == nil {
		return
	}
	containerID := cb.containerIDs[containerConfig.Index]
	if containerID == "" {
		return
	}

	go func(index int, id string) {
		waitC, errC := cb.dockerClient.ContainerWait(ctx, id, container.WaitConditionNotRunning)
		select {
		case <-ctx.Done():
			return
		case err := <-errC:
			exitEvents <- containerExitEvent{index: index, exitedAt: time.Now(), waitError: err}
			return
		case resp := <-waitC:
			var werr error
			if resp.Error != nil {
				werr = fmt.Errorf("container wait error: %s", resp.Error.Message)
			}
			exitEvents <- containerExitEvent{index: index, exitedAt: time.Now(), status: resp.StatusCode, waitError: werr}
			return
		}
	}(containerConfig.Index, containerID)
}

func (cb *ContainerBench) startExecWatcher(ctx context.Context, containerIndex int, containerExecID string, execEvents chan<- containerExecEvent) {
	if containerExecID == "" {
		return
	}

	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				inspect, err := cb.dockerClient.ContainerExecInspect(ctx, containerExecID)
				if err != nil {
					execEvents <- containerExecEvent{index: containerIndex, finishedAt: time.Now(), err: err}
					return
				}
				if !inspect.Running {
					execEvents <- containerExecEvent{index: containerIndex, finishedAt: time.Now(), exitCode: inspect.ExitCode, err: nil}
					return
				}
			}
		}
	}()
}

func (cb *ContainerBench) stopContainer(ctx context.Context, containerConfig *config.ContainerConfig, aborted bool) error {
	logger := logging.GetLogger()
	if containerConfig == nil {
		return fmt.Errorf("containerConfig is nil")
	}
	containerID := cb.containerIDs[containerConfig.Index]
	stopTimeout := 2
	stopRequestedAt := time.Now()

	cb.containerStopTimes[containerConfig.Index] = stopRequestedAt
	cb.timingsMu.Lock()
	if _, exists := cb.containerExitTimes[containerConfig.Index]; !exists {
		cb.containerExitTimes[containerConfig.Index] = stopRequestedAt
	}
	if aborted {
		cb.containerAborted[containerConfig.Index] = true
	} else {
		// Don't overwrite an existing aborted=true with false.
		if _, exists := cb.containerAborted[containerConfig.Index]; !exists {
			cb.containerAborted[containerConfig.Index] = false
		}
	}
	cb.timingsMu.Unlock()

	if err := cb.dockerClient.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &stopTimeout}); err != nil {
		logger.WithFields(logrus.Fields{
			"index":        containerConfig.Index,
			"container_id": containerID[:12],
		}).WithError(err).Warn("Failed to stop container")
		return fmt.Errorf("failed to stop container %d: %w", containerConfig.Index, err)
	}

	cb.containerPIDs[containerConfig.Index] = 0
	// Note: stop/exit timestamps were recorded at stop request time

	// Notify scheduler about the container stop (optional)
	if listener, ok := cb.scheduler.(scheduler.ContainerLifecycleListener); ok {
		_ = listener.OnContainerStop(containerConfig.Index)
	}

	logger.WithField("index", containerConfig.Index).Info("Container stopped")
	return nil
}

func (cb *ContainerBench) startCollectorForContainer(ctx context.Context, containerConfig *config.ContainerConfig, pid int) error {
	logger := logging.GetLogger()
	if containerConfig == nil {
		return fmt.Errorf("containerConfig is nil")
	}
	containerID := cb.containerIDs[containerConfig.Index]

	containerDF := cb.dataframes.AddContainer(containerConfig.Index)

	perfConfig := containerConfig.Data.GetPerfConfig()
	dockerConfig := containerConfig.Data.GetDockerConfig()
	rdtConfig := containerConfig.Data.GetRDTConfig()

	pidSyncInterval := 100 * time.Millisecond
	if cb.config.Benchmark.PIDSyncInterval > 0 {
		pidSyncInterval = time.Duration(cb.config.Benchmark.PIDSyncInterval) * time.Millisecond
	}

	collectorConfig := collectors.CollectorConfig{
		Frequency:       time.Duration(containerConfig.Data.Frequency) * time.Millisecond,
		PIDSyncInterval: pidSyncInterval,
		PerfConfig:      perfConfig,
		DockerConfig:    dockerConfig,
		RDTConfig:       rdtConfig,
	}

	collector := collectors.NewContainerCollector(containerConfig.Index, containerID, collectorConfig, containerDF)
	cgroupPath := fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope", containerID)
	collector.SetContainerInfo(pid, cgroupPath, containerConfig.CPUCores)

	if err := collector.Start(ctx); err != nil {
		logger.WithFields(logrus.Fields{
			"index":        containerConfig.Index,
			"container_id": containerID[:12],
		}).WithError(err).Error("Failed to start collector")
		return fmt.Errorf("failed to start collector for container %d: %w", containerConfig.Index, err)
	}

	cb.collectors[containerConfig.Index] = collector

	logger.WithFields(logrus.Fields{
		"index":        containerConfig.Index,
		"container_id": containerID[:12],
	}).Info("Collector started")
	return nil
}

func (cb *ContainerBench) stopCollectorForContainer(containerIndex int) {
	logger := logging.GetLogger()
	collector, ok := cb.collectors[containerIndex]
	if !ok || collector == nil {
		return
	}
	if err := collector.Stop(); err != nil {
		logger.WithError(err).WithField("index", containerIndex).Warn("Error stopping collector")
	}
	delete(cb.collectors, containerIndex)
}

func (cb *ContainerBench) executeContainerCommand(ctx context.Context, containerConfig *config.ContainerConfig) (string, error) {
	if containerConfig == nil {
		return "", fmt.Errorf("containerConfig is nil")
	}
	if containerConfig.Command == "" {
		return "", nil
	}

	logger := logging.GetLogger()
	containerID := cb.containerIDs[containerConfig.Index]

	logger.WithFields(logrus.Fields{
		"index":        containerConfig.Index,
		"container_id": containerID[:12],
		"command":      containerConfig.Command,
	}).Info("Executing command in container")

	execConfig := types.ExecConfig{
		Cmd:          []string{"sh", "-c", containerConfig.Command},
		AttachStdout: false,
		AttachStderr: false,
		Detach:       true,
	}

	execResp, err := cb.dockerClient.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return "", fmt.Errorf("failed to create exec for container %d: %w", containerConfig.Index, err)
	}

	if err := cb.dockerClient.ContainerExecStart(ctx, execResp.ID, types.ExecStartCheck{Detach: true}); err != nil {
		return "", fmt.Errorf("failed to start exec for container %d: %w", containerConfig.Index, err)
	}

	cb.timingsMu.Lock()
	cb.containerCommandStartTimes[containerConfig.Index] = time.Now()
	cb.timingsMu.Unlock()

	// Sync RDT PIDs for this container after docker exec
	if collector := cb.collectors[containerConfig.Index]; collector != nil {
		if err := collector.SyncRDTPIDs(); err != nil {
			logger.WithError(err).WithField("index", containerConfig.Index).Warn("Failed to sync RDT PIDs after command exec")
		}
	}

	return execResp.ID, nil
}

// Start all collectors
func (cb *ContainerBench) startCollectors(ctx context.Context) error {
	containers := cb.containersSorted
	for _, containerConfig := range containers {
		pid := cb.containerPIDs[containerConfig.Index]
		if pid == 0 {
			continue
		}
		if _, exists := cb.collectors[containerConfig.Index]; exists {
			continue
		}
		if err := cb.startCollectorForContainer(ctx, containerConfig, pid); err != nil {
			return err
		}
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
		if collector == nil {
			continue
		}
		if err := collector.SyncRDTPIDs(); err != nil {
			logger.WithError(err).Warn("Failed to sync RDT PIDs for collector")
		}
	}

	logger.Debug("RDT PIDs synced for all collectors")
	return nil
}

// Run benchmark main loop
func (cb *ContainerBench) runBenchmarkLoop(ctx context.Context) error {
	logger := logging.GetLogger()

	type runtimeState struct {
		cfg            *config.ContainerConfig
		startSec       int
		stopSec        int
		expectedSec    int
		hasExpected    bool
		started        bool
		stopped        bool
		commandStarted bool
		exitWatch      bool
	}

	containers := cb.containersSorted
	states := make(map[int]*runtimeState, len(containers))
	for _, c := range containers {
		startSec := c.GetStartSeconds()
		stopSec := c.GetStopSeconds(cb.config.Benchmark.MaxT)
		expectedSec, hasExpected := c.GetExpectedSeconds()
		states[c.Index] = &runtimeState{
			cfg:         c,
			startSec:    startSec,
			stopSec:     stopSec,
			expectedSec: expectedSec,
			hasExpected: hasExpected,
			started:     false,
			stopped:     false,
		}
	}

	// Start scheduler updates
	schedulerTicker := time.NewTicker(1 * time.Second)
	defer schedulerTicker.Stop()

	// Container schedule processing
	scheduleTicker := time.NewTicker(100 * time.Millisecond)
	defer scheduleTicker.Stop()

	// Set up timeout
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, cb.config.GetMaxDuration())
	defer timeoutCancel()

	logger.WithField("duration", cb.config.GetMaxDuration()).Info("Benchmark running")

	checkExpected := func(containerIndex int) error {
		st := states[containerIndex]
		if st == nil || !st.hasExpected {
			return nil
		}
		cb.timingsMu.Lock()
		wasAborted := cb.containerAborted[containerIndex]
		cb.timingsMu.Unlock()
		if !wasAborted {
			// expected_t is only enforced for aborted jobs (max_t)
			return nil
		}
		var startT time.Time
		var okStart bool
		if st.cfg.Command != "" {
			cb.timingsMu.Lock()
			startT, okStart = cb.containerCommandStartTimes[containerIndex]
			cb.timingsMu.Unlock()
		} else {
			startT = cb.startTime.Add(time.Duration(st.startSec) * time.Second)
			okStart = true
		}
		stopT, okStop := cb.containerStopTimes[containerIndex]
		cb.timingsMu.Lock()
		exitT, okExit := cb.containerExitTimes[containerIndex]
		cb.timingsMu.Unlock()
		if !okStart || (!okStop && !okExit) {
			return nil
		}
		endT := stopT
		if okExit {
			if !okStop || exitT.Before(stopT) {
				endT = exitT
			}
		}
		actual := int(endT.Sub(startT).Seconds())
		if actual < st.expectedSec {
			logger.WithFields(logrus.Fields{
				"index":        containerIndex,
				"actual_sec":   actual,
				"expected_sec": st.expectedSec,
			}).Warn("Container aborted before expected runtime")
		} else {
			logger.WithFields(logrus.Fields{
				"index":        containerIndex,
				"actual_sec":   actual,
				"expected_sec": st.expectedSec,
			}).Warn("Container aborted (max_t)")
		}
		return nil
	}

	exitEvents := make(chan containerExitEvent, len(containers)*2)
	execEvents := make(chan containerExecEvent, len(containers)*2)

	handleExit := func(ev containerExitEvent) error {
		if ev.waitError == nil {
			cb.timingsMu.Lock()
			if _, exists := cb.containerExitTimes[ev.index]; !exists {
				cb.containerExitTimes[ev.index] = ev.exitedAt
			}
			cb.timingsMu.Unlock()
		}

		st := states[ev.index]
		if st == nil || st.stopped {
			return nil
		}

		cb.stopCollectorForContainer(ev.index)
		cb.containerPIDs[ev.index] = 0

		if listener, ok := cb.scheduler.(scheduler.ContainerLifecycleListener); ok {
			_ = listener.OnContainerStop(ev.index)
		}

		st.stopped = true
		return checkExpected(ev.index)
	}

	handleExecFinished := func(ev containerExecEvent) error {
		st := states[ev.index]
		if st == nil || st.stopped || !st.started {
			return nil
		}

		cb.timingsMu.Lock()
		if _, exists := cb.containerExitTimes[ev.index]; !exists {
			cb.containerExitTimes[ev.index] = ev.finishedAt
		}
		cb.timingsMu.Unlock()

		// Exec command finished; stop the container (images typically keep running)
		cb.stopCollectorForContainer(ev.index)
		if err := cb.stopContainer(ctx, st.cfg, false); err != nil {
			return err
		}
		st.stopped = true
		return checkExpected(ev.index)
	}

	processSchedule := func(now time.Time) error {
		elapsedSec := int(now.Sub(cb.startTime).Seconds())
		for idx, st := range states {
			if st.stopped {
				continue
			}
			if !st.started && elapsedSec >= st.startSec {
				pid, err := cb.startContainer(ctx, st.cfg)
				if err != nil {
					return err
				}
				st.started = true
				if err := cb.startCollectorForContainer(ctx, st.cfg, pid); err != nil {
					return err
				}
				execID, err := cb.executeContainerCommand(ctx, st.cfg)
				if err != nil {
					return err
				}
				if execID != "" {
					cb.startExecWatcher(timeoutCtx, st.cfg.Index, execID, execEvents)
				}
				st.commandStarted = true
				if !st.exitWatch {
					cb.startExitWatcher(timeoutCtx, st.cfg, exitEvents)
					st.exitWatch = true
				}
			}
			if st.started && !st.stopped && elapsedSec >= st.stopSec {
				cb.stopCollectorForContainer(idx)
				aborted := cb.config.Benchmark.MaxT > 0 && st.stopSec == cb.config.Benchmark.MaxT && elapsedSec >= cb.config.Benchmark.MaxT
				if err := cb.stopContainer(ctx, st.cfg, aborted); err != nil {
					return err
				}
				st.stopped = true
				if err := checkExpected(idx); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// Kick off any containers scheduled for t=0
	if err := processSchedule(time.Now()); err != nil {
		cb.endTime = time.Now()
		return err
	}

	// Main benchmark loop
	for {
		select {
		case ev := <-exitEvents:
			if err := handleExit(ev); err != nil {
				cb.endTime = time.Now()
				return err
			}
		case ev := <-execEvents:
			if err := handleExecFinished(ev); err != nil {
				cb.endTime = time.Now()
				return err
			}
		case <-timeoutCtx.Done():
			if timeoutCtx.Err() == context.DeadlineExceeded {
				logger.Info("Benchmark timeout reached")
			} else {
				logger.Info("Benchmark interrupted")
			}
			cb.endTime = time.Now()

			// Stop any still-running containers and collectors
			for idx, st := range states {
				if st.started && !st.stopped {
					cb.stopCollectorForContainer(idx)
					_ = cb.stopContainer(ctx, st.cfg, timeoutCtx.Err() == context.DeadlineExceeded)
					st.stopped = true
					if timeoutCtx.Err() == context.DeadlineExceeded {
						if err := checkExpected(idx); err != nil {
							return err
						}
					}
				}
			}

			return nil
		case <-scheduleTicker.C:
			if err := processSchedule(time.Now()); err != nil {
				cb.endTime = time.Now()
				return err
			}
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
	for idx := range cb.collectors {
		cb.stopCollectorForContainer(idx)
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

	cb.timingsMu.Lock()
	exitTimes := make(map[int]time.Time, len(cb.containerExitTimes))
	for k, v := range cb.containerExitTimes {
		exitTimes[k] = v
	}
	startTimes := make(map[int]time.Time, len(cb.containerCommandStartTimes))
	for k, v := range cb.containerCommandStartTimes {
		startTimes[k] = v
	}
	aborted := make(map[int]bool, len(cb.containerAborted))
	for k, v := range cb.containerAborted {
		aborted[k] = v
	}
	cb.timingsMu.Unlock()

	containerMeta := database.CollectContainerTimingMetadata(cb.benchmarkID, cb.config, cb.startTime, startTimes, exitTimes, aborted)
	if err := cb.dbClient.WriteContainerTimingMetadata(containerMeta); err != nil {
		logger.WithError(err).Error("Failed to export container timing metadata")
		return fmt.Errorf("failed to export container timing metadata: %w", err)
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

	// Export allocation probe results if probe_allocation scheduler was used
	if probeAllocScheduler, ok := cb.scheduler.(*scheduler.ProbeAllocationScheduler); ok {
		if probeAllocScheduler.HasAllocationProbeResults() {
			allocProbeResults := probeAllocScheduler.GetAllocationProbeResults()
			logger.WithField("allocation_probe_count", len(allocProbeResults)).Info("Exporting allocation probe results")
			if err := cb.dbClient.WriteAllocationProbeResults(allocProbeResults); err != nil {
				logger.WithError(err).Error("Failed to export allocation probe results")
				return fmt.Errorf("failed to export allocation probe results: %w", err)
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

func generatePolarPlot(probeIndices []int, metricType string, onlyPlot, onlyWrapper bool) error {
	logger := logging.GetLogger()
	logger.WithFields(logrus.Fields{
		"probe_indices": probeIndices,
		"metric_type":   metricType,
	}).Debug("Generating polar plot")

	if len(probeIndices) == 0 {
		return fmt.Errorf("no probe indices specified")
	}

	plotMgr, err := plot.NewPlotManager()
	if err != nil {
		logger.WithError(err).Error("Failed to create plot manager")
		return fmt.Errorf("failed to create plot manager: %w", err)
	}
	defer plotMgr.Close()

	plotTikz, wrapperTex, err := plotMgr.GeneratePolarPlot(probeIndices, metricType)
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

	logger.Debug("Polar plot generated successfully")
	return nil
}

func generateAllocationPlot(benchmarkID int, allocationProbeIndex int, metrics []string, onlyPlot, onlyWrapper bool, outputDir string) error {
	logger := logging.GetLogger()
	logger.WithFields(logrus.Fields{
		"benchmark_id":           benchmarkID,
		"allocation_probe_index": allocationProbeIndex,
		"metrics":                metrics,
		"output_dir":             outputDir,
	}).Debug("Generating allocation plot")

	plotMgr, err := plot.NewPlotManager()
	if err != nil {
		logger.WithError(err).Error("Failed to create plot manager")
		return fmt.Errorf("failed to create plot manager: %w", err)
	}
	defer plotMgr.Close()

	if len(metrics) == 0 {
		return fmt.Errorf("at least one metric must be specified")
	}

	// Trim whitespace from metric names and filter out empty ones
	var cleanMetrics []string
	for _, m := range metrics {
		trimmed := strings.TrimSpace(m)
		if trimmed != "" {
			cleanMetrics = append(cleanMetrics, trimmed)
		}
	}
	metrics = cleanMetrics

	if len(metrics) == 0 {
		return fmt.Errorf("at least one valid metric must be specified")
	}

	// Expand 'all' to all available metrics
	if len(metrics) == 1 && metrics[0] == "all" {
		metrics = []string{"ipc", "ipc_efficiency", "cache_miss_rate", "stalled_cycles", "stalls_l3_miss_percent", "l3_occupancy", "mem_bandwidth_used"}
		logger.WithField("expanded_metrics", metrics).Debug("Expanded 'all' to all available metrics")
	}

	// Generate plots for multiple metrics
	if len(metrics) > 1 {
		return generateAllocationPlotGroup(plotMgr, benchmarkID, allocationProbeIndex, metrics, onlyPlot, onlyWrapper, outputDir)
	}

	// Single metric - use simple wrapper
	plotTikz, wrapperTex, err := plotMgr.GenerateAllocationPlot(benchmarkID, allocationProbeIndex, metrics[0])
	if err != nil {
		logger.WithError(err).Error("Failed to generate allocation plot")
		return fmt.Errorf("failed to generate allocation plot: %w", err)
	}

	// Save to files if output directory specified
	if outputDir != "" {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return fmt.Errorf("failed to create output directory: %w", err)
		}

		plotFile := filepath.Join(outputDir, fmt.Sprintf("%s.tikz.tex", metrics[0]))
		if err := os.WriteFile(plotFile, []byte(plotTikz), 0644); err != nil {
			return fmt.Errorf("failed to write plot file: %w", err)
		}
		logger.WithField("file", plotFile).Info("Saved plot file")

		wrapperFile := filepath.Join(outputDir, "wrapper.tex")
		if err := os.WriteFile(wrapperFile, []byte(wrapperTex), 0644); err != nil {
			return fmt.Errorf("failed to write wrapper file: %w", err)
		}
		logger.WithField("file", wrapperFile).Info("Saved wrapper file")

		return nil
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

	logger.Debug("Allocation plot generated successfully")
	return nil
}

func generateAllocationPlotGroup(plotMgr *plot.PlotManager, benchmarkID int, allocationProbeIndex int, metrics []string, onlyPlot, onlyWrapper bool, outputDir string) error {
	logger := logging.GetLogger()

	// Generate individual plots - first one with named legend, rest without
	var plots []string
	var labelID string

	for i, metric := range metrics {
		includeLegend := (i == 0)
		plotTikz, _, err := plotMgr.GenerateAllocationPlotWithLegend(benchmarkID, allocationProbeIndex, metric, includeLegend)
		if err != nil {
			logger.WithError(err).WithField("metric", metric).Error("Failed to generate allocation plot")
			return fmt.Errorf("failed to generate plot for metric %s: %w", metric, err)
		}
		plots = append(plots, plotTikz)

		if i == 0 {
			labelID = fmt.Sprintf("%d-%d", benchmarkID, allocationProbeIndex)
		}
	}

	// Generate group wrapper
	groupWrapper, err := plotMgr.GenerateAllocationPlotGroupWrapper(benchmarkID, allocationProbeIndex, metrics, labelID)
	if err != nil {
		logger.WithError(err).Error("Failed to generate group wrapper")
		return fmt.Errorf("failed to generate group wrapper: %w", err)
	}

	// Save to files if output directory specified
	if outputDir != "" {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return fmt.Errorf("failed to create output directory: %w", err)
		}

		// Save individual plot files
		for i, plotTikz := range plots {
			plotFile := filepath.Join(outputDir, fmt.Sprintf("%s.tikz.tex", metrics[i]))
			if err := os.WriteFile(plotFile, []byte(plotTikz), 0644); err != nil {
				return fmt.Errorf("failed to write plot file %s: %w", plotFile, err)
			}
			logger.WithField("file", plotFile).Info("Saved plot file")
		}

		// Save group wrapper
		wrapperFile := filepath.Join(outputDir, "wrapper.tex")
		if err := os.WriteFile(wrapperFile, []byte(groupWrapper), 0644); err != nil {
			return fmt.Errorf("failed to write wrapper file: %w", err)
		}
		logger.WithField("file", wrapperFile).Info("Saved wrapper file")

		return nil
	}

	// Print output
	showPlot := !onlyWrapper
	showWrapper := !onlyPlot

	if showPlot {
		for i, plotTikz := range plots {
			if i > 0 {
				fmt.Println()
			}
			fmt.Println(plotTikz)
		}
		if showWrapper {
			fmt.Println()
		}
	}

	if showWrapper {
		fmt.Println(groupWrapper)
	}

	logger.Debug("Allocation plot group generated successfully")
	return nil
}
