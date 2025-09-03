package benchmark

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jakobeberhardt/container-bench/internal/config"
	"github.com/jakobeberhardt/container-bench/internal/container"
	"github.com/jakobeberhardt/container-bench/internal/profiler"
	"github.com/jakobeberhardt/container-bench/internal/scheduler"
	"github.com/jakobeberhardt/container-bench/internal/storage"
	"github.com/jakobeberhardt/container-bench/internal/system"
	"github.com/jakobeberhardt/container-bench/internal/version"
	log "github.com/sirupsen/logrus"
)

// Runner orchestrates the entire benchmark execution
type Runner struct {
	config         *config.Config
	containerMgr   *container.Manager
	profilerMgr    profiler.ProfilerManager
	dataFrameMgr   *profiler.DataFrameProfilerManager // New DataFrame-based profiler
	schedulerMgr   scheduler.Scheduler
	storageMgr     *storage.Manager
	benchmarkID    string
	benchmarkIDNum int64    
	startTime      time.Time
	endTime        time.Time
	samplingStep   int64    
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	
	// Metadata collection
	configFilePath string
	printMetaData  bool
	metadata       *storage.BenchmarkMetadata
}

func NewRunner(cfg *config.Config) (*Runner, error) {
	return NewRunnerWithOptions(cfg, "", false)
}

func NewRunnerWithOptions(cfg *config.Config, configFilePath string, printMetaData bool) (*Runner, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	benchmarkID := fmt.Sprintf("%s_%d", cfg.Benchmark.Name, time.Now().Unix())
	
	containerMgr, err := container.NewManager(cfg.Benchmark.Docker.Auth)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create container manager: %w", err)
	}

	// Initialize storage manager
	storageMgr, err := storage.NewManager(cfg.Benchmark.Data.DB)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create storage manager: %w", err)
	}

	// Get next benchmark ID from database
	benchmarkIDNum, err := storageMgr.GetNextBenchmarkID(ctx)
	if err != nil {
		log.WithError(err).Warn("Failed to get next benchmark ID from database, using timestamp")
		benchmarkIDNum = time.Now().Unix()
	}

	log.WithFields(log.Fields{
		"benchmark_id_string": benchmarkID,
		"benchmark_id_number": benchmarkIDNum,
	}).Info("Benchmark IDs assigned")

	// Initialize DataFrame-based profiler manager
	dataFrameMgr, err := profiler.NewDataFrameManager(
		&cfg.Benchmark.Data,
		benchmarkID,
		benchmarkIDNum,
		time.Now(), // Will be updated when benchmark actually starts
		storageMgr,
		convertContainerConfigs(cfg.Container),
		cfg.Benchmark.Scheduler.Implementation,
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create DataFrame profiler manager: %w", err)
	}

	// Keep the old comprehensive profiler manager for backward compatibility if needed
	// Initialize comprehensive profiler manager with container configs and scheduler info
	profilerMgr, err := profiler.NewComprehensiveManager(
		&cfg.Benchmark.Data,
		benchmarkID,
		benchmarkIDNum,
		time.Now(), // Will be set properly when benchmark starts
		storageMgr,
		convertContainerConfigs(cfg.Container), // Convert to proper format
		cfg.Benchmark.Scheduler.Implementation,
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create profiler manager: %w", err)
	}

	// Initialize scheduler
	schedulerMgr, err := scheduler.NewScheduler(cfg.Benchmark.Scheduler.Implementation, cfg.Benchmark.Scheduler.RDT)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create scheduler: %w", err)
	}

	// Initialize metadata
	metadata, err := createBenchmarkMetadata(cfg, configFilePath, benchmarkIDNum)
	if err != nil {
		log.WithError(err).Warn("Failed to create metadata, continuing without full metadata")
		metadata = &storage.BenchmarkMetadata{
			BenchmarkID:   benchmarkIDNum,
			BenchmarkName: cfg.Benchmark.Name,
		}
	}

	return &Runner{
		config:         cfg,
		containerMgr:   containerMgr,
		profilerMgr:    profilerMgr,
		dataFrameMgr:   dataFrameMgr,
		schedulerMgr:   schedulerMgr,
		storageMgr:     storageMgr,
		benchmarkID:    benchmarkID,
		benchmarkIDNum: benchmarkIDNum,
		samplingStep:   0,
		ctx:            ctx,
		cancel:         cancel,
		configFilePath: configFilePath,
		printMetaData:  printMetaData,
		metadata:       metadata,
	}, nil
}

// Run executes the complete benchmark lifecycle
func (r *Runner) Run() error {
	r.startTime = time.Now()
	
	// Reset storage byte counter for this benchmark run
	r.storageMgr.ResetBytesCounter()
	
	log.WithFields(log.Fields{
		"benchmark_id": r.benchmarkID,
		"start_time":   r.startTime,
	}).Info("Starting benchmark execution")

	// Set up signal handling for graceful shutdown
	r.setupSignalHandling()

	if err := r.prepare(); err != nil {
		return fmt.Errorf("preparation phase failed: %w", err)
	}

	if err := r.execute(); err != nil {
		return fmt.Errorf("execution phase failed: %w", err)
	}

	if err := r.cleanup(); err != nil {
		log.WithError(err).Warn("Cleanup phase completed with warnings")
	}

	return nil
}

func (r *Runner) prepare() error {
	log.Info("Entering preparation phase")

	if err := r.containerMgr.PullImages(r.ctx, r.config.Container); err != nil {
		return fmt.Errorf("failed to pull images: %w", err)
	}

	// Create all containers
	if err := r.containerMgr.CreateContainers(r.ctx, r.config.Container, r.benchmarkID); err != nil {
		return fmt.Errorf("failed to create containers: %w", err)
	}

	// Initialize scheduler
	if err := r.schedulerMgr.Initialize(r.ctx); err != nil {
		return fmt.Errorf("failed to initialize scheduler: %w", err)
	}

	// Initialize DataFrame profiler (new implementation)
	if err := r.dataFrameMgr.Initialize(r.ctx, r.containerMgr.GetContainerIDs()); err != nil {
		return fmt.Errorf("failed to initialize DataFrame profiler: %w", err)
	}

	log.Info("Preparation phase completed successfully")
	return nil
}

func (r *Runner) execute() error {
	log.Info("Entering execution phase")

	// Start DataFrame profiling (new implementation)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := r.dataFrameMgr.StartProfiling(r.ctx, r.containerMgr.GetContainerIDs()); err != nil {
			log.WithError(err).Error("DataFrame profiling failed")
		}
	}()

	// Start scheduler monitoring
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := r.schedulerMgr.Monitor(r.ctx, r.containerMgr.GetContainerIDs()); err != nil {
			log.WithError(err).Error("Scheduler monitoring failed")
		}
	}()

	// Start containers according to schedule
	if err := r.startContainersScheduled(); err != nil {
		return fmt.Errorf("failed to start containers: %w", err)
	}

	// Wait for benchmark completion or manual termination
	r.waitForCompletion()

	r.endTime = time.Now()
	log.Info("Execution phase completed")
	return nil
}

func (r *Runner) startContainersScheduled() error {
	startSchedule := make(map[int][]string)
	for name, containerCfg := range r.config.Container {
		startSchedule[containerCfg.Start] = append(startSchedule[containerCfg.Start], name)
	}

	// Start containers in scheduled order
	for startTime := 0; ; startTime++ {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		default:
		}

		if containers, exists := startSchedule[startTime]; exists {
			log.WithFields(log.Fields{
				"time":       startTime,
				"containers": containers,
			}).Info("Starting scheduled containers")

			for _, containerName := range containers {
				if err := r.containerMgr.StartContainer(r.ctx, containerName); err != nil {
					return fmt.Errorf("failed to start container %s: %w", containerName, err)
				}

				// Apply scheduler policies
				containerID := r.containerMgr.GetContainerID(containerName)
				if containerID != "" {
					if err := r.schedulerMgr.ScheduleContainer(r.ctx, containerID, r.config.Container[containerName]); err != nil {
						log.WithError(err).WithField("container", containerName).Warn("Failed to apply scheduler policy")
					}
				}

				// Execute optional command if specified
				containerConfig := r.config.Container[containerName]
				if containerConfig.Command != "" {
					log.WithFields(log.Fields{
						"container": containerName,
						"command":   containerConfig.Command,
					}).Info("Executing command in container")

					if err := r.containerMgr.ExecuteCommand(r.ctx, containerName, containerConfig.Command); err != nil {
						log.WithError(err).WithFields(log.Fields{
							"container": containerName,
							"command":   containerConfig.Command,
						}).Warn("Failed to execute command in container")
						// Continue with other containers even if command execution fails
					}
				}
			}
		}

		if len(startSchedule) == 0 || startTime > r.getMaxStartTime() {
			break
		}

		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(time.Second):
		}
	}

	return nil
}

func (r *Runner) waitForCompletion() {
	if r.config.Benchmark.MaxT == -1 {
		log.Info("Benchmark set to run indefinitely, waiting for manual termination")
		<-r.ctx.Done()
	} else {
		duration := time.Duration(r.config.Benchmark.MaxT) * time.Second
		log.WithField("duration", duration).Info("Waiting for benchmark completion")
		
		select {
		case <-r.ctx.Done():
			log.Info("Benchmark terminated manually")
		case <-time.After(duration):
			log.Info("Benchmark completed after specified duration")
			r.cancel()
		}
	}

	r.wg.Wait()
}

func (r *Runner) cleanup() error {
	log.Info("Entering cleanup phase")

	// Finalize metadata with completion information
	r.finalizeMetadata()

	var errors []error

	// Stop DataFrame profiler and write data to database
	if err := r.dataFrameMgr.Stop(); err != nil {
		log.WithError(err).Warn("Failed to stop DataFrame profiler cleanly")
		errors = append(errors, fmt.Errorf("failed to stop DataFrame profiler: %w", err))
	}

	// Log DataFrames summary
	r.dataFrameMgr.LogDataFramesSummary()

	// Write DataFrames to database
	if err := r.dataFrameMgr.WriteDataFramesToDatabase(r.ctx); err != nil {
		log.WithError(err).Error("Failed to write DataFrames to database")
		errors = append(errors, fmt.Errorf("failed to write DataFrames to database: %w", err))
	}

	if err := r.containerMgr.StopAndCleanup(r.ctx); err != nil {
		log.WithError(err).Warn("Container cleanup encountered issues")
		errors = append(errors, fmt.Errorf("failed to cleanup containers: %w", err))
	}

	if err := r.schedulerMgr.Finalize(r.ctx); err != nil {
		log.WithError(err).Warn("Failed to finalize scheduler cleanly")
		errors = append(errors, fmt.Errorf("failed to finalize scheduler: %w", err))
	}

	// Write metadata to database before closing storage
	if err := r.writeMetadata(); err != nil {
		log.WithError(err).Warn("Failed to write metadata to storage")
		errors = append(errors, fmt.Errorf("failed to write metadata: %w", err))
	}

	if err := r.storageMgr.Close(); err != nil {
		log.WithError(err).Warn("Failed to close storage cleanly")
		errors = append(errors, fmt.Errorf("failed to close storage: %w", err))
	}

	// Print metadata if requested
	if r.printMetaData {
		r.printBenchmarkMetadata()
	} else {
		r.printBasicSummary()
	}

	if len(errors) > 0 {
		log.WithField("error_count", len(errors)).Warn("Cleanup completed with some warnings")
		return fmt.Errorf("cleanup completed with %d warnings", len(errors))
	}

	log.Info("Cleanup phase completed successfully")
	return nil
}

func (r *Runner) setupSignalHandling() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.WithField("signal", sig).Info("Received termination signal, initiating graceful shutdown")
		r.cancel()
	}()
}

func (r *Runner) getMaxStartTime() int {
	maxStartTime := 0
	for _, containerCfg := range r.config.Container {
		if containerCfg.Start > maxStartTime {
			maxStartTime = containerCfg.Start
		}
	}
	return maxStartTime
}

func convertContainerConfigs(containers map[string]config.ContainerConfig) map[string]*config.ContainerConfig {
	result := make(map[string]*config.ContainerConfig)
	for name, cfg := range containers {
		containerCfg := cfg
		result[name] = &containerCfg
	}
	return result
}

// createBenchmarkMetadata creates comprehensive metadata for the benchmark
func createBenchmarkMetadata(cfg *config.Config, configFilePath string, benchmarkIDNum int64) (*storage.BenchmarkMetadata, error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	// Read original config file content (unexpanded)
	var configFileContent string
	if configFilePath != "" {
		content, err := ioutil.ReadFile(configFilePath)
		if err != nil {
			log.WithError(err).Warn("Failed to read config file for metadata")
			configFileContent = "Failed to read config file"
		} else {
			configFileContent = string(content)
		}
	}

	// Get system information
	cpuInfo := system.GetCPUInfo()
	osInfo := system.GetOSInfo()
	
	// Get scheduler version based on implementation
	schedulerVersion := version.GetSchedulerVersion(cfg.Benchmark.Scheduler.Implementation)
	
	// Build container metadata
	containerImages := make(map[string]int)
	containerDetails := make(map[string]storage.ContainerMeta)
	
	for name, container := range cfg.Container {
		containerImages[container.Image]++
		containerDetails[name] = storage.ContainerMeta{
			Index:     container.Index,
			Image:     container.Image,
			StartTime: container.Start,
			StopTime:  container.Stop,
			CorePin:   container.Core,
			EnvVars:   container.Env,
			Command:   container.Command,
		}
	}

	metadata := &storage.BenchmarkMetadata{
		BenchmarkID:       benchmarkIDNum,
		BenchmarkName:     cfg.Benchmark.Name,
		BenchmarkStarted:  time.Now(),
		ExecutionHost:     hostname,
		CPUExecutedOn:     runtime.NumCPU(), // Will be updated during execution
		TotalCPUCores:     runtime.NumCPU(),
		OSInfo:           fmt.Sprintf("%s %s", osInfo.OS, osInfo.Architecture),
		KernelVersion:    osInfo.Kernel,
		// System information
		DriverVersion:    version.Version,
		BuildDate:       version.BuildDate,
		CPUModel:        cpuInfo.Model,
		CPUVendor:       cpuInfo.Vendor,
		CPUThreads:      cpuInfo.Threads,
		Architecture:    osInfo.Architecture,
		Hostname:        hostname,
		Description:     cfg.Benchmark.Description,
		SchedulerVersion: schedulerVersion,
		ConfigFile:       configFileContent,
		ConfigFilePath:   configFilePath,
		UsedScheduler:    cfg.Benchmark.Scheduler.Implementation,
		SamplingFrequency: cfg.Benchmark.Data.ProfileFrequency,
		MaxDuration:      cfg.Benchmark.MaxT,
		RDTEnabled:       cfg.Benchmark.Data.RDT,
		PerfEnabled:      cfg.Benchmark.Data.Perf,
		DockerStatsEnabled: cfg.Benchmark.Data.DockerStats,
		TotalContainers:  len(cfg.Container),
		ContainerImages:  containerImages,
		ContainerDetails: containerDetails,
		DatabaseHost:     cfg.Benchmark.Data.DB.Host,
		DatabaseName:     cfg.Benchmark.Data.DB.Name,
		DatabaseUser:     cfg.Benchmark.Data.DB.User,
	}

	return metadata, nil
}

// getKernelVersion attempts to get the kernel version
func getKernelVersion() string {
	if runtime.GOOS == "linux" {
		content, err := ioutil.ReadFile("/proc/version")
		if err != nil {
			return "unknown"
		}
		return string(content)
	}
	return runtime.GOOS + " " + runtime.GOARCH
}

// finalizeMetadata completes the metadata with execution results
func (r *Runner) finalizeMetadata() {
	if r.metadata == nil {
		return
	}

	r.metadata.BenchmarkFinished = r.endTime
	r.metadata.CPUExecutedOn = runtime.NumCPU() // Update with actual execution context
	
	// Get profiler statistics if available
	if r.profilerMgr != nil {
		// Note: You might need to add a GetStats method to the profiler manager
		// For now, we'll set reasonable defaults
		duration := r.endTime.Sub(r.startTime)
		if r.metadata.SamplingFrequency > 0 {
			expectedSamples := int64(duration.Milliseconds()) / int64(r.metadata.SamplingFrequency)
			r.metadata.TotalSamplingSteps = expectedSamples
			r.metadata.TotalMeasurements = expectedSamples * int64(r.metadata.TotalContainers)
		}
	}
}

// writeMetadata writes the metadata to the database
func (r *Runner) writeMetadata() error {
	if r.metadata == nil || r.storageMgr == nil {
		return nil
	}

	return r.storageMgr.WriteBenchmarkMetadata(r.ctx, r.metadata)
}

// printBasicSummary prints a concise summary of the benchmark execution
func (r *Runner) printBasicSummary() {
	duration := r.endTime.Sub(r.startTime)
	totalBytes := r.storageMgr.GetTotalBytesWritten()
	dataSizeFormatted := r.storageMgr.FormatDataSize(totalBytes)
	
	// Split into multiple log messages for better readability
	log.WithFields(log.Fields{
		"benchmark_id":   r.benchmarkIDNum,
		"benchmark_name": r.config.Benchmark.Name,
	}).Infof("Benchmark '%s' with ID %d completed in %v", 
		r.config.Benchmark.Name,
		r.benchmarkIDNum,
		duration.Round(time.Second))
	
	totalMeasurements := int64(0)
	if r.metadata != nil {
		totalMeasurements = r.metadata.TotalMeasurements
	}
	
	log.WithFields(log.Fields{
		"total_containers":   len(r.config.Container),
		"total_measurements": totalMeasurements,
	}).Infof("Total Containers: %d, Total Measurements: %d", 
		len(r.config.Container), 
		totalMeasurements)
	
	log.WithFields(log.Fields{
		"data_saved_bytes": totalBytes,
		"data_size_formatted": dataSizeFormatted,
	}).Infof("Data Saved: %s", dataSizeFormatted)
	
	log.Info("Use --print-meta-data flag for detailed metadata")
}

// printBenchmarkMetadata prints comprehensive metadata about the benchmark execution
func (r *Runner) printBenchmarkMetadata() {
	if r.metadata == nil {
		fmt.Println("No metadata available")
		return
	}

	metadata := r.metadata
	duration := r.endTime.Sub(r.startTime)
	totalBytes := r.storageMgr.GetTotalBytesWritten()
	dataSizeFormatted := r.storageMgr.FormatDataSize(totalBytes)

	fmt.Println()
	fmt.Println("=============================================================================")
	fmt.Println("                           BENCHMARK METADATA                               ")
	fmt.Println("=============================================================================")
	fmt.Println()
	
	// Core Information
	fmt.Printf("BENCHMARK IDENTIFICATION\n")
	fmt.Printf("   Name:                  %s\n", metadata.BenchmarkName)
	if metadata.Description != "" {
		fmt.Printf("   Description:           %s\n", metadata.Description)
	}
	fmt.Printf("   ID:                    %d\n", metadata.BenchmarkID)
	fmt.Printf("   Start Time:            %s\n", metadata.BenchmarkStarted.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("   End Time:              %s\n", metadata.BenchmarkFinished.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("   Duration:              %v\n", duration.Round(time.Second))
	fmt.Println()
	
	// System Information
	fmt.Printf("SYSTEM INFORMATION\n")
	fmt.Printf("   Driver Version:        %s\n", metadata.DriverVersion)
	fmt.Printf("   Build Date:            %s\n", metadata.BuildDate)
	fmt.Printf("   Host:                  %s\n", metadata.Hostname)
	fmt.Printf("   CPU Model:             %s\n", metadata.CPUModel)
	fmt.Printf("   CPU Vendor:            %s\n", metadata.CPUVendor)
	fmt.Printf("   CPU Cores:             %d\n", metadata.TotalCPUCores)
	fmt.Printf("   CPU Threads:           %d\n", metadata.CPUThreads)
	fmt.Printf("   OS:                    %s\n", metadata.OSInfo)
	fmt.Printf("   Architecture:          %s\n", metadata.Architecture)
	if metadata.KernelVersion != "" && len(metadata.KernelVersion) < 100 {
		fmt.Printf("   Kernel Version:        %s\n", metadata.KernelVersion)
	}
	fmt.Println()
	
	// Configuration
	fmt.Printf("CONFIGURATION\n")
	fmt.Printf("   Config File:           %s\n", metadata.ConfigFilePath)
	fmt.Printf("   Scheduler:             %s\n", metadata.UsedScheduler)
	fmt.Printf("   Scheduler Version:     %s\n", metadata.SchedulerVersion)
	fmt.Printf("   Sampling Frequency:    %d ms\n", metadata.SamplingFrequency)
	if metadata.MaxDuration == -1 {
		fmt.Printf("   Max Duration:          Indefinite\n")
	} else {
		fmt.Printf("   Max Duration:          %d seconds\n", metadata.MaxDuration)
	}
	fmt.Println()
	
	// Data Collection
	fmt.Printf("DATA COLLECTION\n")
	fmt.Printf("   RDT Enabled:           %t\n", metadata.RDTEnabled)
	fmt.Printf("   Perf Enabled:          %t\n", metadata.PerfEnabled)
	fmt.Printf("   Docker Stats Enabled:  %t\n", metadata.DockerStatsEnabled)
	fmt.Printf("   Total Sampling Steps:  %d\n", metadata.TotalSamplingSteps)
	fmt.Printf("   Total Measurements:    %d\n", metadata.TotalMeasurements)
	if metadata.TotalDataSize > 0 {
		fmt.Printf("   Total Data Size:       %.2f MB\n", float64(metadata.TotalDataSize)/(1024*1024))
	}
	fmt.Println()
	
	// Container Information
	fmt.Printf("CONTAINERS\n")
	fmt.Printf("   Total Containers:      %d\n", metadata.TotalContainers)
	fmt.Printf("   Images Used:\n")
	for image, count := range metadata.ContainerImages {
		fmt.Printf("     - %s: %d container(s)\n", image, count)
	}
	fmt.Println()
	
	// Database Information
	fmt.Printf("DATABASE\n")
	fmt.Printf("   Host:                  %s\n", metadata.DatabaseHost)
	fmt.Printf("   Database:              %s\n", metadata.DatabaseName)
	fmt.Printf("   User:                  %s\n", metadata.DatabaseUser)
	fmt.Printf("   Data Saved to DB:      %s (%d bytes)\n", dataSizeFormatted, totalBytes)
	fmt.Println()
	
	// Configuration File Content (truncated for readability)
	if metadata.ConfigFile != "" {
		fmt.Printf("ORIGINAL CONFIGURATION\n")
		lines := strings.Split(metadata.ConfigFile, "\n")
		maxLines := 20
		if len(lines) > maxLines {
			for i := 0; i < maxLines; i++ {
				fmt.Printf("   %s\n", lines[i])
			}
			fmt.Printf("   ... (%d more lines)\n", len(lines)-maxLines)
		} else {
			for _, line := range lines {
				fmt.Printf("   %s\n", line)
			}
		}
		fmt.Println()
	}
	
	fmt.Println("=============================================================================")
	fmt.Println()
	
	// Also log structured metadata for programmatic access
	log.WithFields(log.Fields{
		"benchmark_id":         metadata.BenchmarkID,
		"benchmark_name":       metadata.BenchmarkName,
		"execution_host":       metadata.ExecutionHost,
		"duration_seconds":     duration.Seconds(),
		"total_containers":     metadata.TotalContainers,
		"total_sampling_steps": metadata.TotalSamplingSteps,
		"total_measurements":   metadata.TotalMeasurements,
		"used_scheduler":       metadata.UsedScheduler,
		"config_file_path":     metadata.ConfigFilePath,
		"data_saved_bytes":     totalBytes,
	}).Info("Benchmark metadata summary")
}
