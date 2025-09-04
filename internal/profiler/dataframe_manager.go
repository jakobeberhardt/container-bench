package profiler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jakobeberhardt/container-bench/internal/config"
	"github.com/jakobeberhardt/container-bench/internal/storage"
	log "github.com/sirupsen/logrus"
)

// DataFrameProfilerManager coordinates profiling activities using DataFrames
type DataFrameProfilerManager struct {
	config           *config.DataConfig
	benchmarkID      string
	benchmarkIDNum   int64
	storage          *storage.Manager
	collectors       []Collector
	stopChan         chan struct{}
	wg               sync.WaitGroup
	metadataProvider *MetadataProvider
	
	// DataFrame management
	dataFrames       *storage.BenchmarkDataFrames
	samplingStep     int64
	startTime        time.Time
	
	// Container configuration
	containerConfigs map[string]*config.ContainerConfig
	schedulerType    string
	
	// Collection goroutines per container
	containerProfilers map[string]*ContainerProfiler
}

// ContainerProfiler handles profiling for a single container in its own goroutine
type ContainerProfiler struct {
	containerName    string
	containerConfig  *config.ContainerConfig
	collectors       []Collector
	dataFrames       *storage.BenchmarkDataFrames
	metadataProvider *MetadataProvider
	samplingStep     int64
	stopChan         chan struct{}
	wg               sync.WaitGroup
	mu               sync.Mutex
}

// NewDataFrameProfilerManager creates a new DataFrame-based profiler manager
func NewDataFrameProfilerManager(
	config *config.DataConfig,
	benchmarkID string,
	benchmarkIDNum int64,
	startTime time.Time,
	storageMgr *storage.Manager,
	containerConfigs map[string]*config.ContainerConfig,
	schedulerType string,
) (*DataFrameProfilerManager, error) {
	// Create metadata provider
	metadataProvider := NewMetadataProvider(benchmarkID, config.ProfileFrequency)
	
	// Set enabled profilers
	enabledProfilers := map[string]bool{
		"docker_stats": config.DockerStats,
		"perf":         config.Perf,
		"rdt":          config.RDT,
	}
	metadataProvider.SetEnabledProfilers(enabledProfilers)

	// Initialize DataFrames
	dataFrames := storage.NewBenchmarkDataFrames(
		benchmarkIDNum,
		startTime,
		config.ProfileFrequency,
		schedulerType,
	)

	// Add containers to DataFrames
	for containerName, containerConfig := range containerConfigs {
		dataFrames.AddContainer(
			containerName,
			containerConfig.Index,
			containerConfig.Image,
			containerConfig.Core,
		)
	}

	pm := &DataFrameProfilerManager{
		config:             config,
		benchmarkID:        benchmarkID,
		benchmarkIDNum:     benchmarkIDNum,
		storage:            storageMgr,
		metadataProvider:   metadataProvider,
		dataFrames:         dataFrames,
		startTime:          startTime,
		containerConfigs:   containerConfigs,
		schedulerType:      schedulerType,
		stopChan:           make(chan struct{}),
		containerProfilers: make(map[string]*ContainerProfiler),
	}

	log.WithFields(log.Fields{
		"containers":       len(containerConfigs),
		"sampling_freq_ms": config.ProfileFrequency,
		"docker_stats":     config.DockerStats,
		"perf":             config.Perf,
		"rdt":              config.RDT,
	}).Info("DataFrame profiler manager initialized")

	return pm, nil
}

// Initialize initializes all collectors and container profilers
func (pm *DataFrameProfilerManager) Initialize(ctx context.Context, containerIDs map[string]string) error {
	log.Info("Initializing DataFrame profiler manager")

	// Set container IDs in metadata provider and initialize
	if err := pm.metadataProvider.SetContainerIDs(containerIDs); err != nil {
		return fmt.Errorf("failed to initialize container metadata: %w", err)
	}
	
	if err := pm.metadataProvider.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize metadata provider: %w", err)
	}

	// Initialize container profilers (one per container)
	for containerName, containerConfig := range pm.containerConfigs {
		containerProfiler, err := pm.createContainerProfiler(containerName, containerConfig)
		if err != nil {
			return fmt.Errorf("failed to create profiler for container %s: %w", containerName, err)
		}
		
		if err := containerProfiler.initialize(ctx); err != nil {
			return fmt.Errorf("failed to initialize profiler for container %s: %w", containerName, err)
		}
		
		pm.containerProfilers[containerName] = containerProfiler
		
		log.WithField("container", containerName).Info("Container profiler initialized")
	}

	return nil
}

// createContainerProfiler creates collectors for a specific container
func (pm *DataFrameProfilerManager) createContainerProfiler(containerName string, containerConfig *config.ContainerConfig) (*ContainerProfiler, error) {
	var collectors []Collector

	// Create collectors for this container
	if pm.config.DockerStats {
		dockerCollector := NewDockerStatsCollector(pm.metadataProvider)
		collectors = append(collectors, dockerCollector)
	}

	if pm.config.Perf {
		perfCollector := NewPerfCollector(pm.metadataProvider)
		collectors = append(collectors, perfCollector)
	}

	if pm.config.RDT {
		rdtCollector := NewRDTCollector(pm.benchmarkID)
		collectors = append(collectors, rdtCollector)
	}

	if len(collectors) == 0 {
		return nil, fmt.Errorf("no profiling collectors configured for container %s", containerName)
	}

	return &ContainerProfiler{
		containerName:    containerName,
		containerConfig:  containerConfig,
		collectors:       collectors,
		dataFrames:       pm.dataFrames,
		metadataProvider: pm.metadataProvider,
		stopChan:         make(chan struct{}),
	}, nil
}

// initialize initializes the container profiler
func (cp *ContainerProfiler) initialize(ctx context.Context) error {
	for _, collector := range cp.collectors {
		if err := collector.Initialize(ctx); err != nil {
			return fmt.Errorf("failed to initialize collector %s for container %s: %w", collector.Name(), cp.containerName, err)
		}

		// Set container IDs for collectors that need them
		if dockerCollector, ok := collector.(*DockerStatsCollector); ok {
			// Create single container map for this collector
			containerIDs := map[string]string{cp.containerName: ""}
			dockerCollector.SetContainerIDs(containerIDs)
		}
		
		if perfCollector, ok := collector.(*PerfCollector); ok {
			containerIDs := map[string]string{cp.containerName: ""}
			perfCollector.SetContainerIDs(containerIDs)
		}
	}
	
	return nil
}

// StartProfiling starts profiling with individual goroutines per container
func (pm *DataFrameProfilerManager) StartProfiling(ctx context.Context, containerIDs map[string]string) error {
	log.WithField("frequency_ms", pm.config.ProfileFrequency).Info("Starting DataFrame-based profiling")

	// Start a profiling goroutine for each container
	for containerName, containerProfiler := range pm.containerProfilers {
		pm.wg.Add(1)
		go pm.runContainerProfiling(ctx, containerName, containerProfiler)
		
		log.WithField("container", containerName).Info("Started container profiling goroutine")
	}

	return nil
}

// runContainerProfiling runs the profiling loop for a single container
func (pm *DataFrameProfilerManager) runContainerProfiling(ctx context.Context, containerName string, containerProfiler *ContainerProfiler) {
	defer pm.wg.Done()
	
	ticker := time.NewTicker(time.Duration(pm.config.ProfileFrequency) * time.Millisecond)
	defer ticker.Stop()

	log.WithFields(log.Fields{
		"container":    containerName,
		"frequency_ms": pm.config.ProfileFrequency,
	}).Debug("Container profiling loop started")

	for {
		select {
		case <-ctx.Done():
			log.WithField("container", containerName).Info("Container profiling stopped due to context cancellation")
			return
		case <-pm.stopChan:
			log.WithField("container", containerName).Info("Container profiling stopped")
			return
		case <-containerProfiler.stopChan:
			log.WithField("container", containerName).Info("Container profiler stopped")
			return
		case timestamp := <-ticker.C:
			containerProfiler.wg.Add(1)
			go pm.collectContainerMetrics(ctx, containerName, containerProfiler, timestamp)
		}
	}
}

// collectContainerMetrics collects metrics for a single container
func (pm *DataFrameProfilerManager) collectContainerMetrics(ctx context.Context, containerName string, containerProfiler *ContainerProfiler, timestamp time.Time) {
	defer containerProfiler.wg.Done()

	// Increment sampling step
	containerProfiler.mu.Lock()
	containerProfiler.samplingStep++
	currentStep := containerProfiler.samplingStep
	containerProfiler.mu.Unlock()

	var perfData *storage.PerfData
	var dockerData *storage.DockerData
	var rdtData *storage.RDTData

	// Collect data from all collectors for this container
	for _, collector := range containerProfiler.collectors {
		measurements, err := collector.Collect(ctx, timestamp)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"collector": collector.Name(),
				"container": containerName,
			}).Error("Failed to collect metrics")
			continue
		}

		// Parse measurements based on collector type
		switch collector.Name() {
		case "docker_stats":
			dockerData = pm.parseDockerMeasurements(measurements, containerName)
		case "perf":
			perfData = pm.parsePerfMeasurements(measurements, containerName)
		case "rdt":
			rdtData = pm.parseRDTMeasurements(measurements, containerName)
		}
		
		if len(measurements) > 0 {
			log.WithFields(log.Fields{
				"collector":    collector.Name(),
				"container":    containerName,
				"measurements": len(measurements),
				"has_data":     (collector.Name() == "docker_stats" && dockerData != nil) || 
				                (collector.Name() == "perf" && perfData != nil) || 
				                (collector.Name() == "rdt" && rdtData != nil),
			}).Debug("Collected and parsed measurements from collector")
		}
	}

	// Calculate relative time
	relativeTime := timestamp.Sub(pm.startTime).Milliseconds()

	// Get current CPU
	cpuExecutedOn := pm.getCurrentCPU()

	// Add data point to DataFrame
	pm.dataFrames.AddDataPoint(
		containerName,
		currentStep,
		timestamp,
		relativeTime,
		cpuExecutedOn,
		perfData,
		dockerData,
		rdtData,
	)

	log.WithFields(log.Fields{
		"container":     containerName,
		"sampling_step": currentStep,
		"data_types":    func() []string {
			types := []string{}
			if perfData != nil { types = append(types, "perf") }
			if dockerData != nil { types = append(types, "docker") }
			if rdtData != nil { types = append(types, "rdt") }
			return types
		}(),
	}).Debug("Added data point to container DataFrame")
}

// Helper methods to parse measurements from collectors
func (pm *DataFrameProfilerManager) parseDockerMeasurements(measurements []storage.Measurement, containerName string) *storage.DockerData {
	dockerData := &storage.DockerData{}
	found := false
	processedCount := 0

	for _, measurement := range measurements {
		// Check if this measurement is for our container
		containerID := measurement.Tags["container_id"]
		measurementContainerName := measurement.Tags["container_name"]
		
		// Match by container name or container ID (allow partial ID matching)
		if measurementContainerName != containerName && !strings.Contains(containerID, containerName) {
			continue
		}

		processedCount++

		// Parse based on measurement name
		switch measurement.Name {
		case "cpu_usage_percent":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.CPUUsagePercent = val
				found = true
			}
		case "cpu_usage_total":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.CPUUsageTotal = uint64(val)
				found = true
			}
		case "cpu_usage_kernel":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.CPUUsageKernel = uint64(val)
				found = true
			}
		case "cpu_usage_user":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.CPUUsageUser = uint64(val)
				found = true
			}
		case "cpu_throttling":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.CPUThrottling = uint64(val)
				found = true
			}
		case "memory_usage_bytes":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.MemoryUsage = uint64(val)
				found = true
			}
		case "memory_limit":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.MemoryLimit = uint64(val)
				found = true
			}
		case "memory_usage_percent":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.MemoryUsagePercent = val
				found = true
			}
		case "memory_cache":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.MemoryCache = uint64(val)
				found = true
			}
		case "memory_rss":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.MemoryRSS = uint64(val)
				found = true
			}
		case "memory_swap":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.MemorySwap = uint64(val)
				found = true
			}
		case "network_rx_bytes":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.NetworkRxBytes = uint64(val)
				found = true
			}
		case "network_tx_bytes":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.NetworkTxBytes = uint64(val)
				found = true
			}
		case "network_rx_packets":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.NetworkRxPackets = uint64(val)
				found = true
			}
		case "network_tx_packets":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.NetworkTxPackets = uint64(val)
				found = true
			}
		case "disk_read_bytes":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.DiskReadBytes = uint64(val)
				found = true
			}
		case "disk_write_bytes":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.DiskWriteBytes = uint64(val)
				found = true
			}
		case "disk_read_ops":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.DiskReadOps = uint64(val)
				found = true
			}
		case "disk_write_ops":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.DiskWriteOps = uint64(val)
				found = true
			}
		}
	}

	if found {
		log.WithFields(log.Fields{
			"container":         containerName,
			"processed":         processedCount,
			"total":            len(measurements),
			"cpu_usage_pct":    dockerData.CPUUsagePercent,
			"memory_usage_mb":  dockerData.MemoryUsage / 1024 / 1024,
		}).Debug("Docker measurements parsed successfully")
		return dockerData
	}
	return nil
}

func (pm *DataFrameProfilerManager) parsePerfMeasurements(measurements []storage.Measurement, containerName string) *storage.PerfData {
	perfData := &storage.PerfData{}
	found := false

	for _, measurement := range measurements {
		// Perf measurements should have Name: "perf_counter" and the actual metric in the "metric" tag
		if measurement.Name != "perf_counter" {
			continue
		}
		
		// Check if this measurement is for our container
		measurementContainerName := measurement.Tags["container"]
		containerID := measurement.Tags["container_id"]
		
		// Match by container name or container ID
		if measurementContainerName != containerName && !strings.Contains(containerID, containerName) {
			continue
		}

		metricName := measurement.Tags["metric"]
		if metricName == "" {
			continue
		}

		value := measurement.Fields["value"]
		if value == nil {
			continue
		}

		// Set the appropriate field based on metric name
		switch metricName {
		case "cpu_cycles":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.CPUCycles = val
				found = true
			}
		case "instructions":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.Instructions = val
				found = true
			}
		case "cache_references":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.CacheReferences = val
				found = true
			}
		case "cache_misses":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.CacheMisses = val
				found = true
			}
		case "branch_instructions":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.BranchInstructions = val
				found = true
			}
		case "branch_misses":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.BranchMisses = val
				found = true
			}
		case "bus_cycles":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.BusCycles = val
				found = true
			}
		case "ref_cycles":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.RefCycles = val
				found = true
			}
		case "stalled_cycles_frontend":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.StalledCyclesFrontend = val
				found = true
			}
		case "l1_dcache_loads":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.L1DCacheLoads = val
				found = true
			}
		case "l1_dcache_load_misses":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.L1DCacheLoadMisses = val
				found = true
			}
		case "l1_dcache_stores":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.L1DCacheStores = val
				found = true
			}
		case "l1_dcache_store_misses":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.L1DCacheStoreMisses = val
				found = true
			}
		case "l1_icache_load_misses":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.L1ICacheLoadMisses = val
				found = true
			}
		case "llc_loads":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.LLCLoads = val
				found = true
			}
		case "llc_load_misses":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.LLCLoadMisses = val
				found = true
			}
		case "llc_stores":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.LLCStores = val
				found = true
			}
		case "llc_store_misses":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.LLCStoreMisses = val
				found = true
			}
		case "page_faults":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.PageFaults = val
				found = true
			}
		case "context_switches":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.ContextSwitches = val
				found = true
			}
		case "cpu_migrations":
			if val, ok := pm.convertToUint64(value); ok {
				perfData.CPUMigrations = val
				found = true
			}
		case "ipc":
			// IPC is calculated as a float
			if val, ok := value.(float64); ok {
				perfData.IPC = val
				found = true
			}
		case "cache_miss_rate":
			// Cache miss rate is calculated as a float
			if val, ok := value.(float64); ok {
				perfData.CacheMissRate = val
				found = true
			}
		}
	}

	// Calculate derived metrics if we have the base metrics and they weren't already calculated
	if found {
		if perfData.IPC == 0 && perfData.Instructions > 0 && perfData.CPUCycles > 0 {
			perfData.IPC = float64(perfData.Instructions) / float64(perfData.CPUCycles)
		}
		if perfData.CacheMissRate == 0 && perfData.CacheMisses > 0 && perfData.CacheReferences > 0 {
			perfData.CacheMissRate = float64(perfData.CacheMisses) / float64(perfData.CacheReferences)
		}
		return perfData
	}
	return nil
}

func (pm *DataFrameProfilerManager) parseRDTMeasurements(measurements []storage.Measurement, containerName string) *storage.RDTData {
	for _, measurement := range measurements {
		if measurement.Tags["container"] == containerName && measurement.Name == "rdt_stats" {
			// Convert measurement to RDTData
			return pm.convertMeasurementToRDTData(measurement)
		}
	}
	return nil
}

// Helper methods for data conversion
func (pm *DataFrameProfilerManager) convertMeasurementToDockerData(measurement storage.Measurement) *storage.DockerData {
	// Extract Docker stats from measurement fields
	dockerData := &storage.DockerData{}
	
	if val, ok := measurement.Fields["cpu_usage_percent"].(float64); ok {
		dockerData.CPUUsagePercent = val
	}
	if val, ok := measurement.Fields["memory_usage"].(uint64); ok {
		dockerData.MemoryUsage = val
	}
	if val, ok := measurement.Fields["memory_usage_percent"].(float64); ok {
		dockerData.MemoryUsagePercent = val
	}
	// Add other Docker fields as needed...
	
	return dockerData
}

func (pm *DataFrameProfilerManager) convertMeasurementToRDTData(measurement storage.Measurement) *storage.RDTData {
	// Extract RDT stats from measurement fields
	rdtData := &storage.RDTData{}
	
	if val, ok := measurement.Fields["llc_occupancy"].(uint64); ok {
		rdtData.LLCOccupancy = val
	}
	if val, ok := measurement.Fields["total_mem_bw"].(float64); ok {
		rdtData.TotalMemBW = val
	}
	// Add other RDT fields as needed...
	
	return rdtData
}

// Helper function to convert interface{} values to uint64
func (pm *DataFrameProfilerManager) convertToUint64(value interface{}) (uint64, bool) {
	switch v := value.(type) {
	case uint64:
		return v, true
	case int64:
		if v >= 0 {
			return uint64(v), true
		}
	case int:
		if v >= 0 {
			return uint64(v), true
		}
	case float64:
		if v >= 0 {
			return uint64(v), true
		}
	case float32:
		if v >= 0 {
			return uint64(v), true
		}
	}
	return 0, false
}

func (pm *DataFrameProfilerManager) setPerfMetric(perfData *storage.PerfData, metricName string, value interface{}) {
	// This function is deprecated - the new parsePerfMeasurements handles this directly
	// Keeping for backward compatibility but it's not used in the new implementation
}

// getCurrentCPU returns the current CPU (placeholder implementation)
func (pm *DataFrameProfilerManager) getCurrentCPU() int {
	// This would typically use runtime.LockOSThread() and get the CPU
	// For now, return 0 as placeholder
	return 0
}

// Stop stops the profiling and all container profilers
func (pm *DataFrameProfilerManager) Stop() error {
	log.Info("Stopping DataFrame profiler manager")

	// Signal all goroutines to stop
	close(pm.stopChan)
	
	// Stop individual container profilers
	for _, containerProfiler := range pm.containerProfilers {
		close(containerProfiler.stopChan)
		containerProfiler.wg.Wait()
	}

	// Wait for all profiling goroutines to finish
	pm.wg.Wait()

	// Stop metadata provider
	if err := pm.metadataProvider.Stop(); err != nil {
		log.WithError(err).Warn("Failed to stop metadata provider")
	}

	// Stop collectors
	for _, containerProfiler := range pm.containerProfilers {
		for _, collector := range containerProfiler.collectors {
			if err := collector.Close(); err != nil {
				log.WithError(err).WithField("collector", collector.Name()).Warn("Failed to close collector")
			}
		}
	}

	// Set benchmark finished time
	pm.dataFrames.SetBenchmarkFinished(time.Now())

	log.Info("DataFrame profiler manager stopped")
	return nil
}

// GetDataFrames returns the benchmark DataFrames
func (pm *DataFrameProfilerManager) GetDataFrames() *storage.BenchmarkDataFrames {
	return pm.dataFrames
}

// WriteDataFramesToDatabase writes all DataFrame data to the database
func (pm *DataFrameProfilerManager) WriteDataFramesToDatabase(ctx context.Context) error {
	log.Info("Writing DataFrames to database")
	
	// Convert DataFrames to BenchmarkMetrics
	metrics := pm.dataFrames.ConvertToStorableMetrics()
	
	// Write each metric to the database
	var writeErrors []error
	for _, metric := range metrics {
		if err := pm.storage.WriteBenchmarkMetrics(ctx, metric); err != nil {
			writeErrors = append(writeErrors, err)
			log.WithError(err).WithFields(log.Fields{
				"container":     metric.ContainerName,
				"sampling_step": metric.SamplingStep,
			}).Error("Failed to write benchmark metrics")
		}
	}

	if len(writeErrors) > 0 {
		return fmt.Errorf("failed to write %d metrics to database", len(writeErrors))
	}

	log.WithField("metrics_written", len(metrics)).Info("Successfully wrote DataFrames to database")
	return nil
}

// LogDataFramesSummary logs a summary of the collected DataFrames
func (pm *DataFrameProfilerManager) LogDataFramesSummary() {
	pm.dataFrames.LogDataFramesSummary()
}

// ExportDataFramesToCSV exports the collected DataFrames to CSV files
func (pm *DataFrameProfilerManager) ExportDataFramesToCSV(exportPath string, benchmarkName string) error {
	if pm.dataFrames == nil {
		return fmt.Errorf("no DataFrames available for export")
	}
	
	// Finalize benchmark timestamp if not already set
	if pm.dataFrames.BenchmarkFinished.IsZero() {
		pm.dataFrames.BenchmarkFinished = time.Now()
	}
	
	return pm.dataFrames.ExportToCSV(exportPath, benchmarkName)
}
