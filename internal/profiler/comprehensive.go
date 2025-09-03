package profiler

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/jakobeberhardt/container-bench/internal/config"
	"github.com/jakobeberhardt/container-bench/internal/storage"
	log "github.com/sirupsen/logrus"
)

// ComprehensiveManager coordinates all profiling and creates comprehensive metrics
type ComprehensiveManager struct {
	config           *config.DataConfig
	benchmarkID      string
	benchmarkIDNum   int64
	startTime        time.Time
	endTime          time.Time
	storage          *storage.Manager
	collectors       []Collector
	ticker           *time.Ticker
	stopChan         chan struct{}
	wg               sync.WaitGroup
	samplingStep     int64 
	metadataProvider *MetadataProvider // Add metadata provider
	
	containerConfigs map[string]*config.ContainerConfig
	schedulerType    string
}

func NewComprehensiveManager(
	config *config.DataConfig,
	benchmarkID string,
	benchmarkIDNum int64,
	startTime time.Time,
	storage *storage.Manager,
	containerConfigs map[string]*config.ContainerConfig,
	schedulerType string,
) (*ComprehensiveManager, error) {
	// Create metadata provider
	metadataProvider := NewMetadataProvider(benchmarkID, config.ProfileFrequency)
	
	// Set enabled profilers
	enabledProfilers := map[string]bool{
		"docker_stats": config.DockerStats,
		"perf":         config.Perf,
		"rdt":          config.RDT,
	}
	metadataProvider.SetEnabledProfilers(enabledProfilers)

	pm := &ComprehensiveManager{
		config:           config,
		benchmarkID:      benchmarkID,
		benchmarkIDNum:   benchmarkIDNum,
		startTime:        startTime,
		storage:          storage,
		collectors:       make([]Collector, 0),
		stopChan:         make(chan struct{}),
		samplingStep:     0,
		metadataProvider: metadataProvider,
		containerConfigs: containerConfigs,
		schedulerType:    schedulerType,
	}

	// Initialize collectors based on configuration
	if config.DockerStats {
		dockerCollector := NewDockerStatsCollector(metadataProvider)
		pm.collectors = append(pm.collectors, dockerCollector)
	}

	if config.Perf {
		perfCollector := NewPerfCollector(metadataProvider)
		pm.collectors = append(pm.collectors, perfCollector)
	}

	if config.RDT {
		rdtCollector := NewRDTCollector(benchmarkID)
		pm.collectors = append(pm.collectors, rdtCollector)
	}

	if len(pm.collectors) == 0 {
		return nil, fmt.Errorf("no profiling collectors configured")
	}

	log.WithField("collectors", len(pm.collectors)).Info("Comprehensive profiler manager initialized")
	return pm, nil
}

func (pm *ComprehensiveManager) Initialize(ctx context.Context, containerIDs map[string]string) error {
	log.Info("Initializing comprehensive profiler collectors")

	// Set container IDs in metadata provider and initialize
	if err := pm.metadataProvider.SetContainerIDs(containerIDs); err != nil {
		return fmt.Errorf("failed to initialize container metadata: %w", err)
	}
	
	if err := pm.metadataProvider.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize metadata provider: %w", err)
	}

	for _, collector := range pm.collectors {
		if err := collector.Initialize(ctx); err != nil {
			return fmt.Errorf("failed to initialize collector %s: %w", collector.Name(), err)
		}
		
		// Set container IDs for collectors that still need backward compatibility
		if dockerCollector, ok := collector.(*DockerStatsCollector); ok {
			dockerCollector.SetContainerIDs(containerIDs)
		}
		
		// Set container IDs for perf collector
		if perfCollector, ok := collector.(*PerfCollector); ok {
			perfCollector.SetContainerIDs(containerIDs)
			perfCollector.SetSamplingRate(pm.config.ProfileFrequency)
		}
		
		log.WithField("collector", collector.Name()).Info("Collector initialized")
	}

	return nil
}

func (pm *ComprehensiveManager) StartProfiling(ctx context.Context, containerIDs map[string]string) error {
	log.WithField("frequency_ms", pm.config.ProfileFrequency).Info("Starting comprehensive profiling")
	log.WithField("ticker_duration", time.Duration(pm.config.ProfileFrequency)*time.Millisecond).Debug("Setting up profiling ticker")

	duration := time.Duration(pm.config.ProfileFrequency) * time.Millisecond
	pm.ticker = time.NewTicker(duration)
	defer pm.ticker.Stop()

	log.Debug("Entering comprehensive profiling loop")
	log.WithField("profile_frequency", pm.config.ProfileFrequency).Debug("Comprehensive profiler config check")

	for {
		select {
		case <-ctx.Done():
			log.Info("Profiling stopped due to context cancellation")
			return ctx.Err()
		case <-pm.stopChan:
			log.Info("Profiling stopped")
			return nil
		case timestamp := <-pm.ticker.C:
			log.WithField("timestamp", timestamp).Debug("Ticker triggered - starting comprehensive metrics collection")
			pm.wg.Add(1)
			go pm.collectComprehensiveMetrics(ctx, timestamp, containerIDs)
		}
	}
}

func (pm *ComprehensiveManager) collectComprehensiveMetrics(ctx context.Context, timestamp time.Time, containerIDs map[string]string) {
	defer pm.wg.Done()

	log.WithField("timestamp", timestamp).Debug("Starting comprehensive metrics collection")

	pm.samplingStep++
	step := pm.samplingStep

	relativeTime := timestamp.Sub(pm.startTime).Milliseconds()
	
	currentCPU := runtime.NumCPU() 

	dockerData := make(map[string]*storage.DockerData)
	perfData := make(map[string]*storage.PerfData)
	rdtData := make(map[string]*storage.RDTData)

	for _, collector := range pm.collectors {
		log.WithField("collector_name", collector.Name()).Debug("Collecting from collector")
		measurements, err := collector.Collect(ctx, timestamp)
		if err != nil {
			log.WithError(err).WithField("collector", collector.Name()).Debug("Failed to collect metrics")
			continue
		}

		log.WithFields(log.Fields{
			"collector":         collector.Name(),
			"measurement_count": len(measurements),
		}).Debug("Collected measurements from collector")

		switch collector.Name() {
		case "docker_stats":
			dockerData = pm.parseDockerMeasurements(measurements)
			log.WithField("docker_containers", len(dockerData)).Debug("Parsed Docker measurements")
		case "perf":
			perfData = pm.parsePerfMeasurements(measurements)
			log.WithField("perf_containers", len(perfData)).Debug("Parsed Perf measurements")
		case "rdt":
			rdtData = pm.parseRDTMeasurements(measurements)
			log.WithField("rdt_containers", len(rdtData)).Debug("Parsed RDT measurements")
		}
	}

	for containerName, containerID := range containerIDs {
		containerConfig, exists := pm.containerConfigs[containerName]
		if !exists {
			log.WithField("container", containerName).Warn("Container config not found")
			continue
		}

		shortContainerID := containerID[:12]
		dockerMetrics := dockerData[shortContainerID] 
		
		log.WithFields(log.Fields{
			"container_name":       containerName,
			"full_container_id":    containerID,
			"short_container_id":   shortContainerID,
			"docker_data_found":    dockerMetrics != nil,
			"available_docker_keys": fmt.Sprintf("%v", getKeys(dockerData)),
		}).Debug("Looking up Docker metrics for container")

		metrics := &storage.BenchmarkMetrics{
			BenchmarkID:       pm.benchmarkIDNum,
			BenchmarkStarted:  pm.startTime,
			BenchmarkFinished: pm.endTime, 
			SamplingFrequency: pm.config.ProfileFrequency,
			UsedScheduler:     pm.schedulerType,

			ContainerName:  containerName,
			ContainerIndex: containerConfig.Index,
			ContainerImage: containerConfig.Image,
			ContainerCore:  containerConfig.Core,

			UTCTimestamp:  timestamp,
			RelativeTime:  relativeTime,
			SamplingStep:  step,
			CPUExecutedOn: currentCPU,

			DockerMetrics: dockerMetrics, // Use the corrected lookup
			PerfMetrics:   perfData[containerName], // Perf might use container name
			RDTMetrics:    rdtData[containerName],  // RDT might use container name
		}

		log.WithFields(log.Fields{
			"container":    containerName,
			"perf_data":    perfData[containerName] != nil,
			"docker_data":  dockerMetrics != nil,
			"rdt_data":     rdtData[containerName] != nil,
		}).Debug("Writing comprehensive metrics")

		if err := pm.storage.WriteBenchmarkMetrics(ctx, metrics); err != nil {
			log.WithError(err).WithField("container", containerName).Error("Failed to write comprehensive metrics")
		}
	}
}

func (pm *ComprehensiveManager) parseDockerMeasurements(measurements []storage.Measurement) map[string]*storage.DockerData {
	result := make(map[string]*storage.DockerData)
	
	// Group measurements by container ID
	for _, measurement := range measurements {
		containerID := measurement.Tags["container_id"]
		if containerID == "" {
			continue
		}

		// Initialize docker data for this container if not exists
		if _, exists := result[containerID]; !exists {
			result[containerID] = &storage.DockerData{}
		}
		
		dockerData := result[containerID]
		
		// Parse based on measurement name
		switch measurement.Name {
		case "cpu_usage_percent":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.CPUUsagePercent = val
			}
		case "cpu_usage_total":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.CPUUsageTotal = uint64(val)
			}
		case "cpu_usage_kernel":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.CPUUsageKernel = uint64(val)
			}
		case "cpu_usage_user":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.CPUUsageUser = uint64(val)
			}
		case "cpu_throttling":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.CPUThrottling = uint64(val)
			}
		case "memory_usage_bytes":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.MemoryUsage = uint64(val)
			}
		case "memory_limit":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.MemoryLimit = uint64(val)
			}
		case "memory_usage_percent":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.MemoryUsagePercent = val
			}
		case "memory_cache":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.MemoryCache = uint64(val)
			}
		case "memory_rss":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.MemoryRSS = uint64(val)
			}
		case "memory_swap":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.MemorySwap = uint64(val)
			}
		case "network_rx_bytes":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.NetworkRxBytes = uint64(val)
			}
		case "network_tx_bytes":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.NetworkTxBytes = uint64(val)
			}
		case "network_rx_packets":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.NetworkRxPackets = uint64(val)
			}
		case "network_tx_packets":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.NetworkTxPackets = uint64(val)
			}
		case "disk_read_bytes":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.DiskReadBytes = uint64(val)
			}
		case "disk_write_bytes":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.DiskWriteBytes = uint64(val)
			}
		case "disk_read_ops":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.DiskReadOps = uint64(val)
			}
		case "disk_write_ops":
			if val, ok := measurement.Fields["value"].(float64); ok {
				dockerData.DiskWriteOps = uint64(val)
			}
		}
	}
	
	return result
}

// Helper function to get keys from a map for debugging
func getKeys(m map[string]*storage.DockerData) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (pm *ComprehensiveManager) parsePerfMeasurements(measurements []storage.Measurement) map[string]*storage.PerfData {
	result := make(map[string]*storage.PerfData)
	
	log.WithFields(log.Fields{
		"measurement_count": len(measurements),
	}).Debug("Parsing perf measurements")
	
	for _, measurement := range measurements {
		containerName := measurement.Tags["container"]
		if containerName == "" {
			continue
		}

		// Initialize PerfData if not exists
		if result[containerName] == nil {
			result[containerName] = &storage.PerfData{}
		}
		perfData := result[containerName]

		// The perf collector creates measurements with "metric" tag and "value" field
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
			if val, ok := value.(uint64); ok {
				perfData.CPUCycles = val
			}
		case "instructions":
			if val, ok := value.(uint64); ok {
				perfData.Instructions = val
			}
		case "cache_references":
			if val, ok := value.(uint64); ok {
				perfData.CacheReferences = val
			}
		case "cache_misses":
			if val, ok := value.(uint64); ok {
				perfData.CacheMisses = val
			}
		case "branch_instructions":
			if val, ok := value.(uint64); ok {
				perfData.BranchInstructions = val
			}
		case "branch_misses":
			if val, ok := value.(uint64); ok {
				perfData.BranchMisses = val
			}
		case "bus_cycles":
			if val, ok := value.(uint64); ok {
				perfData.BusCycles = val
			}
		case "ref_cycles":
			if val, ok := value.(uint64); ok {
				perfData.RefCycles = val
			}
		case "stalled_cycles_frontend":
			if val, ok := value.(uint64); ok {
				perfData.StalledCyclesFrontend = val
			}
		case "l1_dcache_loads":
			if val, ok := value.(uint64); ok {
				perfData.L1DCacheLoads = val
			}
		case "l1_dcache_load_misses":
			if val, ok := value.(uint64); ok {
				perfData.L1DCacheLoadMisses = val
			}
		case "l1_dcache_stores":
			if val, ok := value.(uint64); ok {
				perfData.L1DCacheStores = val
			}
		case "l1_dcache_store_misses":
			if val, ok := value.(uint64); ok {
				perfData.L1DCacheStoreMisses = val
			}
		case "l1_icache_load_misses":
			if val, ok := value.(uint64); ok {
				perfData.L1ICacheLoadMisses = val
			}
		case "llc_loads":
			if val, ok := value.(uint64); ok {
				perfData.LLCLoads = val
			}
		case "llc_load_misses":
			if val, ok := value.(uint64); ok {
				perfData.LLCLoadMisses = val
			}
		case "llc_stores":
			if val, ok := value.(uint64); ok {
				perfData.LLCStores = val
			}
		case "llc_store_misses":
			if val, ok := value.(uint64); ok {
				perfData.LLCStoreMisses = val
			}
		case "ipc":
			if val, ok := value.(float64); ok {
				perfData.IPC = val
			}
		case "cache_miss_rate":
			if val, ok := value.(float64); ok {
				perfData.CacheMissRate = val
			}
		case "page_faults":
			if val, ok := value.(uint64); ok {
				perfData.PageFaults = val
			}
		case "context_switches":
			if val, ok := value.(uint64); ok {
				perfData.ContextSwitches = val
			}
		case "cpu_migrations":
			if val, ok := value.(uint64); ok {
				perfData.CPUMigrations = val
			}
		}
	}
	
	// Calculate derived metrics for each container
	for containerName, perfData := range result {
		if perfData.Instructions > 0 && perfData.CPUCycles > 0 {
			perfData.IPC = float64(perfData.Instructions) / float64(perfData.CPUCycles)
		}
		if perfData.CacheReferences > 0 && perfData.CacheMisses > 0 {
			perfData.CacheMissRate = float64(perfData.CacheMisses) / float64(perfData.CacheReferences)
		}
		
		log.WithFields(log.Fields{
			"container":        containerName,
			"cpu_cycles":       perfData.CPUCycles,
			"instructions":     perfData.Instructions,
			"cache_references": perfData.CacheReferences,
			"cache_misses":     perfData.CacheMisses,
			"ipc":              perfData.IPC,
		}).Debug("Final perf data for container")
	}
	
	return result
}

func (pm *ComprehensiveManager) parseRDTMeasurements(measurements []storage.Measurement) map[string]*storage.RDTData {
	result := make(map[string]*storage.RDTData)
	
	for _, measurement := range measurements {
		containerName := measurement.Tags["container_name"]
		if containerName == "" {
			continue
		}

		rdtData := &storage.RDTData{}
		
		// Parse RDT fields from measurement
		if val, ok := measurement.Fields["llc_occupancy"].(uint64); ok {
			rdtData.LLCOccupancy = val
		}
		if val, ok := measurement.Fields["local_mem_bw"].(float64); ok {
			rdtData.LocalMemBW = val
		}
		if val, ok := measurement.Fields["remote_mem_bw"].(float64); ok {
			rdtData.RemoteMemBW = val
		}
		if val, ok := measurement.Fields["total_mem_bw"].(float64); ok {
			rdtData.TotalMemBW = val
		}

		result[containerName] = rdtData
	}
	
	return result
}

func (pm *ComprehensiveManager) SetEndTime(endTime time.Time) {
	pm.endTime = endTime
}

// Stop stops the profiling
func (pm *ComprehensiveManager) Stop() error {
	log.Info("Stopping profiler")
	close(pm.stopChan)
	pm.wg.Wait()

	// Stop metadata provider
	if err := pm.metadataProvider.Stop(); err != nil {
		log.WithError(err).Warn("Failed to stop metadata provider")
	}

	var errors []error
	for _, collector := range pm.collectors {
		if err := collector.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close collector %s: %w", collector.Name(), err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("encountered %d errors while stopping collectors", len(errors))
	}
	return nil
}
