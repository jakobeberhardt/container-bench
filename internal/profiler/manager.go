package profiler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jakobeberhardt/container-bench/internal/config"
	"github.com/jakobeberhardt/container-bench/internal/storage"
	log "github.com/sirupsen/logrus"
)

// Manager coordinates all profiling activities
type Manager struct {
	config           *config.DataConfig
	benchmarkID      string
	benchmarkIDNum   int64
	storage          *storage.Manager
	collectors       []Collector
	ticker           *time.Ticker
	stopChan         chan struct{}
	wg               sync.WaitGroup
	containerConfigs map[string]*config.ContainerConfig // Add container configs
	schedulerType    string                              // Add scheduler type
	metadataProvider *MetadataProvider                   // Centralized metadata provider
}

// ProfilerManager defines the interface that all profiler managers must implement
type ProfilerManager interface {
	Initialize(ctx context.Context, containerIDs map[string]string) error
	StartProfiling(ctx context.Context, containerIDs map[string]string) error
	Stop() error
}

// DataFrameProfilerManager interface extends ProfilerManager with DataFrame-specific methods
type DataFrameProfilerManagerInterface interface {
	ProfilerManager
	GetDataFrames() *storage.BenchmarkDataFrames
	WriteDataFramesToDatabase(ctx context.Context) error
	LogDataFramesSummary()
}

// Collector defines the interface for different profiling collectors
type Collector interface {
	Initialize(ctx context.Context) error
	Collect(ctx context.Context, timestamp time.Time) ([]storage.Measurement, error)
	Close() error
	Name() string
}

// NewManager creates a new profiler manager
func NewManager(config *config.DataConfig, benchmarkID string, benchmarkIDNum int64, storage *storage.Manager) (*Manager, error) {
	// Create metadata provider
	metadataProvider := NewMetadataProvider(benchmarkID, config.ProfileFrequency)
	
	// Set enabled profilers
	enabledProfilers := map[string]bool{
		"docker_stats": config.DockerStats,
		"perf":         config.Perf,
		"rdt":          config.RDT,
	}
	metadataProvider.SetEnabledProfilers(enabledProfilers)

	pm := &Manager{
		config:           config,
		benchmarkID:      benchmarkID,
		benchmarkIDNum:   benchmarkIDNum,
		storage:          storage,
		collectors:       make([]Collector, 0),
		stopChan:         make(chan struct{}),
		metadataProvider: metadataProvider,
	}

	// Initialize collectors based on configuration
	var dockerCollector *DockerStatsCollector
	var perfCollector *PerfCollector
	var rdtCollector *RDTCollector
	
	if config.DockerStats {
		dockerCollector = NewDockerStatsCollector(metadataProvider)
		pm.collectors = append(pm.collectors, dockerCollector)
	}

	if config.Perf {
		perfCollector = NewPerfCollector(metadataProvider)
		pm.collectors = append(pm.collectors, perfCollector)
	}

	if config.RDT {
		rdtCollector = NewRDTCollector(benchmarkID) // RDT collector not yet refactored
		pm.collectors = append(pm.collectors, rdtCollector)
	}

	// Add a comprehensive collector that aggregates data from all sources
	// This ensures benchmark metrics are written with proper data
	if config.DockerStats || config.Perf || config.RDT {
		comprehensiveCollector := NewDataAggregatorCollector(
			benchmarkID, 
			benchmarkIDNum, 
			storage, 
			dockerCollector, 
			perfCollector, 
			rdtCollector,
		)
		pm.collectors = append(pm.collectors, comprehensiveCollector)
	}

	if len(pm.collectors) == 0 {
		return nil, fmt.Errorf("no profiling collectors configured")
	}

	log.WithField("collectors", len(pm.collectors)).Info("Profiler manager initialized")
	return pm, nil
}

// NewDataFrameManager creates a new DataFrame-based profiler manager
func NewDataFrameManager(
	config *config.DataConfig,
	benchmarkID string,
	benchmarkIDNum int64,
	startTime time.Time,
	storageMgr *storage.Manager,
	containerConfigs map[string]*config.ContainerConfig,
	schedulerType string,
) (*DataFrameProfilerManager, error) {
	return NewDataFrameProfilerManager(
		config,
		benchmarkID,
		benchmarkIDNum,
		startTime,
		storageMgr,
		containerConfigs,
		schedulerType,
	)
}

// Initialize initializes all collectors
func (pm *Manager) Initialize(ctx context.Context, containerIDs map[string]string) error {
	log.Info("Initializing profiler collectors")

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
		
		// Set container IDs and sampling rate for perf collector
		if perfCollector, ok := collector.(*PerfCollector); ok {
			perfCollector.SetContainerIDs(containerIDs)
			perfCollector.SetSamplingRate(pm.config.ProfileFrequency)
		}

		log.WithField("collector", collector.Name()).Info("Collector initialized")
	}

	return nil
}

// StartProfiling starts the profiling loop
func (pm *Manager) StartProfiling(ctx context.Context, containerIDs map[string]string) error {
	log.WithField("frequency_ms", pm.config.ProfileFrequency).Info("Starting profiling")

	duration := time.Duration(pm.config.ProfileFrequency) * time.Millisecond
	pm.ticker = time.NewTicker(duration)
	defer pm.ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("Profiling stopped due to context cancellation")
			return ctx.Err()
		case <-pm.stopChan:
			log.Info("Profiling stopped")
			return nil
		case timestamp := <-pm.ticker.C:
			pm.wg.Add(1)
			go pm.collectMetrics(ctx, timestamp)
		}
	}
}

// collectMetrics collects metrics from all collectors
func (pm *Manager) collectMetrics(ctx context.Context, timestamp time.Time) {
	defer pm.wg.Done()

	var allMeasurements []storage.Measurement

	for _, collector := range pm.collectors {
		measurements, err := collector.Collect(ctx, timestamp)
		if err != nil {
			log.WithError(err).WithField("collector", collector.Name()).Error("Failed to collect metrics")
			continue
		}
		allMeasurements = append(allMeasurements, measurements...)
	}

	if len(allMeasurements) > 0 {
		if err := pm.storage.WriteMeasurements(ctx, allMeasurements); err != nil {
			log.WithError(err).Error("Failed to write measurements to storage")
		}
	}
}

// Stop stops the profiling
func (pm *Manager) Stop() error {
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
		return fmt.Errorf("encountered %d errors while stopping profiler", len(errors))
	}

	return nil
}

// DataAggregatorCollector collects data from all sources and writes comprehensive benchmark metrics
type DataAggregatorCollector struct {
	benchmarkID     string
	benchmarkIDNum  int64
	storage         *storage.Manager
	startTime       time.Time
	samplingStep    int64
	dockerCollector *DockerStatsCollector
	perfCollector   *PerfCollector
	rdtCollector    *RDTCollector
}

// NewDataAggregatorCollector creates a new data aggregator collector
func NewDataAggregatorCollector(
	benchmarkID string, 
	benchmarkIDNum int64, 
	storage *storage.Manager,
	dockerCollector *DockerStatsCollector,
	perfCollector *PerfCollector,
	rdtCollector *RDTCollector,
) *DataAggregatorCollector {
	return &DataAggregatorCollector{
		benchmarkID:     benchmarkID,
		benchmarkIDNum:  benchmarkIDNum,
		storage:         storage,
		dockerCollector: dockerCollector,
		perfCollector:   perfCollector,
		rdtCollector:    rdtCollector,
	}
}

// Initialize initializes the data aggregator collector
func (dac *DataAggregatorCollector) Initialize(ctx context.Context) error {
	dac.startTime = time.Now()
	dac.samplingStep = 0
	return nil
}

// Collect aggregates data from all sources and writes comprehensive benchmark metrics
func (dac *DataAggregatorCollector) Collect(ctx context.Context, timestamp time.Time) ([]storage.Measurement, error) {
	dac.samplingStep++
	
	// Collect data from all available sources
	var dockerMeasurements []storage.Measurement
	var perfMeasurements []storage.Measurement
	var rdtMeasurements []storage.Measurement
	
	if dac.dockerCollector != nil {
		dockerMeasurements, _ = dac.dockerCollector.Collect(ctx, timestamp)
	}
	
	if dac.perfCollector != nil {
		perfMeasurements, _ = dac.perfCollector.Collect(ctx, timestamp)
	}
	
	if dac.rdtCollector != nil {
		rdtMeasurements, _ = dac.rdtCollector.Collect(ctx, timestamp)
	}
	
	// Aggregate data by container and write comprehensive metrics
	containerData := dac.aggregateDataByContainer(dockerMeasurements, perfMeasurements, rdtMeasurements)
	
	// Write comprehensive metrics for each container
	for containerInfo, data := range containerData {
		relativeTime := timestamp.Sub(dac.startTime).Milliseconds()
		
		metrics := &storage.BenchmarkMetrics{
			// Benchmark metadata
			BenchmarkID:       dac.benchmarkIDNum,
			BenchmarkStarted:  dac.startTime,
			SamplingFrequency: 500, // TODO: Get this from config
			UsedScheduler:     "default", // TODO: Get this from scheduler
			
			// Container metadata
			ContainerName:  containerInfo,
			ContainerIndex: 0, // TODO: Get from container config
			ContainerImage: "unknown", // TODO: Get from container config  
			ContainerCore:  0, // TODO: Get from container config
			
			// Timing information
			UTCTimestamp:  timestamp,
			RelativeTime:  relativeTime,
			SamplingStep:  dac.samplingStep,
			CPUExecutedOn: dac.getCurrentCPU(),
			
			// Performance data
			DockerMetrics: data.DockerData,
			PerfMetrics:   data.PerfData,
			RDTMetrics:    data.RDTData,
		}
		
		// Write comprehensive metrics to storage
		if err := dac.storage.WriteBenchmarkMetrics(ctx, metrics); err != nil {
			log.WithError(err).WithField("container", containerInfo).Error("Failed to write comprehensive metrics")
		}
	}
	
	// Return empty measurements since the writing is handled above
	return []storage.Measurement{}, nil
}

// ContainerData holds aggregated data for a container
type ContainerData struct {
	DockerData *storage.DockerData
	PerfData   *storage.PerfData
	RDTData    *storage.RDTData
}

// aggregateDataByContainer organizes measurements by container
func (dac *DataAggregatorCollector) aggregateDataByContainer(
	dockerMeasurements, perfMeasurements, rdtMeasurements []storage.Measurement,
) map[string]*ContainerData {
	containerData := make(map[string]*ContainerData)
	
	// Process Docker measurements
	for _, measurement := range dockerMeasurements {
		containerName := dac.extractContainerName(measurement)
		if containerName == "" {
			continue
		}
		
		if _, exists := containerData[containerName]; !exists {
			containerData[containerName] = &ContainerData{}
		}
		
		// Convert measurement to DockerData
		dockerData := dac.convertToDockerData(measurement)
		containerData[containerName].DockerData = dockerData
	}
	
	// Process Perf measurements - aggregate multiple metrics per container
	for _, measurement := range perfMeasurements {
		containerName := dac.extractContainerName(measurement)
		if containerName == "" {
			continue
		}
		
		if _, exists := containerData[containerName]; !exists {
			containerData[containerName] = &ContainerData{}
		}
		
		if containerData[containerName].PerfData == nil {
			containerData[containerName].PerfData = &storage.PerfData{}
		}
		
		// Aggregate perf metrics by metric name from tags
		if metricName, ok := measurement.Tags["metric"]; ok {
			if value, ok := measurement.Fields["value"]; ok {
				dac.setPerfMetric(containerData[containerName].PerfData, metricName, value)
			}
		}
	}
	
	// Process RDT measurements
	for _, measurement := range rdtMeasurements {
		containerName := dac.extractContainerName(measurement)
		if containerName == "" {
			continue
		}
		
		if _, exists := containerData[containerName]; !exists {
			containerData[containerName] = &ContainerData{}
		}
		
		// Convert measurement to RDTData
		rdtData := dac.convertToRDTData(measurement)
		containerData[containerName].RDTData = rdtData
	}
	
	return containerData
}

// Helper methods to extract and convert data
func (dac *DataAggregatorCollector) extractContainerName(measurement storage.Measurement) string {
	if containerName, exists := measurement.Tags["container"]; exists {
		return containerName
	}
	if containerName, exists := measurement.Tags["container_name"]; exists {
		return containerName
	}
	return ""
}

func (dac *DataAggregatorCollector) convertToDockerData(measurement storage.Measurement) *storage.DockerData {
	dockerData := &storage.DockerData{}
	
	// Extract CPU metrics
	if val, ok := measurement.Fields["cpu_usage_percent"]; ok {
		if f, ok := val.(float64); ok {
			dockerData.CPUUsagePercent = f
		}
	}
	
	// Extract memory metrics
	if val, ok := measurement.Fields["memory_usage"]; ok {
		if u, ok := val.(uint64); ok {
			dockerData.MemoryUsage = u
		}
	}
	
	// Add more field extractions as needed
	return dockerData
}

func (dac *DataAggregatorCollector) convertToPerfData(measurement storage.Measurement) *storage.PerfData {
	perfData := &storage.PerfData{}
	
	// This function is now mostly unused since we aggregate metrics individually
	// Keep it for backward compatibility
	return perfData
}

// setPerfMetric sets a specific perf metric in PerfData based on metric name and value
func (dac *DataAggregatorCollector) setPerfMetric(perfData *storage.PerfData, metricName string, value interface{}) {
	switch metricName {
	case "cpu_cycles":
		if u, ok := value.(uint64); ok {
			perfData.CPUCycles = u
		}
	case "instructions":
		if u, ok := value.(uint64); ok {
			perfData.Instructions = u
		}
	case "cache_references":
		if u, ok := value.(uint64); ok {
			perfData.CacheReferences = u
		}
	case "cache_misses":
		if u, ok := value.(uint64); ok {
			perfData.CacheMisses = u
		}
	case "branch_instructions":
		if u, ok := value.(uint64); ok {
			perfData.BranchInstructions = u
		}
	case "branch_misses":
		if u, ok := value.(uint64); ok {
			perfData.BranchMisses = u
		}
	case "bus_cycles":
		if u, ok := value.(uint64); ok {
			perfData.BusCycles = u
		}
	case "ref_cycles":
		if u, ok := value.(uint64); ok {
			perfData.RefCycles = u
		}
	case "stalled_cycles_frontend":
		if u, ok := value.(uint64); ok {
			perfData.StalledCyclesFrontend = u
		}
	case "l1_dcache_loads":
		if u, ok := value.(uint64); ok {
			perfData.L1DCacheLoads = u
		}
	case "l1_dcache_load_misses":
		if u, ok := value.(uint64); ok {
			perfData.L1DCacheLoadMisses = u
		}
	case "l1_dcache_stores":
		if u, ok := value.(uint64); ok {
			perfData.L1DCacheStores = u
		}
	case "l1_dcache_store_misses":
		if u, ok := value.(uint64); ok {
			perfData.L1DCacheStoreMisses = u
		}
	case "l1_icache_load_misses":
		if u, ok := value.(uint64); ok {
			perfData.L1ICacheLoadMisses = u
		}
	case "llc_loads":
		if u, ok := value.(uint64); ok {
			perfData.LLCLoads = u
		}
	case "llc_load_misses":
		if u, ok := value.(uint64); ok {
			perfData.LLCLoadMisses = u
		}
	case "llc_stores":
		if u, ok := value.(uint64); ok {
			perfData.LLCStores = u
		}
	case "llc_store_misses":
		if u, ok := value.(uint64); ok {
			perfData.LLCStoreMisses = u
		}
	case "ipc":
		if f, ok := value.(float64); ok {
			perfData.IPC = f
		}
	case "cache_miss_rate":
		if f, ok := value.(float64); ok {
			perfData.CacheMissRate = f
		}
	// Legacy metrics
	case "page_faults":
		if u, ok := value.(uint64); ok {
			perfData.PageFaults = u
		}
	case "context_switches":
		if u, ok := value.(uint64); ok {
			perfData.ContextSwitches = u
		}
	case "cpu_migrations":
		if u, ok := value.(uint64); ok {
			perfData.CPUMigrations = u
		}
	}
}

func (dac *DataAggregatorCollector) convertToRDTData(measurement storage.Measurement) *storage.RDTData {
	rdtData := &storage.RDTData{}
	
	// Extract RDT metrics
	if val, ok := measurement.Fields["llc_occupancy"]; ok {
		if u, ok := val.(uint64); ok {
			rdtData.LLCOccupancy = u
		}
	}
	
	// Add more field extractions as needed
	return rdtData
}

func (dac *DataAggregatorCollector) getCurrentCPU() int {
	// TODO: Implement actual CPU detection
	return 0
}

// Close closes the data aggregator collector
func (dac *DataAggregatorCollector) Close() error {
	// No cleanup needed
	return nil
}

// Name returns the name of this collector
func (dac *DataAggregatorCollector) Name() string {
	return "data_aggregator"
}
