package storage

import (
	"context"
	"fmt"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

// BenchmarkMetadata represents the complete metadata for a benchmark run
type BenchmarkMetadata struct {
	// Core benchmark identification
	BenchmarkID       int64     `json:"benchmark_id"`
	BenchmarkName     string    `json:"benchmark_name"`
	BenchmarkStarted  time.Time `json:"benchmark_started"`
	BenchmarkFinished time.Time `json:"benchmark_finished,omitempty"`
	
	// Execution environment
	ExecutionHost     string    `json:"execution_host"`
	CPUExecutedOn     int       `json:"cpu_executed_on"`
	TotalCPUCores     int       `json:"total_cpu_cores"`
	OSInfo           string    `json:"os_info"`
	KernelVersion    string    `json:"kernel_version"`
	
	// System information
	DriverVersion     string    `json:"driver_version"`
	BuildDate         string    `json:"build_date"`
	CPUModel          string    `json:"cpu_model"`
	CPUVendor         string    `json:"cpu_vendor"`
	CPUThreads        int       `json:"cpu_threads"`
	Architecture      string    `json:"architecture"`
	Hostname          string    `json:"hostname"`
	Description       string    `json:"description"`
	SchedulerVersion  string    `json:"scheduler_version"`
	
	// Configuration
	ConfigFile        string    `json:"config_file"`        // Original YAML content (with secrets unexpanded)
	ConfigFilePath    string    `json:"config_file_path"`   // Path to config file
	UsedScheduler     string    `json:"used_scheduler"`
	SamplingFrequency int       `json:"sampling_frequency_ms"`
	MaxDuration       int       `json:"max_duration_seconds"`
	
	// Data collection settings
	RDTEnabled        bool      `json:"rdt_enabled"`
	PerfEnabled       bool      `json:"perf_enabled"`
	DockerStatsEnabled bool     `json:"docker_stats_enabled"`
	
	// Container information
	TotalContainers   int                      `json:"total_containers"`
	ContainerImages   map[string]int          `json:"container_images"`     // image -> count
	ContainerDetails  map[string]ContainerMeta `json:"container_details"`   // name -> details
	
	// Results summary
	TotalSamplingSteps int64     `json:"total_sampling_steps"`
	TotalMeasurements  int64     `json:"total_measurements"`
	TotalDataSize      int64     `json:"total_data_size_bytes"`
	
	// Database information
	DatabaseHost       string    `json:"database_host"`
	DatabaseName       string    `json:"database_name"`
	DatabaseUser       string    `json:"database_user"`
}

// ContainerMeta represents metadata for a single container
type ContainerMeta struct {
	Index     int               `json:"index"`
	Image     string            `json:"image"`
	StartTime int               `json:"start_time"`
	StopTime  int               `json:"stop_time"`
	CorePin   int               `json:"core_pin"`
	EnvVars   map[string]string `json:"env_vars,omitempty"`
	Command   string            `json:"command,omitempty"`
}

// BenchmarkMetrics represents a comprehensive data point for a container at a specific time
type BenchmarkMetrics struct {
	// Benchmark-level metadata
	BenchmarkID       int64     `json:"benchmark_id"`
	BenchmarkStarted  time.Time `json:"benchmark_started"`
	BenchmarkFinished time.Time `json:"benchmark_finished,omitempty"`
	SamplingFrequency int       `json:"sampling_frequency_ms"`
	UsedScheduler     string    `json:"used_scheduler"`

	// Container-level metadata
	ContainerName  string `json:"container_name"`
	ContainerIndex int    `json:"container_index"`
	ContainerImage string `json:"container_image"`
	ContainerCore  int    `json:"container_core"`

	// Timing information
	UTCTimestamp     time.Time `json:"utc_timestamp"`
	RelativeTime     int64     `json:"relative_time_ms"` // milliseconds since benchmark start
	SamplingStep     int64     `json:"sampling_step"`
	CPUExecutedOn    int       `json:"cpu_executed_on"`

	// Performance data from different sources
	PerfMetrics   *PerfData   `json:"perf_metrics,omitempty"`
	DockerMetrics *DockerData `json:"docker_metrics,omitempty"`
	RDTMetrics    *RDTData    `json:"rdt_metrics,omitempty"`
}

// PerfData represents performance counter data
type PerfData struct {
	// Basic hardware counters
	CPUCycles               uint64  `json:"cpu_cycles,omitempty"`
	Instructions            uint64  `json:"instructions,omitempty"`
	CacheReferences         uint64  `json:"cache_references,omitempty"`
	CacheMisses             uint64  `json:"cache_misses,omitempty"`
	BranchInstructions      uint64  `json:"branch_instructions,omitempty"`
	BranchMisses            uint64  `json:"branch_misses,omitempty"`
	BusCycles               uint64  `json:"bus_cycles,omitempty"`
	RefCycles               uint64  `json:"ref_cycles,omitempty"`
	StalledCyclesFrontend   uint64  `json:"stalled_cycles_frontend,omitempty"`
	
	// Cache events
	L1DCacheLoads           uint64  `json:"l1_dcache_loads,omitempty"`
	L1DCacheLoadMisses      uint64  `json:"l1_dcache_load_misses,omitempty"`
	L1DCacheStores          uint64  `json:"l1_dcache_stores,omitempty"`
	L1DCacheStoreMisses     uint64  `json:"l1_dcache_store_misses,omitempty"`
	L1ICacheLoadMisses      uint64  `json:"l1_icache_load_misses,omitempty"`
	LLCLoads                uint64  `json:"llc_loads,omitempty"`
	LLCLoadMisses           uint64  `json:"llc_load_misses,omitempty"`
	LLCStores               uint64  `json:"llc_stores,omitempty"`
	LLCStoreMisses          uint64  `json:"llc_store_misses,omitempty"`
	
	// Calculated metrics
	IPC                     float64 `json:"ipc,omitempty"`           // Instructions per cycle
	CacheMissRate           float64 `json:"cache_miss_rate,omitempty"`
	
	// Legacy fields (for backward compatibility)
	PageFaults              uint64  `json:"page_faults,omitempty"`
	ContextSwitches         uint64  `json:"context_switches,omitempty"`
	CPUMigrations           uint64  `json:"cpu_migrations,omitempty"`
}

// DockerData represents Docker container statistics
type DockerData struct {
	// CPU metrics
	CPUUsagePercent    float64 `json:"cpu_usage_percent"`
	CPUUsageTotal      uint64  `json:"cpu_usage_total"`
	CPUUsageKernel     uint64  `json:"cpu_usage_kernel"`
	CPUUsageUser       uint64  `json:"cpu_usage_user"`
	CPUThrottling      uint64  `json:"cpu_throttling,omitempty"`
	
	// Memory metrics
	MemoryUsage        uint64  `json:"memory_usage"`
	MemoryLimit        uint64  `json:"memory_limit"`
	MemoryUsagePercent float64 `json:"memory_usage_percent"`
	MemoryCache        uint64  `json:"memory_cache,omitempty"`
	MemoryRSS          uint64  `json:"memory_rss,omitempty"`
	MemorySwap         uint64  `json:"memory_swap,omitempty"`
	
	// Network metrics
	NetworkRxBytes   uint64 `json:"network_rx_bytes,omitempty"`
	NetworkTxBytes   uint64 `json:"network_tx_bytes,omitempty"`
	NetworkRxPackets uint64 `json:"network_rx_packets,omitempty"`
	NetworkTxPackets uint64 `json:"network_tx_packets,omitempty"`
	
	// Disk I/O metrics
	DiskReadBytes  uint64 `json:"disk_read_bytes,omitempty"`
	DiskWriteBytes uint64 `json:"disk_write_bytes,omitempty"`
	DiskReadOps    uint64 `json:"disk_read_ops,omitempty"`
	DiskWriteOps   uint64 `json:"disk_write_ops,omitempty"`
}

// RDTData represents Intel RDT (Resource Director Technology) metrics
type RDTData struct {
	// Cache monitoring
	LLCOccupancy     uint64  `json:"llc_occupancy,omitempty"`      // Last Level Cache occupancy in bytes
	LocalMemBW       float64 `json:"local_mem_bw,omitempty"`       // Local memory bandwidth in MB/s
	RemoteMemBW      float64 `json:"remote_mem_bw,omitempty"`      // Remote memory bandwidth in MB/s
	TotalMemBW       float64 `json:"total_mem_bw,omitempty"`       // Total memory bandwidth in MB/s
	
	// Cache allocation technology (CAT)
	CacheAllocation  string  `json:"cache_allocation,omitempty"`   // Cache allocation bitmask
	
	// Memory bandwidth allocation (MBA)
	MBAllocation     int     `json:"mb_allocation,omitempty"`      // Memory bandwidth allocation percentage
	
	// Additional RDT metrics
	IPCRate          float64 `json:"ipc_rate,omitempty"`          // Instructions per cycle from RDT
	MissesPerKI      float64 `json:"misses_per_ki,omitempty"`     // Cache misses per thousand instructions
}

// SchedulingData interface for real-time scheduling decisions
type SchedulingData interface {
	GetContainerName() string
	GetCPUUsage() float64
	GetMemoryUsage() uint64
	GetCacheOccupancy() uint64
	GetMemoryBandwidth() float64
	GetTimestamp() time.Time
	GetRelativeTime() int64
}

// Implement SchedulingData interface for BenchmarkMetrics
func (b *BenchmarkMetrics) GetContainerName() string {
	return b.ContainerName
}

func (b *BenchmarkMetrics) GetCPUUsage() float64 {
	if b.DockerMetrics != nil {
		return b.DockerMetrics.CPUUsagePercent
	}
	return 0.0
}

func (b *BenchmarkMetrics) GetMemoryUsage() uint64 {
	if b.DockerMetrics != nil {
		return b.DockerMetrics.MemoryUsage
	}
	return 0
}

func (b *BenchmarkMetrics) GetCacheOccupancy() uint64 {
	if b.RDTMetrics != nil {
		return b.RDTMetrics.LLCOccupancy
	}
	return 0
}

func (b *BenchmarkMetrics) GetMemoryBandwidth() float64 {
	if b.RDTMetrics != nil {
		return b.RDTMetrics.TotalMemBW
	}
	return 0.0
}

func (b *BenchmarkMetrics) GetTimestamp() time.Time {
	return b.UTCTimestamp
}

func (b *BenchmarkMetrics) GetRelativeTime() int64 {
	return b.RelativeTime
}

// GetNextBenchmarkID queries the database to get the next available benchmark ID
func (sm *Manager) GetNextBenchmarkID(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: -365d)
		|> filter(fn: (r) => r._measurement == "benchmark_metrics")
		|> filter(fn: (r) => r._field == "benchmark_id")
		|> max()
		|> last()
		|> yield()
	`, sm.config.Name)

	results, err := sm.Query(ctx, query)
	if err != nil {
		// If query fails, start from 1
		log.WithError(err).Warn("Failed to query max benchmark ID, starting from 1")
		log.WithField("next_benchmark_id", 1).Info("Using benchmark ID")
		return 1, nil
	}

	log.WithField("query_results_count", len(results)).Debug("Queried benchmark ID records from database")

	if len(results) == 0 {
		// No previous benchmarks found, start from 1
		log.Info("No previous benchmarks found in database")
		log.WithField("next_benchmark_id", 1).Info("Using benchmark ID")
		return 1, nil
	}

	// Find the maximum benchmark ID from all results
	var maxID int64 = 0
	var foundValidID bool = false
	
	for _, result := range results {
		if value, ok := result["_value"]; ok {
			var currentID int64
			var parsed bool
			
			if maxIDInt64, ok := value.(int64); ok {
				currentID = maxIDInt64
				parsed = true
			} else if maxIDFloat, ok := value.(float64); ok {
				currentID = int64(maxIDFloat)
				parsed = true
			} else if maxIDStr, ok := value.(string); ok {
				if parsedID, err := strconv.ParseInt(maxIDStr, 10, 64); err == nil {
					currentID = parsedID
					parsed = true
				}
			}
			
			if parsed {
				if currentID > maxID {
					maxID = currentID
				}
				foundValidID = true
			}
		}
	}
	
	if foundValidID {
		nextID := maxID + 1
		log.WithFields(log.Fields{
			"max_benchmark_id":  maxID,
			"next_benchmark_id": nextID,
		}).Info("Queried max benchmark ID from database")
		return nextID, nil
	}

	// Fallback to 1 if parsing fails
	log.Warn("Failed to parse benchmark ID from database result, starting from 1")
	log.WithField("next_benchmark_id", 1).Info("Using benchmark ID")
	return 1, nil
}

func (sm *Manager) WriteBenchmarkMetrics(ctx context.Context, metrics *BenchmarkMetrics) error {
	tags := map[string]string{
		"benchmark_id":     strconv.FormatInt(metrics.BenchmarkID, 10),
		"container_name":   metrics.ContainerName,
		"container_index":  strconv.Itoa(metrics.ContainerIndex),
		"container_image":  metrics.ContainerImage,
		"used_scheduler":   metrics.UsedScheduler,
	}

	fields := map[string]interface{}{
		"benchmark_id":        metrics.BenchmarkID,
		"container_core":      metrics.ContainerCore,
		"sampling_frequency":  metrics.SamplingFrequency,
		"sampling_step":       metrics.SamplingStep,
		"relative_time_ms":    metrics.RelativeTime,
		"cpu_executed_on":     metrics.CPUExecutedOn,
		"benchmark_started":   metrics.BenchmarkStarted.Unix(),
		"utc_timestamp":       metrics.UTCTimestamp.Unix(),
	}

	if !metrics.BenchmarkFinished.IsZero() {
		fields["benchmark_finished"] = metrics.BenchmarkFinished.Unix()
	}

	if metrics.DockerMetrics != nil {
		addDockerFields(fields, metrics.DockerMetrics)
	} else {
		addDockerFields(fields, &DockerData{}) 
	}

	if metrics.PerfMetrics != nil {
		addPerfFields(fields, metrics.PerfMetrics)
	} else {
		addPerfFields(fields, &PerfData{}) 
	}

	if metrics.RDTMetrics != nil {
		addRDTFields(fields, metrics.RDTMetrics)
	} else {
		addRDTFields(fields, &RDTData{}) 
	}

	measurement := Measurement{
		Name:      "benchmark_metrics",
		Tags:      tags,
		Fields:    fields,
		Timestamp: metrics.UTCTimestamp,
	}

	return sm.WriteMeasurement(ctx, measurement)
}

func addDockerFields(fields map[string]interface{}, docker *DockerData) {
	fields["docker_cpu_usage_percent"] = docker.CPUUsagePercent
	fields["docker_cpu_usage_total"] = docker.CPUUsageTotal
	fields["docker_cpu_usage_kernel"] = docker.CPUUsageKernel
	fields["docker_cpu_usage_user"] = docker.CPUUsageUser
	fields["docker_cpu_throttling"] = docker.CPUThrottling
	fields["docker_memory_usage"] = docker.MemoryUsage
	fields["docker_memory_limit"] = docker.MemoryLimit
	fields["docker_memory_usage_percent"] = docker.MemoryUsagePercent
	fields["docker_memory_cache"] = docker.MemoryCache
	fields["docker_memory_rss"] = docker.MemoryRSS
	fields["docker_memory_swap"] = docker.MemorySwap
	fields["docker_network_rx_bytes"] = docker.NetworkRxBytes
	fields["docker_network_tx_bytes"] = docker.NetworkTxBytes
	fields["docker_network_rx_packets"] = docker.NetworkRxPackets
	fields["docker_network_tx_packets"] = docker.NetworkTxPackets
	fields["docker_disk_read_bytes"] = docker.DiskReadBytes
	fields["docker_disk_write_bytes"] = docker.DiskWriteBytes
	fields["docker_disk_read_ops"] = docker.DiskReadOps
	fields["docker_disk_write_ops"] = docker.DiskWriteOps
}

func addPerfFields(fields map[string]interface{}, perf *PerfData) {
	// Basic hardware counters
	fields["perf_cpu_cycles"] = perf.CPUCycles
	fields["perf_instructions"] = perf.Instructions
	fields["perf_cache_references"] = perf.CacheReferences
	fields["perf_cache_misses"] = perf.CacheMisses
	fields["perf_branch_instructions"] = perf.BranchInstructions
	fields["perf_branch_misses"] = perf.BranchMisses
	fields["perf_bus_cycles"] = perf.BusCycles
	fields["perf_ref_cycles"] = perf.RefCycles
	fields["perf_stalled_cycles_frontend"] = perf.StalledCyclesFrontend
	
	// Cache events
	fields["perf_l1_dcache_loads"] = perf.L1DCacheLoads
	fields["perf_l1_dcache_load_misses"] = perf.L1DCacheLoadMisses
	fields["perf_l1_dcache_stores"] = perf.L1DCacheStores
	fields["perf_l1_dcache_store_misses"] = perf.L1DCacheStoreMisses
	fields["perf_l1_icache_load_misses"] = perf.L1ICacheLoadMisses
	fields["perf_llc_loads"] = perf.LLCLoads
	fields["perf_llc_load_misses"] = perf.LLCLoadMisses
	fields["perf_llc_stores"] = perf.LLCStores
	fields["perf_llc_store_misses"] = perf.LLCStoreMisses
	
	// Calculated metrics
	fields["perf_ipc"] = perf.IPC
	fields["perf_cache_miss_rate"] = perf.CacheMissRate
	
	// Legacy fields
	fields["perf_page_faults"] = perf.PageFaults
	fields["perf_context_switches"] = perf.ContextSwitches
	fields["perf_cpu_migrations"] = perf.CPUMigrations
}

func addRDTFields(fields map[string]interface{}, rdt *RDTData) {
	fields["rdt_llc_occupancy"] = rdt.LLCOccupancy
	fields["rdt_local_mem_bw"] = rdt.LocalMemBW
	fields["rdt_remote_mem_bw"] = rdt.RemoteMemBW  
	fields["rdt_total_mem_bw"] = rdt.TotalMemBW
	fields["rdt_cache_allocation"] = rdt.CacheAllocation
	fields["rdt_mb_allocation"] = rdt.MBAllocation
	fields["rdt_ipc_rate"] = rdt.IPCRate
	fields["rdt_misses_per_ki"] = rdt.MissesPerKI
}
