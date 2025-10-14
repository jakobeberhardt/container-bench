package datahandeling

import (
	"time"

	"container-bench/internal/config"
	"container-bench/internal/dataframe"
)

// processed metrics ready for database storage
type BenchmarkMetrics struct {
	ContainerMetrics []ContainerMetrics `json:"container_metrics"`
}

// ContainerMetrics holds all metrics for a single container across all sampling steps
type ContainerMetrics struct {
	ContainerIndex int          `json:"container_index"`
	ContainerName  string       `json:"container_name"`
	ContainerImage string       `json:"container_image"`
	ContainerCore  int          `json:"container_core"`
	Steps          []MetricStep `json:"steps"`
}

// processed metrics for a single sampling step
type MetricStep struct {
	StepNumber   int       `json:"step_number"`
	Timestamp    time.Time `json:"timestamp"`
	RelativeTime int64     `json:"relative_time"` // Time from benchmark start in nanoseconds

	// Basic Hardware Counters
	PerfCacheMisses        *uint64 `json:"perf_cache_misses,omitempty"`
	PerfCacheReferences    *uint64 `json:"perf_cache_references,omitempty"`
	PerfInstructions       *uint64 `json:"perf_instructions,omitempty"`
	PerfCycles             *uint64 `json:"perf_cycles,omitempty"`
	PerfBranchInstructions *uint64 `json:"perf_branch_instructions,omitempty"`
	PerfBranchMisses       *uint64 `json:"perf_branch_misses,omitempty"`
	PerfBusCycles          *uint64 `json:"perf_bus_cycles,omitempty"`

	// L1 Data Cache Events
	PerfL1DCacheLoadMisses *uint64 `json:"perf_l1d_cache_load_misses,omitempty"`
	PerfL1DCacheLoads      *uint64 `json:"perf_l1d_cache_loads,omitempty"`
	PerfL1DCacheStores     *uint64 `json:"perf_l1d_cache_stores,omitempty"`

	// L1 Instruction Cache Events
	PerfL1ICacheLoadMisses *uint64 `json:"perf_l1i_cache_load_misses,omitempty"`

	// Last Level Cache (LLC) Events
	PerfLLCLoadMisses  *uint64 `json:"perf_llc_load_misses,omitempty"`
	PerfLLCLoads       *uint64 `json:"perf_llc_loads,omitempty"`
	PerfLLCStoreMisses *uint64 `json:"perf_llc_store_misses,omitempty"`
	PerfLLCStores      *uint64 `json:"perf_llc_stores,omitempty"`

	// Branch Predictor Events
	PerfBranchLoadMisses *uint64 `json:"perf_branch_load_misses,omitempty"`
	PerfBranchLoads      *uint64 `json:"perf_branch_loads,omitempty"`

	// Data Translation Lookaside Buffer Events
	PerfDTLBLoadMisses  *uint64 `json:"perf_dtlb_load_misses,omitempty"`
	PerfDTLBLoads       *uint64 `json:"perf_dtlb_loads,omitempty"`
	PerfDTLBStoreMisses *uint64 `json:"perf_dtlb_store_misses,omitempty"`
	PerfDTLBStores      *uint64 `json:"perf_dtlb_stores,omitempty"`

	// Instruction Translation Lookaside Buffer Events
	PerfITLBLoadMisses *uint64 `json:"perf_itlb_load_misses,omitempty"`

	// NUMA Node Events
	PerfNodeLoadMisses  *uint64 `json:"perf_node_load_misses,omitempty"`
	PerfNodeLoads       *uint64 `json:"perf_node_loads,omitempty"`
	PerfNodeStoreMisses *uint64 `json:"perf_node_store_misses,omitempty"`
	PerfNodeStores      *uint64 `json:"perf_node_stores,omitempty"`

	// Basic Derived Metrics
	PerfCacheMissRate        *float64 `json:"perf_cache_miss_rate,omitempty"`
	PerfInstructionsPerCycle *float64 `json:"perf_instructions_per_cycle,omitempty"`

	// Advanced Derived Metrics
	PerfL1DCacheMissRate *float64 `json:"perf_l1d_cache_miss_rate,omitempty"`
	PerfLLCMissRate      *float64 `json:"perf_llc_miss_rate,omitempty"`
	PerfDTLBMissRate     *float64 `json:"perf_dtlb_miss_rate,omitempty"`
	PerfBranchMissRate   *float64 `json:"perf_branch_miss_rate,omitempty"`

	DockerCPUUsageTotal      *uint64  `json:"docker_cpu_usage_total,omitempty"`
	DockerCPUUsageKernel     *uint64  `json:"docker_cpu_usage_kernel,omitempty"`
	DockerCPUUsageUser       *uint64  `json:"docker_cpu_usage_user,omitempty"`
	DockerCPUUsagePercent    *float64 `json:"docker_cpu_usage_percent,omitempty"`
	DockerCPUThrottling      *uint64  `json:"docker_cpu_throttling,omitempty"`
	DockerMemoryUsage        *uint64  `json:"docker_memory_usage,omitempty"`
	DockerMemoryLimit        *uint64  `json:"docker_memory_limit,omitempty"`
	DockerMemoryCache        *uint64  `json:"docker_memory_cache,omitempty"`
	DockerMemoryRSS          *uint64  `json:"docker_memory_rss,omitempty"`
	DockerMemorySwap         *uint64  `json:"docker_memory_swap,omitempty"`
	DockerMemoryUsagePercent *float64 `json:"docker_memory_usage_percent,omitempty"`
	DockerNetworkRxBytes     *uint64  `json:"docker_network_rx_bytes,omitempty"`
	DockerNetworkTxBytes     *uint64  `json:"docker_network_tx_bytes,omitempty"`
	DockerDiskReadBytes      *uint64  `json:"docker_disk_read_bytes,omitempty"`
	DockerDiskWriteBytes     *uint64  `json:"docker_disk_write_bytes,omitempty"`

	RDTClassName                   *string  `json:"rdt_class_name,omitempty"`
	RDTMonGroupName                *string  `json:"rdt_mon_group_name,omitempty"`
	RDTL3CacheOccupancy            *uint64  `json:"rdt_l3_cache_occupancy,omitempty"`
	RDTMemoryBandwidthTotal        *uint64  `json:"rdt_memory_bandwidth_total,omitempty"`
	RDTMemoryBandwidthLocal        *uint64  `json:"rdt_memory_bandwidth_local,omitempty"`
	RDTL3CacheAllocation           *uint64  `json:"rdt_l3_cache_allocation,omitempty"`
	RDTL3CacheAllocationPct        *float64 `json:"rdt_l3_cache_allocation_pct,omitempty"`
	RDTMBAThrottle                 *uint64  `json:"rdt_mba_throttle,omitempty"`
	RDTCacheLLCUtilizationPercent  *float64 `json:"rdt_cache_llc_utilization_percent,omitempty"`
	RDTBandwidthUtilizationPercent *float64 `json:"rdt_bandwidth_utilization_percent,omitempty"`
}

// process raw dataframes into structured benchmark metrics
type DataHandler interface {
	ProcessDataFrames(benchmarkID int, benchmarkConfig *config.BenchmarkConfig, dataframes *dataframe.DataFrames, startTime, endTime time.Time) (*BenchmarkMetrics, error)
}

type DefaultDataHandler struct{}

func NewDefaultDataHandler() *DefaultDataHandler {
	return &DefaultDataHandler{}
}

// ProcessDataFrames converts raw dataframes to processed benchmark metrics
// It uses a two-pass approach to ensure relative time starts from actual profiling start:
// 1. First pass: Find the earliest timestamp across all containers (actual profiling start)
// 2. Second pass: Calculate relative times using this reference point
func (h *DefaultDataHandler) ProcessDataFrames(benchmarkID int, benchmarkConfig *config.BenchmarkConfig, dataframes *dataframe.DataFrames, startTime, endTime time.Time) (*BenchmarkMetrics, error) {
	var profilingStartTime time.Time
	var hasData bool

	containers := dataframes.GetAllContainers()
	for _, containerDF := range containers {
		stepMap := containerDF.GetAllSteps()
		for _, step := range stepMap {
			if step == nil {
				continue
			}
			if !hasData || step.Timestamp.Before(profilingStartTime) {
				profilingStartTime = step.Timestamp
				hasData = true
			}
		}
	}

	// If no data found, fallback to benchmark start time
	if !hasData {
		profilingStartTime = startTime
	}

	// Second pass: process all container data using the actual profiling start time
	var containerMetrics []ContainerMetrics

	for containerIndex, containerDF := range containers {
		containerConfig := h.getContainerConfig(benchmarkConfig, containerIndex)
		if containerConfig == nil {
			continue
		}

		// Get the effective container name
		containerName := containerConfig.GetContainerName(benchmarkID)

		var steps []MetricStep
		stepMap := containerDF.GetAllSteps()
		for stepNumber, step := range stepMap {
			if step == nil {
				continue
			}

			// Calculate relative time from actual profiling start (first data point)
			relativeTime := step.Timestamp.Sub(profilingStartTime).Nanoseconds()

			// Create metric step with original data
			metricStep := MetricStep{
				StepNumber:   stepNumber,
				Timestamp:    step.Timestamp,
				RelativeTime: relativeTime,
			}

			if step.Perf != nil {
				h.processPerfMetrics(step.Perf, &metricStep)
			}

			if step.Docker != nil {
				h.processDockerMetrics(step.Docker, &metricStep)
			}

			if step.RDT != nil {
				h.processRDTMetrics(step.RDT, &metricStep)
			}

			steps = append(steps, metricStep)
		}

		containerMetrics = append(containerMetrics, ContainerMetrics{
			ContainerIndex: containerIndex,
			ContainerName:  containerName,
			ContainerImage: containerConfig.Image,
			ContainerCore:  containerConfig.Core,
			Steps:          steps,
		})
	}

	return &BenchmarkMetrics{
		ContainerMetrics: containerMetrics,
	}, nil
}

// copy perf metrics and calculates derived values
func (h *DefaultDataHandler) processPerfMetrics(perf *dataframe.PerfMetrics, step *MetricStep) {
	// Basic Hardware Counters
	step.PerfCacheMisses = perf.CacheMisses
	step.PerfCacheReferences = perf.CacheReferences
	step.PerfInstructions = perf.Instructions
	step.PerfCycles = perf.Cycles
	step.PerfBranchInstructions = perf.BranchInstructions
	step.PerfBranchMisses = perf.BranchMisses
	step.PerfBusCycles = perf.BusCycles

	// L1 Data Cache Events
	step.PerfL1DCacheLoadMisses = perf.L1DCacheLoadMisses
	step.PerfL1DCacheLoads = perf.L1DCacheLoads
	step.PerfL1DCacheStores = perf.L1DCacheStores

	// L1 Instruction Cache Events
	step.PerfL1ICacheLoadMisses = perf.L1ICacheLoadMisses

	// Last Level Cache (LLC) Events
	step.PerfLLCLoadMisses = perf.LLCLoadMisses
	step.PerfLLCLoads = perf.LLCLoads
	step.PerfLLCStoreMisses = perf.LLCStoreMisses
	step.PerfLLCStores = perf.LLCStores

	// Branch Predictor Events
	step.PerfBranchLoadMisses = perf.BranchLoadMisses
	step.PerfBranchLoads = perf.BranchLoads

	// Data Translation Lookaside Buffer Events
	step.PerfDTLBLoadMisses = perf.DTLBLoadMisses
	step.PerfDTLBLoads = perf.DTLBLoads
	step.PerfDTLBStoreMisses = perf.DTLBStoreMisses
	step.PerfDTLBStores = perf.DTLBStores

	// Instruction Translation Lookaside Buffer Events
	step.PerfITLBLoadMisses = perf.ITLBLoadMisses

	// NUMA Node Events
	step.PerfNodeLoadMisses = perf.NodeLoadMisses
	step.PerfNodeLoads = perf.NodeLoads
	step.PerfNodeStoreMisses = perf.NodeStoreMisses
	step.PerfNodeStores = perf.NodeStores

	// Copy derived metrics from dataframe
	step.PerfCacheMissRate = perf.CacheMissRate
	step.PerfInstructionsPerCycle = perf.InstructionsPerCycle
	step.PerfL1DCacheMissRate = perf.L1DCacheMissRate
	step.PerfLLCMissRate = perf.LLCMissRate
	step.PerfDTLBMissRate = perf.DTLBMissRate
	step.PerfBranchMissRate = perf.BranchMissRate
}

func (h *DefaultDataHandler) processDockerMetrics(docker *dataframe.DockerMetrics, step *MetricStep) {
	step.DockerCPUUsageTotal = docker.CPUUsageTotal
	step.DockerCPUUsageKernel = docker.CPUUsageKernel
	step.DockerCPUUsageUser = docker.CPUUsageUser
	step.DockerCPUUsagePercent = docker.CPUUsagePercent
	step.DockerCPUThrottling = docker.CPUThrottling
	step.DockerMemoryUsage = docker.MemoryUsage
	step.DockerMemoryLimit = docker.MemoryLimit
	step.DockerMemoryCache = docker.MemoryCache
	step.DockerMemoryRSS = docker.MemoryRSS
	step.DockerMemorySwap = docker.MemorySwap
	step.DockerMemoryUsagePercent = docker.MemoryUsagePercent
	step.DockerNetworkRxBytes = docker.NetworkRxBytes
	step.DockerNetworkTxBytes = docker.NetworkTxBytes
	step.DockerDiskReadBytes = docker.DiskReadBytes
	step.DockerDiskWriteBytes = docker.DiskWriteBytes
}

// processRDTMetrics copies RDT metrics and calculates derived values
func (h *DefaultDataHandler) processRDTMetrics(rdt *dataframe.RDTMetrics, step *MetricStep) {
	step.RDTClassName = rdt.RDTClassName
	step.RDTMonGroupName = rdt.MonGroupName
	step.RDTL3CacheOccupancy = rdt.L3CacheOccupancy
	step.RDTMemoryBandwidthTotal = rdt.MemoryBandwidthTotal
	step.RDTMemoryBandwidthLocal = rdt.MemoryBandwidthLocal
	step.RDTL3CacheAllocation = rdt.L3CacheAllocation
	step.RDTL3CacheAllocationPct = rdt.L3CacheAllocationPct
	step.RDTMBAThrottle = rdt.MBAThrottle

	// Copy derived metrics from dataframe (calculated by collectors)
	step.RDTCacheLLCUtilizationPercent = rdt.CacheLLCUtilizationPercent
	step.RDTBandwidthUtilizationPercent = rdt.BandwidthUtilizationPercent
}

// returns the container configuration for a given index
func (h *DefaultDataHandler) getContainerConfig(benchmarkConfig *config.BenchmarkConfig, containerIndex int) *config.ContainerConfig {
	for _, container := range benchmarkConfig.Containers {
		if container.Index == containerIndex {
			return &container
		}
	}
	return nil
}
