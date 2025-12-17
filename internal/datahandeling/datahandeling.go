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
	ContainerCore  string       `json:"container_core"`
	Steps          []MetricStep `json:"steps"`
}

// processed metrics for a single sampling step
type MetricStep struct {
	StepNumber   int       `json:"step_number"`
	Timestamp    time.Time `json:"timestamp"`
	RelativeTime int64     `json:"relative_time"` // Time from benchmark start in nanoseconds

	PerfCacheMisses        *uint64 `json:"perf_cache_misses,omitempty"`
	PerfCacheReferences    *uint64 `json:"perf_cache_references,omitempty"`
	PerfInstructions       *uint64 `json:"perf_instructions,omitempty"`
	PerfCycles             *uint64 `json:"perf_cycles,omitempty"`
	PerfBranchInstructions *uint64 `json:"perf_branch_instructions,omitempty"`
	PerfBranchMisses       *uint64 `json:"perf_branch_misses,omitempty"`
	PerfBusCycles          *uint64 `json:"perf_bus_cycles,omitempty"`

	// CPU stall counters
	PerfStallsTotal              *uint64 `json:"perf_stalls_total,omitempty"`
	PerfStallsL3Miss             *uint64 `json:"perf_stalls_l3_miss,omitempty"`
	PerfStallsL2Miss             *uint64 `json:"perf_stalls_l2_miss,omitempty"`
	PerfStallsL1dMiss            *uint64 `json:"perf_stalls_l1d_miss,omitempty"`
	PerfStallsMemAny             *uint64 `json:"perf_stalls_mem_any,omitempty"`
	PerfResourceStallsSB         *uint64 `json:"perf_resource_stalls_sb,omitempty"`
	PerfResourceStallsScoreboard *uint64 `json:"perf_resource_stalls_scoreboard,omitempty"`

	PerfCacheMissRate        *float64 `json:"perf_cache_miss_rate,omitempty"`
	PerfInstructionsPerCycle *float64 `json:"perf_instructions_per_cycle,omitempty"`

	// Derived perf metrics
	PerfBranchMissRate       *float64 `json:"perf_branch_miss_rate,omitempty"`
	PerfStalledCyclesPercent *float64 `json:"perf_stalled_cycles_percent,omitempty"`
	PerfStallsL3MissPercent  *float64 `json:"perf_stalls_l3_miss_percent,omitempty"`
	PerfTheoreticalIPC       *float64 `json:"perf_theoretical_ipc,omitempty"`
	PerfIPCEfficancy         *float64 `json:"perf_ipc_efficancy,omitempty"`

	DockerCPUUsageTotal      *uint64  `json:"docker_cpu_usage_total,omitempty"`
	DockerCPUUsageKernel     *uint64  `json:"docker_cpu_usage_kernel,omitempty"`
	DockerCPUUsageUser       *uint64  `json:"docker_cpu_usage_user,omitempty"`
	DockerCPUUsagePercent    *float64 `json:"docker_cpu_usage_percent,omitempty"`
	DockerCPUThrottling      *uint64  `json:"docker_cpu_throttling,omitempty"`
	DockerAssignedCoresCSV   *string  `json:"docker_assigned_cores_csv,omitempty"`
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

	RDTClassName    *string `json:"rdt_class_name,omitempty"`
	RDTMonGroupName *string `json:"rdt_mon_group_name,omitempty"`
	RDTMBAThrottle  *uint64 `json:"rdt_mba_throttle,omitempty"`

	// Per-socket monitoring metrics
	RDTL3OccupancyPerSocket          map[int]uint64  `json:"rdt_l3_occupancy_per_socket,omitempty"`
	RDTMemoryBandwidthTotalPerSocket map[int]uint64  `json:"rdt_memory_bandwidth_total_per_socket,omitempty"`
	RDTMemoryBandwidthLocalPerSocket map[int]uint64  `json:"rdt_memory_bandwidth_local_per_socket,omitempty"`
	RDTL3UtilizationPctPerSocket     map[int]float64 `json:"rdt_l3_utilization_pct_per_socket,omitempty"`
	RDTMemBandwidthMbpsPerSocket     map[int]float64 `json:"rdt_mem_bandwidth_mbps_per_socket,omitempty"`

	// Per-socket allocation details
	RDTL3BitmaskPerSocket       map[int]string  `json:"rdt_l3_bitmask_per_socket,omitempty"`
	RDTL3WaysPerSocket          map[int]uint64  `json:"rdt_l3_ways_per_socket,omitempty"`
	RDTL3AllocationPctPerSocket map[int]float64 `json:"rdt_l3_allocation_pct_per_socket,omitempty"`
	RDTMBAPercentPerSocket      map[int]uint64  `json:"rdt_mba_percent_per_socket,omitempty"`

	// Full allocation strings
	RDTL3AllocationString  *string `json:"rdt_l3_allocation_string,omitempty"`
	RDTMBAAllocationString *string `json:"rdt_mba_allocation_string,omitempty"`
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
	step.PerfCacheMisses = perf.CacheMisses
	step.PerfCacheReferences = perf.CacheReferences
	step.PerfInstructions = perf.Instructions
	step.PerfCycles = perf.Cycles
	step.PerfBranchInstructions = perf.BranchInstructions
	step.PerfBranchMisses = perf.BranchMisses
	step.PerfBusCycles = perf.BusCycles
	step.PerfCacheMissRate = perf.CacheMissRate
	step.PerfInstructionsPerCycle = perf.InstructionsPerCycle

	step.PerfStallsTotal = perf.StallsTotal
	step.PerfStallsL3Miss = perf.StallsL3Miss
	step.PerfStallsL2Miss = perf.StallsL2Miss
	step.PerfStallsL1dMiss = perf.StallsL1dMiss
	step.PerfStallsMemAny = perf.StallsMemAny
	step.PerfResourceStallsSB = perf.ResourceStallsSB
	step.PerfResourceStallsScoreboard = perf.ResourceStallsScoreboard

	step.PerfStalledCyclesPercent = perf.StalledCyclesPercent
	step.PerfStallsL3MissPercent = perf.StallsL3MissPercent
	step.PerfTheoreticalIPC = perf.TheoreticalIPC
	step.PerfIPCEfficancy = perf.IPCEfficancy

	// Calculate derived metrics: Branch miss rate
	if perf.BranchInstructions != nil && perf.BranchMisses != nil && *perf.BranchInstructions > 0 {
		branchMissRate := float64(*perf.BranchMisses) / float64(*perf.BranchInstructions) * 100.0
		step.PerfBranchMissRate = &branchMissRate
	}
}

func (h *DefaultDataHandler) processDockerMetrics(docker *dataframe.DockerMetrics, step *MetricStep) {
	step.DockerCPUUsageTotal = docker.CPUUsageTotal
	step.DockerCPUUsageKernel = docker.CPUUsageKernel
	step.DockerCPUUsageUser = docker.CPUUsageUser
	step.DockerCPUUsagePercent = docker.CPUUsagePercent
	step.DockerCPUThrottling = docker.CPUThrottling
	step.DockerAssignedCoresCSV = docker.AssignedCoresCSV
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
	step.RDTMBAThrottle = rdt.MBAThrottle

	// Copy per-socket monitoring metrics
	step.RDTL3OccupancyPerSocket = rdt.L3OccupancyPerSocket
	step.RDTMemoryBandwidthTotalPerSocket = rdt.MemoryBandwidthTotalPerSocket
	step.RDTMemoryBandwidthLocalPerSocket = rdt.MemoryBandwidthLocalPerSocket
	step.RDTL3UtilizationPctPerSocket = rdt.L3UtilizationPctPerSocket
	step.RDTMemBandwidthMbpsPerSocket = rdt.MemBandwidthMbpsPerSocket

	// Copy per-socket allocation details
	step.RDTL3BitmaskPerSocket = rdt.L3BitmaskPerSocket
	step.RDTL3WaysPerSocket = rdt.L3WaysPerSocket
	step.RDTL3AllocationPctPerSocket = rdt.L3AllocationPctPerSocket
	step.RDTMBAPercentPerSocket = rdt.MBAPercentPerSocket

	// Copy full allocation strings
	step.RDTL3AllocationString = rdt.L3AllocationString
	step.RDTMBAAllocationString = rdt.MBAAllocationString
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
