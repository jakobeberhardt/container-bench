package datahandeling

import (
	"time"

	"container-bench/internal/config"
	"container-bench/internal/dataframe"
)

// BenchmarkMetrics represents processed metrics ready for database storage
type BenchmarkMetrics struct {
	ContainerMetrics []ContainerMetrics `json:"container_metrics"`
}

// ContainerMetrics holds all metrics for a single container across all sampling steps
type ContainerMetrics struct {
	ContainerIndex int           `json:"container_index"`
	ContainerName  string        `json:"container_name"`
	ContainerImage string        `json:"container_image"`
	ContainerCore  int           `json:"container_core"`
	Steps          []MetricStep  `json:"steps"`
}

// MetricStep represents processed metrics for a single sampling step
type MetricStep struct {
	StepNumber   int       `json:"step_number"`
	Timestamp    time.Time `json:"timestamp"`
	RelativeTime int64     `json:"relative_time"` // Time from benchmark start in nanoseconds

	// Perf metrics (original)
	PerfCacheMisses          *uint64  `json:"perf_cache_misses,omitempty"`
	PerfCacheReferences      *uint64  `json:"perf_cache_references,omitempty"`
	PerfInstructions         *uint64  `json:"perf_instructions,omitempty"`
	PerfCycles               *uint64  `json:"perf_cycles,omitempty"`
	PerfBranchInstructions   *uint64  `json:"perf_branch_instructions,omitempty"`
	PerfBranchMisses         *uint64  `json:"perf_branch_misses,omitempty"`
	PerfBusCycles            *uint64  `json:"perf_bus_cycles,omitempty"`
	PerfCacheMissRate        *float64 `json:"perf_cache_miss_rate,omitempty"`
	PerfInstructionsPerCycle *float64 `json:"perf_instructions_per_cycle,omitempty"`

	// Derived perf metrics
	PerfBranchMissRate *float64 `json:"perf_branch_miss_rate,omitempty"`

	// Docker metrics
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
	DockerNetworkRxPackets   *uint64  `json:"docker_network_rx_packets,omitempty"`
	DockerNetworkTxPackets   *uint64  `json:"docker_network_tx_packets,omitempty"`
	DockerDiskReadBytes      *uint64  `json:"docker_disk_read_bytes,omitempty"`
	DockerDiskWriteBytes     *uint64  `json:"docker_disk_write_bytes,omitempty"`
	DockerDiskReadOps        *uint64  `json:"docker_disk_read_ops,omitempty"`
	DockerDiskWriteOps       *uint64  `json:"docker_disk_write_ops,omitempty"`

	// RDT metrics
	RDTClassName               *string  `json:"rdt_class_name,omitempty"`
	RDTL3CacheOccupancy        *uint64  `json:"rdt_l3_cache_occupancy,omitempty"`
	RDTL3CacheOccupancyKB      *float64 `json:"rdt_l3_cache_occupancy_kb,omitempty"`
	RDTL3CacheOccupancyMB      *float64 `json:"rdt_l3_cache_occupancy_mb,omitempty"`
	RDTMemoryBandwidthTotal    *uint64  `json:"rdt_memory_bandwidth_total,omitempty"`
	RDTMemoryBandwidthLocal    *uint64  `json:"rdt_memory_bandwidth_local,omitempty"`
	RDTMemoryBandwidthMBps     *float64 `json:"rdt_memory_bandwidth_mbps,omitempty"`
}

// DataHandler processes raw dataframes into structured benchmark metrics
type DataHandler interface {
	ProcessDataFrames(benchmarkID int, benchmarkConfig *config.BenchmarkConfig, dataframes *dataframe.DataFrames, startTime, endTime time.Time) (*BenchmarkMetrics, error)
}

// DefaultDataHandler implements the DataHandler interface
type DefaultDataHandler struct{}

// NewDefaultDataHandler creates a new DefaultDataHandler
func NewDefaultDataHandler() *DefaultDataHandler {
	return &DefaultDataHandler{}
}

// ProcessDataFrames converts raw dataframes to processed benchmark metrics
func (h *DefaultDataHandler) ProcessDataFrames(benchmarkID int, benchmarkConfig *config.BenchmarkConfig, dataframes *dataframe.DataFrames, startTime, endTime time.Time) (*BenchmarkMetrics, error) {
	var containerMetrics []ContainerMetrics

	containers := dataframes.GetAllContainers()
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

			// Calculate relative time from benchmark start
			relativeTime := step.Timestamp.Sub(startTime).Nanoseconds()

			// Create metric step with original data
			metricStep := MetricStep{
				StepNumber:   stepNumber,
				Timestamp:    step.Timestamp,
				RelativeTime: relativeTime,
			}

			// Process Perf metrics
			if step.Perf != nil {
				h.processPerfMetrics(step.Perf, &metricStep)
			}

			// Process Docker metrics  
			if step.Docker != nil {
				h.processDockerMetrics(step.Docker, &metricStep)
			}

			// Process RDT metrics
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

// processPerfMetrics copies perf metrics and calculates derived values
func (h *DefaultDataHandler) processPerfMetrics(perf *dataframe.PerfMetrics, step *MetricStep) {
	// Copy original metrics
	step.PerfCacheMisses = perf.CacheMisses
	step.PerfCacheReferences = perf.CacheReferences
	step.PerfInstructions = perf.Instructions
	step.PerfCycles = perf.Cycles
	step.PerfBranchInstructions = perf.BranchInstructions
	step.PerfBranchMisses = perf.BranchMisses
	step.PerfBusCycles = perf.BusCycles
	step.PerfCacheMissRate = perf.CacheMissRate
	step.PerfInstructionsPerCycle = perf.InstructionsPerCycle

	// Calculate derived metrics: Branch miss rate
	if perf.BranchInstructions != nil && perf.BranchMisses != nil && *perf.BranchInstructions > 0 {
		branchMissRate := float64(*perf.BranchMisses) / float64(*perf.BranchInstructions) * 100.0
		step.PerfBranchMissRate = &branchMissRate
	}
}

// processDockerMetrics copies docker metrics
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
	step.DockerNetworkRxPackets = docker.NetworkRxPackets
	step.DockerNetworkTxPackets = docker.NetworkTxPackets
	step.DockerDiskReadBytes = docker.DiskReadBytes
	step.DockerDiskWriteBytes = docker.DiskWriteBytes
	step.DockerDiskReadOps = docker.DiskReadOps
	step.DockerDiskWriteOps = docker.DiskWriteOps
}

// processRDTMetrics copies RDT metrics
func (h *DefaultDataHandler) processRDTMetrics(rdt *dataframe.RDTMetrics, step *MetricStep) {
	step.RDTClassName = rdt.RDTClassName
	step.RDTL3CacheOccupancy = rdt.L3CacheOccupancy
	step.RDTL3CacheOccupancyKB = rdt.L3CacheOccupancyKB
	step.RDTL3CacheOccupancyMB = rdt.L3CacheOccupancyMB
	step.RDTMemoryBandwidthTotal = rdt.MemoryBandwidthTotal
	step.RDTMemoryBandwidthLocal = rdt.MemoryBandwidthLocal
	step.RDTMemoryBandwidthMBps = rdt.MemoryBandwidthMBps
}

// getContainerConfig returns the container configuration for a given index
func (h *DefaultDataHandler) getContainerConfig(benchmarkConfig *config.BenchmarkConfig, containerIndex int) *config.ContainerConfig {
	for _, container := range benchmarkConfig.Containers {
		if container.Index == containerIndex {
			return &container
		}
	}
	return nil
}
