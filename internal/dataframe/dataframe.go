package dataframe

import (
	"sync"
	"time"
)

type DataFrames struct {
	containers map[int]*ContainerDataFrame
	mutex      sync.RWMutex
}

type ContainerDataFrame struct {
	steps map[int]*SamplingStep
	mutex sync.RWMutex
}

type SamplingStep struct {
	Timestamp time.Time    `json:"timestamp"`
	Perf      *PerfMetrics `json:"perf,omitempty"`
	Docker    *DockerMetrics `json:"docker,omitempty"`
	RDT       *RDTMetrics  `json:"rdt,omitempty"`
}

type PerfMetrics struct {
	CacheMisses          *uint64 `json:"cache_misses,omitempty"`
	CacheReferences      *uint64 `json:"cache_references,omitempty"`
	Instructions         *uint64 `json:"instructions,omitempty"`
	Cycles               *uint64 `json:"cycles,omitempty"`
	BranchInstructions   *uint64 `json:"branch_instructions,omitempty"`
	BranchMisses         *uint64 `json:"branch_misses,omitempty"`
	BusCycles            *uint64 `json:"bus_cycles,omitempty"`
	L1DCacheLoadMisses   *uint64 `json:"l1d_cache_load_misses,omitempty"`
	L1DCacheLoads        *uint64 `json:"l1d_cache_loads,omitempty"`
	L1DCacheStores       *uint64 `json:"l1d_cache_stores,omitempty"`
	L1ICacheLoadMisses   *uint64 `json:"l1i_cache_load_misses,omitempty"`
	LLCLoadMisses        *uint64 `json:"llc_load_misses,omitempty"`
	LLCLoads             *uint64 `json:"llc_loads,omitempty"`
	LLCStoreMisses       *uint64 `json:"llc_store_misses,omitempty"`
	LLCStores            *uint64 `json:"llc_stores,omitempty"`
	CacheMissRate        *float64 `json:"cache_miss_rate,omitempty"`
	InstructionsPerCycle *float64 `json:"instructions_per_cycle,omitempty"`
}

type DockerMetrics struct {
	CPUUsageTotal       *uint64  `json:"cpu_usage_total,omitempty"`
	CPUUsageKernel      *uint64  `json:"cpu_usage_kernel,omitempty"`
	CPUUsageUser        *uint64  `json:"cpu_usage_user,omitempty"`
	CPUUsagePercent     *float64 `json:"cpu_usage_percent,omitempty"`
	CPUThrottling       *uint64  `json:"cpu_throttling,omitempty"`
	MemoryUsage         *uint64  `json:"memory_usage,omitempty"`
	MemoryLimit         *uint64  `json:"memory_limit,omitempty"`
	MemoryCache         *uint64  `json:"memory_cache,omitempty"`
	MemoryRSS           *uint64  `json:"memory_rss,omitempty"`
	MemorySwap          *uint64  `json:"memory_swap,omitempty"`
	MemoryUsagePercent  *float64 `json:"memory_usage_percent,omitempty"`
	NetworkRxBytes      *uint64  `json:"network_rx_bytes,omitempty"`
	NetworkTxBytes      *uint64  `json:"network_tx_bytes,omitempty"`
	DiskReadBytes       *uint64  `json:"disk_read_bytes,omitempty"`
	DiskWriteBytes      *uint64  `json:"disk_write_bytes,omitempty"`
}

type RDTMetrics struct {
	// L3 Cache Monitoring
	L3CacheOccupancy     *uint64  `json:"l3_cache_occupancy,omitempty"`     // L3 cache occupancy in bytes
	
	// Memory Bandwidth Monitoring
	MemoryBandwidthTotal *uint64  `json:"memory_bandwidth_total,omitempty"` // Total memory bandwidth in bytes/sec
	MemoryBandwidthLocal *uint64  `json:"memory_bandwidth_local,omitempty"` // Local memory bandwidth in bytes/sec
	MemoryBandwidthMBps  *float64 `json:"memory_bandwidth_mbps,omitempty"`  // Memory bandwidth in MB/s
	
	// RDT Class Information
	RDTClassName         *string `json:"rdt_class_name,omitempty"`         // Name of the RDT class/CLOS group
	MonGroupName         *string `json:"mon_group_name,omitempty"`         // Name of the monitoring group
	
	// Cache Allocation Information (if available)
	L3CacheAllocation    *uint64  `json:"l3_cache_allocation,omitempty"`    // Allocated L3 cache ways/percentage
	L3CacheAllocationPct *float64 `json:"l3_cache_allocation_pct,omitempty"` // L3 cache allocation percentage
	
	// Memory Bandwidth Allocation (if available)
	MBAThrottle          *uint64  `json:"mba_throttle,omitempty"`           // Memory bandwidth throttle percentage
	
	// Derived Metrics
	CacheLLCUtilizationPercent   *float64 `json:"cache_llc_utilization_percent,omitempty"`   // LLC cache utilization percentage
	BandwidthUtilizationPercent  *float64 `json:"bandwidth_utilization_percent,omitempty"`  // Bandwidth utilization percentage
}

func NewDataFrames() *DataFrames {
	return &DataFrames{
		containers: make(map[int]*ContainerDataFrame),
	}
}

func (df *DataFrames) GetContainer(index int) *ContainerDataFrame {
	df.mutex.RLock()
	defer df.mutex.RUnlock()
	return df.containers[index]
}

func (df *DataFrames) AddContainer(index int) *ContainerDataFrame {
	df.mutex.Lock()
	defer df.mutex.Unlock()
	
	cdf := &ContainerDataFrame{
		steps: make(map[int]*SamplingStep),
	}
	df.containers[index] = cdf
	return cdf
}

func (df *DataFrames) GetAllContainers() map[int]*ContainerDataFrame {
	df.mutex.RLock()
	defer df.mutex.RUnlock()
	
	result := make(map[int]*ContainerDataFrame)
	for k, v := range df.containers {
		result[k] = v
	}
	return result
}

func (cdf *ContainerDataFrame) AddStep(stepNumber int, step *SamplingStep) {
	cdf.mutex.Lock()
	defer cdf.mutex.Unlock()
	cdf.steps[stepNumber] = step
}

func (cdf *ContainerDataFrame) AddOrMergeStep(stepNumber int, step *SamplingStep) {
	cdf.mutex.Lock()
	defer cdf.mutex.Unlock()
	
	if existing, exists := cdf.steps[stepNumber]; exists {
		// Merge the new step data into the existing step
		if step.Perf != nil {
			existing.Perf = step.Perf
		}
		if step.Docker != nil {
			existing.Docker = step.Docker
		}
		if step.RDT != nil {
			existing.RDT = step.RDT
		}
		// Update timestamp to the latest
		if step.Timestamp.After(existing.Timestamp) {
			existing.Timestamp = step.Timestamp
		}
	} else {
		// Create new step
		cdf.steps[stepNumber] = step
	}
}

func (cdf *ContainerDataFrame) GetStep(stepNumber int) *SamplingStep {
	cdf.mutex.RLock()
	defer cdf.mutex.RUnlock()
	return cdf.steps[stepNumber]
}

func (cdf *ContainerDataFrame) GetAllSteps() map[int]*SamplingStep {
	cdf.mutex.RLock()
	defer cdf.mutex.RUnlock()
	
	result := make(map[int]*SamplingStep)
	for k, v := range cdf.steps {
		result[k] = v
	}
	return result
}

func (cdf *ContainerDataFrame) GetLatestStep() *SamplingStep {
	cdf.mutex.RLock()
	defer cdf.mutex.RUnlock()
	
	maxStep := -1
	var latest *SamplingStep
	for step, data := range cdf.steps {
		if step > maxStep {
			maxStep = step
			latest = data
		}
	}
	return latest
}
