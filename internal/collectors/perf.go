package collectors

import (
	"container-bench/internal/dataframe"
	"container-bench/internal/logging"
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/jakobeberhardt/go-perf"
)

type PerfCollector struct {
	events   []*perf.Event
	cgroupFd int
	cpuCore  int

	lastValues map[int]uint64
	mutex      sync.Mutex
}

func NewPerfCollector(pid int, cgroupPath string, cpuCore int) (*PerfCollector, error) {
	logger := logging.GetLogger()

	// Check if cgroup path exists
	if _, err := os.Stat(cgroupPath); os.IsNotExist(err) {
		logger.WithField("cgroup_path", cgroupPath).Error("Cgroup path does not exist")
		return nil, err
	}

	// Open cgroup file descriptor
	cgroupFd, err := os.Open(cgroupPath)
	if err != nil {
		logger.WithField("cgroup_path", cgroupPath).WithError(err).Error("Failed to open cgroup path")
		return nil, err
	}

	collector := &PerfCollector{
		cgroupFd:   int(cgroupFd.Fd()),
		cpuCore:    cpuCore, // Use the specific CPU core assigned to the container TODO: Check if we can use all, e.g like -1 in case a container is migrated
		lastValues: make(map[int]uint64),
	}

	// Define the hardware events we want to monitor using the new cache events
	// This includes both basic hardware counters and detailed cache events
	var configurators []perf.Configurator

	// Add basic hardware counters
	basicCounters := []perf.HardwareCounter{
		perf.CacheMisses,
		perf.CacheReferences,
		perf.Instructions,
		perf.CPUCycles,
		perf.BranchInstructions,
		perf.BranchMisses,
		perf.BusCycles,
	}

	for _, counter := range basicCounters {
		configurators = append(configurators, counter)
	}

	// Add comprehensive cache events for detailed analysis using predefined cache events
	cacheEvents := []perf.Configurator{
		// L1 Data Cache Events
		perf.L1DCacheLoadMisses,
		perf.L1DCacheLoads,
		perf.L1DCacheStores,

		// L1 Instruction Cache Events
		perf.L1ICacheLoadMisses,

		// Last Level Cache (LLC) Events
		perf.LLCLoadMisses,
		perf.LLCLoads,
		perf.LLCStoreMisses,
		perf.LLCStores,

		// Branch Predictor Events
		perf.BranchLoadMisses,
		perf.BranchLoads,

		// Data Translation Lookaside Buffer Events
		perf.DTLBLoadMisses,
		perf.DTLBLoads,
		perf.DTLBStoreMisses,
		perf.DTLBStores,

		// Instruction Translation Lookaside Buffer Events
		perf.ITLBLoadMisses,

		// NUMA Node Events
		perf.NodeLoadMisses,
		perf.NodeLoads,
		perf.NodeStoreMisses,
		perf.NodeStores,
	}

	configurators = append(configurators, cacheEvents...)

	// Create perf events for all configurators
	for _, configurator := range configurators {
		attr := &perf.Attr{}
		err := configurator.Configure(attr)
		if err != nil {
			logger.WithError(err).Warn("Failed to configure perf event, skipping")
			continue
		}

		event, err := perf.OpenCGroup(attr, collector.cgroupFd, collector.cpuCore, nil)
		if err != nil {
			// Log but don't fail completely - some events might not be available on all systems
			logger.WithField("event", attr.Label).WithError(err).Warn("Failed to open perf event, skipping")
			continue
		}

		collector.events = append(collector.events, event)
	}

	// Enable all events
	for _, event := range collector.events {
		if err := event.Enable(); err != nil {
			collector.Close()
			return nil, fmt.Errorf("failed to enable perf event: %w", err)
		}
	}

	return collector, nil
}

func (pc *PerfCollector) Collect() *dataframe.PerfMetrics {
	if len(pc.events) == 0 {
		return nil
	}

	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	metrics := &dataframe.PerfMetrics{}
	hasAnyData := false

	// Read values from events and calculate deltas
	for i, event := range pc.events {
		count, err := event.ReadCount()
		if err != nil {
			continue
		}

		currentValue := uint64(count.Value)

		// Calculate delta from previous measurement
		if lastValue, exists := pc.lastValues[i]; exists {
			delta := currentValue - lastValue
			hasAnyData = true // We have a measurement, even if delta is 0

			// Map to appropriate metric based on event index
			// This relies on the order we added events in NewPerfCollector
			pc.mapEventToMetrics(i, delta, metrics)
		}

		pc.lastValues[i] = currentValue
	}

	if !hasAnyData {
		return nil
	}

	// Calculate derived metrics
	pc.calculateDerivedMetrics(metrics)

	return metrics
}

// mapEventToMetrics maps event index to the appropriate metric field
func (pc *PerfCollector) mapEventToMetrics(eventIndex int, delta uint64, metrics *dataframe.PerfMetrics) {
	// Basic hardware counters (first 7 events)
	switch eventIndex {
	case 0: // CACHE_MISSES
		metrics.CacheMisses = &delta
	case 1: // CACHE_REFERENCES
		metrics.CacheReferences = &delta
	case 2: // INSTRUCTIONS
		metrics.Instructions = &delta
	case 3: // CPU_CYCLES
		metrics.Cycles = &delta
	case 4: // BRANCH_INSTRUCTIONS
		metrics.BranchInstructions = &delta
	case 5: // BRANCH_MISSES
		metrics.BranchMisses = &delta
	case 6: // BUS_CYCLES
		metrics.BusCycles = &delta
	case 7: // L1D Cache Load Misses
		metrics.L1DCacheLoadMisses = &delta
	case 8: // L1D Cache Loads
		metrics.L1DCacheLoads = &delta
	case 9: // L1D Cache Stores
		metrics.L1DCacheStores = &delta
	case 10: // L1I Cache Load Misses
		metrics.L1ICacheLoadMisses = &delta
	case 11: // LLC Load Misses
		metrics.LLCLoadMisses = &delta
	case 12: // LLC Loads
		metrics.LLCLoads = &delta
	case 13: // LLC Store Misses
		metrics.LLCStoreMisses = &delta
	case 14: // LLC Stores
		metrics.LLCStores = &delta
	case 15: // Branch Load Misses
		metrics.BranchLoadMisses = &delta
	case 16: // Branch Loads
		metrics.BranchLoads = &delta
	case 17: // DTLB Load Misses
		metrics.DTLBLoadMisses = &delta
	case 18: // DTLB Loads
		metrics.DTLBLoads = &delta
	case 19: // DTLB Store Misses
		metrics.DTLBStoreMisses = &delta
	case 20: // DTLB Stores
		metrics.DTLBStores = &delta
	case 21: // ITLB Load Misses
		metrics.ITLBLoadMisses = &delta
	case 22: // Node Load Misses
		metrics.NodeLoadMisses = &delta
	case 23: // Node Loads
		metrics.NodeLoads = &delta
	case 24: // Node Store Misses
		metrics.NodeStoreMisses = &delta
	case 25: // Node Stores
		metrics.NodeStores = &delta
	}
}

// calculateDerivedMetrics computes derived performance metrics
func (pc *PerfCollector) calculateDerivedMetrics(metrics *dataframe.PerfMetrics) {
	// Original cache miss rate (using basic counters)
	if metrics.CacheMisses != nil && metrics.CacheReferences != nil && *metrics.CacheReferences > 0 {
		rate := float64(*metrics.CacheMisses) / float64(*metrics.CacheReferences)
		metrics.CacheMissRate = &rate
	}

	// Instructions per cycle
	if metrics.Instructions != nil && metrics.Cycles != nil && *metrics.Cycles > 0 {
		ipc := float64(*metrics.Instructions) / float64(*metrics.Cycles)
		metrics.InstructionsPerCycle = &ipc
	}

	// L1D Cache miss rate
	if metrics.L1DCacheLoadMisses != nil && metrics.L1DCacheLoads != nil && *metrics.L1DCacheLoads > 0 {
		rate := float64(*metrics.L1DCacheLoadMisses) / float64(*metrics.L1DCacheLoads)
		metrics.L1DCacheMissRate = &rate
	}

	// LLC miss rate
	if metrics.LLCLoadMisses != nil && metrics.LLCLoads != nil && *metrics.LLCLoads > 0 {
		rate := float64(*metrics.LLCLoadMisses) / float64(*metrics.LLCLoads)
		metrics.LLCMissRate = &rate
	}

	// DTLB miss rate (combining load and store misses vs total accesses)
	if metrics.DTLBLoadMisses != nil && metrics.DTLBStoreMisses != nil &&
		metrics.DTLBLoads != nil && metrics.DTLBStores != nil {
		totalMisses := *metrics.DTLBLoadMisses + *metrics.DTLBStoreMisses
		totalAccesses := *metrics.DTLBLoads + *metrics.DTLBStores
		if totalAccesses > 0 {
			rate := float64(totalMisses) / float64(totalAccesses)
			metrics.DTLBMissRate = &rate
		}
	}

	// Branch miss rate (using detailed branch events if available, fallback to basic)
	if metrics.BranchLoadMisses != nil && metrics.BranchLoads != nil && *metrics.BranchLoads > 0 {
		rate := float64(*metrics.BranchLoadMisses) / float64(*metrics.BranchLoads)
		metrics.BranchMissRate = &rate
	} else if metrics.BranchMisses != nil && metrics.BranchInstructions != nil && *metrics.BranchInstructions > 0 {
		rate := float64(*metrics.BranchMisses) / float64(*metrics.BranchInstructions)
		metrics.BranchMissRate = &rate
	}
}

func (pc *PerfCollector) Close() {
	for _, event := range pc.events {
		if event != nil {
			event.Close()
		}
	}
	pc.events = nil

	if pc.cgroupFd >= 0 {
		syscall.Close(pc.cgroupFd)
		pc.cgroupFd = -1
	}
}
