package collectors

import (
	"fmt"
	"os"
	"syscall"

	"container-bench/internal/dataframe"
	"github.com/elastic/go-perf"
)

type PerfCollector struct {
	events    []*perf.Event
	cgroupFd  int
	cpuCore   int
}

func NewPerfCollector(pid int, cgroupPath string) (*PerfCollector, error) {
	// Open cgroup file descriptor
	cgroupFd, err := os.Open(cgroupPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open cgroup path %s: %w", cgroupPath, err)
	}

	collector := &PerfCollector{
		cgroupFd: int(cgroupFd.Fd()),
		cpuCore:  -1, // Monitor all CPUs
	}

	// Define the hardware events we want to monitor
	eventConfigs := []struct {
		Type   int
		Config uint64
	}{
		{1, 3},  // PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES
		{1, 4},  // PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_REFERENCES
		{1, 0},  // PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS
		{1, 1},  // PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES
		{1, 5},  // PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_INSTRUCTIONS
		{1, 6},  // PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES
		{1, 2},  // PERF_TYPE_HARDWARE, PERF_COUNT_HW_BUS_CYCLES
	}

	// Create perf events
	for _, config := range eventConfigs {
		attr := &perf.Attr{
			Type:   perf.EventType(config.Type),
			Config: config.Config,
		}

		event, err := perf.OpenCGroup(attr, collector.cgroupFd, collector.cpuCore, nil)
		if err != nil {
			// Clean up previously opened events
			collector.Close()
			return nil, fmt.Errorf("failed to open perf event: %w", err)
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

	metrics := &dataframe.PerfMetrics{}

	// Read values from events
	for i, event := range pc.events {
		count, err := event.ReadCount()
		if err != nil {
			// If we can't read this event, continue with others
			continue
		}

		value := uint64(count.Value)

		// Map to appropriate metric based on event index
		switch i {
		case 0: // CACHE_MISSES
			metrics.CacheMisses = &value
		case 1: // CACHE_REFERENCES
			metrics.CacheReferences = &value
		case 2: // INSTRUCTIONS
			metrics.Instructions = &value
		case 3: // CPU_CYCLES
			metrics.Cycles = &value
		case 4: // BRANCH_INSTRUCTIONS
			metrics.BranchInstructions = &value
		case 5: // BRANCH_MISSES
			metrics.BranchMisses = &value
		case 6: // BUS_CYCLES
			metrics.BusCycles = &value
		}
	}

	// Calculate derived metrics
	if metrics.CacheMisses != nil && metrics.CacheReferences != nil && *metrics.CacheReferences > 0 {
		rate := float64(*metrics.CacheMisses) / float64(*metrics.CacheReferences)
		metrics.CacheMissRate = &rate
	}

	if metrics.BranchMisses != nil && metrics.BranchInstructions != nil && *metrics.BranchInstructions > 0 {
		rate := float64(*metrics.BranchMisses) / float64(*metrics.BranchInstructions)
		metrics.BranchMissRate = &rate
	}

	return metrics
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
