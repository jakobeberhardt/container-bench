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

func NewPerfCollector(pid int, cgroupPath string, cpuCore int) (*PerfCollector, error) {
	// Check if cgroup path exists
	if _, err := os.Stat(cgroupPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("cgroup path does not exist: %s", cgroupPath)
	}
	
	// Open cgroup file descriptor
	cgroupFd, err := os.Open(cgroupPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open cgroup path %s: %w", cgroupPath, err)
	}

	collector := &PerfCollector{
		cgroupFd: int(cgroupFd.Fd()),
		cpuCore:  cpuCore, // Use the specific CPU core assigned to the container
	}

	// Define the hardware events we want to monitor using proper go-perf constants
	hardwareCounters := []perf.HardwareCounter{
		perf.CacheMisses,
		perf.CacheReferences,
		perf.Instructions,
		perf.CPUCycles,
		perf.BranchInstructions,
		perf.BranchMisses,
		perf.BusCycles,
	}

	// Create perf events
	for _, counter := range hardwareCounters {
		attr := &perf.Attr{}
		counter.Configure(attr)

		event, err := perf.OpenCGroup(attr, collector.cgroupFd, collector.cpuCore, nil)
		if err != nil {
			// Clean up previously opened events
			collector.Close()
			return nil, fmt.Errorf("failed to open perf event %v: %w", counter, err)
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

	if metrics.Instructions != nil && metrics.Cycles != nil && *metrics.Cycles > 0 {
		ipc := float64(*metrics.Instructions) / float64(*metrics.Cycles)
		metrics.InstructionsPerCycle = &ipc
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
