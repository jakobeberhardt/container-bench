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

	// Define the hardware events we want to monitor using proper go-perf constants
	// TODO: Extend these
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
			logger.WithField("counter", counter).WithError(err).Error("Failed to open perf event")
			return nil, err
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
			switch i {
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
			}
		}

		pc.lastValues[i] = currentValue
	}

	if !hasAnyData {
		return nil
	}

	// Calculate derived metrics only if we have the base data
	if metrics.CacheMisses != nil && metrics.CacheReferences != nil && *metrics.CacheReferences > 0 {
		rate := float64(*metrics.CacheMisses) / float64(*metrics.CacheReferences)
		metrics.CacheMissRate = &rate
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
