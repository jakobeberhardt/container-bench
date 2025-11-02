package collectors

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"syscall"

	"container-bench/internal/dataframe"
	"container-bench/internal/logging"

	"github.com/elastic/go-perf"
)

type PerfCollector struct {
	events   []*perf.Event
	cgroupFd int
	cpus     []int // List of CPUs to monitor// TODOmay be no longer needed

	lastValues map[int]uint64
	mutex      sync.Mutex
}

func NewPerfCollector(pid int, cgroupPath string, cpus []int) (*PerfCollector, error) {
	logger := logging.GetLogger()

	numCPUs := runtime.NumCPU() // TODO pass the host config
	allCPUs := make([]int, numCPUs)
	for i := 0; i < numCPUs; i++ {
		allCPUs[i] = i
	}

	logger.WithField("num_cpus", numCPUs).Debug("Monitoring all CPUs for cgroup-based perf collection")

	if _, err := os.Stat(cgroupPath); os.IsNotExist(err) {
		logger.WithField("cgroup_path", cgroupPath).Error("Cgroup path does not exist")
		return nil, err
	}

	cgroupFd, err := os.Open(cgroupPath)
	if err != nil {
		logger.WithField("cgroup_path", cgroupPath).WithError(err).Error("Failed to open cgroup path")
		return nil, err
	}

	collector := &PerfCollector{
		cgroupFd:   int(cgroupFd.Fd()),
		cpus:       allCPUs,
		lastValues: make(map[int]uint64),
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

	// Create perf events for each CPU and each counter
	for _, cpu := range collector.cpus {
		for _, counter := range hardwareCounters {
			attr := &perf.Attr{}
			counter.Configure(attr)
			// Enable time tracking for multiplexing correction
			attr.CountFormat.Enabled = true
			attr.CountFormat.Running = true
			event, err := perf.OpenCGroup(attr, collector.cgroupFd, cpu, nil)
			if err != nil {
				collector.Close()
				logger.WithFields(map[string]interface{}{
					"counter": counter,
					"cpu":     cpu,
				}).WithError(err).Error("Failed to open perf event")
				return nil, err
			}

			collector.events = append(collector.events, event)
		}
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

	numCounters := 7

	counterSums := make([]uint64, numCounters)

	// Read values from all events and aggregate by counter type
	for i, event := range pc.events {
		count, err := event.ReadCount()
		if err != nil {
			continue
		}

		// Scale the value to account for multiplexing
		// When counters are multiplexed, they only run for a fraction of the time
		// Scaling: scaled_value = raw_value * (time_enabled / time_running)
		currentValue := uint64(count.Value)
		if count.Running > 0 && count.Enabled > 0 {
			// Scale the raw value by the ratio of enabled to running time
			scaleFactor := float64(count.Enabled) / float64(count.Running)
			currentValue = uint64(float64(count.Value) * scaleFactor)
		}

		counterIndex := i % numCounters

		if lastValue, exists := pc.lastValues[i]; exists {
			delta := currentValue - lastValue
			counterSums[counterIndex] += delta
		}

		pc.lastValues[i] = currentValue
	}

	hasAnyData := false
	for _, sum := range counterSums {
		if sum > 0 {
			hasAnyData = true
			break
		}
	}

	if !hasAnyData {
		return nil
	}

	metrics := &dataframe.PerfMetrics{}

	if counterSums[0] > 0 {
		metrics.CacheMisses = &counterSums[0]
	}
	if counterSums[1] > 0 {
		metrics.CacheReferences = &counterSums[1]
	}
	if counterSums[2] > 0 {
		metrics.Instructions = &counterSums[2]
	}
	if counterSums[3] > 0 {
		metrics.Cycles = &counterSums[3]
	}
	if counterSums[4] > 0 {
		metrics.BranchInstructions = &counterSums[4]
	}
	if counterSums[5] > 0 {
		metrics.BranchMisses = &counterSums[5]
	}
	if counterSums[6] > 0 {
		metrics.BusCycles = &counterSums[6]
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
