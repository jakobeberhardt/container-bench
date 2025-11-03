package collectors

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"container-bench/internal/dataframe"
	"container-bench/internal/logging"

	"github.com/elastic/go-perf"
)

type eventState struct {
	value   uint64
	enabled time.Duration
	running time.Duration
}

type PerfCollector struct {
	events     []*perf.Event
	cgroupFile *os.File
	cgroupFd   int
	cpus       []int // List of CPUs to monitor// TODOmay be no longer needed

	lastState map[int]*eventState
	mutex     sync.Mutex
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

	cgroupFile, err := os.Open(cgroupPath)
	if err != nil {
		logger.WithField("cgroup_path", cgroupPath).WithError(err).Error("Failed to open cgroup path")
		return nil, err
	}

	collector := &PerfCollector{
		cgroupFile: cgroupFile,
		cgroupFd:   int(cgroupFile.Fd()),
		cpus:       allCPUs,
		lastState:  make(map[int]*eventState),
	}

	hardwareCounters := []perf.HardwareCounter{
		perf.CacheMisses,
		perf.CacheReferences,
		perf.Instructions,
		perf.CPUCycles,
		perf.BranchInstructions,
		perf.BranchMisses,
		perf.BusCycles,
	}

	// Intel-specific
	// These are not available in go-perf predefined counters
	rawStallEvents := []struct {
		name   string
		config uint64
	}{
		{"cycle_activity.stalls_total", 0x40004a3},
		{"cycle_activity.stalls_l3_miss", 0x60006a3},
		{"cycle_activity.stalls_l2_miss", 0x50005a3},
		{"cycle_activity.stalls_l1d_miss", 0xc000ca3},
		{"cycle_activity.stalls_mem_any", 0x140014a3},
		{"resource_stalls.sb", 0x8a2},
		{"resource_stalls.scoreboard", 0x2a2},
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

		// Add raw stall events
		for _, rawEvent := range rawStallEvents {
			attr := &perf.Attr{
				Type:   perf.RawEvent,
				Config: rawEvent.config,
				Label:  rawEvent.name,
			}
			attr.CountFormat.Enabled = true
			attr.CountFormat.Running = true
			event, err := perf.OpenCGroup(attr, collector.cgroupFd, cpu, nil)
			if err != nil {
				logger.WithFields(map[string]interface{}{
					"event": rawEvent.name,
					"cpu":   cpu,
				}).WithError(err).Warn("Failed to open raw perf event, continuing without it")
				continue
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

	// Aggregate counters by label/name across all CPUs
	counterSums := make(map[string]uint64)

	// Read values from all events and aggregate by counter type
	for i, event := range pc.events {
		count, err := event.ReadCount()
		if err != nil {
			continue
		}

		// Get the current cumulative values
		currentValue := uint64(count.Value)
		currentEnabled := count.Enabled
		currentRunning := count.Running

		// Calculate deltas from the last sample
		if lastState, exists := pc.lastState[i]; exists {
			deltaValue := currentValue - lastState.value
			deltaEnabled := currentEnabled - lastState.enabled
			deltaRunning := currentRunning - lastState.running

			// Apply multiplexing correction to the delta using the delta times
			// based on the time deltas for this interval
			scaledDelta := deltaValue
			if deltaRunning > 0 && deltaEnabled > 0 && deltaRunning != deltaEnabled {
				// Scale the delta by the ratio of enabled to running time in this interval
				scaleFactor := float64(deltaEnabled) / float64(deltaRunning)
				scaledDelta = uint64(float64(deltaValue) * scaleFactor)
			}

			counterSums[count.Label] += scaledDelta
		}

		pc.lastState[i] = &eventState{
			value:   currentValue,
			enabled: currentEnabled,
			running: currentRunning,
		}
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

	setValue := func(label string) *uint64 {
		if val, ok := counterSums[label]; ok && val > 0 {
			v := val
			return &v
		}
		return nil
	}

	metrics.CacheMisses = setValue("cache-misses")
	metrics.CacheReferences = setValue("cache-references")
	metrics.Instructions = setValue("instructions")
	metrics.Cycles = setValue("cpu-cycles")
	metrics.BranchInstructions = setValue("branch-instructions")
	metrics.BranchMisses = setValue("branch-misses")
	metrics.BusCycles = setValue("bus-cycles")

	metrics.StallsTotal = setValue("cycle_activity.stalls_total")
	metrics.StallsL3Miss = setValue("cycle_activity.stalls_l3_miss")
	metrics.StallsL2Miss = setValue("cycle_activity.stalls_l2_miss")
	metrics.StallsL1dMiss = setValue("cycle_activity.stalls_l1d_miss")
	metrics.StallsMemAny = setValue("cycle_activity.stalls_mem_any")
	metrics.ResourceStallsSB = setValue("resource_stalls.sb")
	metrics.ResourceStallsScoreboard = setValue("resource_stalls.scoreboard")

	if metrics.CacheMisses != nil && metrics.CacheReferences != nil && *metrics.CacheReferences > 0 {
		rate := float64(*metrics.CacheMisses) / float64(*metrics.CacheReferences)
		metrics.CacheMissRate = &rate
	}

	if metrics.Instructions != nil && metrics.Cycles != nil && *metrics.Cycles > 0 {
		ipc := float64(*metrics.Instructions) / float64(*metrics.Cycles)
		metrics.InstructionsPerCycle = &ipc
	}

	if metrics.StallsTotal != nil && metrics.Cycles != nil && *metrics.Cycles > 0 {
		stalledPercent := (float64(*metrics.StallsTotal) / float64(*metrics.Cycles)) * 100.0
		metrics.StalledCyclesPercent = &stalledPercent
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

	if pc.cgroupFile != nil {
		pc.cgroupFile.Close()
		pc.cgroupFile = nil
		pc.cgroupFd = -1
	}
}
