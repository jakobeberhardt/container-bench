package scheduler

import (
	"container-bench/internal/accounting"
	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/probe"
	proberesources "container-bench/internal/probe/resources"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

type ProbeAllocationScheduler struct {
	name               string
	version            string
	schedulerLogger    *logrus.Logger
	hostConfig         *host.HostConfig
	containers         []ContainerInfo
	rdtAccountant      *accounting.RDTAccountant
	prober             *probe.Probe
	config             *config.SchedulerConfig
	accounting         Accounts
	warmupCompleteTime time.Time
	benchmarkID        int

	// Probing state
	probeStarted  bool
	probeComplete bool
	probeResults  []*proberesources.AllocationProbeResult
}

func NewProbeAllocationScheduler() *ProbeAllocationScheduler {
	return &ProbeAllocationScheduler{
		name:            "probe-allocation",
		version:         "1.0.0",
		schedulerLogger: logging.GetSchedulerLogger(),
		probeStarted:    false,
		probeComplete:   false,
	}
}

func (as *ProbeAllocationScheduler) Initialize(accountant *accounting.RDTAccountant, containers []ContainerInfo, schedulerConfig *config.SchedulerConfig) error {
	as.rdtAccountant = accountant
	as.containers = containers
	as.config = schedulerConfig
	as.accounting = Accounts{}
	as.accounting.accounts = make([]Account, len(containers))

	for i := range containers {
		as.accounting.accounts[i] = Account{
			L3Allocation:              0.0,
			MemoryBandwidthAllocation: 0.0,
		}
	}

	warmupSeconds := 0
	if as.config != nil && as.config.Allocator != nil && as.config.Allocator.WarmupT > 0 {
		warmupSeconds = as.config.Allocator.WarmupT
		as.schedulerLogger.WithField("seconds", as.config.Allocator.WarmupT).Info("Warming up benchmark")
	}

	as.warmupCompleteTime = time.Now().Add(time.Duration(warmupSeconds) * time.Second)

	as.schedulerLogger.WithFields(logrus.Fields{
		"containers":     len(containers),
		"warmup_seconds": warmupSeconds,
	}).Info("Probe allocation scheduler initialized")
	return nil
}

func (as *ProbeAllocationScheduler) ProcessDataFrames(dataframes *dataframe.DataFrames) error {
	if time.Now().Before(as.warmupCompleteTime) {
		return nil
	}

	if as.probeComplete {
		return nil
	}

	if as.probeStarted {
		return nil
	}

	// Start the allocation probe
	as.probeStarted = true

	// Get configuration from scheduler config
	allocCfg := as.config.Allocator
	if allocCfg == nil {
		as.schedulerLogger.Warn("No allocator config, using defaults")
		allocCfg = &config.AllocatorConfig{
			Duration:   10,
			StepSizeL3: 1,
			StepSizeMB: 10,
		}
	}

	// Apply defaults for step sizes if not set
	stepSizeL3 := allocCfg.StepSizeL3
	if stepSizeL3 == 0 {
		stepSizeL3 = 1
	}
	stepSizeMB := allocCfg.StepSizeMB
	if stepSizeMB == 0 {
		stepSizeMB = 10
	}

	// Get MBA hardware granularity from host config
	mbaGranularity := 10  // Default fallback
	mbaMinBandwidth := 10 // Default fallback
	if as.hostConfig != nil && as.hostConfig.RDT.MBAGranularity > 0 {
		mbaGranularity = as.hostConfig.RDT.MBAGranularity
	}
	if as.hostConfig != nil && as.hostConfig.RDT.MBAMinBandwidth > 0 {
		mbaMinBandwidth = as.hostConfig.RDT.MBAMinBandwidth
	}

	// Validate and adjust MBA step size to hardware granularity
	if stepSizeMB%mbaGranularity != 0 {
		originalStep := stepSizeMB
		stepSizeMB = ((stepSizeMB + mbaGranularity - 1) / mbaGranularity) * mbaGranularity
		as.schedulerLogger.WithFields(logrus.Fields{
			"requested_step": originalStep,
			"adjusted_step":  stepSizeMB,
			"hw_granularity": mbaGranularity,
		}).Warn("MBA step size adjusted to match hardware granularity")
	}

	// Get total ways from host config (single source of truth)
	totalWays := 12 // Fallback default
	if as.hostConfig != nil && as.hostConfig.L3Cache.WaysPerCache > 0 {
		totalWays = as.hostConfig.L3Cache.WaysPerCache
	}

	// Determine allocation range from config or defaults
	minL3Ways := 1
	maxL3Ways := totalWays
	minMemBandwidth := 10.0
	maxMemBandwidth := 100.0
	order := "asc"
	isolateOthers := true
	forceReallocation := false

	if allocCfg.MinL3Ways > 0 {
		minL3Ways = allocCfg.MinL3Ways
	}
	if allocCfg.MaxL3Ways > 0 {
		maxL3Ways = allocCfg.MaxL3Ways
	}
	if allocCfg.MinMemBandwidth > 0 {
		minMemBandwidth = allocCfg.MinMemBandwidth
	}
	if allocCfg.MaxMemBandwidth > 0 {
		maxMemBandwidth = allocCfg.MaxMemBandwidth
	}
	if allocCfg.Order != "" {
		order = allocCfg.Order
	}
	isolateOthers = allocCfg.IsolateOthers
	forceReallocation = allocCfg.ForceReallocation

	// Validate and adjust memory bandwidth min/max to hardware granularity
	if int(minMemBandwidth)%mbaGranularity != 0 {
		originalMin := minMemBandwidth
		minMemBandwidth = float64(((int(minMemBandwidth) + mbaGranularity - 1) / mbaGranularity) * mbaGranularity)
		if minMemBandwidth < float64(mbaMinBandwidth) {
			minMemBandwidth = float64(mbaMinBandwidth)
		}
		as.schedulerLogger.WithFields(logrus.Fields{
			"requested_min":  originalMin,
			"adjusted_min":   minMemBandwidth,
			"hw_granularity": mbaGranularity,
			"hw_minimum":     mbaMinBandwidth,
		}).Warn("MBA minimum bandwidth adjusted to match hardware constraints")
	}

	if int(maxMemBandwidth)%mbaGranularity != 0 {
		originalMax := maxMemBandwidth
		maxMemBandwidth = float64((int(maxMemBandwidth) / mbaGranularity) * mbaGranularity)
		if maxMemBandwidth < minMemBandwidth {
			maxMemBandwidth = minMemBandwidth
		}
		as.schedulerLogger.WithFields(logrus.Fields{
			"requested_max":  originalMax,
			"adjusted_max":   maxMemBandwidth,
			"hw_granularity": mbaGranularity,
		}).Warn("MBA maximum bandwidth adjusted to match hardware constraints")
	}

	// Calculate number of distinct allocations to determine duration per allocation
	// Duration in config is total time, so we need to divide by number of allocations
	numL3Steps := ((maxL3Ways - minL3Ways) / stepSizeL3) + 1
	numMemSteps := int((maxMemBandwidth-minMemBandwidth)/float64(stepSizeMB)) + 1
	totalAllocations := numL3Steps * numMemSteps

	// Calculate duration per allocation in milliseconds
	durationPerAllocMs := 1000 // Default 1 second
	if totalAllocations > 0 && allocCfg.Duration > 0 {
		// Convert total duration (seconds) to milliseconds and divide by allocations
		durationPerAllocMs = (allocCfg.Duration * 1000) / totalAllocations
		if durationPerAllocMs < 500 {
			as.schedulerLogger.WithFields(logrus.Fields{
				"total_allocations":     totalAllocations,
				"total_duration":        allocCfg.Duration,
				"duration_per_alloc_ms": durationPerAllocMs,
				"duration_per_alloc_s":  float64(durationPerAllocMs) / 1000.0,
			}).Warn("Duration per allocation is less than 500ms - this may result in insufficient time for metrics collection")
		}
	}

	estimatedTotalTimeMs := totalAllocations * durationPerAllocMs

	as.schedulerLogger.WithFields(logrus.Fields{
		"configured_duration_s": allocCfg.Duration,
		"estimated_total_s":     float64(estimatedTotalTimeMs) / 1000.0,
		"step_size_l3":          stepSizeL3,
		"step_size_mb":          stepSizeMB,
		"l3_range":              fmt.Sprintf("%d-%d", minL3Ways, maxL3Ways),
		"mb_range":              fmt.Sprintf("%.0f%%-%.0f%%", minMemBandwidth, maxMemBandwidth),
		"num_l3_steps":          numL3Steps,
		"num_mem_steps":         numMemSteps,
		"total_allocations":     totalAllocations,
		"duration_per_alloc_ms": durationPerAllocMs,
		"duration_per_alloc_s":  float64(durationPerAllocMs) / 1000.0,
	}).Info("Calculated allocation probe timing")

	// Build allocation range configuration
	probeRange := proberesources.AllocationRange{
		MinL3Ways:         minL3Ways,
		MaxL3Ways:         maxL3Ways,
		MinMemBandwidth:   minMemBandwidth,
		MaxMemBandwidth:   maxMemBandwidth,
		StepL3Ways:        stepSizeL3,
		StepMemBandwidth:  float64(stepSizeMB),
		Order:             order,
		DurationPerAlloc:  durationPerAllocMs,
		MaxTotalDuration:  allocCfg.Duration,
		SocketID:          0,
		IsolateOthers:     isolateOthers,
		ForceReallocation: forceReallocation,
	}

	// Only probe first container for now
	if len(as.containers) == 0 {

		as.schedulerLogger.Error("No containers to probe")
		as.probeComplete = true
		return nil
	}
	if as.containers[0].PID == 0 {
		// Target container not running yet
		return nil
	}

	// Build target container info from scheduler's ContainerInfo
	targetContainer := proberesources.ContainerInfo{
		Index:   as.containers[0].Index,
		PID:     as.containers[0].PID,
		ID:      as.containers[0].ContainerID,
		Name:    "", // Not available in scheduler ContainerInfo
		Cores:   "", // Not available in scheduler ContainerInfo
		Socket:  0,  // Assume socket 0
		Image:   "", // Not available in scheduler ContainerInfo
		Command: "", // Not available in scheduler ContainerInfo
	}

	// Build list of other containers for isolation
	var otherContainers []proberesources.ContainerInfo
	for i := 1; i < len(as.containers); i++ {
		otherContainers = append(otherContainers, proberesources.ContainerInfo{
			Index:   as.containers[i].Index,
			PID:     as.containers[i].PID,
			ID:      as.containers[i].ContainerID,
			Name:    "",
			Cores:   "",
			Socket:  0,
			Image:   "",
			Command: "",
		})
	}

	as.schedulerLogger.WithFields(logrus.Fields{
		"target_index":   targetContainer.Index,
		"target_pid":     targetContainer.PID,
		"min_l3_ways":    probeRange.MinL3Ways,
		"max_l3_ways":    probeRange.MaxL3Ways,
		"step_l3_ways":   probeRange.StepL3Ways,
		"min_mem_bw":     probeRange.MinMemBandwidth,
		"max_mem_bw":     probeRange.MaxMemBandwidth,
		"step_mem_bw":    probeRange.StepMemBandwidth,
		"duration_per":   probeRange.DurationPerAlloc,
		"isolate_others": probeRange.IsolateOthers,
	}).Info("Starting allocation probe")

	// Define break condition based on configuration
	var breakCondition func(*proberesources.AllocationResult, []proberesources.AllocationResult) bool
	if allocCfg.BreakOnEfficiency != nil && *allocCfg.BreakOnEfficiency > 0 {
		threshold := *allocCfg.BreakOnEfficiency
		breakCondition = func(result *proberesources.AllocationResult, allResults []proberesources.AllocationResult) bool {
			if result.IPCEfficiency > threshold {
				as.schedulerLogger.WithFields(logrus.Fields{
					"ipc_efficiency": result.IPCEfficiency,
					"threshold":      threshold,
					"l3_ways":        result.L3Ways,
					"mem_bandwidth":  result.MemBandwidth,
				}).Info("Break condition met: IPC efficiency exceeded threshold")
				return true
			}
			return false
		}
		as.schedulerLogger.WithField("threshold", threshold).Info("Break condition enabled for IPC efficiency")
	} else {
		// No break condition - test all allocations
		breakCondition = nil
		as.schedulerLogger.Info("Break condition disabled - will test all allocations")
	}

	// Run the probe - note the parameter order matches the function signature
	result, err := proberesources.ProbeAllocation(
		targetContainer,
		otherContainers,
		dataframes,
		as.rdtAccountant,
		probeRange,
		breakCondition,
		as.benchmarkID,
	)

	if err != nil {
		as.schedulerLogger.WithError(err).Error("Allocation probe failed")
		as.probeComplete = true
		return err
	}

	as.probeResults = append(as.probeResults, result)
	as.probeComplete = true

	// Log summary
	as.schedulerLogger.WithFields(logrus.Fields{
		"probe_run":         len(as.probeResults),
		"total_probe_time":  result.TotalProbeTime,
		"allocations_tried": len(result.Allocations),
		"aborted":           result.Aborted,
	}).Info("Allocation probe complete")

	return nil
}

func (as *ProbeAllocationScheduler) Shutdown() error {
	err := as.rdtAccountant.Cleanup()
	if err != nil {
		as.schedulerLogger.WithError(err).Error("Could not clean up RDT classes")
	}
	return nil
}

func (as *ProbeAllocationScheduler) GetVersion() string {
	return as.version
}

func (as *ProbeAllocationScheduler) SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	as.schedulerLogger.SetLevel(logLevel)
	return nil
}

func (as *ProbeAllocationScheduler) SetHostConfig(hostConfig *host.HostConfig) {
	as.hostConfig = hostConfig
}

func (as *ProbeAllocationScheduler) SetProbe(prober *probe.Probe) {
	as.prober = prober
	as.schedulerLogger.Debug("Probe injected into scheduler")
}

func (as *ProbeAllocationScheduler) SetBenchmarkID(benchmarkID int) {
	as.benchmarkID = benchmarkID
	as.schedulerLogger.WithField("benchmark_id", benchmarkID).Debug("Benchmark ID set")
}

func (as *ProbeAllocationScheduler) OnContainerStart(info ContainerInfo) error {
	for i := range as.containers {
		if as.containers[i].Index == info.Index {
			as.containers[i].PID = info.PID
			as.containers[i].ContainerID = info.ContainerID
			return nil
		}
	}
	as.containers = append(as.containers, info)
	return nil
}

func (as *ProbeAllocationScheduler) OnContainerStop(containerIndex int) error {
	for i := range as.containers {
		if as.containers[i].Index == containerIndex {
			as.containers[i].PID = 0
			return nil
		}
	}
	return nil
}

// GetAllocationProbeResults returns allocation probe results if available
func (as *ProbeAllocationScheduler) GetAllocationProbeResults() []*proberesources.AllocationProbeResult {
	return as.probeResults
}

// HasAllocationProbeResults returns true if allocation probe results are available
func (as *ProbeAllocationScheduler) HasAllocationProbeResults() bool {
	return len(as.probeResults) > 0
}
