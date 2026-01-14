// Package scheduler contains the dynamic scheduler implementation.
// This file provides a cleaner implementation that uses the rdtmanager package.
package scheduler

import (
	"container-bench/internal/accounting"
	"container-bench/internal/allocation"
	"container-bench/internal/config"
	"container-bench/internal/cpuallocator"
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/probe"
	proberesources "container-bench/internal/probe/resources"
	"container-bench/internal/rdtmanager"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// DynamicSchedulerV2 is a refactored dynamic scheduler that uses the RDTManager
// for clean socket-aware resource management with automatic consolidation.
//
// Design principles:
// 1. Priority containers get dedicated RDT allocations
// 2. When a priority job arrives, assume it needs maximum resources initially
// 3. Admit to the socket with most available resources
// 4. After probing completes, commit the actual needed allocation
// 5. When containers finish, consolidate to avoid fragmentation
// 6. QoS enforcement: ensure IPC_current >= IPC_goal
type DynamicSchedulerV2 struct {
	name            string
	version         string
	schedulerLogger *logrus.Logger

	hostConfig    *host.HostConfig
	containers    []ContainerInfo
	rdtAccountant *accounting.RDTAccountant
	rdtManager    *rdtmanager.RDTManager
	prober        *probe.Probe
	config        *config.SchedulerConfig
	collectorFreq CollectorFrequencyController
	cpuAllocator  cpuallocator.Allocator
	benchmarkID   int

	mu sync.Mutex

	// Container tracking
	profiles map[int]*dynamicV2Profile

	// RDT configuration
	totalWays int
	sockets   int

	// Probing state
	activeProbe   *activeProbeState
	lastProbeDone time.Time

	// Queue for containers awaiting probing
	probeQueue  []int
	probeQueued map[int]bool

	// Results from allocation probes
	allocationProbeResults []*proberesources.AllocationProbeResult
}

// dynamicV2Profile tracks state for a single container.
type dynamicV2Profile struct {
	index       int
	pid         int
	containerID string
	containerKey string
	startedAt   time.Time

	critical bool
	socket   int

	// Granted resources from probe allocation (upper bound for probing)
	grantedWays int
	grantedMem  float64

	// needsReprobe is set when QoS was not met during probing.
	// Container will be re-probed when resources become available.
	needsReprobe bool

	// For non-critical containers, track stalls for rebalancing
	stallsL3MissPercent float64
	stallsL3MissProbed  bool
	stallsUpdatedAt     time.Time
}

// activeProbeState tracks an in-progress allocation probe.
type activeProbeState struct {
	containerIndex int
	socket         int
	runner         *proberesources.AllocationProbeRunner

	// QoS guarantee search state
	targetIPCE    float64
	hasFoundValid bool
	validWays     int
	validMem      float64

	// Async stepping
	stepCtx    context.Context
	stepCancel context.CancelFunc
	stepDone   chan struct{}
}

const (
	// System reserve per socket - ensures shared pool always has minimum resources
	systemReserveWays = 1
	systemReserveMem  = 10.0
)

// NewDynamicSchedulerV2 creates a new instance of the refactored dynamic scheduler.
func NewDynamicSchedulerV2() *DynamicSchedulerV2 {
	return &DynamicSchedulerV2{
		name:            "dynamic-v2",
		version:         "2.0.0",
		schedulerLogger: logging.GetSchedulerLogger(),
		profiles:        make(map[int]*dynamicV2Profile),
		probeQueued:     make(map[int]bool),
	}
}

func (s *DynamicSchedulerV2) GetVersion() string { return s.version }

func (s *DynamicSchedulerV2) SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	s.schedulerLogger.SetLevel(logLevel)
	return nil
}

func (s *DynamicSchedulerV2) SetHostConfig(hostConfig *host.HostConfig) {
	s.hostConfig = hostConfig
}

func (s *DynamicSchedulerV2) SetCPUAllocator(allocator cpuallocator.Allocator) {
	s.cpuAllocator = allocator
}

func (s *DynamicSchedulerV2) SetProbe(prober *probe.Probe) {
	s.prober = prober
}

func (s *DynamicSchedulerV2) SetBenchmarkID(benchmarkID int) {
	s.benchmarkID = benchmarkID
}

func (s *DynamicSchedulerV2) SetCollectorFrequencyController(controller CollectorFrequencyController) {
	s.collectorFreq = controller
}

// GetAllocationProbeResults returns completed probe results.
func (s *DynamicSchedulerV2) GetAllocationProbeResults() []*proberesources.AllocationProbeResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*proberesources.AllocationProbeResult(nil), s.allocationProbeResults...)
}

func (s *DynamicSchedulerV2) AssignCPUCores(containerIndex int) ([]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cpuAllocator == nil {
		return nil, nil
	}

	var cfg *config.ContainerConfig
	for i := range s.containers {
		if s.containers[i].Index == containerIndex {
			cfg = s.containers[i].Config
			break
		}
	}
	if cfg == nil {
		return nil, nil
	}

	assigned, err := s.cpuAllocator.EnsureAssigned(containerIndex, cfg)
	if err != nil {
		return nil, err
	}

	// Track socket from CPU assignment
	sock := 0
	if s.hostConfig != nil && len(assigned) > 0 {
		if v, err := s.hostConfig.SocketOfPhysicalCPUs(assigned); err == nil {
			sock = v
		}
	}
	p := s.getOrCreateProfile(containerIndex)
	p.socket = sock

	return assigned, nil
}

func (s *DynamicSchedulerV2) Initialize(accountant *accounting.RDTAccountant, containers []ContainerInfo, schedulerConfig *config.SchedulerConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rdtAccountant = accountant
	s.containers = containers
	s.config = schedulerConfig
	s.activeProbe = nil
	s.probeQueue = nil
	s.allocationProbeResults = nil
	for k := range s.probeQueued {
		delete(s.probeQueued, k)
	}

	if s.hostConfig == nil {
		hc, err := host.GetHostConfig()
		if err != nil {
			return fmt.Errorf("failed to get host config: %w", err)
		}
		s.hostConfig = hc
	}
	s.sockets = s.hostConfig.Topology.Sockets
	if s.sockets <= 0 {
		s.sockets = 1
	}
	if s.sockets > 2 {
		return fmt.Errorf("dynamic scheduler supports up to 2 sockets, got %d", s.sockets)
	}

	// Initialize profiles for all containers (always, even without RDT)
	for _, c := range containers {
		p := s.getOrCreateProfile(c.Index)
		p.index = c.Index
		p.critical = c.Config != nil && c.Config.Critical
		if p.containerKey == "" && c.Config != nil {
			p.containerKey = c.Config.KeyName
		}
		if p.socket == 0 && s.cpuAllocator != nil {
			if cpus, ok := s.cpuAllocator.Get(c.Index); ok && s.hostConfig != nil {
				if sock, err := s.hostConfig.SocketOfPhysicalCPUs(cpus); err == nil {
					p.socket = sock
				}
			}
		}
	}

	if s.rdtAccountant == nil {
		s.schedulerLogger.Info("Dynamic scheduler V2 initialized without RDT (critical allocations disabled)")
		return nil
	}

	s.totalWays = s.rdtAccountant.GetTotalWays(0)
	if s.totalWays <= 0 {
		return fmt.Errorf("invalid total cache ways: %d", s.totalWays)
	}

	// Initialize RDT manager with the accountant's allocator
	backend := rdtmanager.NewRDTAllocatorAdapter(
		s.createRDTAllocatorFromAccountant(),
	)

	mgr, err := rdtmanager.NewRDTManager(rdtmanager.Config{
		TotalWays:    s.totalWays,
		Sockets:      s.sockets,
		ReservedWays: systemReserveWays,
		ReservedMem:  systemReserveMem,
		SharedClass:  "system/default",
	}, backend)
	if err != nil {
		return fmt.Errorf("failed to create RDT manager: %w", err)
	}

	if err := mgr.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize RDT manager: %w", err)
	}

	s.rdtManager = mgr

	s.schedulerLogger.WithFields(logrus.Fields{
		"containers": len(containers),
		"rdt":        true,
		"ways":       s.totalWays,
		"sockets":    s.sockets,
	}).Info("Dynamic scheduler V2 initialized")

	return nil
}

// createRDTAllocatorFromAccountant creates an allocator interface from the accountant.
// This is a workaround until we fully migrate away from the accountant.
func (s *DynamicSchedulerV2) createRDTAllocatorFromAccountant() allocation.RDTAllocator {
	// The accountant wraps an allocator internally, but we need direct access.
	// For now, create a new allocator instance.
	return allocation.NewDefaultRDTAllocator()
}

func (s *DynamicSchedulerV2) ProcessDataFrames(dfs *dataframe.DataFrames) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update non-critical stall metrics for rebalancing
	s.updateNonCriticalStalls(dfs)

	// Check if active probe completed
	if s.activeProbe != nil {
		select {
		case <-s.activeProbe.stepDone:
			return s.finalizeProbe()
		default:
			return nil
		}
	}

	if s.rdtManager == nil {
		return nil
	}

	// Try to start next queued probe
	idx, ok := s.peekProbeHead()
	if !ok {
		return nil
	}

	// Check cooldown
	cooldownSeconds := s.getCooldownSeconds()
	if cooldownSeconds > 0 && !s.lastProbeDone.IsZero() {
		if time.Since(s.lastProbeDone) < time.Duration(cooldownSeconds)*time.Second {
			return nil
		}
	}

	// Check warmup
	warmupSeconds := s.getWarmupSeconds()
	if p := s.profiles[idx]; p != nil {
		if warmupSeconds > 0 && time.Since(p.startedAt) < time.Duration(warmupSeconds)*time.Second {
			return nil
		}
	}

	started, err := s.startProbe(dfs, idx)
	if err != nil {
		return err
	}
	if started {
		s.popProbeHead()
	}

	return nil
}

func (s *DynamicSchedulerV2) OnContainerStart(info ContainerInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update container list
	for i := range s.containers {
		if s.containers[i].Index == info.Index {
			s.containers[i].PID = info.PID
			s.containers[i].ContainerID = info.ContainerID
			s.containers[i].Config = info.Config
			break
		}
	}

	p := s.getOrCreateProfile(info.Index)
	p.pid = info.PID
	p.containerID = info.ContainerID
	p.startedAt = time.Now()
	p.critical = info.Config != nil && info.Config.Critical
	if info.Config != nil {
		p.containerKey = info.Config.KeyName
	}

	if s.rdtManager == nil || info.PID == 0 {
		return nil
	}

	// Priority containers: Admission strategy
	// 1. Assign to socket symbolically (for placement, no capacity consumed)
	// 2. Container stays in system default COS until probing starts
	// 3. Queue for probing - probe will allocate actual resources
	if p.critical {
		maxWays, maxMem := s.getMaxProbeResources()

		// Symbolic socket assignment (no capacity consumed yet)
		// Container stays in system default until probing starts
		assignment, canSatisfy, err := s.rdtManager.AssignSocket(
			info.Index,
			p.containerKey,
			maxWays,
			maxMem,
		)

		if err != nil {
			s.schedulerLogger.WithError(err).WithFields(logrus.Fields{
				"container": info.Index,
			}).Error("Failed to assign socket for critical container")
			// Continue without RDT isolation - container runs in shared pool
		} else {
			// Update socket and CPUs if needed
			if assignment.Socket != p.socket && s.cpuAllocator != nil {
				if moved, err := s.cpuAllocator.Move(info.Index, info.ContainerID, assignment.Socket); err == nil {
					s.schedulerLogger.WithFields(logrus.Fields{
						"container":   info.Index,
						"from_socket": p.socket,
						"to_socket":   assignment.Socket,
						"cpus":        moved,
					}).Debug("Moved critical container to assigned socket")
				}
			}
			p.socket = assignment.Socket

			if !canSatisfy {
				s.schedulerLogger.WithFields(logrus.Fields{
					"container":     info.Index,
					"socket":        assignment.Socket,
					"requested_ways": maxWays,
				}).Warn("Socket may not have enough resources - probing will use available")
			} else {
				s.schedulerLogger.WithFields(logrus.Fields{
					"container": info.Index,
					"socket":    assignment.Socket,
				}).Info("Socket assigned for critical container (stays in system default until probing)")
			}

			// NOTE: Container stays in system default - resources allocated at probe start
		}

		// Queue for probing to determine actual needs
		s.enqueueProbe(info.Index)
	}

	return nil
}

func (s *DynamicSchedulerV2) OnContainerStop(containerIndex int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cpuAllocator != nil {
		s.cpuAllocator.Release(containerIndex)
	}

	// Remove from probe queue
	delete(s.probeQueued, containerIndex)

	// Abort if actively probing
	if s.activeProbe != nil && s.activeProbe.containerIndex == containerIndex {
		if s.activeProbe.runner != nil {
			s.activeProbe.runner.Abort("container_stopped")
		}
		if s.activeProbe.stepCancel != nil {
			s.activeProbe.stepCancel()
		}
		s.lastProbeDone = time.Now()
		s.activeProbe = nil
	}

	// Release RDT allocation and consolidate
	if s.rdtManager != nil {
		if err := s.rdtManager.Release(containerIndex); err != nil {
			s.schedulerLogger.WithError(err).WithField("container", containerIndex).
				Warn("Failed to release RDT allocation")
		}
	}

	p := s.profiles[containerIndex]
	if p != nil {
		p.pid = 0
		p.containerID = ""
	}

	// Re-queue containers that need reprobing (QoS wasn't met)
	// Now that resources are freed, they may be able to meet QoS
	s.requeueContainersNeedingReprobe()

	// Allow queued probes to start immediately
	if s.activeProbe == nil {
		if _, has := s.peekProbeHead(); has {
			s.lastProbeDone = time.Time{}
		}
	}

	return nil
}

// requeueContainersNeedingReprobe finds containers that failed to meet QoS
// and re-queues them for probing now that resources may be available.
func (s *DynamicSchedulerV2) requeueContainersNeedingReprobe() {
	for idx, p := range s.profiles {
		if p != nil && p.critical && p.needsReprobe && p.containerID != "" {
			if !s.probeQueued[idx] {
				s.enqueueProbe(idx)
				p.needsReprobe = false // Will be set again if probe still fails
				s.schedulerLogger.WithField("container", idx).
					Info("Re-queued container for probing (QoS retry)")
			}
		}
	}
}

func (s *DynamicSchedulerV2) Shutdown() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.activeProbe != nil {
		if s.activeProbe.runner != nil {
			s.activeProbe.runner.Abort("shutdown")
		}
		if s.activeProbe.stepCancel != nil {
			s.activeProbe.stepCancel()
		}
		s.activeProbe = nil
	}

	if s.rdtManager != nil {
		return s.rdtManager.Cleanup()
	}

	return nil
}

// --- Internal methods ---

func (s *DynamicSchedulerV2) getOrCreateProfile(containerIndex int) *dynamicV2Profile {
	p := s.profiles[containerIndex]
	if p == nil {
		p = &dynamicV2Profile{index: containerIndex}
		s.profiles[containerIndex] = p
	}
	return p
}

func (s *DynamicSchedulerV2) getMaxProbeResources() (ways int, mem float64) {
	cfg := s.config
	ways = 1
	mem = 10.0
	if cfg != nil && cfg.Prober != nil {
		if cfg.Prober.MaxL3Ways > 0 {
			ways = cfg.Prober.MaxL3Ways
		}
		if cfg.Prober.MaxMemBandwidth > 0 {
			mem = cfg.Prober.MaxMemBandwidth
		}
	}
	return
}

func (s *DynamicSchedulerV2) getMinProbeResources() (ways int, mem float64) {
	cfg := s.config
	ways = 1
	mem = 10.0
	if cfg != nil && cfg.Prober != nil {
		if cfg.Prober.MinL3Ways > 0 {
			ways = cfg.Prober.MinL3Ways
		}
		if cfg.Prober.MinMemBandwidth > 0 {
			mem = cfg.Prober.MinMemBandwidth
		}
	}
	return
}

func (s *DynamicSchedulerV2) getCooldownSeconds() int {
	if s.config != nil {
		if s.config.Prober != nil && s.config.Prober.CooldownT > 0 {
			return s.config.Prober.CooldownT
		}
		if s.config.CooldownT > 0 {
			return s.config.CooldownT
		}
	}
	return 2
}

func (s *DynamicSchedulerV2) getWarmupSeconds() int {
	if s.config != nil {
		if s.config.Prober != nil && s.config.Prober.WarmupT > 0 {
			return s.config.Prober.WarmupT
		}
		if s.config.WarmupT > 0 {
			return s.config.WarmupT
		}
	}
	return 5
}

func (s *DynamicSchedulerV2) updateNonCriticalStalls(dfs *dataframe.DataFrames) {
	if dfs == nil {
		return
	}
	for idx, p := range s.profiles {
		if p == nil || p.pid == 0 || p.critical {
			continue
		}
		cdf := dfs.GetContainer(idx)
		if cdf == nil {
			continue
		}
		step := cdf.GetLatestStep()
		if step == nil || step.Perf == nil || step.Perf.StallsL3MissPercent == nil {
			continue
		}
		p.stallsL3MissPercent = *step.Perf.StallsL3MissPercent
		p.stallsL3MissProbed = true
		p.stallsUpdatedAt = step.Timestamp
	}
}

func (s *DynamicSchedulerV2) enqueueProbe(containerIndex int) {
	if s.probeQueued == nil {
		s.probeQueued = make(map[int]bool)
	}
	if s.probeQueued[containerIndex] {
		return
	}
	s.probeQueued[containerIndex] = true
	s.probeQueue = append(s.probeQueue, containerIndex)
}

func (s *DynamicSchedulerV2) peekProbeHead() (int, bool) {
	for len(s.probeQueue) > 0 {
		idx := s.probeQueue[0]
		if s.probeQueued[idx] {
			return idx, true
		}
		s.probeQueue = s.probeQueue[1:]
	}
	return -1, false
}

func (s *DynamicSchedulerV2) popProbeHead() {
	if len(s.probeQueue) == 0 {
		return
	}
	idx := s.probeQueue[0]
	s.probeQueue = s.probeQueue[1:]
	delete(s.probeQueued, idx)
}

func (s *DynamicSchedulerV2) startProbe(dfs *dataframe.DataFrames, containerIndex int) (bool, error) {
	p := s.profiles[containerIndex]
	if p == nil || !p.critical || p.containerID == "" {
		return false, nil
	}

	// Get container config
	var containerCfg *config.ContainerConfig
	for i := range s.containers {
		if s.containers[i].Index == containerIndex {
			containerCfg = s.containers[i].Config
			break
		}
	}

	// Get probe parameters
	minWays, minMem := s.getMinProbeResources()
	stepWays := 1
	stepMem := 10.0
	order := "asc"
	budgetSeconds := 2.0
	var probingFrequency time.Duration
	outlierDrop := 0

	// Start probe allocation - allocates max available resources on the socket
	// This converts socket assignment to actual RDT allocation
	handle, err := s.rdtManager.StartProbeAllocation(containerIndex)
	if err != nil {
		s.schedulerLogger.WithError(err).WithField("container", containerIndex).
			Warn("Failed to start probe allocation - will retry later")
		return false, nil // Retry later, don't error
	}

	// Apply the configuration
	if err := s.rdtManager.ApplyIfDirty(); err != nil {
		s.schedulerLogger.WithError(err).Warn("Failed to apply RDT configuration")
	}

	// Move container processes to the dedicated class
	if pids, err := readContainerCgroupPIDs(p.containerID); err == nil {
		for _, pid := range pids {
			_ = s.rdtManager.MoveContainerToClass(pid, containerIndex)
		}
	}

	// Use allocated resources as max for probing
	maxWays := handle.L3Ways
	maxMem := handle.MemBandwidth
	socket := handle.Socket
	p.socket = socket
	p.grantedWays = maxWays
	p.grantedMem = maxMem

	s.schedulerLogger.WithFields(logrus.Fields{
		"container": containerIndex,
		"socket":    socket,
		"max_ways":  maxWays,
		"max_mem":   maxMem,
		"class":     handle.ClassName,
	}).Info("Started probe with max available resources")

	if s.config != nil && s.config.Prober != nil {
		pc := s.config.Prober
		if pc.StepL3Ways > 0 {
			stepWays = pc.StepL3Ways
		}
		if pc.StepMemBandwidth > 0 {
			stepMem = pc.StepMemBandwidth
		}
		if pc.Order != "" {
			order = pc.Order
		}
		if pc.ProbingT > 0 {
			budgetSeconds = pc.ProbingT
		}
		if pc.ProbingFrequency > 0 {
			probingFrequency = time.Duration(pc.ProbingFrequency) * time.Millisecond
		}
		if pc.DropOutliers > 0 {
			outlierDrop = pc.DropOutliers
		}
	}

	// Validate we have enough for minimum probe
	if maxWays < minWays || maxMem < minMem {
		s.schedulerLogger.WithFields(logrus.Fields{
			"container": containerIndex,
			"socket":    socket,
			"min_ways":  minWays,
			"max_ways":  maxWays,
		}).Debug("Insufficient resources for probing")
		return false, nil
	}

	// Get target IPCE for QoS guarantee
	var targetIPCE float64
	if containerCfg != nil {
		if v, ok := containerCfg.GetIPCEfficancy(); ok && v > 0 {
			if v > 1 {
				targetIPCE = v / 100.0
			} else {
				targetIPCE = v
			}
		}
	}

	// Build probe configuration
	probeRange := proberesources.AllocationRange{
		MinL3Ways:        minWays,
		MaxL3Ways:        maxWays,
		StepL3Ways:       stepWays,
		MinMemBandwidth:  minMem,
		MaxMemBandwidth:  maxMem,
		StepMemBandwidth: stepMem,
		Order:            order,
		SocketID:         socket,
	}

	breaks := proberesources.AllocationProbeBreakPolicy{}
	if targetIPCE > 0 {
		breaks.AcceptableIPCEfficiency = &targetIPCE
	}

	opts := proberesources.AllocationProbeOptions{
		ProbingFrequency: probingFrequency,
		OutlierDrop:      outlierDrop,
		BaselineFirst:    false,
	}

	target := proberesources.AllocationProbeTarget{
		BenchmarkID:    s.benchmarkID,
		ContainerID:    p.containerID,
		ContainerName:  p.containerKey,
		ContainerIndex: containerIndex,
		ContainerSocket: socket,
	}
	if containerCfg != nil {
		target.ContainerImage = containerCfg.Image
		target.ContainerCommand = containerCfg.Command
		target.ContainerCores = containerCfg.Core
	}

	cb := proberesources.AllocationProbeCallbacks{
		ApplyAllocation: func(ways int, mem float64) error {
			return s.applyProbeAllocation(containerIndex, socket, ways, mem)
		},
		ResetToBenchmark: func() error {
			return s.resetToShared(containerIndex)
		},
		LatestStepNumber: func(dfs *dataframe.DataFrames, idx int) int {
			return latestStepNumber(dfs, idx)
		},
		OverrideContainerCollectorFrequency: func(idx int, freq time.Duration) (func(), error) {
			if s.collectorFreq == nil {
				return func() {}, nil
			}
			return s.collectorFreq.OverrideContainerFrequency(idx, freq)
		},
	}

	runner := proberesources.NewAllocationProbeRunnerFromRange(
		target,
		probeRange,
		time.Duration(float64(time.Second)*budgetSeconds),
		500*time.Millisecond,
		breaks,
		opts,
		cb,
	)
	if runner == nil || runner.NumCandidates() == 0 {
		return false, fmt.Errorf("no probe candidates")
	}

	ap := &activeProbeState{
		containerIndex: containerIndex,
		socket:         socket,
		runner:         runner,
		targetIPCE:     targetIPCE,
	}
	s.activeProbe = ap

	if err := runner.Start(dfs); err != nil {
		s.activeProbe = nil
		return false, err
	}
	s.startProbeStepper(dfs, ap)

	s.schedulerLogger.WithFields(logrus.Fields{
		"container":   containerIndex,
		"socket":      socket,
		"candidates":  runner.NumCandidates(),
		"min_ways":    minWays,
		"max_ways":    maxWays,
		"target_ipce": targetIPCE,
		"order":       order,
	}).Info("Started allocation probe for critical container")

	return true, nil
}

func (s *DynamicSchedulerV2) startProbeStepper(dfs *dataframe.DataFrames, ap *activeProbeState) {
	if ap == nil || ap.runner == nil {
		return
	}

	cd := ap.runner.CandidateDuration()
	stepEvery := 100 * time.Millisecond
	if cd > 0 {
		stepEvery = cd / 10
	}
	if stepEvery < 10*time.Millisecond {
		stepEvery = 10 * time.Millisecond
	}

	ap.stepCtx, ap.stepCancel = context.WithCancel(context.Background())
	ap.stepDone = make(chan struct{})

	go func() {
		defer close(ap.stepDone)
		t := time.NewTicker(stepEvery)
		defer t.Stop()
		for {
			select {
			case <-ap.stepCtx.Done():
				return
			case <-t.C:
				s.mu.Lock()
				if s.activeProbe == nil || s.activeProbe != ap || ap.runner == nil {
					s.mu.Unlock()
					return
				}

				before := 0
				if ap.runner.Result() != nil {
					before = len(ap.runner.Result().Allocations)
				}

				done, _ := ap.runner.Step(dfs)

				after := before
				if ap.runner.Result() != nil {
					after = len(ap.runner.Result().Allocations)
				}

				// QoS guarantee search: track valid allocations
				if ap.targetIPCE > 0 && after > before {
					last := ap.runner.Result().Allocations[after-1]
					if last.IPCEfficiency >= ap.targetIPCE {
						ap.hasFoundValid = true
						ap.validWays = last.L3Ways
						ap.validMem = last.MemBandwidth
						// For ascending order, stop at first valid
						if s.config != nil && s.config.Prober != nil && s.config.Prober.Order == "asc" {
							ap.runner.Abort("met_qos_guarantee")
							done = true
						}
					}
				}

				if done {
					if ap.stepCancel != nil {
						ap.stepCancel()
					}
					s.mu.Unlock()
					return
				}
				s.mu.Unlock()
			}
		}
	}()
}

func (s *DynamicSchedulerV2) finalizeProbe() error {
	ap := s.activeProbe
	if ap == nil || ap.runner == nil {
		s.activeProbe = nil
		return nil
	}

	res := ap.runner.Result()
	if res != nil {
		s.allocationProbeResults = append(s.allocationProbeResults, res)
	}

	// Determine final allocation
	bestWays := ap.runner.BestWays()
	bestMem := ap.runner.BestMem()

	// If we found a valid QoS allocation, use it
	metQoS := ap.hasFoundValid
	if metQoS {
		bestWays = ap.validWays
		bestMem = ap.validMem
	}

	// Commit the probe result - this releases excess resources
	if s.rdtManager != nil && bestWays > 0 {
		handle, err := s.rdtManager.CommitProbeResult(ap.containerIndex, bestWays, bestMem)
		if err != nil {
			s.schedulerLogger.WithError(err).WithField("container", ap.containerIndex).
				Warn("Failed to commit probe result")
		} else {
			// Apply the RDT configuration
			if err := s.rdtManager.ApplyIfDirty(); err != nil {
				s.schedulerLogger.WithError(err).Warn("Failed to apply RDT configuration after probe commit")
			}

			// Ensure container is in its dedicated class
			p := s.profiles[ap.containerIndex]
			if p != nil && p.containerID != "" {
				if pids, err := readContainerCgroupPIDs(p.containerID); err == nil {
					for _, pid := range pids {
						_ = s.rdtManager.MoveContainerToClass(pid, ap.containerIndex)
					}
					s.schedulerLogger.WithFields(logrus.Fields{
						"container": ap.containerIndex,
						"class":     handle.ClassName,
						"ways":      handle.L3Ways,
						"mem":       handle.MemBandwidth,
					}).Info("Container assigned to dedicated RDT class after probe")
				}
			}
		}
	}

	s.lastProbeDone = time.Now()
	s.activeProbe = nil

	s.schedulerLogger.WithFields(logrus.Fields{
		"container":    ap.containerIndex,
		"socket":       ap.socket,
		"ways":         bestWays,
		"mem":          bestMem,
		"best_eff":     ap.runner.BestEff(),
		"reason":       ap.runner.StopReason(),
		"met_qos":      metQoS,
	}).Info("Finished allocation probe")

	// If QoS was not met, mark for retry when resources become available
	if !metQoS && ap.targetIPCE > 0 {
		p := s.profiles[ap.containerIndex]
		if p != nil {
			p.needsReprobe = true
			s.schedulerLogger.WithFields(logrus.Fields{
				"container":   ap.containerIndex,
				"target_ipce": ap.targetIPCE,
			}).Warn("QoS not met - will retry probe when resources become available")
		}
	}

	return nil
}

func (s *DynamicSchedulerV2) applyProbeAllocation(containerIndex int, socket int, ways int, mem float64) error {
	if s.rdtManager == nil {
		return nil
	}

	_, err := s.rdtManager.UpdateAllocation(containerIndex, ways, mem, true)
	if err != nil {
		return err
	}

	return s.rdtManager.ApplyIfDirty()
}

func (s *DynamicSchedulerV2) resetToShared(containerIndex int) error {
	p := s.profiles[containerIndex]
	if p == nil || p.containerID == "" {
		return nil
	}

	if s.rdtManager == nil {
		return nil
	}

	pids, err := readContainerCgroupPIDs(p.containerID)
	if err != nil {
		return err
	}

	for _, pid := range pids {
		_ = s.rdtManager.MoveContainerToShared(pid)
	}

	return nil
}
