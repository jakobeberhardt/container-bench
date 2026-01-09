package scheduler

import (
	"container-bench/internal/accounting"
	"container-bench/internal/config"
	"container-bench/internal/cpuallocator"
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/probe"
	proberesources "container-bench/internal/probe/resources"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// DynamicScheduler gives critical containers dedicated RDT resources.
// Non-critical containers remain in a shared benchmark pool class.
//
// First step implementation:
// - When a critical container starts, run an allocation probe (usually with min==max)
//   and keep the resulting allocation.
// - Multiple critical containers are handled sequentially (queue) to keep accounting simple.
// - Never reduce an already-assigned critical container allocation.
//
// NOTE: This intentionally reuses the allocation-prober runner to leverage
// collector-frequency overrides and async stepping.

type dynamicContainerProfile struct {
	index       int
	pid         int
	containerID string
	containerKey string
	startedAt   time.Time

	critical bool
	socket   int

	className string
	ways      int
	mem       float64
	l3Mask    uint64
}

type dynamicActiveProbe struct {
	containerIndex int
	socket         int
	runner         *proberesources.AllocationProbeRunner

	stepCtx    context.Context
	stepCancel context.CancelFunc
	stepDone   chan struct{}
}

type DynamicScheduler struct {
	name            string
	version         string
	schedulerLogger *logrus.Logger

	hostConfig    *host.HostConfig
	containers    []ContainerInfo
	rdtAccountant *accounting.RDTAccountant
	prober        *probe.Probe
	config        *config.SchedulerConfig
	collectorFreq CollectorFrequencyController
	cpuAllocator  cpuallocator.Allocator
	benchmarkID   int

	mu sync.Mutex

	profiles map[int]*dynamicContainerProfile // containerIndex -> profile

	benchmarkClass string
	benchmarkMask  [2]uint64
	benchmarkMem   [2]float64
	totalWays      int
	sockets        int

	probing       *dynamicActiveProbe
	lastProbeDone time.Time

	probeQueue  []int
	probeQueued map[int]bool

	allocationProbeResults []*proberesources.AllocationProbeResult
}

func NewDynamicScheduler() *DynamicScheduler {
	return &DynamicScheduler{
		name:            "dynamic",
		version:         "0.1.0",
		schedulerLogger: logging.GetSchedulerLogger(),
		profiles:        make(map[int]*dynamicContainerProfile),
		probeQueued:     make(map[int]bool),
	}
}

// GetAllocationProbeResults returns allocation probe results if available.
func (s *DynamicScheduler) GetAllocationProbeResults() []*proberesources.AllocationProbeResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*proberesources.AllocationProbeResult(nil), s.allocationProbeResults...)
}

// HasAllocationProbeResults returns true if allocation probe results are available.
func (s *DynamicScheduler) HasAllocationProbeResults() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.allocationProbeResults) > 0
}

func (s *DynamicScheduler) GetVersion() string { return s.version }

func (s *DynamicScheduler) SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	s.schedulerLogger.SetLevel(logLevel)
	return nil
}

func (s *DynamicScheduler) SetHostConfig(hostConfig *host.HostConfig) {
	s.hostConfig = hostConfig
}

func (s *DynamicScheduler) SetCPUAllocator(allocator cpuallocator.Allocator) {
	s.cpuAllocator = allocator
}

func (s *DynamicScheduler) SetProbe(prober *probe.Probe) {
	s.prober = prober
}

func (s *DynamicScheduler) SetBenchmarkID(benchmarkID int) {
	s.benchmarkID = benchmarkID
}

func (s *DynamicScheduler) SetCollectorFrequencyController(controller CollectorFrequencyController) {
	s.collectorFreq = controller
}

func (s *DynamicScheduler) AssignCPUCores(containerIndex int) ([]int, error) {
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

	// Track preferred socket based on assignment.
	sock := 0
	if s.hostConfig != nil && len(assigned) > 0 {
		if v, err := s.hostConfig.SocketOfPhysicalCPUs(assigned); err == nil {
			sock = v
		}
	}
	p := s.profileLocked(containerIndex)
	p.socket = sock

	return assigned, nil
}

func (s *DynamicScheduler) Initialize(accountant *accounting.RDTAccountant, containers []ContainerInfo, schedulerConfig *config.SchedulerConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rdtAccountant = accountant
	s.containers = containers
	s.config = schedulerConfig
	s.probing = nil
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

	if s.rdtAccountant == nil {
		s.schedulerLogger.WithField("rdt", false).Info("Dynamic scheduler initialized without RDT accountant (critical allocations disabled)")
		return nil
	}

	s.totalWays = s.rdtAccountant.GetTotalWays(0)
	if s.totalWays <= 0 {
		return fmt.Errorf("invalid total cache ways: %d", s.totalWays)
	}

	// Initialize benchmark pool class as full resources; we will shrink it as we allocate to critical containers.
	mask, err := fullMask(s.totalWays)
	if err != nil {
		return err
	}
	s.benchmarkMask[0] = mask
	s.benchmarkMem[0] = 100
	if s.sockets > 1 {
		s.benchmarkMask[1] = mask
		s.benchmarkMem[1] = 100
	}
	s.benchmarkClass = s.benchmarkClassNameLocked()

	if err := s.createOrUpdateBenchmarkClassLocked(); err != nil {
		return err
	}

	// Ensure we track all containers and default them to benchmark pool.
	for _, c := range containers {
		p := s.profileLocked(c.Index)
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

	s.schedulerLogger.WithFields(logrus.Fields{
		"containers": len(containers),
		"rdt":        true,
		"ways":       s.totalWays,
		"sockets":    s.sockets,
	}).Info("Dynamic scheduler initialized")

	return nil
}

func (s *DynamicScheduler) ProcessDataFrames(dfs *dataframe.DataFrames) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rdtAccountant == nil {
		return nil
	}

	// If a probe is active, step it (async stepper closes stepDone when done).
	if s.probing != nil {
		select {
		case <-s.probing.stepDone:
			// Probe finished.
			return s.finalizeProbeLocked()
		default:
			return nil
		}
	}

	// Start next probe if queued.
	idx, ok := s.peekProbeHeadLocked()
	if !ok {
		return nil
	}
	s.popProbeHeadLocked()
	return s.startProbeLocked(dfs, idx)
}

func (s *DynamicScheduler) Shutdown() error {
	s.mu.Lock()
	if s.probing != nil && s.probing.runner != nil {
		s.probing.runner.Abort("shutdown")
		if s.probing.stepCancel != nil {
			s.probing.stepCancel()
		}
	}
	s.probing = nil
	s.mu.Unlock()

	return nil
}

func (s *DynamicScheduler) OnContainerStart(info ContainerInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update container list entry.
	for i := range s.containers {
		if s.containers[i].Index == info.Index {
			s.containers[i].PID = info.PID
			s.containers[i].ContainerID = info.ContainerID
			s.containers[i].Config = info.Config
			break
		}
	}

	p := s.profileLocked(info.Index)
	p.pid = info.PID
	p.containerID = info.ContainerID
	p.startedAt = time.Now()
	p.critical = info.Config != nil && info.Config.Critical
	if info.Config != nil {
		p.containerKey = info.Config.KeyName
	}
	if p.className == "" {
		p.className = s.containerClassNameLocked(info.Index, p.containerKey)
	}

	if s.rdtAccountant == nil || info.PID == 0 {
		return nil
	}

	// Default: move all benchmark containers into the shared benchmark pool.
	_ = s.moveContainerCgroupLocked(info.ContainerID, s.benchmarkClass)

	// Critical containers are queued for allocation probing.
	if p.critical {
		s.enqueueProbeLocked(info.Index)
	}
	return nil
}

func (s *DynamicScheduler) OnContainerStop(containerIndex int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cpuAllocator != nil {
		s.cpuAllocator.Release(containerIndex)
	}

	// Remove from probe queue and abort if active.
	if s.probeQueued != nil {
		delete(s.probeQueued, containerIndex)
	}
	if s.probing != nil && s.probing.containerIndex == containerIndex {
		if s.probing.runner != nil {
			s.probing.runner.Abort("container_stopped")
		}
		if s.probing.stepCancel != nil {
			s.probing.stepCancel()
		}
		s.probing = nil
	}

	p := s.profiles[containerIndex]
	if p == nil {
		return nil
	}

	// Reclaim critical container resources.
	if s.rdtAccountant != nil && p.critical && p.className != "" {
		// Delete class first (frees allocations in accountant), then return them to benchmark pool.
		_ = s.rdtAccountant.DeleteClass(p.className)
		_ = s.reclaimContainerAllocationLocked(p)
		p.ways = 0
	}

	p.pid = 0
	p.containerID = ""
	return nil
}

// ------------------------
// Internals (locked)
// ------------------------

func (s *DynamicScheduler) profileLocked(containerIndex int) *dynamicContainerProfile {
	p := s.profiles[containerIndex]
	if p == nil {
		p = &dynamicContainerProfile{index: containerIndex}
		s.profiles[containerIndex] = p
	}
	return p
}

func (s *DynamicScheduler) benchmarkClassNameLocked() string {
	if s.benchmarkID > 0 {
		return fmt.Sprintf("bench-%d-benchmark", s.benchmarkID)
	}
	return "bench-benchmark"
}

func (s *DynamicScheduler) containerClassNameLocked(containerIndex int, containerKey string) string {
	key := sanitizeResctrlNamePart(containerKey)
	if s.benchmarkID > 0 {
		if key != "" {
			return sanitizeResctrlNamePart(fmt.Sprintf("b%d-dyn-%s", s.benchmarkID, key))
		}
		return fmt.Sprintf("bench-%d-dyn-c%d", s.benchmarkID, containerIndex)
	}
	if key != "" {
		return sanitizeResctrlNamePart(fmt.Sprintf("dyn-%s", key))
	}
	return fmt.Sprintf("bench-dyn-c%d", containerIndex)
}

func (s *DynamicScheduler) createOrUpdateBenchmarkClassLocked() error {
	if s.rdtAccountant == nil {
		return nil
	}

	req0 := &accounting.AllocationRequest{L3Bitmask: fmt.Sprintf("0x%x", s.benchmarkMask[0]), MemBandwidth: s.benchmarkMem[0]}
	var req1 *accounting.AllocationRequest
	if s.sockets > 1 {
		req1 = &accounting.AllocationRequest{L3Bitmask: fmt.Sprintf("0x%x", s.benchmarkMask[1]), MemBandwidth: s.benchmarkMem[1]}
	}

	if err := s.rdtAccountant.CreateClass(s.benchmarkClass, req0, req1); err != nil {
		// If class exists (e.g. rerun), update.
		if err := s.rdtAccountant.UpdateClass(s.benchmarkClass, req0, req1); err != nil {
			return err
		}
	}
	return nil
}

func (s *DynamicScheduler) enqueueProbeLocked(containerIndex int) {
	if s.probeQueued == nil {
		s.probeQueued = make(map[int]bool)
	}
	if s.probeQueued[containerIndex] {
		return
	}
	s.probeQueued[containerIndex] = true
	s.probeQueue = append(s.probeQueue, containerIndex)
}

func (s *DynamicScheduler) peekProbeHeadLocked() (int, bool) {
	for len(s.probeQueue) > 0 {
		idx := s.probeQueue[0]
		if s.probeQueued[idx] {
			return idx, true
		}
		// stale
		s.probeQueue = s.probeQueue[1:]
	}
	return -1, false
}

func (s *DynamicScheduler) popProbeHeadLocked() {
	if len(s.probeQueue) == 0 {
		return
	}
	idx := s.probeQueue[0]
	s.probeQueue = s.probeQueue[1:]
	delete(s.probeQueued, idx)
}

func (s *DynamicScheduler) startProbeLocked(dfs *dataframe.DataFrames, containerIndex int) error {
	p := s.profiles[containerIndex]
	if p == nil || !p.critical {
		return nil
	}
	if p.containerID == "" {
		return nil
	}
	if s.rdtAccountant == nil {
		return nil
	}
	if !s.isAllocatingProbeEnabledLocked() {
		s.schedulerLogger.WithField("container", containerIndex).Warn("Dynamic scheduler requires prober.allocate=true for critical allocations")
		return nil
	}

	sock := p.socket
	if sock < 0 || sock >= s.sockets {
		sock = 0
	}

	// Find container config for metadata + target thresholds.
	var containerCfg *config.ContainerConfig
	for i := range s.containers {
		if s.containers[i].Index == containerIndex {
			containerCfg = s.containers[i].Config
			break
		}
	}

	containerName := p.containerKey
	if containerName == "" && containerCfg != nil {
		containerName = containerCfg.GetContainerName(s.benchmarkID)
	}
	containerCores := ""
	containerImage := ""
	containerCommand := ""
	if containerCfg != nil {
		containerCores = containerCfg.Core
		containerImage = containerCfg.Image
		containerCommand = containerCfg.Command
	}

	// Configure range from scheduler.prober settings (reuse allocation prober config).
	cfg := s.config
	minL3 := 1
	maxL3 := 1
	stepL3 := 1
	minMem := 10.0
	maxMem := 10.0
	stepMem := 10.0
	budgetSeconds := 2.0
	var probingFrequency time.Duration
	outlierDrop := 0
	greedy := false
	if cfg != nil && cfg.Prober != nil {
		pc := cfg.Prober
		if pc.MinL3Ways > 0 {
			minL3 = pc.MinL3Ways
		}
		if pc.MaxL3Ways > 0 {
			maxL3 = pc.MaxL3Ways
		}
		if pc.StepL3Ways > 0 {
			stepL3 = pc.StepL3Ways
		}
		if pc.MinMemBandwidth > 0 {
			minMem = pc.MinMemBandwidth
		}
		if pc.MaxMemBandwidth > 0 {
			maxMem = pc.MaxMemBandwidth
		}
		if pc.StepMemBandwidth > 0 {
			stepMem = pc.StepMemBandwidth
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
		greedy = pc.GreedyAllocation
	}

	probeRange := proberesources.AllocationRange{
		MinL3Ways:        minL3,
		MaxL3Ways:        maxL3,
		StepL3Ways:       stepL3,
		MinMemBandwidth:  minMem,
		MaxMemBandwidth:  maxMem,
		StepMemBandwidth: stepMem,
		Order:            "asc",
		SocketID:         sock,
		IsolateOthers:    false,
		ForceReallocation: false,
	}

	// Break policy: prefer per-container target IPCE, fallback to scheduler break_condition.
	var acceptable *float64
	if containerCfg != nil {
		if v, ok := containerCfg.GetIPCEfficancy(); ok {
			acceptable = normalizeIPCEPercentThreshold(v)
		}
	}
	if acceptable == nil && cfg != nil && cfg.BreakCondition > 0 {
		acceptable = normalizeIPCEPercentThreshold(cfg.BreakCondition)
	}
	breaks := proberesources.AllocationProbeBreakPolicy{AcceptableIPCEfficiency: acceptable}

	opts := proberesources.AllocationProbeOptions{
		GreedyAllocation:  greedy,
		ProbingFrequency:  probingFrequency,
		OutlierDrop:       outlierDrop,
		BaselineFirst:     true,
	}

	target := proberesources.AllocationProbeTarget{
		BenchmarkID:      s.benchmarkID,
		ContainerID:      p.containerID,
		ContainerName:    containerName,
		ContainerIndex:   containerIndex,
		ContainerCores:   containerCores,
		ContainerSocket:  sock,
		ContainerImage:   containerImage,
		ContainerCommand: containerCommand,
	}

	cb := proberesources.AllocationProbeCallbacks{
		ApplyAllocation: func(ways int, mem float64) error {
			return s.applyAllocationLocked(containerIndex, sock, ways, mem)
		},
		ResetToBenchmark: func() error {
			return s.resetContainerToBenchmarkLocked(containerIndex)
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
		return fmt.Errorf("no probe candidates")
	}

	ap := &dynamicActiveProbe{containerIndex: containerIndex, socket: sock, runner: runner}
	s.probing = ap

	// Start probe + async stepping.
	if err := runner.Start(dfs); err != nil {
		s.probing = nil
		return err
	}
	s.startProbeStepperLocked(dfs, ap)

	s.schedulerLogger.WithFields(logrus.Fields{
		"container":  containerIndex,
		"socket":     sock,
		"candidates": runner.NumCandidates(),
		"min_l3":     minL3,
		"max_l3":     maxL3,
		"min_mem":    minMem,
		"max_mem":    maxMem,
	}).Info("Started dynamic allocation probing for critical container")

	return nil
}

func (s *DynamicScheduler) startProbeStepperLocked(dfs *dataframe.DataFrames, ap *dynamicActiveProbe) {
	if ap == nil || ap.runner == nil {
		return
	}
	if ap.stepCancel != nil {
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
				if s.probing == nil || s.probing != ap || ap.runner == nil {
					s.mu.Unlock()
					return
				}
				done, _ := ap.runner.Step(dfs)
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

func (s *DynamicScheduler) finalizeProbeLocked() error {
	ap := s.probing
	if ap == nil || ap.runner == nil {
		s.probing = nil
		return nil
	}

	res := ap.runner.Result()
	if res != nil {
		s.allocationProbeResults = append(s.allocationProbeResults, res)
	}

	idx := ap.containerIndex
	p := s.profiles[idx]
	bestWays := ap.runner.BestWays()
	bestMem := ap.runner.BestMem()

	// Enforce monotonic: never lower an existing critical allocation.
	if p != nil {
		if bestWays < p.ways {
			bestWays = p.ways
		}
		if bestMem < p.mem {
			bestMem = p.mem
		}
		p.ways = bestWays
		p.mem = bestMem
	}

	s.lastProbeDone = time.Now()
	s.probing = nil

	s.schedulerLogger.WithFields(logrus.Fields{
		"container": idx,
		"socket":    ap.socket,
		"ways":      bestWays,
		"mem":       bestMem,
		"best_eff":  ap.runner.BestEff(),
		"reason":    ap.runner.StopReason(),
	}).Info("Finished dynamic allocation probing")

	return nil
}

func (s *DynamicScheduler) isAllocatingProbeEnabledLocked() bool {
	if s.config == nil || s.config.Prober == nil {
		return true
	}
	if s.config.Prober.Allocate == nil {
		return true
	}
	return *s.config.Prober.Allocate
}

func (s *DynamicScheduler) resetContainerToBenchmarkLocked(containerIndex int) error {
	p := s.profiles[containerIndex]
	if p == nil {
		return nil
	}
	if p.containerID == "" {
		return nil
	}
	return s.moveContainerCgroupLocked(p.containerID, s.benchmarkClass)
}

func (s *DynamicScheduler) moveContainerCgroupLocked(containerID string, className string) error {
	if s.rdtAccountant == nil {
		return nil
	}
	pids, err := readContainerCgroupPIDs(containerID)
	if err != nil {
		return err
	}
	for _, pid := range pids {
		_ = s.rdtAccountant.MoveContainer(pid, className)
	}
	return nil
}

func (s *DynamicScheduler) reclaimContainerAllocationLocked(p *dynamicContainerProfile) error {
	if p == nil {
		return nil
	}
	sock := p.socket
	if sock < 0 || sock >= s.sockets {
		sock = 0
	}
	// Return resources to benchmark pool (local accounting).
	if p.l3Mask != 0 {
		s.benchmarkMask[sock] |= p.l3Mask
		p.l3Mask = 0
	}
	if p.mem > 0 {
		s.benchmarkMem[sock] += p.mem
		if s.benchmarkMem[sock] > 100 {
			s.benchmarkMem[sock] = 100
		}
		p.mem = 0
	}
	if err := s.createOrUpdateBenchmarkClassLocked(); err != nil {
		return err
	}
	return nil
}

func (s *DynamicScheduler) applyAllocationLocked(containerIndex int, sock int, ways int, mem float64) error {
	if s.rdtAccountant == nil {
		return nil
	}
	p := s.profiles[containerIndex]
	if p == nil {
		return nil
	}
	if p.containerID == "" {
		return nil
	}
	if !p.critical {
		return nil
	}
	if sock < 0 || sock >= s.sockets {
		sock = 0
	}

	// Baseline candidate: ensure in benchmark class and do not change allocations.
	if ways == 0 && mem == 0 {
		return s.resetContainerToBenchmarkLocked(containerIndex)
	}

	// Monotonic: don't decrease.
	if ways < p.ways {
		ways = p.ways
	}
	if mem < p.mem {
		mem = p.mem
	}

	// If this container already has a class allocation, reclaim it back to the benchmark pool
	// and clear the class allocation in the accountant before applying a new (larger) candidate.
	if p.className != "" && (p.l3Mask != 0 || p.mem > 0) {
		// Move back to benchmark class during reconfiguration.
		_ = s.resetContainerToBenchmarkLocked(containerIndex)

		// Free allocation in accountant but keep the class directory.
		_ = s.rdtAccountant.UpdateClass(p.className, nil, nil)

		if err := s.reclaimContainerAllocationLocked(p); err != nil {
			return err
		}
	}

	// Allocate from benchmark pool: shrink benchmark first (frees resources in accountant).
	if ways <= 0 {
		ways = 1
	}
	if mem < 0 {
		mem = 0
	}

	// L3: take contiguous ways from benchmark mask.
	taken := uint64(0)
	if ways > 0 {
		var err error
		taken, s.benchmarkMask[sock], err = takeContiguousFromMask(s.benchmarkMask[sock], ways, s.totalWays)
		if err != nil {
			return err
		}
	}
	// Mem: take from benchmark mem.
	if mem > s.benchmarkMem[sock] {
		return fmt.Errorf("insufficient benchmark mem headroom on socket %d: have %.2f need %.2f", sock, s.benchmarkMem[sock], mem)
	}
	s.benchmarkMem[sock] -= mem
	if s.benchmarkMem[sock] < 0 {
		s.benchmarkMem[sock] = 0
	}
	if err := s.createOrUpdateBenchmarkClassLocked(); err != nil {
		return err
	}

	// Create/update dedicated class for this critical container.
	p.className = s.containerClassNameLocked(containerIndex, p.containerKey)
	var req0, req1 *accounting.AllocationRequest
	if sock == 0 {
		req0 = &accounting.AllocationRequest{L3Bitmask: fmt.Sprintf("0x%x", taken), MemBandwidth: mem}
	} else {
		req1 = &accounting.AllocationRequest{L3Bitmask: fmt.Sprintf("0x%x", taken), MemBandwidth: mem}
	}

	if err := s.rdtAccountant.CreateClass(p.className, req0, req1); err != nil {
		// Class exists: update.
		if err := s.rdtAccountant.UpdateClass(p.className, req0, req1); err != nil {
			return err
		}
	}

	// Move all container PIDs to the dedicated class.
	if err := s.moveContainerCgroupLocked(p.containerID, p.className); err != nil {
		return err
	}

	p.ways = ways
	p.mem = mem
	p.l3Mask = taken
	return nil
}
