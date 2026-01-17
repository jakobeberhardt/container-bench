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
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Gives critical containers dedicated RDT resources.
// Non critical containers remain in a shared benchmark pool class.
// When a critical container starts, run an allocation probe (usually with min==max) and
// keep the resulting allocation.
// Multiple critical containers are handled sequentially to keep accounting simple.
// Never reduce an already assigned critical container allocation.
//
// NOTE: This reuses the allocation prober runner to leverage collector frequency overrides
// and async stepping.

type dynamicContainerProfile struct {
	index        int
	pid          int
	containerID  string
	containerKey string
	startedAt    time.Time

	critical bool
	socket   int

	className string
	ways      int
	mem       float64
	l3Mask    uint64

	// Socket-balancing heuristic (non-critical only)
	stallsL3MissPercent float64
	stallsL3MissProbed  bool
	stallsUpdatedAt     time.Time

	// floorWays/floorMem represent the committed minimum allocation for this critical
	// container. Probing may temporarily move below this, but finalize must never commit
	// a lower allocation than the floor.
	floorWays int
	floorMem  float64
}

type dynamicActiveProbe struct {
	containerIndex int
	socket         int
	runner         *proberesources.AllocationProbeRunner

	// Descending guarantee search (order: desc + target IPCE available):
	// start from max allocation and lower it until IPCE falls below the guarantee;
	// then keep the previous (last >= guarantee).
	descGuarantee bool
	// Ascending guarantee search (order: asc + target IPCE available):
	// start from min allocation and increase it until IPCE reaches the guarantee;
	// then keep that first (>= guarantee) allocation.
	ascGuarantee bool
	guarantee    float64

	hasFallback  bool
	fallbackWays int
	fallbackMem  float64

	hasLastAbove  bool
	lastAboveWays int
	lastAboveMem  float64

	hasFirstAbove  bool
	firstAboveWays int
	firstAboveMem  float64

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

	// lastDFS stores the most recent DataFrames snapshot observed by ProcessDataFrames.
	// Probing uses it both for starting and stepping so probe progress does not depend
	// on ProcessDataFrames being called at a high cadence.
	lastDFS *dataframe.DataFrames

	probeLoopCancel context.CancelFunc
	probeLoopDone   chan struct{}

	mu sync.Mutex

	profiles map[int]*dynamicContainerProfile // containerIndex -> profile

	benchmarkClass string
	benchmarkMask  [2]uint64
	benchmarkMem   [2]float64
	totalWays      int
	sockets        int

	probing         *dynamicActiveProbe
	lastProbeDone   time.Time
	lastRebalanceAt time.Time

	lastProbeDelayLogAt  time.Time
	lastProbeDelayReason string
	lastProbeDelayIndex  int

	probeQueue  []int
	probeQueued map[int]bool

	allocationProbeResults []*proberesources.AllocationProbeResult
}

func (s *DynamicScheduler) logProbeDelayLocked(reason string, idx int, fields logrus.Fields) {
	// Avoid spamming: log at most once every 5s unless the reason/container changes.
	now := time.Now()
	if reason == "" {
		reason = "unknown"
	}
	if idx < 0 {
		idx = -1
	}
	if reason == s.lastProbeDelayReason && idx == s.lastProbeDelayIndex {
		if !s.lastProbeDelayLogAt.IsZero() && now.Sub(s.lastProbeDelayLogAt) < 5*time.Second {
			return
		}
	}
	s.lastProbeDelayLogAt = now
	s.lastProbeDelayReason = reason
	s.lastProbeDelayIndex = idx

	if fields == nil {
		fields = logrus.Fields{}
	}
	fields["reason"] = reason
	fields["queue_len"] = len(s.probeQueue)
	fields["container"] = idx
	fields["has_active_probe"] = (s.probing != nil)

	s.schedulerLogger.WithFields(fields).Info("Next critical probe delayed")
}

const (
	// Hard reservation for system/default per socket.
	// This ensures there is always a minimum amount of shared resources available.
	systemDefaultReserveWays = 1
	systemDefaultReserveMem  = 10.0
)

func NewDynamicScheduler() *DynamicScheduler {
	return &DynamicScheduler{
		name:            "dynamic",
		version:         "0.1.0",
		schedulerLogger: logging.GetSchedulerLogger(),
		profiles:        make(map[int]*dynamicContainerProfile),
		probeQueued:     make(map[int]bool),
	}
}

func (s *DynamicScheduler) startProbeLoopLocked() {
	if s.probeLoopCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.probeLoopCancel = cancel
	s.probeLoopDone = make(chan struct{})

	go func() {
		defer close(s.probeLoopDone)
		// Short cadence to avoid long "silent" gaps when ProcessDataFrames is sparse.
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.mu.Lock()
				// Only handle probe progression/start; avoid doing any rebalancing here.
				if s.probing == nil {
					_ = s.maybeStartNextProbeLocked("background")
				}
				s.mu.Unlock()
			}
		}
	}()
}

func (s *DynamicScheduler) stopProbeLoopLocked() {
	if s.probeLoopCancel != nil {
		s.probeLoopCancel()
		s.probeLoopCancel = nil
	}
	// Don't block indefinitely; shutdown should be best-effort.
	if s.probeLoopDone != nil {
		done := s.probeLoopDone
		s.probeLoopDone = nil
		s.mu.Unlock()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
		s.mu.Lock()
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

	// (Re)start the probe loop for this benchmark run.
	s.startProbeLoopLocked()

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
	// Keep non-critical containers in system/default and shrink system/default
	// so critical containers can be truly isolated.
	s.benchmarkClass = "system/default"

	if err := s.createOrUpdateBenchmarkClassLocked(); err != nil {
		return err
	}

	// Ensure we track all containers.
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

	// Remember last DataFrames snapshot for probe starting/stepping.
	s.lastDFS = dfs

	// Update non-critical socket-balancing heuristic (best-effort).
	s.updateNonCriticalStallsLocked(dfs)

	// If a probe is active, step it (async stepper closes stepDone when done).
	if s.probing != nil {
		if idx, ok := s.peekProbeHeadLocked(); ok {
			s.logProbeDelayLocked("active_probe_running", idx, nil)
		}
		select {
		case <-s.probing.stepDone:
			// Probe finished.
			return s.finalizeProbeLocked()
		default:
			return nil
		}
	}

	// Rebalancing is independent from RDT probing; allow it even without an RDT accountant.
	if s.rdtAccountant == nil {
		s.maybeRebalanceNonCriticalLocked("tick")
		return nil
	}

	// Start next probe if queued.
	idx, ok := s.peekProbeHeadLocked()
	if !ok {
		s.maybeRebalanceNonCriticalLocked("tick")
		return nil
	}

	warmupSeconds := 5
	cooldownSeconds := 2
	if s.config != nil {
		// Prober-specific warmup takes precedence over scheduler-level warmup
		if s.config.Prober != nil && s.config.Prober.WarmupT > 0 {
			warmupSeconds = s.config.Prober.WarmupT
		} else if s.config.WarmupT > 0 {
			warmupSeconds = s.config.WarmupT
		}
		// Prober-specific cooldown takes precedence over scheduler-level cooldown
		if s.config.Prober != nil && s.config.Prober.CooldownT > 0 {
			cooldownSeconds = s.config.Prober.CooldownT
		} else if s.config.CooldownT > 0 {
			cooldownSeconds = s.config.CooldownT
		}
	}

	if cooldownSeconds > 0 && !s.lastProbeDone.IsZero() {
		if time.Since(s.lastProbeDone) < time.Duration(cooldownSeconds)*time.Second {
			remaining := time.Duration(cooldownSeconds)*time.Second - time.Since(s.lastProbeDone)
			if remaining < 0 {
				remaining = 0
			}
			s.logProbeDelayLocked("cooldown", idx, logrus.Fields{
				"cooldown_s":           cooldownSeconds,
				"cooldown_remaining_s": int(remaining.Round(time.Second).Seconds()),
			})
			return nil
		}
	}

	if p := s.profiles[idx]; p != nil {
		if p.startedAt.IsZero() {
			p.startedAt = time.Now()
		}
		if warmupSeconds > 0 && time.Since(p.startedAt) < time.Duration(warmupSeconds)*time.Second {
			remaining := time.Duration(warmupSeconds)*time.Second - time.Since(p.startedAt)
			if remaining < 0 {
				remaining = 0
			}
			s.logProbeDelayLocked("warmup", idx, logrus.Fields{
				"warmup_s":           warmupSeconds,
				"warmup_remaining_s": int(remaining.Round(time.Second).Seconds()),
			})
			return nil
		}
	}
	started, err := s.startProbeLocked(dfs, idx)
	if err != nil {
		return err
	}
	if started {
		s.popProbeHeadLocked()
	}
	return nil
}

// maybeStartNextProbeLocked attempts to start the next queued critical probe if possible.
// It logs gating reasons (warmup/cooldown/waiting_for_data) and relies on s.lastDFS for
// probe execution.
func (s *DynamicScheduler) maybeStartNextProbeLocked(trigger string) error {
	if s.rdtAccountant == nil {
		return nil
	}
	if s.probing != nil {
		return nil
	}

	idx, ok := s.peekProbeHeadLocked()
	if !ok {
		return nil
	}

	// Without any DataFrames snapshot, the allocation probe cannot observe progress.
	if s.lastDFS == nil {
		s.logProbeDelayLocked("waiting_for_data", idx, logrus.Fields{"trigger": trigger})
		return nil
	}

	warmupSeconds := 5
	cooldownSeconds := 2
	if s.config != nil {
		if s.config.Prober != nil && s.config.Prober.WarmupT > 0 {
			warmupSeconds = s.config.Prober.WarmupT
		} else if s.config.WarmupT > 0 {
			warmupSeconds = s.config.WarmupT
		}
		if s.config.Prober != nil && s.config.Prober.CooldownT > 0 {
			cooldownSeconds = s.config.Prober.CooldownT
		} else if s.config.CooldownT > 0 {
			cooldownSeconds = s.config.CooldownT
		}
	}

	if cooldownSeconds > 0 && !s.lastProbeDone.IsZero() {
		if time.Since(s.lastProbeDone) < time.Duration(cooldownSeconds)*time.Second {
			remaining := time.Duration(cooldownSeconds)*time.Second - time.Since(s.lastProbeDone)
			if remaining < 0 {
				remaining = 0
			}
			s.logProbeDelayLocked("cooldown", idx, logrus.Fields{
				"trigger":              trigger,
				"cooldown_s":           cooldownSeconds,
				"cooldown_remaining_s": int(remaining.Round(time.Second).Seconds()),
			})
			return nil
		}
	}

	if p := s.profiles[idx]; p != nil {
		if p.startedAt.IsZero() {
			p.startedAt = time.Now()
		}
		if warmupSeconds > 0 && time.Since(p.startedAt) < time.Duration(warmupSeconds)*time.Second {
			remaining := time.Duration(warmupSeconds)*time.Second - time.Since(p.startedAt)
			if remaining < 0 {
				remaining = 0
			}
			s.logProbeDelayLocked("warmup", idx, logrus.Fields{
				"trigger":            trigger,
				"warmup_s":           warmupSeconds,
				"warmup_remaining_s": int(remaining.Round(time.Second).Seconds()),
			})
			return nil
		}
	}

	started, err := s.startProbeLocked(s.lastDFS, idx)
	if err != nil {
		return err
	}
	if started {
		s.popProbeHeadLocked()
	}
	return nil
}

func (s *DynamicScheduler) rebalanceModeLocked() (enabled bool, batch bool) {
	if s.config == nil {
		return false, false
	}

	// New semantics: `rebalance` enables the feature; `rebalance_batch` controls batch mode.
	if s.config.Rebalance != nil {
		enabled = *s.config.Rebalance
		batch = s.config.RebalanceBatch != nil && *s.config.RebalanceBatch
		return enabled, batch
	}

	// Backward-compat: treat `rebalance_batch: true` as "enable rebalancing (batch)".
	if s.config.RebalanceBatch != nil && *s.config.RebalanceBatch {
		return true, true
	}
	return false, false
}

func (s *DynamicScheduler) updateNonCriticalStallsLocked(dfs *dataframe.DataFrames) {
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

func (s *DynamicScheduler) currentSocketForContainerLocked(containerIndex int) int {
	if s.cpuAllocator == nil || s.hostConfig == nil {
		return 0
	}
	cpus, ok := s.cpuAllocator.Get(containerIndex)
	if !ok || len(cpus) == 0 {
		return 0
	}
	sock, err := s.hostConfig.SocketOfPhysicalCPUs(cpus)
	if err != nil {
		return 0
	}
	if sock < 0 || sock >= s.sockets {
		return 0
	}
	return sock
}

func (s *DynamicScheduler) socketStallsLoadLocked(sock int) float64 {
	// Worst-case interference assumption for missing data.
	const worstCaseStalls = 100.0
	load := 0.0
	for idx, p := range s.profiles {
		if p == nil || p.pid == 0 || p.critical {
			continue
		}
		curSock := p.socket
		if curSock < 0 || curSock >= s.sockets {
			curSock = s.currentSocketForContainerLocked(idx)
		}
		if curSock != sock {
			continue
		}
		if p.stallsL3MissProbed {
			load += p.stallsL3MissPercent
		} else {
			load += worstCaseStalls
		}
	}
	return load
}

func (s *DynamicScheduler) maybeRebalanceNonCriticalLocked(trigger string) {
	enabled, batch := s.rebalanceModeLocked()
	if !enabled {
		return
	}
	if s.sockets <= 1 || s.cpuAllocator == nil {
		return
	}

	// Only attempt to rebalance when no critical allocation probe is active.
	if s.probing != nil {
		return
	}

	// Avoid thrashing by imposing a small cooldown.
	const rebalanceCooldownSeconds = 2.0
	if !s.lastRebalanceAt.IsZero() {
		if time.Since(s.lastRebalanceAt).Seconds() < rebalanceCooldownSeconds {
			return
		}
	}

	// Only move if there's a meaningful imbalance.
	const imbalanceThreshold = 5.0
	const minImprovement = 1.0

	load := make([]float64, s.sockets)
	for sock := 0; sock < s.sockets; sock++ {
		load[sock] = s.socketStallsLoadLocked(sock)
	}

	minSock, maxSock := 0, 0
	for i := 1; i < s.sockets; i++ {
		if load[i] < load[minSock] {
			minSock = i
		}
		if load[i] > load[maxSock] {
			maxSock = i
		}
	}
	currentDiff := load[maxSock] - load[minSock]
	if currentDiff <= imbalanceThreshold {
		return
	}

	// Collect candidate non-critical containers from the most loaded socket.
	type candidate struct {
		idx     int
		startAt time.Time
		stalls  float64
	}
	candidates := make([]candidate, 0)
	for idx, p := range s.profiles {
		if p == nil || p.pid == 0 || p.critical || p.containerID == "" {
			continue
		}
		srcSock := p.socket
		if srcSock < 0 || srcSock >= s.sockets {
			srcSock = s.currentSocketForContainerLocked(idx)
		}
		if srcSock != maxSock {
			continue
		}
		stalls := 100.0
		if p.stallsL3MissProbed {
			stalls = p.stallsL3MissPercent
		}
		candidates = append(candidates, candidate{idx: idx, startAt: p.startedAt, stalls: stalls})
	}
	if len(candidates) == 0 {
		return
	}

	// Youngest-first.
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].startAt.Equal(candidates[j].startAt) {
			return candidates[i].startAt.After(candidates[j].startAt)
		}
		return candidates[i].idx < candidates[j].idx
	})

	maxMoves := 1
	if batch {
		maxMoves = len(candidates)
		if maxMoves > 10 {
			maxMoves = 10
		}
	}

	moves := 0
	for _, c := range candidates {
		if moves >= maxMoves {
			break
		}
		p := s.profiles[c.idx]
		if p == nil || p.containerID == "" {
			continue
		}

		beforeDiff := currentDiff
		newLoadMax := load[maxSock] - c.stalls
		newLoadMin := load[minSock] + c.stalls
		newDiff := newLoadMax - newLoadMin
		if newDiff < 0 {
			newDiff = -newDiff
		}
		if beforeDiff-newDiff < minImprovement && newDiff > imbalanceThreshold {
			continue
		}

		assigned, err := s.cpuAllocator.Move(c.idx, p.containerID, minSock)
		if err != nil {
			s.schedulerLogger.WithError(err).WithFields(logrus.Fields{
				"trigger":   trigger,
				"container": c.idx,
				"src_sock":  maxSock,
				"dst_sock":  minSock,
			}).Debug("Failed to move container for stalls-based rebalancing")
			continue
		}

		p.socket = minSock
		load[maxSock] = newLoadMax
		load[minSock] = newLoadMin
		currentDiff = load[maxSock] - load[minSock]
		if currentDiff < 0 {
			currentDiff = -currentDiff
		}
		moves++
		s.lastRebalanceAt = time.Now()

		s.schedulerLogger.WithFields(logrus.Fields{
			"trigger":              trigger,
			"container":            c.idx,
			"stalls_l3_miss_pct":   fmt.Sprintf("%.2f", c.stalls),
			"src_sock":             maxSock,
			"dst_sock":             minSock,
			"diff_before":          fmt.Sprintf("%.2f", beforeDiff),
			"diff_after":           fmt.Sprintf("%.2f", currentDiff),
			"assigned_cpus":        assigned,
			"batch":                batch,
			"remaining_candidates": len(candidates) - moves,
		}).Info("Rebalanced non-critical container based on StallsL3MissPercent")

		// Recompute min/max sockets after each move.
		minSock, maxSock = 0, 0
		for i := 1; i < s.sockets; i++ {
			if load[i] < load[minSock] {
				minSock = i
			}
			if load[i] > load[maxSock] {
				maxSock = i
			}
		}
		if load[maxSock]-load[minSock] <= imbalanceThreshold {
			break
		}
	}
}

func (s *DynamicScheduler) Shutdown() error {
	s.mu.Lock()
	if s.probing != nil && s.probing.runner != nil {
		s.probing.runner.Abort("shutdown")
		if s.probing.stepCancel != nil {
			s.probing.stepCancel()
		}
		s.lastProbeDone = time.Now()
	}
	s.probing = nil
	// Stop background probe loop.
	s.stopProbeLoopLocked()
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

	// Non-critical containers remain in system/default.

	// Critical containers are queued for allocation probing.
	if p.critical {
		s.enqueueProbeLocked(info.Index)
		// Avoid long silent gaps: attempt to start probe immediately (or at least log why not).
		_ = s.maybeStartNextProbeLocked("container_start")
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
		s.lastProbeDone = time.Now()
		s.probing = nil
	}

	p := s.profiles[containerIndex]
	if p == nil {
		return nil
	}

	// Reclaim critical container resources.
	if s.rdtAccountant != nil && p.critical && p.className != "" {
		// Delete class first (frees allocations in accountant), then consolidate to keep
		// system/default contiguous (avoid fragmented bitmasks).
		_ = s.rdtAccountant.DeleteClass(p.className)

		// Clear local allocation bookkeeping for this container.
		p.ways = 0
		p.mem = 0
		p.l3Mask = 0
		p.floorWays = 0
		p.floorMem = 0
		p.className = ""

		_ = s.consolidateRDTPartitionsLocked()
	}

	// If a critical container stopped and released resources, allow queued critical probes
	// to restart immediately (bypass cooldown based on lastProbeDone).
	if p.critical {
		if s.probing == nil {
			if _, has := s.peekProbeHeadLocked(); has {
				s.lastProbeDone = time.Time{}
			}
		}
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

func (s *DynamicScheduler) benchmarkClassNameLocked() string { return "system/default" }

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

func (s *DynamicScheduler) startProbeLocked(dfs *dataframe.DataFrames, containerIndex int) (bool, error) {
	p := s.profiles[containerIndex]
	if p == nil || !p.critical {
		return false, nil
	}
	if p.containerID == "" {
		return false, nil
	}
	if s.rdtAccountant == nil {
		return false, nil
	}
	if !s.isAllocatingProbeEnabledLocked() {
		s.schedulerLogger.WithField("container", containerIndex).Warn("Dynamic scheduler requires prober.allocate=true for critical allocations")
		return false, nil
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
	order := "asc"
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
		greedy = pc.GreedyAllocation
	}

	// Admission: for critical jobs, prefer the socket with more currently free resources
	// so their probe can reach deeper candidates / find a satisfying allocation.
	preferredSock := p.socket
	if preferredSock < 0 || preferredSock >= s.sockets {
		preferredSock = 0
	}
	sock := preferredSock
	if s.sockets > 1 {
		sock = pickSocketForCriticalAdmission(preferredSock, s.totalWays, maxL3, maxMem, s.benchmarkMask, s.benchmarkMem, systemDefaultReserveWays, systemDefaultReserveMem)
		if sock != preferredSock && s.cpuAllocator != nil {
			// Best-effort: move the container to the chosen socket before probing.
			if moved, err := s.cpuAllocator.Move(containerIndex, p.containerID, sock); err == nil {
				p.socket = sock
				s.schedulerLogger.WithFields(logrus.Fields{
					"container":     containerIndex,
					"from_socket":   preferredSock,
					"to_socket":     sock,
					"need_max_l3":   maxL3,
					"need_max_mem":  maxMem,
					"assigned_cpus": moved,
				}).Debug("Moved critical container to preferred socket for probing")
			} else {
				s.schedulerLogger.WithError(err).WithFields(logrus.Fields{
					"container":   containerIndex,
					"from_socket": preferredSock,
					"to_socket":   sock,
				}).Debug("Failed to move critical container to preferred socket; probing in-place")
				sock = preferredSock
			}
		}
	}

	// IMPORTANT: AllocationProbeRunner aborts the entire probe on the first ApplyAllocation error.
	// Avoid premature "apply_failed" by ensuring the first candidate is feasible.
	//
	// Policy:
	// - If the socket cannot satisfy the configured MINIMUM (min_l3/min_mem), we do NOT start
	//   the probe yet; it stays queued and will be retried once resources are reclaimed.
	// - If the socket can satisfy minimums but not the configured MAXIMUM, we clamp max down to
	//   current availability.
	availMaxWays := maxContiguousWaysAvailableForCritical(s.benchmarkMask[sock], s.totalWays, systemDefaultReserveWays)
	if availMaxWays < minL3 {
		s.schedulerLogger.WithFields(logrus.Fields{
			"container": containerIndex,
			"socket":    sock,
			"min_l3":    minL3,
			"avail_l3":  availMaxWays,
			"order":     order,
			"queue_len": len(s.probeQueue),
		}).Info("Insufficient cache-way headroom for critical probing; will retry when resources are reclaimed")
		return false, nil
	}
	if maxL3 > availMaxWays {
		maxL3 = availMaxWays
	}

	availMaxMem := s.benchmarkMem[sock] - systemDefaultReserveMem
	if availMaxMem < 0 {
		availMaxMem = 0
	}
	if availMaxMem+1e-9 < minMem {
		s.schedulerLogger.WithFields(logrus.Fields{
			"container": containerIndex,
			"socket":    sock,
			"min_mem":   minMem,
			"avail_mem": fmt.Sprintf("%.2f", availMaxMem),
			"order":     order,
			"queue_len": len(s.probeQueue),
		}).Info("Insufficient memory-bandwidth headroom for critical probing; will retry when resources are reclaimed")
		return false, nil
	}
	if maxMem > availMaxMem {
		maxMem = availMaxMem
	}

	probeRange := proberesources.AllocationRange{
		MinL3Ways:         minL3,
		MaxL3Ways:         maxL3,
		StepL3Ways:        stepL3,
		MinMemBandwidth:   minMem,
		MaxMemBandwidth:   maxMem,
		StepMemBandwidth:  stepMem,
		Order:             order,
		SocketID:          sock,
		IsolateOthers:     false,
		ForceReallocation: false,
	}

	// Break/threshold policy:
	// - Prefer per-container target IPCE, fallback to scheduler break_condition.
	// - If order=desc, we do a "guarantee" search: start at max and lower until we
	//   fall below the guarantee, then keep the previous allocation.
	var acceptable *float64
	if containerCfg != nil {
		if v, ok := containerCfg.GetIPCEfficancy(); ok {
			acceptable = normalizeIPCEPercentThreshold(v)
		}
	}
	if acceptable == nil && cfg != nil && cfg.BreakCondition > 0 {
		acceptable = normalizeIPCEPercentThreshold(cfg.BreakCondition)
	}

	descGuarantee := (order == "desc" && acceptable != nil && *acceptable >= 0)
	ascGuarantee := (order == "asc" && acceptable != nil && *acceptable >= 0)
	breaks := proberesources.AllocationProbeBreakPolicy{}
	if !(descGuarantee || ascGuarantee) {
		breaks.AcceptableIPCEfficiency = acceptable
	}

	opts := proberesources.AllocationProbeOptions{
		GreedyAllocation: greedy,
		ProbingFrequency: probingFrequency,
		OutlierDrop:      outlierDrop,
		// Critical workloads must always probe real allocations (skip baseline) so the
		// resulting decision is robust to future interference.
		BaselineFirst: false,
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
		return false, fmt.Errorf("no probe candidates")
	}

	ap := &dynamicActiveProbe{containerIndex: containerIndex, socket: sock, runner: runner}
	if descGuarantee {
		ap.descGuarantee = true
		ap.guarantee = *acceptable
	}
	if ascGuarantee {
		ap.ascGuarantee = true
		ap.guarantee = *acceptable
	}
	s.probing = ap

	// Start probe + async stepping.
	if err := runner.Start(dfs); err != nil {
		s.probing = nil
		return false, err
	}
	s.startProbeStepperLocked(dfs, ap)

	s.schedulerLogger.WithFields(logrus.Fields{
		"container":      containerIndex,
		"socket":         sock,
		"candidates":     runner.NumCandidates(),
		"min_l3":         minL3,
		"max_l3":         maxL3,
		"min_mem":        minMem,
		"max_mem":        maxMem,
		"order":          order,
		"asc_guarantee":  ascGuarantee,
		"desc_guarantee": descGuarantee,
	}).Info("Started dynamic allocation probing for critical container")

	return true, nil
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
				before := 0
				if ap.runner.Result() != nil {
					before = len(ap.runner.Result().Allocations)
				}
				done, _ := ap.runner.Step(dfs)
				after := before
				if ap.runner.Result() != nil {
					after = len(ap.runner.Result().Allocations)
				}

				// Descending guarantee search: stop when we first fall below the guarantee,
				// then revert to the previous (last >= guarantee) allocation.
				if (ap.descGuarantee || ap.ascGuarantee) && after > before {
					last := ap.runner.Result().Allocations[after-1]
					if ap.descGuarantee {
						// Fallback = first tested (highest allocation)
						if !ap.hasFallback {
							ap.hasFallback = true
							ap.fallbackWays = last.L3Ways
							ap.fallbackMem = last.MemBandwidth
						}
					} else if ap.ascGuarantee {
						// Fallback = last tested (highest allocation in asc mode)
						ap.hasFallback = true
						ap.fallbackWays = last.L3Ways
						ap.fallbackMem = last.MemBandwidth
					}
					if last.IPCEfficiency >= 0 {
						// Ascending: stop at the first candidate that satisfies the guarantee.
						if ap.ascGuarantee && !ap.hasFirstAbove && last.IPCEfficiency >= ap.guarantee {
							ap.hasFirstAbove = true
							ap.firstAboveWays = last.L3Ways
							ap.firstAboveMem = last.MemBandwidth
							ap.runner.Abort("met_guarantee")
							done = true
						}

						if last.IPCEfficiency >= ap.guarantee {
							ap.hasLastAbove = true
							ap.lastAboveWays = last.L3Ways
							ap.lastAboveMem = last.MemBandwidth
						} else if ap.hasLastAbove {
							// We just crossed below the guarantee; revert and stop.
							_ = s.applyAllocationLocked(ap.containerIndex, ap.socket, ap.lastAboveWays, ap.lastAboveMem)
							ap.runner.Abort("below_guarantee")
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
	if ap.descGuarantee {
		if ap.hasLastAbove {
			bestWays = ap.lastAboveWays
			bestMem = ap.lastAboveMem
		} else if ap.hasFallback {
			// No allocation met the guarantee: keep the highest (first tested) allocation.
			bestWays = ap.fallbackWays
			bestMem = ap.fallbackMem
		}
	}
	if ap.ascGuarantee {
		if ap.hasFirstAbove {
			bestWays = ap.firstAboveWays
			bestMem = ap.firstAboveMem
		} else if ap.hasFallback {
			// No allocation met the guarantee: keep best-effort (highest tested in asc mode).
			bestWays = ap.fallbackWays
			bestMem = ap.fallbackMem
		}
	}

	// Optional headroom buffer: if the selected allocation meets the guarantee, try to
	// commit one additional step of resources (best-effort).
	meetsGuarantee := (ap.descGuarantee && ap.hasLastAbove) || (ap.ascGuarantee && ap.hasFirstAbove)
	bufWays := 0
	bufMemSteps := 0
	if meetsGuarantee && s.config != nil && s.config.Prober != nil {
		if s.config.Prober.BufferWays > 0 {
			bufWays = s.config.Prober.BufferWays
		}
		if s.config.Prober.BufferMemory > 0 {
			bufMemSteps = s.config.Prober.BufferMemory
		}
	}

	// Determine step/max bounds from the probe range/result.
	maxWays := 0
	maxMem := 0.0
	memStep := 0.0
	if res != nil {
		maxWays = res.Range.MaxL3Ways
		maxMem = res.Range.MaxMemBandwidth
		memStep = res.Range.StepMemBandwidth
	}
	if memStep <= 0 {
		if s.hostConfig != nil && s.hostConfig.RDT.MBAGranularity > 0 {
			memStep = float64(s.hostConfig.RDT.MBAGranularity)
		} else {
			memStep = 10
		}
	}
	if maxWays <= 0 {
		maxWays = bestWays
	}
	if maxMem <= 0 {
		maxMem = bestMem
	}

	desiredWays := bestWays
	desiredMem := bestMem
	if meetsGuarantee && (bufWays > 0 || bufMemSteps > 0) {
		if bufWays > 0 {
			desiredWays = bestWays + bufWays
			if desiredWays > maxWays {
				desiredWays = maxWays
			}
		}
		if bufMemSteps > 0 {
			desiredMem = bestMem + float64(bufMemSteps)*memStep
			if desiredMem > maxMem {
				desiredMem = maxMem
			}
		}
	}

	// Enforce monotonic commit: never lower the committed floor.
	if p != nil {
		if desiredWays < p.floorWays {
			desiredWays = p.floorWays
		}
		if desiredMem < p.floorMem {
			desiredMem = p.floorMem
		}
		if bestWays < p.floorWays {
			bestWays = p.floorWays
		}
		if bestMem < p.floorMem {
			bestMem = p.floorMem
		}
	}

	s.lastProbeDone = time.Now()
	// Mark probe as inactive before applying the final allocation so applyAllocationLocked
	// treats it as a committed (monotonic) update.
	s.probing = nil

	appliedWays := bestWays
	appliedMem := bestMem
	if desiredWays != bestWays || (desiredMem-bestMem) > 1e-9 {
		if err := s.applyAllocationLocked(idx, ap.socket, desiredWays, desiredMem); err == nil {
			appliedWays = desiredWays
			appliedMem = desiredMem
		} else {
			// Fallback to the original best allocation.
			_ = s.applyAllocationLocked(idx, ap.socket, bestWays, bestMem)
		}
	} else {
		_ = s.applyAllocationLocked(idx, ap.socket, bestWays, bestMem)
	}

	// Update committed floor after successful apply (best-effort).
	if p != nil {
		if appliedWays > p.floorWays {
			p.floorWays = appliedWays
		}
		if appliedMem > p.floorMem {
			p.floorMem = appliedMem
		}
	}

	s.schedulerLogger.WithFields(logrus.Fields{
		"container":        idx,
		"socket":           ap.socket,
		"ways":             appliedWays,
		"mem":              appliedMem,
		"best_eff":         ap.runner.BestEff(),
		"reason":           ap.runner.StopReason(),
		"desc_guarantee":   ap.descGuarantee,
		"asc_guarantee":    ap.ascGuarantee,
		"buffer_ways":      bufWays,
		"buffer_mem_steps": bufMemSteps,
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
	// Instead of OR-ing masks back (which can fragment system/default), clear the
	// allocation and repack all remaining critical classes contiguously.
	p.ways = 0
	p.mem = 0
	p.l3Mask = 0
	return s.consolidateRDTPartitionsLocked()
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

	// Monotonic (committed) floor: allow temporary decreases while probing, but never
	// commit below the container's floor outside probing.
	inProbe := s.probing != nil && s.probing.containerIndex == containerIndex
	if !inProbe {
		if ways < p.floorWays {
			ways = p.floorWays
		}
		if mem < p.floorMem {
			mem = p.floorMem
		}
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

	// If benchmark mask is already fragmented (e.g., from earlier runs), repack first.
	if !isSingleContiguousRunOfOnes(s.benchmarkMask[sock], s.totalWays) {
		if err := s.consolidateRDTPartitionsLocked(); err != nil {
			return err
		}
	}

	// L3: take contiguous ways from the HIGH end of the benchmark block.
	// We keep system/default as the LOW contiguous block; critical allocations are packed at the top.
	taken := uint64(0)
	if ways > 0 {
		// Always preserve a minimum reserve for system/default.
		bitsAvail := int(countBits(s.benchmarkMask[sock])) - systemDefaultReserveWays
		if bitsAvail < 0 {
			bitsAvail = 0
		}
		if ways > bitsAvail {
			return fmt.Errorf("insufficient benchmark cache headroom on socket %d: have %d need %d", sock, bitsAvail, ways)
		}
		benchWays := int(countBits(s.benchmarkMask[sock]))
		if benchWays < ways {
			return fmt.Errorf("insufficient benchmark cache headroom on socket %d: have %d need %d", sock, benchWays, ways)
		}
		var err error
		taken, err = takeFromHighEndOfLowBlock(benchWays, ways)
		if err != nil {
			return err
		}
		s.benchmarkMask[sock] &^= taken
	}
	// Mem: take from benchmark mem.
	// Always preserve a minimum reserve for system/default.
	memAvail := s.benchmarkMem[sock] - systemDefaultReserveMem
	if memAvail < 0 {
		memAvail = 0
	}
	if mem > memAvail {
		return fmt.Errorf("insufficient benchmark mem headroom on socket %d: have %.2f need %.2f", sock, memAvail, mem)
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

// This avoids failures from goresctrl validation ("more than one continuous block of ones")
// when classes finish and their freed blocks would otherwise leave holes in system/default.
func (s *DynamicScheduler) consolidateRDTPartitionsLocked() error {
	if s.rdtAccountant == nil {
		return nil
	}
	if s.totalWays <= 0 {
		return nil
	}

	// Collect active critical profiles by socket.
	bySock := make([][]*dynamicContainerProfile, s.sockets)
	for _, p := range s.profiles {
		if p == nil || !p.critical {
			continue
		}
		if p.containerID == "" {
			continue
		}
		if p.className == "" {
			continue
		}
		if p.ways <= 0 {
			continue
		}
		sock := p.socket
		if sock < 0 || sock >= s.sockets {
			sock = 0
		}
		bySock[sock] = append(bySock[sock], p)
	}

	// Deterministic packing order: older first, then index.
	for sock := 0; sock < s.sockets; sock++ {
		sort.Slice(bySock[sock], func(i, j int) bool {
			pi := bySock[sock][i]
			pj := bySock[sock][j]
			if pi.startedAt != pj.startedAt {
				// If startedAt is zero for some reason, treat it as newest.
				zi := pi.startedAt.IsZero()
				zj := pj.startedAt.IsZero()
				if zi != zj {
					return !zi
				}
				return pi.startedAt.Before(pj.startedAt)
			}
			return pi.index < pj.index
		})
	}

	// Compute new masks and benchmark pools.
	for sock := 0; sock < s.sockets; sock++ {
		waysList := make([]int, 0, len(bySock[sock]))
		sumMem := 0.0
		sumWays := 0
		for _, p := range bySock[sock] {
			waysList = append(waysList, p.ways)
			sumMem += p.mem
			sumWays += p.ways
		}
		assigned, benchMask, err := repackCriticalMasksHigh(s.totalWays, waysList)
		if err != nil {
			return err
		}
		// Update profile masks.
		for i := range bySock[sock] {
			bySock[sock][i].l3Mask = assigned[i]
		}
		s.benchmarkMask[sock] = benchMask
		s.benchmarkMem[sock] = 100.0 - sumMem
		if s.benchmarkMem[sock] < 0 {
			s.benchmarkMem[sock] = 0
		}
		if s.benchmarkMem[sock] > 100 {
			s.benchmarkMem[sock] = 100
		}
		// Sanity: benchmark ways should equal total - sumWays.
		_ = sumWays
	}

	// Build full class set for a single allocator SetConfig update.
	classes := make(map[string]struct {
		Socket0 *allocation.SocketAllocation
		Socket1 *allocation.SocketAllocation
	})

	// Benchmark pool (system/default)
	classes[s.benchmarkClassNameLocked()] = struct {
		Socket0 *allocation.SocketAllocation
		Socket1 *allocation.SocketAllocation
	}{
		Socket0: &allocation.SocketAllocation{L3Bitmask: fmt.Sprintf("0x%x", s.benchmarkMask[0]), MemBandwidth: s.benchmarkMem[0]},
		Socket1: func() *allocation.SocketAllocation {
			if s.sockets > 1 {
				return &allocation.SocketAllocation{L3Bitmask: fmt.Sprintf("0x%x", s.benchmarkMask[1]), MemBandwidth: s.benchmarkMem[1]}
			}
			return nil
		}(),
	}

	for sock := 0; sock < s.sockets; sock++ {
		for _, p := range bySock[sock] {
			var s0, s1 *allocation.SocketAllocation
			if sock == 0 {
				s0 = &allocation.SocketAllocation{L3Bitmask: fmt.Sprintf("0x%x", p.l3Mask), MemBandwidth: p.mem}
			} else {
				s1 = &allocation.SocketAllocation{L3Bitmask: fmt.Sprintf("0x%x", p.l3Mask), MemBandwidth: p.mem}
			}
			classes[p.className] = struct {
				Socket0 *allocation.SocketAllocation
				Socket1 *allocation.SocketAllocation
			}{Socket0: s0, Socket1: s1}
		}
	}

	return s.rdtAccountant.ReplaceAllClasses(classes)
}

func isSingleContiguousRunOfOnes(mask uint64, totalWays int) bool {
	if totalWays <= 0 {
		return true
	}
	// Scan from LSB to totalWays.
	seenOne := false
	seenZeroAfterOne := false
	for i := 0; i < totalWays; i++ {
		bit := (mask & (1 << i)) != 0
		if bit {
			if seenZeroAfterOne {
				return false
			}
			seenOne = true
			continue
		}
		if seenOne {
			seenZeroAfterOne = true
		}
	}
	return true
}

// takeFromHighEndOfLowBlock returns a contiguous bitmask of length ways positioned at the
// high end of a low-contiguous block of length benchWays.
func takeFromHighEndOfLowBlock(benchWays int, ways int) (uint64, error) {
	if ways <= 0 {
		return 0, fmt.Errorf("ways must be > 0")
	}
	if benchWays < ways {
		return 0, fmt.Errorf("benchWays must be >= ways")
	}
	if ways >= 64 {
		return ^uint64(0), nil
	}
	low, err := lowMask(ways)
	if err != nil {
		return 0, err
	}
	shift := benchWays - ways
	if shift <= 0 {
		return low, nil
	}
	return low << shift, nil
}

func lowMask(n int) (uint64, error) {
	if n <= 0 {
		return 0, fmt.Errorf("n must be > 0")
	}
	if n >= 64 {
		return ^uint64(0), nil
	}
	return (uint64(1) << n) - 1, nil
}

// repackCriticalMasksHigh packs critical allocations as adjacent blocks at the high end
// of the cache-way space and returns the assigned masks and the remaining benchmark mask
// as a low-contiguous block.
func repackCriticalMasksHigh(totalWays int, criticalWays []int) ([]uint64, uint64, error) {
	if totalWays <= 0 {
		return nil, 0, fmt.Errorf("invalid totalWays")
	}
	sum := 0
	for _, w := range criticalWays {
		if w <= 0 {
			return nil, 0, fmt.Errorf("invalid critical ways")
		}
		sum += w
	}
	if sum > totalWays {
		return nil, 0, fmt.Errorf("critical ways exceed totalWays")
	}
	assigned := make([]uint64, len(criticalWays))
	cur := totalWays
	for i, w := range criticalWays {
		cur -= w
		m, err := lowMask(w)
		if err != nil {
			return nil, 0, err
		}
		assigned[i] = m << cur
	}
	benchWays := totalWays - sum
	if benchWays == 0 {
		return assigned, 0, nil
	}
	benchMask, err := lowMask(benchWays)
	if err != nil {
		return nil, 0, err
	}
	return assigned, benchMask, nil
}

func selectDescGuaranteeAllocation(allocs []proberesources.AllocationResult, thr float64) (ways int, mem float64) {
	if len(allocs) == 0 {
		return 0, 0
	}

	// Fallback: first tested allocation (highest, for order=desc).
	fallbackWays := allocs[0].L3Ways
	fallbackMem := allocs[0].MemBandwidth

	hasAbove := false
	lastAboveWays := 0
	lastAboveMem := 0.0

	for i := range allocs {
		eff := allocs[i].IPCEfficiency
		if eff < 0 {
			continue
		}
		if eff >= thr {
			hasAbove = true
			lastAboveWays = allocs[i].L3Ways
			lastAboveMem = allocs[i].MemBandwidth
			continue
		}
		// First failure after at least one success: keep the previous (last >= thr).
		if hasAbove {
			return lastAboveWays, lastAboveMem
		}
	}

	if hasAbove {
		return lastAboveWays, lastAboveMem
	}
	return fallbackWays, fallbackMem
}

func selectAscGuaranteeAllocation(allocs []proberesources.AllocationResult, thr float64) (ways int, mem float64) {
	if len(allocs) == 0 {
		return 0, 0
	}

	// Best-effort fallback: last tested allocation (highest, for order=asc).
	fallbackWays := allocs[len(allocs)-1].L3Ways
	fallbackMem := allocs[len(allocs)-1].MemBandwidth

	for i := range allocs {
		eff := allocs[i].IPCEfficiency
		if eff < 0 {
			continue
		}
		if eff >= thr {
			return allocs[i].L3Ways, allocs[i].MemBandwidth
		}
	}
	return fallbackWays, fallbackMem
}

func maxContiguousWaysFromMask(mask uint64, totalWays int) int {
	if totalWays <= 0 {
		return 0
	}
	best := 0
	cur := 0
	for i := 0; i < totalWays; i++ {
		if (mask & (1 << i)) != 0 {
			cur++
			if cur > best {
				best = cur
			}
		} else {
			cur = 0
		}
	}
	return best
}

func maxContiguousWaysAvailableForCritical(mask uint64, totalWays int, reserveWays int) int {
	if reserveWays < 0 {
		reserveWays = 0
	}
	bits := int(countBits(mask))
	if bits <= reserveWays {
		return 0
	}
	maxByBits := bits - reserveWays
	contig := maxContiguousWaysFromMask(mask, totalWays)
	// If all available bits are one contiguous run, keeping a reserve reduces what we can take.
	if bits == contig {
		contig -= reserveWays
	}
	if contig < 0 {
		contig = 0
	}
	if contig > maxByBits {
		contig = maxByBits
	}
	return contig
}

func pickSocketForCriticalAdmission(preferredSock int, totalWays int, needMaxWays int, needMaxMem float64, masks [2]uint64, mem [2]float64, reserveWays int, reserveMem float64) int {
	// Only supports up to 2 sockets (same constraint as DynamicScheduler).
	if preferredSock != 0 && preferredSock != 1 {
		preferredSock = 0
	}

	score := func(sock int) float64 {
		contig := float64(maxContiguousWaysAvailableForCritical(masks[sock], totalWays, reserveWays))
		mba := mem[sock] - reserveMem
		if mba < 0 {
			mba = 0
		}
		// Prefer more headroom. We intentionally do NOT hard-penalize sockets that
		// can't satisfy the configured max, because we cap the probe range to what's
		// actually free on the chosen socket.
		_ = needMaxWays
		_ = needMaxMem
		return contig + (mba/100.0)*float64(totalWays)
	}

	s0 := score(0)
	s1 := score(1)
	if s1 > s0 {
		return 1
	}
	if s0 > s1 {
		return 0
	}
	return preferredSock
}
