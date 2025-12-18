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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	defaultProbeTotalSeconds          = 20.0
	defaultProbeMemStepPercent        = 10.0
	defaultMinCandidateDurationMillis = 500
	// Stop probing if adding resources improves <5% (relative)
	defaultDiminishingReturnThreshold = 0.05
)

type interferenceCandidate struct {
	ways int
	mem  float64
}

type containerProfile struct {
	index        int
	pid          int
	containerID  string
	containerKey string
	startedAt    time.Time

	evaluated bool
	unbound   bool

	demandWays int
	demandMem  float64

	socket int

	className string
	l3Mask    uint64
	memAlloc  float64
}

type activeProbe struct {
	containerIndex int
	originalSocket int
	socket         int
	socketOrder    []int
	socketPos      int
	socketResults  map[int]*probeSocketResult
	candidates     []interferenceCandidate
	candidateIdx   int
	candidateDur   time.Duration
	startStep      int
	bestIdx        int
	bestEff        float64
	bestWays       int
	bestMem        float64
	prevBestEff    float64
	startedAt      time.Time
	stopEarly      bool
	stopReason     string
}

type probeSocketResult struct {
	socket     int
	bestIdx    int
	bestEff    float64
	bestWays   int
	bestMem    float64
	earlyStop  bool
	stopReason string
	unbound    bool
}

type InterferenceAwareScheduler struct {
	name            string
	version         string
	schedulerLogger *logrus.Logger

	hostConfig    *host.HostConfig
	containers    []ContainerInfo
	rdtAccountant *accounting.RDTAccountant
	prober        *probe.Probe
	config        *config.SchedulerConfig
	cpuAllocator  cpuallocator.Allocator
	benchmarkID   int

	mu sync.Mutex

	profiles map[int]*containerProfile // containerIndex -> profile

	benchmarkClass string
	benchmarkMask  [2]uint64
	benchmarkMem   [2]float64
	totalWays      int
	sockets        int

	probing           *activeProbe
	lastProbeFinished time.Time

	probeQueue  []int
	probeQueued map[int]bool
}

func NewInterferenceAwareScheduler() *InterferenceAwareScheduler {
	return &InterferenceAwareScheduler{
		name:            "interference-aware",
		version:         "0.1.0",
		schedulerLogger: logging.GetSchedulerLogger(),
		profiles:        make(map[int]*containerProfile),
		probeQueued:     make(map[int]bool),
	}
}

func (s *InterferenceAwareScheduler) enqueueProbeLocked(containerIndex int) {
	if s.probeQueued == nil {
		s.probeQueued = make(map[int]bool)
	}
	if s.probeQueued[containerIndex] {
		return
	}
	s.probeQueued[containerIndex] = true
	s.probeQueue = append(s.probeQueue, containerIndex)
}

func (s *InterferenceAwareScheduler) peekProbeHeadLocked() (int, bool) {
	for len(s.probeQueue) > 0 {
		idx := s.probeQueue[0]
		p := s.profiles[idx]
		if p == nil || p.pid == 0 || p.evaluated {
			// Drop finished/unknown/already evaluated.
			s.probeQueue = s.probeQueue[1:]
			delete(s.probeQueued, idx)
			continue
		}
		return idx, true
	}
	return -1, false
}

func (s *InterferenceAwareScheduler) popProbeHeadLocked() {
	if len(s.probeQueue) == 0 {
		return
	}
	idx := s.probeQueue[0]
	s.probeQueue = s.probeQueue[1:]
	delete(s.probeQueued, idx)
}

func (s *InterferenceAwareScheduler) Initialize(accountant *accounting.RDTAccountant, containers []ContainerInfo, schedulerConfig *config.SchedulerConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rdtAccountant = accountant
	s.containers = containers
	s.config = schedulerConfig

	s.probeQueue = nil
	if s.probeQueued == nil {
		s.probeQueued = make(map[int]bool)
	} else {
		for k := range s.probeQueued {
			delete(s.probeQueued, k)
		}
	}

	if s.hostConfig != nil {
		s.totalWays = s.hostConfig.L3Cache.WaysPerCache
		s.sockets = s.hostConfig.Topology.Sockets
	}
	if s.totalWays <= 0 {
		s.totalWays = 12
	}
	if s.sockets <= 0 {
		s.sockets = 1
	}
	if s.sockets > 2 {
		s.schedulerLogger.WithField("sockets", s.sockets).Warn("More than 2 sockets not fully supported; treating as 2")
		s.sockets = 2
	}

	for _, c := range containers {
		containerKey := ""
		if c.Config != nil {
			containerKey = c.Config.KeyName
			if containerKey == "" {
				containerKey = c.Config.Name
			}
		}
		p := &containerProfile{
			index:        c.Index,
			pid:          c.PID,
			containerID:  c.ContainerID,
			containerKey: containerKey,
			startedAt:    time.Now(),
			socket:       -1,
			className:    s.containerClassName(c.Index, containerKey),
		}
		s.profiles[c.Index] = p
		if c.PID != 0 {
			s.enqueueProbeLocked(c.Index)
		}
	}

	// Create benchmark pool class (and move running containers into it).
	if s.rdtAccountant != nil {
		s.benchmarkClass = s.benchmarkClassName()
		if err := s.ensureBenchmarkClassLocked(); err != nil {
			s.schedulerLogger.WithError(err).Warn("Failed to initialize benchmark RDT class; continuing without RDT allocations")
			s.rdtAccountant = nil
		}
	}

	s.schedulerLogger.WithFields(logrus.Fields{
		"containers": len(containers),
		"rdt":        s.rdtAccountant != nil,
		"ways":       s.totalWays,
		"sockets":    s.sockets,
	}).Info("Interference-aware scheduler initialized")

	return nil
}

func (s *InterferenceAwareScheduler) ProcessDataFrames(dfs *dataframe.DataFrames) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rdtAccountant == nil {
		return nil
	}

	warmupSeconds := 5
	cooldownSeconds := 2
	if s.config != nil {
		if s.config.WarmupT > 0 {
			warmupSeconds = s.config.WarmupT
		}
		if s.config.CooldownT > 0 {
			cooldownSeconds = s.config.CooldownT
		}
	}
	if cooldownSeconds > 0 && !s.lastProbeFinished.IsZero() {
		if time.Since(s.lastProbeFinished) < time.Duration(cooldownSeconds)*time.Second {
			return nil
		}
	}

	// Advance active probe.
	if s.probing != nil {
		return s.stepProbeLocked(dfs)
	}

	// FIFO: only probe the head of the queue.
	headIdx, ok := s.peekProbeHeadLocked()
	if !ok {
		return nil
	}
	p := s.profiles[headIdx]
	if p == nil {
		// Shouldn't happen due to peekProbeHeadLocked, but be safe.
		s.popProbeHeadLocked()
		return nil
	}
	if p.startedAt.IsZero() {
		p.startedAt = time.Now()
	}
	if warmupSeconds > 0 && time.Since(p.startedAt) < time.Duration(warmupSeconds)*time.Second {
		// Head-of-line warmup; don't probe others yet.
		return nil
	}

	// Pop now so we don't re-enter if startProbeLocked fails early; failures will re-enqueue on next tick if still not evaluated.
	s.popProbeHeadLocked()
	if err := s.startProbeLocked(dfs, headIdx); err != nil {
		// Retry later.
		s.enqueueProbeLocked(headIdx)
		return err
	}
	return nil
}

func (s *InterferenceAwareScheduler) Shutdown() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rdtAccountant != nil {
		_ = s.rdtAccountant.Cleanup()
	}
	return nil
}

func (s *InterferenceAwareScheduler) GetVersion() string { return s.version }

func (s *InterferenceAwareScheduler) SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	s.schedulerLogger.SetLevel(logLevel)
	return nil
}

func (s *InterferenceAwareScheduler) SetHostConfig(hostConfig *host.HostConfig) {
	s.hostConfig = hostConfig
}

func (s *InterferenceAwareScheduler) SetCPUAllocator(allocator cpuallocator.Allocator) {
	s.cpuAllocator = allocator
}

func (s *InterferenceAwareScheduler) AssignCPUCores(containerIndex int) ([]int, error) {
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
	return s.cpuAllocator.EnsureAssigned(containerIndex, cfg)
}

func (s *InterferenceAwareScheduler) SetProbe(prober *probe.Probe) {
	s.prober = prober
}

func (s *InterferenceAwareScheduler) SetBenchmarkID(benchmarkID int) {
	s.benchmarkID = benchmarkID
}

func (s *InterferenceAwareScheduler) OnContainerStart(info ContainerInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	containerKey := ""
	if info.Config != nil {
		containerKey = info.Config.KeyName
		if containerKey == "" {
			containerKey = info.Config.Name
		}
	}

	for i := range s.containers {
		if s.containers[i].Index == info.Index {
			s.containers[i].PID = info.PID
			s.containers[i].ContainerID = info.ContainerID
			break
		}
	}
	p := s.profiles[info.Index]
	if p == nil {
		p = &containerProfile{index: info.Index}
		s.profiles[info.Index] = p
	}
	p.pid = info.PID
	p.containerID = info.ContainerID
	p.containerKey = containerKey
	p.startedAt = time.Now()

	desiredClass := s.containerClassName(info.Index, containerKey)
	if p.className != "" && p.className != desiredClass && s.rdtAccountant != nil {
		_ = s.rdtAccountant.DeleteClass(p.className)
		p.l3Mask = 0
		p.memAlloc = 0
	}
	p.className = desiredClass

	// Re-probe on restart.
	p.evaluated = false
	p.unbound = false
	p.demandWays = 0
	p.demandMem = 0

	// Best-effort: place new container into benchmark pool immediately.
	if s.rdtAccountant != nil && info.PID != 0 {
		_ = s.moveContainerCgroupLocked(info.Index, s.benchmarkClass)
	}

	// Interference-aware admission: place new container on the least-loaded socket.
	if s.cpuAllocator != nil && p.containerID != "" {
		if bestSock, ok := s.pickBestSocketForContainerLocked(info.Index); ok {
			if p.socket != bestSock {
				if _, err := s.cpuAllocator.Move(info.Index, p.containerID, bestSock); err == nil {
					p.socket = bestSock
					_ = s.moveContainerCgroupLocked(info.Index, s.benchmarkClass)
				}
			}
		}
	}

	// Enqueue for probing.
	if info.PID != 0 {
		s.enqueueProbeLocked(info.Index)
	}

	return nil
}

func (s *InterferenceAwareScheduler) OnContainerStop(containerIndex int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Prevent stale queue entries from blocking head-of-line.
	if s.probeQueued != nil {
		delete(s.probeQueued, containerIndex)
	}

	if s.cpuAllocator != nil {
		s.cpuAllocator.Release(containerIndex)
	}

	for i := range s.containers {
		if s.containers[i].Index == containerIndex {
			s.containers[i].PID = 0
			break
		}
	}
	p := s.profiles[containerIndex]
	if p != nil {
		p.pid = 0
	}

	if s.rdtAccountant != nil {
		_ = s.releaseContainerAllocationLocked(containerIndex)
		if s.config != nil && s.config.Reallocate {
			_ = s.rebalanceDemandLocked()
		}
	}

	return nil
}

func (s *InterferenceAwareScheduler) benchmarkClassName() string {
	if s.benchmarkID > 0 {
		return fmt.Sprintf("bench-%d-benchmark", s.benchmarkID)
	}
	return "bench-benchmark"
}

func sanitizeResctrlNamePart(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > 32 {
		out = out[:32]
	}
	return out
}

func (s *InterferenceAwareScheduler) containerClassName(containerIndex int, containerKey string) string {
	key := sanitizeResctrlNamePart(containerKey)
	if s.benchmarkID > 0 {
		if key != "" {
			return fmt.Sprintf("bench-%d-c%d-%s", s.benchmarkID, containerIndex, key)
		}
		return fmt.Sprintf("bench-%d-c%d", s.benchmarkID, containerIndex)
	}
	if key != "" {
		return fmt.Sprintf("bench-c%d-%s", containerIndex, key)
	}
	return fmt.Sprintf("bench-c%d", containerIndex)
}

func (s *InterferenceAwareScheduler) probeMaxFromConfigLocked() (int, float64) {
	maxL3 := 4
	maxMem := 30.0
	if s.config != nil {
		if s.config.MaxL3 > 0 {
			maxL3 = s.config.MaxL3
		}
		if s.config.MaxMem > 0 {
			maxMem = s.config.MaxMem
		}
	}
	return maxL3, maxMem
}

func (s *InterferenceAwareScheduler) estimatedDemandForIndexLocked(containerIndex int) (int, float64, bool) {
	p := s.profiles[containerIndex]
	if p == nil || p.pid == 0 {
		return 0, 0, false
	}
	if p.evaluated {
		ways := p.demandWays
		mem := p.demandMem
		if ways <= 0 {
			ways = 1
		}
		if mem < 0 {
			mem = 0
		}
		return ways, mem, true
	}
	maxL3, maxMem := s.probeMaxFromConfigLocked()
	return maxL3, maxMem, true
}

func socketInterferenceScore(loadWays int, loadMem float64, capWays int) float64 {
	if capWays <= 0 {
		capWays = 12
	}
	pw := float64(loadWays) / float64(capWays)
	pm := loadMem / 100.0
	if pm > pw {
		return pm
	}
	return pw
}

func (s *InterferenceAwareScheduler) socketLoadExcludingLocked(socket int, excludeIndex int) (int, float64) {
	ways := 0
	mem := 0.0
	for idx := range s.profiles {
		if idx == excludeIndex {
			continue
		}
		p := s.profiles[idx]
		if p == nil || p.pid == 0 {
			continue
		}
		if p.socket != socket {
			continue
		}
		dw, dm, ok := s.estimatedDemandForIndexLocked(idx)
		if !ok {
			continue
		}
		ways += dw
		mem += dm
	}
	return ways, mem
}

func (s *InterferenceAwareScheduler) pickBestSocketForContainerLocked(containerIndex int) (int, bool) {
	if s.sockets <= 0 {
		s.sockets = 1
	}
	dw, dm, ok := s.estimatedDemandForIndexLocked(containerIndex)
	if !ok {
		return 0, false
	}
	bestSock := 0
	bestScore := 1e9
	for sock := 0; sock < s.sockets; sock++ {
		lw, lm := s.socketLoadExcludingLocked(sock, containerIndex)
		score := socketInterferenceScore(lw+dw, lm+dm, s.totalWays)
		if score < bestScore {
			bestScore = score
			bestSock = sock
		}
	}
	return bestSock, true
}

func (s *InterferenceAwareScheduler) currentSocketForContainerLocked(containerIndex int) int {
	// Prefer scheduler-tracked socket.
	if p := s.profiles[containerIndex]; p != nil {
		if p.socket >= 0 && p.socket < s.sockets {
			return p.socket
		}
	}
	// Fallback: infer from current CPU assignment.
	if s.cpuAllocator != nil && s.hostConfig != nil {
		if cpuIDs, ok := s.cpuAllocator.Get(containerIndex); ok {
			if sock, err := s.hostConfig.SocketOfPhysicalCPUs(cpuIDs); err == nil {
				return sock
			}
		}
	}
	return 0
}

func (s *InterferenceAwareScheduler) ensureBenchmarkClassLocked() error {
	// Initial pool: all ways and all mem on each socket.
	initMask, err := fullMask(s.totalWays)
	if err != nil {
		return err
	}
	s.benchmarkMask[0] = initMask
	s.benchmarkMem[0] = 100
	if s.sockets > 1 {
		s.benchmarkMask[1] = initMask
		s.benchmarkMem[1] = 100
	}

	err = s.rdtAccountant.CreateClass(
		s.benchmarkClass,
		&accounting.AllocationRequest{L3Bitmask: fmt.Sprintf("0x%x", s.benchmarkMask[0]), MemBandwidth: s.benchmarkMem[0]},
		func() *accounting.AllocationRequest {
			if s.sockets > 1 {
				return &accounting.AllocationRequest{L3Bitmask: fmt.Sprintf("0x%x", s.benchmarkMask[1]), MemBandwidth: s.benchmarkMem[1]}
			}
			return nil
		}(),
	)
	if err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return fmt.Errorf("failed to create benchmark class %s: %w", s.benchmarkClass, err)
		}
		// If class already exists (e.g. rerun), try update.
		if uerr := s.rdtAccountant.UpdateClass(
			s.benchmarkClass,
			&accounting.AllocationRequest{L3Bitmask: fmt.Sprintf("0x%x", s.benchmarkMask[0]), MemBandwidth: s.benchmarkMem[0]},
			func() *accounting.AllocationRequest {
				if s.sockets > 1 {
					return &accounting.AllocationRequest{L3Bitmask: fmt.Sprintf("0x%x", s.benchmarkMask[1]), MemBandwidth: s.benchmarkMem[1]}
				}
				return nil
			}(),
		); uerr != nil {
			return fmt.Errorf("failed to update existing benchmark class %s after create error (%v): %w", s.benchmarkClass, err, uerr)
		}
	}

	// Move already-running containers into benchmark pool.
	for _, c := range s.containers {
		if c.PID == 0 {
			continue
		}
		_ = s.moveContainerCgroupLocked(c.Index, s.benchmarkClass)
	}
	return nil
}

func (s *InterferenceAwareScheduler) startProbeLocked(dfs *dataframe.DataFrames, containerIndex int) error {
	p := s.profiles[containerIndex]
	if p == nil || p.pid == 0 {
		return nil
	}

	// Ensure container starts in benchmark class.
	_ = s.moveContainerCgroupLocked(containerIndex, s.benchmarkClass)

	cfg := s.config
	maxL3 := 4
	maxMem := 30.0
	budgetSeconds := defaultProbeTotalSeconds
	breakCond := 0.9
	breakImprovement := 0.1
	if cfg != nil {
		if cfg.MaxL3 > 0 {
			maxL3 = cfg.MaxL3
		}
		if cfg.MaxMem > 0 {
			maxMem = cfg.MaxMem
		}
		if cfg.ProbingT > 0 {
			budgetSeconds = cfg.ProbingT
		}
		if cfg.BreakCondition > 0 {
			breakCond = cfg.BreakCondition
		}
		if cfg.BreakImprovement > 0 {
			breakImprovement = cfg.BreakImprovement
		}
	}

	minMem := defaultProbeMemStepPercent
	if maxMem <= 0 {
		minMem = 0
		maxMem = 0
	} else if maxMem < minMem {
		minMem = maxMem
	}

	probeRange := proberesources.AllocationRange{
		MinL3Ways:        1,
		MaxL3Ways:        maxL3,
		MinMemBandwidth:  minMem,
		MaxMemBandwidth:  maxMem,
		StepL3Ways:       1,
		StepMemBandwidth: defaultProbeMemStepPercent,
		Order:            "asc",
	}
	specs := proberesources.GenerateAllocationSequence(probeRange)
	candidates := make([]interferenceCandidate, 0, len(specs))
	for _, sp := range specs {
		candidates = append(candidates, interferenceCandidate{ways: sp.L3Ways, mem: sp.MemBandwidth})
	}
	if len(candidates) == 0 {
		return fmt.Errorf("no probe candidates")
	}

	// Probe is about finding resource demand; it does not depend on socket.
	// Stay on the container's current/admitted socket for the entire probe.
	startSock := s.currentSocketForContainerLocked(containerIndex)
	socketOrder := []int{startSock}

	candDur := time.Duration(float64(time.Second) * (budgetSeconds / float64(len(candidates))))
	if candDur < time.Duration(defaultMinCandidateDurationMillis)*time.Millisecond {
		candDur = time.Duration(defaultMinCandidateDurationMillis) * time.Millisecond
	}

	origSock := startSock
	if origSock < 0 || origSock >= s.sockets {
		origSock = 0
	}

	s.probing = &activeProbe{
		containerIndex: containerIndex,
		originalSocket: origSock,
		socketOrder:    socketOrder,
		socketPos:      0,
		socketResults:  make(map[int]*probeSocketResult),
		candidates:     candidates,
		candidateIdx:   0,
		candidateDur:   candDur,
		bestIdx:        -1,
		bestEff:        -1,
		startedAt:      time.Now(),
	}
	ap := s.probing

	// Start probing on the first socket we can actually move to.
	startedOn := -1
	for ap.socketPos < len(socketOrder) {
		sock := socketOrder[ap.socketPos]
		if err := s.startProbeOnSocketLocked(dfs, containerIndex, sock); err != nil {
			s.schedulerLogger.WithError(err).WithFields(logrus.Fields{"container": containerIndex, "socket": sock}).Warn("Failed to start probing on socket")
			ap.socketResults[sock] = nil
			ap.socketPos++
			continue
		}
		startedOn = sock
		break
	}
	if startedOn == -1 {
		s.schedulerLogger.WithFields(logrus.Fields{"container": containerIndex}).Warn("No probe socket available; marking container unbound")
		p := s.profiles[containerIndex]
		if p != nil {
			p.evaluated = true
			p.unbound = true
		}
		s.probing = nil
		return nil
	}

	s.schedulerLogger.WithFields(logrus.Fields{
		"container":     containerIndex,
		"socket":        startedOn,
		"candidates":    len(candidates),
		"candidate_dur": candDur.Seconds(),
		"break_cond":    breakCond,
		"break_improve": breakImprovement,
		"max_l3":        maxL3,
		"max_mem":       maxMem,
	}).Info("Started interference-aware allocation probing")

	return nil
}

func (s *InterferenceAwareScheduler) stepProbeLocked(dfs *dataframe.DataFrames) error {
	ap := s.probing
	if ap == nil {
		return nil
	}

	cfg := s.config
	breakCondEnabled := true
	breakImprovementEnabled := true
	breakCond := 0.9
	breakImprovement := 0.1
	if cfg != nil {
		if cfg.BreakCondition < 0 {
			breakCondEnabled = false
		} else if cfg.BreakCondition > 0 {
			breakCond = cfg.BreakCondition
			if breakCond > 1 && breakCond <= 100 {
				breakCond = breakCond / 100
			}
		}
		if cfg.BreakImprovement < 0 {
			breakImprovementEnabled = false
		} else if cfg.BreakImprovement > 0 {
			breakImprovement = cfg.BreakImprovement
			if breakImprovement > 1 && breakImprovement <= 100 {
				breakImprovement = breakImprovement / 100
			}
		}
	}

	// Wait until candidate duration elapsed.
	if time.Since(ap.startedAt) < ap.candidateDur {
		return nil
	}

	endStep := latestStepNumber(dfs, ap.containerIndex)
	eff := averageIPCEfficiency(dfs, ap.containerIndex, ap.startStep, endStep)
	cand := ap.candidates[ap.candidateIdx]

	if eff >= 0 {
		if ap.bestIdx == -1 || eff > ap.bestEff {
			ap.prevBestEff = ap.bestEff
			ap.bestIdx = ap.candidateIdx
			ap.bestEff = eff
			ap.bestWays = cand.ways
			ap.bestMem = cand.mem
		}
	}

	// Stop conditions (per-socket).
	if breakCondEnabled && ap.bestEff >= breakCond {
		ap.stopEarly = true
		ap.stopReason = "break_condition"
		return s.finishSocketProbeLocked(dfs)
	}
	if breakImprovementEnabled && ap.bestIdx != -1 && ap.prevBestEff > 0 {
		rel := (ap.bestEff - ap.prevBestEff) / ap.prevBestEff
		if rel >= 0 && rel < breakImprovement {
			ap.stopEarly = true
			ap.stopReason = "break_improvement"
			return s.finishSocketProbeLocked(dfs)
		}
	}
	if breakImprovementEnabled && ap.bestIdx != -1 && ap.prevBestEff > 0 {
		rel := (ap.bestEff - ap.prevBestEff) / ap.prevBestEff
		if rel >= 0 && rel < defaultDiminishingReturnThreshold {
			ap.stopEarly = true
			ap.stopReason = "diminishing_returns"
			return s.finishSocketProbeLocked(dfs)
		}
	}

	ap.candidateIdx++
	if ap.candidateIdx >= len(ap.candidates) {
		ap.stopEarly = false
		ap.stopReason = "completed"
		return s.finishSocketProbeLocked(dfs)
	}

	// Apply next candidate.
	next := ap.candidates[ap.candidateIdx]
	ap.startedAt = time.Now()
	ap.startStep = latestStepNumber(dfs, ap.containerIndex)
	if err := s.setContainerAllocationLocked(ap.containerIndex, ap.socket, next.ways, next.mem); err != nil {
		s.schedulerLogger.WithError(err).WithFields(logrus.Fields{"container": ap.containerIndex, "socket": ap.socket, "ways": next.ways, "mem": next.mem}).Warn("Failed to apply probe allocation")
	}

	return nil
}

func (s *InterferenceAwareScheduler) startProbeOnSocketLocked(dfs *dataframe.DataFrames, containerIndex int, socket int) error {
	ap := s.probing
	if ap == nil {
		return fmt.Errorf("no active probe")
	}
	p := s.profiles[containerIndex]
	if p == nil {
		return fmt.Errorf("unknown container %d", containerIndex)
	}

	// Ensure we probe on the container's current socket (no CPU moves during probing).
	socket = s.currentSocketForContainerLocked(containerIndex)

	// Reset candidate tracking for this socket.
	ap.socket = socket
	ap.candidateIdx = 0
	ap.bestIdx = -1
	ap.bestEff = -1
	ap.prevBestEff = -1
	ap.stopEarly = false
	ap.stopReason = ""

	// Ensure container starts from benchmark pool on this socket.
	_ = s.resetContainerToBenchmarkLocked(containerIndex)
	// Keep p.socket intact; probing must not change placement.

	ap.startedAt = time.Now()
	ap.startStep = latestStepNumber(dfs, containerIndex)

	first := ap.candidates[0]
	if err := s.setContainerAllocationLocked(containerIndex, socket, first.ways, first.mem); err != nil {
		return err
	}

	return nil
}

func (s *InterferenceAwareScheduler) finishSocketProbeLocked(dfs *dataframe.DataFrames) error {
	ap := s.probing
	if ap == nil {
		return nil
	}
	containerIndex := ap.containerIndex

	cfg := s.config
	maxL3 := 4
	maxMem := 30.0
	if cfg != nil {
		if cfg.MaxL3 > 0 {
			maxL3 = cfg.MaxL3
		}
		if cfg.MaxMem > 0 {
			maxMem = cfg.MaxMem
		}
	}

	res := &probeSocketResult{
		socket:     ap.socket,
		bestIdx:    ap.bestIdx,
		bestEff:    ap.bestEff,
		bestWays:   ap.bestWays,
		bestMem:    ap.bestMem,
		earlyStop:  ap.stopEarly,
		stopReason: ap.stopReason,
		unbound:    false,
	}
	if res.bestIdx == -1 {
		// Treat as unbound if we couldn't measure.
		res.bestWays = maxL3
		res.bestMem = maxMem
		res.unbound = true
	} else {
		// Unbound if we exhausted max without early stop.
		if res.bestWays >= maxL3 && res.bestMem >= maxMem && !res.earlyStop {
			res.unbound = true
		}
	}
	ap.socketResults[ap.socket] = res

	// Return probe allocation to benchmark before moving to next socket.
	_ = s.resetContainerToBenchmarkLocked(containerIndex)

	ap.socketPos++
	for ap.socketPos < len(ap.socketOrder) {
		sock := ap.socketOrder[ap.socketPos]
		if err := s.startProbeOnSocketLocked(dfs, containerIndex, sock); err != nil {
			s.schedulerLogger.WithError(err).WithFields(logrus.Fields{"container": containerIndex, "socket": sock}).Warn("Failed to start probing on socket")
			ap.socketResults[sock] = nil
			ap.socketPos++
			continue
		}
		return nil
	}

	return s.finalizeProbeLocked()
}

func (s *InterferenceAwareScheduler) finalizeProbeLocked() error {
	ap := s.probing
	if ap == nil {
		return nil
	}
	containerIndex := ap.containerIndex
	p := s.profiles[containerIndex]
	if p == nil {
		s.probing = nil
		return nil
	}

	cfg := s.config
	maxL3 := 4
	maxMem := 30.0
	skipAlloc := true
	allocateUnbound := false
	if cfg != nil {
		if cfg.MaxL3 > 0 {
			maxL3 = cfg.MaxL3
		}
		if cfg.MaxMem > 0 {
			maxMem = cfg.MaxMem
		}
		if cfg.SkipAllocationAfterProbing != nil {
			skipAlloc = *cfg.SkipAllocationAfterProbing
		}
		allocateUnbound = cfg.AllocateUnbound
	}

	// Choose best socket based on best efficiency; tie-break on lower demand.
	bestSocket := -1
	bestEff := -1.0
	bestWays := maxL3
	bestMem := maxMem
	bestUnbound := true
	bestReason := "no_data"
	for sock, res := range ap.socketResults {
		if res == nil {
			continue
		}
		eff := res.bestEff
		if eff < 0 {
			// Still consider but lower priority.
			eff = -1
		}
		better := false
		if bestSocket == -1 {
			better = true
		} else if eff > bestEff {
			better = true
		} else if eff == bestEff {
			if res.bestWays < bestWays || (res.bestWays == bestWays && res.bestMem < bestMem) {
				better = true
			}
		}
		if better {
			bestSocket = sock
			bestEff = res.bestEff
			bestWays = res.bestWays
			bestMem = res.bestMem
			bestUnbound = res.unbound
			bestReason = res.stopReason
		}
	}
	if bestSocket < 0 {
		bestSocket = 0
	}

	// Persist profile.
	p.evaluated = true
	p.unbound = bestUnbound
	p.demandWays = bestWays
	p.demandMem = bestMem

	// Ensure container is in benchmark pool before final decision.
	_ = s.resetContainerToBenchmarkLocked(containerIndex)

	// Only move sockets if it reduces estimated interference.
	// NOTE: p.socket is mutated during probing as we hop sockets; use the original socket captured at probe start.
	currentSocket := ap.originalSocket
	if currentSocket < 0 || currentSocket >= s.sockets {
		currentSocket = 0
	}
	shouldMove := false
	if s.sockets > 1 && bestSocket != currentSocket {
		lwCur, lmCur := s.socketLoadExcludingLocked(currentSocket, containerIndex)
		lwBest, lmBest := s.socketLoadExcludingLocked(bestSocket, containerIndex)
		curScore := socketInterferenceScore(lwCur+p.demandWays, lmCur+p.demandMem, s.totalWays)
		bestScore := socketInterferenceScore(lwBest+p.demandWays, lmBest+p.demandMem, s.totalWays)
		if bestScore+0.01 < curScore {
			shouldMove = true
		}
	}

	if s.cpuAllocator != nil && p.containerID != "" {
		target := currentSocket
		if shouldMove {
			target = bestSocket
		}
		if target != p.socket {
			if _, err := s.cpuAllocator.Move(containerIndex, p.containerID, target); err != nil {
				s.schedulerLogger.WithError(err).WithFields(logrus.Fields{"container": containerIndex, "from_socket": p.socket, "to_socket": target}).Warn("Failed to move container socket")
			} else {
				p.socket = target
			}
		}
	} else {
		p.socket = currentSocket
	}
	_ = s.moveContainerCgroupLocked(containerIndex, s.benchmarkClass)

	// Allocation retention policy:
	// - default: skip allocation after probing
	// - if allocate_unbound: keep allocations for unbound containers (still requiring headroom)
	keep := false
	if p.unbound && (!skipAlloc || allocateUnbound) {
		keep = s.wouldLeaveProbeHeadroomLocked(bestSocket, p.demandWays, p.demandMem, maxL3, maxMem)
	}
	if keep {
		if err := s.setContainerAllocationLocked(containerIndex, bestSocket, p.demandWays, p.demandMem); err != nil {
			s.schedulerLogger.WithError(err).WithFields(logrus.Fields{"container": containerIndex, "socket": bestSocket}).Warn("Failed to keep post-probe allocation; leaving in benchmark")
			_ = s.resetContainerToBenchmarkLocked(containerIndex)
			keep = false
		}
	}

	s.schedulerLogger.WithFields(logrus.Fields{
		"container":     containerIndex,
		"socket":        bestSocket,
		"ways":          p.demandWays,
		"mem":           p.demandMem,
		"best_eff":      bestEff,
		"unbound":       p.unbound,
		"kept_alloc":    keep,
		"skip_alloc":    skipAlloc,
		"alloc_unbound": allocateUnbound,
		"stop_reason":   bestReason,
		"sockets":       len(ap.socketResults),
	}).Info("Finished interference-aware probing")

	if s.config != nil && s.config.Reallocate {
		_ = s.rebalanceDemandLocked()
	}

	s.lastProbeFinished = time.Now()
	s.probing = nil
	return nil
}

func (s *InterferenceAwareScheduler) rebalanceDemandLocked() error {
	if s.sockets <= 1 || s.cpuAllocator == nil {
		return nil
	}

	const eps = 0.02

	type item struct {
		idx       int
		curSocket int
		ways      int
		mem       float64
	}

	capWays := s.totalWays
	if capWays <= 0 {
		capWays = 12
	}

	// Base loads include containers we won't move (kept allocations) plus all containers with unknown IDs.
	baseWays := make([]int, s.sockets)
	baseMem := make([]float64, s.sockets)

	items := make([]item, 0, len(s.profiles))
	for idx, p := range s.profiles {
		if p == nil || p.pid == 0 {
			continue
		}
		curSock := p.socket
		if curSock < 0 || curSock >= s.sockets {
			curSock = 0
		}
		dw, dm, ok := s.estimatedDemandForIndexLocked(idx)
		if !ok {
			continue
		}

		movable := (p.l3Mask == 0 && p.memAlloc == 0 && p.containerID != "")
		if !movable {
			baseWays[curSock] += dw
			baseMem[curSock] += dm
			continue
		}

		items = append(items, item{idx: idx, curSocket: curSock, ways: dw, mem: dm})
	}
	if len(items) == 0 {
		return nil
	}

	// Current objective.
	curWays := make([]int, s.sockets)
	curMem := make([]float64, s.sockets)
	copy(curWays, baseWays)
	copy(curMem, baseMem)
	for _, it := range items {
		curWays[it.curSocket] += it.ways
		curMem[it.curSocket] += it.mem
	}
	currentObj := 0.0
	for sock := 0; sock < s.sockets; sock++ {
		s := socketInterferenceScore(curWays[sock], curMem[sock], capWays)
		if s > currentObj {
			currentObj = s
		}
	}

	// Planned objective (greedy minimize max socket score).
	sort.Slice(items, func(i, j int) bool {
		if items[i].ways != items[j].ways {
			return items[i].ways > items[j].ways
		}
		return items[i].mem > items[j].mem
	})

	plWays := make([]int, s.sockets)
	plMem := make([]float64, s.sockets)
	copy(plWays, baseWays)
	copy(plMem, baseMem)
	targetSock := make(map[int]int, len(items))

	for _, it := range items {
		bestSock := 0
		bestObj := 1e9
		for sock := 0; sock < s.sockets; sock++ {
			// Compute objective if we place this item on sock.
			maxScore := 0.0
			for sidx := 0; sidx < s.sockets; sidx++ {
				w := plWays[sidx]
				m := plMem[sidx]
				if sidx == sock {
					w += it.ways
					m += it.mem
				}
				score := socketInterferenceScore(w, m, capWays)
				if score > maxScore {
					maxScore = score
				}
			}
			if maxScore < bestObj {
				bestObj = maxScore
				bestSock = sock
			}
		}
		targetSock[it.idx] = bestSock
		plWays[bestSock] += it.ways
		plMem[bestSock] += it.mem
	}

	plannedObj := 0.0
	for sock := 0; sock < s.sockets; sock++ {
		s := socketInterferenceScore(plWays[sock], plMem[sock], capWays)
		if s > plannedObj {
			plannedObj = s
		}
	}

	if plannedObj+eps >= currentObj {
		return nil
	}

	for _, it := range items {
		p := s.profiles[it.idx]
		if p == nil || p.containerID == "" {
			continue
		}
		dst := targetSock[it.idx]
		if dst == it.curSocket {
			continue
		}
		if _, err := s.cpuAllocator.Move(it.idx, p.containerID, dst); err != nil {
			continue
		}
		p.socket = dst
		_ = s.moveContainerCgroupLocked(it.idx, s.benchmarkClass)
	}

	return nil
}

func (s *InterferenceAwareScheduler) setContainerAllocationLocked(containerIndex int, socket int, ways int, mem float64) error {
	p := s.profiles[containerIndex]
	if p == nil {
		return fmt.Errorf("unknown container %d", containerIndex)
	}
	if socket < 0 {
		socket = 0
	}
	if socket >= s.sockets {
		socket = 0
	}

	// Ensure benchmark class exists.
	if s.benchmarkClass == "" {
		s.benchmarkClass = s.benchmarkClassName()
	}

	// If container already has an allocation, reclaim it to benchmark first.
	if p.l3Mask != 0 || p.memAlloc > 0 {
		prevSocket := p.socket
		if prevSocket < 0 {
			prevSocket = socket
		}
		if prevSocket >= s.sockets {
			prevSocket = 0
		}

		// Clear container class allocation.
		_ = s.rdtAccountant.UpdateClass(p.className, nil, nil)

		// Reclaim into pool.
		s.benchmarkMask[prevSocket] |= p.l3Mask
		s.benchmarkMem[prevSocket] += p.memAlloc
		if s.benchmarkMem[prevSocket] > 100 {
			s.benchmarkMem[prevSocket] = 100
		}
		if err := s.updateBenchmarkClassLocked(); err != nil {
			return err
		}
		p.l3Mask = 0
		p.memAlloc = 0
	}

	if ways <= 0 {
		ways = 1
	}
	if mem < 0 {
		mem = 0
	}

	// Take from benchmark pool.
	taken, remaining, err := takeContiguousFromMask(s.benchmarkMask[socket], ways, s.totalWays)
	if err != nil {
		return err
	}
	if s.benchmarkMem[socket]-mem < 0 {
		return fmt.Errorf("insufficient benchmark memory bandwidth on socket %d", socket)
	}

	s.benchmarkMask[socket] = remaining
	s.benchmarkMem[socket] -= mem
	if err := s.updateBenchmarkClassLocked(); err != nil {
		return err
	}

	bitmaskStr := fmt.Sprintf("0x%x", taken)

	// Create or update container class.
	createErr := s.rdtAccountant.CreateClass(
		p.className,
		func() *accounting.AllocationRequest {
			if socket == 0 {
				return &accounting.AllocationRequest{L3Bitmask: bitmaskStr, MemBandwidth: mem}
			}
			return nil
		}(),
		func() *accounting.AllocationRequest {
			if s.sockets > 1 && socket == 1 {
				return &accounting.AllocationRequest{L3Bitmask: bitmaskStr, MemBandwidth: mem}
			}
			return nil
		}(),
	)
	if createErr != nil {
		// Only fall back to UpdateClass if the error is about an existing class.
		if !strings.Contains(strings.ToLower(createErr.Error()), "already exists") {
			// Roll back pool shrink.
			s.benchmarkMask[socket] |= taken
			s.benchmarkMem[socket] += mem
			if s.benchmarkMem[socket] > 100 {
				s.benchmarkMem[socket] = 100
			}
			_ = s.updateBenchmarkClassLocked()
			return fmt.Errorf("failed to create class %s: %w", p.className, createErr)
		}

		if err := s.rdtAccountant.UpdateClass(
			p.className,
			func() *accounting.AllocationRequest {
				if socket == 0 {
					return &accounting.AllocationRequest{L3Bitmask: bitmaskStr, MemBandwidth: mem}
				}
				return nil
			}(),
			func() *accounting.AllocationRequest {
				if s.sockets > 1 && socket == 1 {
					return &accounting.AllocationRequest{L3Bitmask: bitmaskStr, MemBandwidth: mem}
				}
				return nil
			}(),
		); err != nil {
			// Roll back pool shrink.
			s.benchmarkMask[socket] |= taken
			s.benchmarkMem[socket] += mem
			if s.benchmarkMem[socket] > 100 {
				s.benchmarkMem[socket] = 100
			}
			_ = s.updateBenchmarkClassLocked()
			return fmt.Errorf("failed to update existing class %s after create error (%v): %w", p.className, createErr, err)
		}
	}

	p.l3Mask = taken
	p.memAlloc = mem
	p.socket = socket

	// Ensure all cgroup PIDs are moved.
	if err := s.moveContainerCgroupLocked(containerIndex, p.className); err != nil {
		s.schedulerLogger.WithError(err).WithFields(logrus.Fields{"container": containerIndex, "class": p.className}).Warn("Failed to move container cgroup PIDs to class")
	}

	return nil
}

func (s *InterferenceAwareScheduler) resetContainerToBenchmarkLocked(containerIndex int) error {
	_ = s.releaseContainerAllocationLocked(containerIndex)
	return s.moveContainerCgroupLocked(containerIndex, s.benchmarkClass)
}

func (s *InterferenceAwareScheduler) wouldLeaveProbeHeadroomLocked(socket int, demandWays int, demandMem float64, probeWays int, probeMem float64) bool {
	if socket < 0 {
		socket = 0
	}
	if socket >= s.sockets {
		socket = 0
	}
	freeWays := int(countBits(s.benchmarkMask[socket]))
	freeMem := s.benchmarkMem[socket]
	return (freeWays-demandWays) >= probeWays && (freeMem-demandMem) >= probeMem
}

func (s *InterferenceAwareScheduler) updateBenchmarkClassLocked() error {
	if s.rdtAccountant == nil {
		return nil
	}
	return s.rdtAccountant.UpdateClass(
		s.benchmarkClass,
		&accounting.AllocationRequest{L3Bitmask: fmt.Sprintf("0x%x", s.benchmarkMask[0]), MemBandwidth: s.benchmarkMem[0]},
		func() *accounting.AllocationRequest {
			if s.sockets > 1 {
				return &accounting.AllocationRequest{L3Bitmask: fmt.Sprintf("0x%x", s.benchmarkMask[1]), MemBandwidth: s.benchmarkMem[1]}
			}
			return nil
		}(),
	)
}

func (s *InterferenceAwareScheduler) releaseContainerAllocationLocked(containerIndex int) error {
	p := s.profiles[containerIndex]
	if p == nil {
		return nil
	}
	if p.l3Mask == 0 && p.memAlloc == 0 {
		return nil
	}

	socket := p.socket
	if socket < 0 {
		socket = 0
	}

	// Delete class (frees accounting state).
	_ = s.rdtAccountant.DeleteClass(p.className)

	// Reclaim to pool and update benchmark.
	s.benchmarkMask[socket] |= p.l3Mask
	s.benchmarkMem[socket] += p.memAlloc
	if s.benchmarkMem[socket] > 100 {
		s.benchmarkMem[socket] = 100
	}
	p.l3Mask = 0
	p.memAlloc = 0

	return s.updateBenchmarkClassLocked()
}

func (s *InterferenceAwareScheduler) reallocateLocked() error {
	// Simple consolidation heuristic:
	// Try to move evaluated, bound containers to sockets where they fit best.
	indices := make([]int, 0, len(s.profiles))
	for idx := range s.profiles {
		indices = append(indices, idx)
	}
	sort.Ints(indices)

	for _, idx := range indices {
		p := s.profiles[idx]
		if p == nil || p.pid == 0 || !p.evaluated || p.unbound {
			continue
		}
		best := s.pickBestSocketForDemandLocked(p.demandWays, p.demandMem)
		if best < 0 || best == p.socket {
			continue
		}
		// Try to re-place.
		if s.cpuAllocator != nil {
			if _, err := s.cpuAllocator.Move(idx, p.containerID, best); err != nil {
				continue
			}
		}
		_ = s.releaseContainerAllocationLocked(idx)
		_ = s.setContainerAllocationLocked(idx, best, p.demandWays, p.demandMem)
	}
	return nil
}

func (s *InterferenceAwareScheduler) pickBestSocketForDemandLocked(ways int, mem float64) int {
	best := -1
	bestOver := 1e9
	for socket := 0; socket < s.sockets; socket++ {
		freeWays := countBits(s.benchmarkMask[socket])
		freeMem := s.benchmarkMem[socket]
		overWays := float64(0)
		if float64(ways) > freeWays {
			overWays = float64(ways) - freeWays
		}
		overMem := float64(0)
		if mem > freeMem {
			overMem = mem - freeMem
		}
		score := overWays*10 + overMem
		if best == -1 || score < bestOver {
			best = socket
			bestOver = score
		}
	}
	return best
}

func (s *InterferenceAwareScheduler) moveContainerCgroupLocked(containerIndex int, className string) error {
	p := s.profiles[containerIndex]
	if p == nil {
		return fmt.Errorf("unknown container")
	}
	if p.pid == 0 || p.containerID == "" {
		return nil
	}

	pids, err := readContainerCgroupPIDs(p.containerID)
	if err != nil {
		// Fall back to moving only the main PID.
		return s.rdtAccountant.MoveContainer(p.pid, className)
	}
	for _, pid := range pids {
		_ = s.rdtAccountant.MoveContainer(pid, className)
	}
	return nil
}

func readContainerCgroupPIDs(containerID string) ([]int, error) {
	paths := []string{
		fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope", containerID),
		filepath.Join("/sys/fs/cgroup/docker", containerID),
		filepath.Join("/sys/fs/cgroup/system.slice", fmt.Sprintf("docker-%s.scope", containerID)),
	}

	var cgroupPath string
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			cgroupPath = p
			break
		}
	}
	if cgroupPath == "" {
		return nil, fmt.Errorf("cgroup path not found for container %s", containerID)
	}

	data, err := os.ReadFile(filepath.Join(cgroupPath, "cgroup.procs"))
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	pids := make([]int, 0, len(lines))
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		pid, err := strconv.Atoi(ln)
		if err != nil {
			continue
		}
		pids = append(pids, pid)
	}
	if len(pids) == 0 {
		return nil, fmt.Errorf("no PIDs in cgroup")
	}
	return pids, nil
}

func latestStepNumber(dfs *dataframe.DataFrames, containerIndex int) int {
	cdf := dfs.GetContainer(containerIndex)
	if cdf == nil {
		return 0
	}
	steps := cdf.GetAllSteps()
	max := 0
	for n := range steps {
		if n > max {
			max = n
		}
	}
	return max
}

func averageIPCEfficiency(dfs *dataframe.DataFrames, containerIndex int, startStep, endStep int) float64 {
	cdf := dfs.GetContainer(containerIndex)
	if cdf == nil {
		return -1
	}
	steps := cdf.GetAllSteps()
	var sum float64
	var n int
	for step := startStep + 1; step <= endStep; step++ {
		st := steps[step]
		if st == nil || st.Perf == nil {
			continue
		}
		if st.Perf.IPCEfficancy != nil {
			sum += *st.Perf.IPCEfficancy
			n++
			continue
		}
		if st.Perf.InstructionsPerCycle != nil && st.Perf.TheoreticalIPC != nil && *st.Perf.TheoreticalIPC > 0 {
			sum += (*st.Perf.InstructionsPerCycle / *st.Perf.TheoreticalIPC)
			n++
		}
	}
	if n == 0 {
		return -1
	}
	return sum / float64(n)
}

func buildMemSteps(maxMem float64, step float64) []float64 {
	if maxMem <= 0 {
		return []float64{0}
	}
	if step <= 0 {
		step = 10
	}
	steps := make([]float64, 0)
	for v := step; v <= maxMem+1e-9; v += step {
		steps = append(steps, v)
	}
	if len(steps) == 0 {
		steps = append(steps, maxMem)
	}
	return steps
}

func fullMask(totalWays int) (uint64, error) {
	if totalWays <= 0 {
		return 0, fmt.Errorf("invalid totalWays")
	}
	if totalWays >= 64 {
		return ^uint64(0), nil
	}
	return (uint64(1) << totalWays) - 1, nil
}

func takeContiguousFromMask(mask uint64, ways int, totalWays int) (taken uint64, remaining uint64, err error) {
	if ways <= 0 {
		return 0, mask, fmt.Errorf("ways must be > 0")
	}
	if totalWays <= 0 {
		return 0, mask, fmt.Errorf("totalWays must be > 0")
	}

	bestStart := -1
	bestLen := 0
	for start := 0; start < totalWays; start++ {
		if (mask & (1 << start)) == 0 {
			continue
		}
		len := 0
		for i := start; i < totalWays && (mask&(1<<i)) != 0; i++ {
			len++
		}
		if len >= ways && (bestStart == -1 || len < bestLen) {
			bestStart = start
			bestLen = len
		}
	}
	if bestStart == -1 {
		return 0, mask, fmt.Errorf("no contiguous %d ways available", ways)
	}

	var take uint64
	for i := 0; i < ways; i++ {
		take |= (1 << (bestStart + i))
	}
	return take, mask &^ take, nil
}

func countBits(mask uint64) float64 {
	cnt := 0
	for mask != 0 {
		cnt += int(mask & 1)
		mask >>= 1
	}
	return float64(cnt)
}
