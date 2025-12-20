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
)

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
	runner         *proberesources.AllocationProbeRunner

	stepCtx    context.Context
	stepCancel context.CancelFunc
	stepDone   chan struct{}
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
	collectorFreq CollectorFrequencyController
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

	allocationProbeResults []*proberesources.AllocationProbeResult

	probeQueue  []int
	probeQueued map[int]bool
}

func (s *InterferenceAwareScheduler) SetCollectorFrequencyController(controller CollectorFrequencyController) {
	s.collectorFreq = controller
}

func (s *InterferenceAwareScheduler) debugStateLocked(event string) {
	if s.schedulerLogger == nil || !s.schedulerLogger.IsLevelEnabled(logrus.DebugLevel) {
		return
	}

	running := 0
	perSocket := make([]int, s.sockets)
	for idx, p := range s.profiles {
		if p == nil || p.pid == 0 {
			continue
		}
		running++
		sock := p.socket
		if sock < 0 || sock >= s.sockets {
			sock = s.currentSocketForContainerLocked(idx)
		}
		if sock < 0 || sock >= s.sockets {
			sock = 0
		}
		perSocket[sock]++
	}

	probeHead, hasHead := s.peekProbeHeadLocked()
	probeActive := -1
	if s.probing != nil {
		probeActive = s.probing.containerIndex
	}

	s.schedulerLogger.WithFields(logrus.Fields{
		"event":          event,
		"running":        running,
		"per_socket":     perSocket,
		"probe_active":   probeActive,
		"probe_queue":    len(s.probeQueue),
		"probe_has_head": hasHead,
		"probe_head":     probeHead,
	}).Debug("Scheduler state")
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
	s.allocationProbeResults = nil
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

// GetAllocationProbeResults returns allocation probe results if available.
func (s *InterferenceAwareScheduler) GetAllocationProbeResults() []*proberesources.AllocationProbeResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allocationProbeResults
}

// HasAllocationProbeResults returns true if allocation probe results are available.
func (s *InterferenceAwareScheduler) HasAllocationProbeResults() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.allocationProbeResults) > 0
}

func (s *InterferenceAwareScheduler) ProcessDataFrames(dfs *dataframe.DataFrames) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.debugStateLocked("tick")

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

	// Active probe is stepped asynchronously (see startProbeLocked).
	if s.probing != nil {
		return nil
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
		s.schedulerLogger.WithFields(logrus.Fields{
			"container": headIdx,
			"warmup_s":  warmupSeconds,
		}).Debug("Probe delayed by warmup")
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
	// Abort any active probe so we reliably restore any temporary collector frequency override.
	if s.probing != nil {
		ap := s.probing
		if ap.stepCancel != nil {
			ap.stepCancel()
		}
		if ap.runner != nil {
			ap.runner.Abort("shutdown")
		}
		_ = s.resetContainerToBenchmarkLocked(ap.containerIndex)
		s.lastProbeFinished = time.Now()
		s.probing = nil
	}
	s.mu.Unlock()

	if s.rdtAccountant != nil {
		_ = s.rdtAccountant.Cleanup()
	}
	return nil
}

func (s *InterferenceAwareScheduler) startProbeStepperLocked(dfs *dataframe.DataFrames, ap *activeProbe) {
	if ap == nil || ap.runner == nil {
		return
	}
	if ap.stepCancel != nil {
		// Already started.
		return
	}

	cd := ap.runner.CandidateDuration()
	stepEvery := 100 * time.Millisecond
	if cd > 0 {
		q := cd / 5
		if q > 0 && q < stepEvery {
			stepEvery = q
		}
	}
	if stepEvery < 10*time.Millisecond {
		stepEvery = 10 * time.Millisecond
	}

	ap.stepCtx, ap.stepCancel = context.WithCancel(context.Background())
	ap.stepDone = make(chan struct{})

	// Step the probe independently from ProcessDataFrames cadence so the probe finishes
	// close to its configured budget/candidate durations.
	go func() {
		defer close(ap.stepDone)
		ticker := time.NewTicker(stepEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ap.stepCtx.Done():
				return
			case <-ticker.C:
				s.mu.Lock()
				// Probe may have been cleared/replaced.
				if s.probing != ap {
					s.mu.Unlock()
					return
				}
				err := s.stepProbeLocked(dfs)
				s.mu.Unlock()
				if err != nil {
					if s.schedulerLogger != nil {
						s.schedulerLogger.WithError(err).Warn("Asynchronous probe step failed")
					}
					return
				}
			}
		}
	}()
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

	// Track current socket from assigned CPUs (pre-start).
	curSock := 0
	if s.hostConfig != nil && len(assigned) > 0 {
		if sock, err := s.hostConfig.SocketOfPhysicalCPUs(assigned); err == nil {
			curSock = sock
		}
	}
	if p := s.profiles[containerIndex]; p != nil {
		p.socket = curSock
	}

	// Admission decision: place the container on the least-interfering socket before start.
	if p := s.profiles[containerIndex]; p != nil && p.containerID != "" && s.sockets > 1 {
		if bestSock, ok := s.pickBestSocketForContainerLocked(containerIndex); ok {
			lw0, lm0 := s.socketLoadExcludingLocked(0, containerIndex)
			lw1, lm1 := s.socketLoadExcludingLocked(1, containerIndex)
			s.schedulerLogger.WithFields(logrus.Fields{
				"container":    containerIndex,
				"current_sock": curSock,
				"chosen_sock":  bestSock,
				"load_s0_ways": lw0,
				"load_s0_mem":  lm0,
				"load_s1_ways": lw1,
				"load_s1_mem":  lm1,
			}).Info("Admission decision")

			if bestSock != curSock {
				if moved, err := s.cpuAllocator.Move(containerIndex, p.containerID, bestSock); err == nil {
					p.socket = bestSock
					assigned = moved
				} else {
					s.schedulerLogger.WithError(err).WithFields(logrus.Fields{
						"container":    containerIndex,
						"current_sock": curSock,
						"target_sock":  bestSock,
					}).Warn("Admission move deferred (insufficient CPU resources?)")
				}
			}
		}
	}

	return assigned, nil
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
	// Admission placement happens in AssignCPUCores (pre-start). Here we only track socket.
	p.socket = s.currentSocketForContainerLocked(info.Index)

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
	}
	if s.config != nil && s.config.Reallocate {
		_ = s.relocateYoungestIfImprovesLocked("container_stop")
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
		if s.config.Prober != nil {
			if s.config.Prober.MaxL3Ways > 0 {
				maxL3 = s.config.Prober.MaxL3Ways
			}
			if s.config.Prober.MaxMemBandwidth > 0 {
				maxMem = s.config.Prober.MaxMemBandwidth
			}
		}
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
	if p == nil {
		return 0, 0, false
	}
	// Allow estimating demand for not-yet-started containers (pid=0) as long as they
	// exist in the benchmark (have a container ID). This is used for admission decisions.
	if p.pid == 0 && p.containerID == "" {
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
	minL3 := 1
	maxL3 := 4
	stepL3 := 1
	minMem := defaultProbeMemStepPercent
	maxMem := 30.0
	stepMem := defaultProbeMemStepPercent
	budgetSeconds := defaultProbeTotalSeconds
	var probingFrequency time.Duration
	var outlierDrop int
	if cfg != nil {
		if cfg.Prober != nil {
			if cfg.Prober.MinL3Ways > 0 {
				minL3 = cfg.Prober.MinL3Ways
			}
			if cfg.Prober.MaxL3Ways > 0 {
				maxL3 = cfg.Prober.MaxL3Ways
			}
			if cfg.Prober.StepL3Ways > 0 {
				stepL3 = cfg.Prober.StepL3Ways
			}
			if cfg.Prober.MinMemBandwidth > 0 {
				minMem = cfg.Prober.MinMemBandwidth
			}
			if cfg.Prober.MaxMemBandwidth > 0 {
				maxMem = cfg.Prober.MaxMemBandwidth
			}
			if cfg.Prober.StepMemBandwidth > 0 {
				stepMem = cfg.Prober.StepMemBandwidth
			}
			if cfg.Prober.ProbingT > 0 {
				budgetSeconds = cfg.Prober.ProbingT
			}
			if cfg.Prober.ProbingFrequency > 0 {
				probingFrequency = time.Duration(cfg.Prober.ProbingFrequency) * time.Millisecond
			}
			if cfg.Prober.DropOutliers != 0 {
				outlierDrop = cfg.Prober.DropOutliers
			}
		}

		if cfg.MaxL3 > 0 {
			maxL3 = cfg.MaxL3
		}
		if cfg.MaxMem > 0 {
			maxMem = cfg.MaxMem
		}
		if cfg.ProbingT > 0 {
			budgetSeconds = cfg.ProbingT
		}
	}
	if maxMem <= 0 {
		minMem = 0
		maxMem = 0
	} else if maxMem < minMem {
		minMem = maxMem
	}
	if minL3 <= 0 {
		minL3 = 1
	}
	if stepL3 <= 0 {
		stepL3 = 1
	}
	if maxL3 < minL3 {
		maxL3 = minL3
	}
	if stepMem <= 0 {
		stepMem = defaultProbeMemStepPercent
	}
	if maxMem < minMem {
		maxMem = minMem
	}

	probeRange := proberesources.AllocationRange{
		MinL3Ways:        minL3,
		MaxL3Ways:        maxL3,
		MinMemBandwidth:  minMem,
		MaxMemBandwidth:  maxMem,
		StepL3Ways:       stepL3,
		StepMemBandwidth: stepMem,
		Order:            "asc",
	}

	// Probe is about finding resource demand; it does not depend on socket.
	startSock := s.currentSocketForContainerLocked(containerIndex)
	probeRange.SocketID = startSock
	probeRange.IsolateOthers = false
	probeRange.ForceReallocation = false

	// Build metadata for stable probe results.
	var containerCfg *config.ContainerConfig
	for i := range s.containers {
		if s.containers[i].Index == containerIndex {
			containerCfg = s.containers[i].Config
			break
		}
	}
	containerName := p.containerKey
	if containerName == "" && containerCfg != nil {
		containerName = containerCfg.Name
	}
	containerCores := ""
	containerImage := ""
	containerCommand := ""
	if containerCfg != nil {
		containerCores = containerCfg.Core
		containerImage = containerCfg.Image
		containerCommand = containerCfg.Command
	}

	// Break policy (IPCE threshold + diminishing returns).
	var acceptable *float64
	var diminishing *float64
	if cfg != nil {
		acceptable = normalizeIPCEPercentThreshold(cfg.BreakCondition)
		if cfg.BreakImprovement != 0 {
			v := cfg.BreakImprovement
			if v > 1 && v <= 100 {
				v = v / 100
			}
			diminishing = &v
		}
	}
	breaks := proberesources.AllocationProbeBreakPolicy{
		AcceptableIPCEfficiency:     acceptable,
		DiminishingReturnsThreshold: diminishing,
	}
	opts := proberesources.AllocationProbeOptions{ProbingFrequency: probingFrequency, OutlierDrop: outlierDrop}
	if cfg != nil {
		opts.GreedyAllocation = cfg.GreedyAllocation
	}

	target := proberesources.AllocationProbeTarget{
		BenchmarkID:      s.benchmarkID,
		ContainerID:      p.containerID,
		ContainerName:    containerName,
		ContainerIndex:   containerIndex,
		ContainerCores:   containerCores,
		ContainerSocket:  startSock,
		ContainerImage:   containerImage,
		ContainerCommand: containerCommand,
	}

	cb := proberesources.AllocationProbeCallbacks{
		ApplyAllocation: func(ways int, mem float64) error {
			return s.setContainerAllocationLocked(containerIndex, startSock, ways, mem)
		},
		ResetToBenchmark: func() error {
			return s.resetContainerToBenchmarkLocked(containerIndex)
		},
		LatestStepNumber: func(dfs *dataframe.DataFrames, idx int) int {
			return latestStepNumber(dfs, idx)
		},
		OverrideContainerCollectorFrequency: func(idx int, freq time.Duration) (func(), error) {
			if freq <= 0 {
				return nil, nil
			}
			if s.collectorFreq == nil {
				return nil, fmt.Errorf("collector frequency controller not configured")
			}
			return s.collectorFreq.OverrideContainerFrequency(idx, freq)
		},
	}

	runner := proberesources.NewAllocationProbeRunnerFromRange(
		target,
		probeRange,
		time.Duration(float64(time.Second)*budgetSeconds),
		time.Duration(defaultMinCandidateDurationMillis)*time.Millisecond,
		breaks,
		opts,
		cb,
	)
	if runner == nil || runner.NumCandidates() == 0 {
		return fmt.Errorf("no probe candidates")
	}

	origSock := startSock
	if origSock < 0 || origSock >= s.sockets {
		origSock = 0
	}

	ap := &activeProbe{containerIndex: containerIndex, originalSocket: origSock, runner: runner}
	s.probing = ap

	if err := runner.Start(dfs); err != nil {
		return err
	}

	// Run probing asynchronously at a finer cadence than the scheduler tick.
	s.startProbeStepperLocked(dfs, ap)

	s.schedulerLogger.WithFields(logrus.Fields{
		"container":     containerIndex,
		"socket":        startSock,
		"candidates":    runner.NumCandidates(),
		"candidate_dur": runner.CandidateDuration().Seconds(),
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
	if ap.runner == nil {
		return fmt.Errorf("probe runner missing")
	}

	done, err := ap.runner.Step(dfs)
	if err != nil {
		// Keep runner result for export, but mark probe finished.
		_ = s.resetContainerToBenchmarkLocked(ap.containerIndex)
		s.lastProbeFinished = time.Now()
		s.probing = nil
		return err
	}
	if !done {
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
	if ap.runner == nil {
		s.probing = nil
		return fmt.Errorf("probe runner missing")
	}

	cfg := s.config
	maxL3, maxMem := s.probeMaxFromConfigLocked()
	skipAlloc := true
	allocateUnbound := false
	if cfg != nil {
		if cfg.SkipAllocationAfterProbing != nil {
			skipAlloc = *cfg.SkipAllocationAfterProbing
		}
		allocateUnbound = cfg.AllocateUnbound
	}

	bestSocket := ap.originalSocket
	if bestSocket < 0 || bestSocket >= s.sockets {
		bestSocket = 0
	}
	bestWays := ap.runner.BestWays()
	bestMem := ap.runner.BestMem()
	bestEff := ap.runner.BestEff()
	bestUnbound := ap.runner.Unbound()
	bestReason := ap.runner.StopReason()

	// Persist profile.
	p.evaluated = true
	p.unbound = bestUnbound
	p.demandWays = bestWays
	p.demandMem = bestMem

	// Store allocation probe result (for DB export).
	if res := ap.runner.Result(); res != nil {
		s.allocationProbeResults = append(s.allocationProbeResults, res)
	}

	// Ensure container is in benchmark pool before final decision.
	_ = s.resetContainerToBenchmarkLocked(containerIndex)
	_ = s.moveContainerCgroupLocked(containerIndex, s.benchmarkClass)

	// Allocation retention policy:
	// - default: skip allocation after probing
	// - if allocate_unbound: keep allocations for unbound containers (still requiring headroom)
	keep := false
	if p.unbound && (!skipAlloc || allocateUnbound) {
		keep = s.wouldLeaveProbeHeadroomLocked(bestSocket, p.demandWays, p.demandMem, maxL3, maxMem)
		if !keep {
			s.schedulerLogger.WithFields(logrus.Fields{
				"container":      containerIndex,
				"socket":         bestSocket,
				"demand_ways":    p.demandWays,
				"demand_mem":     p.demandMem,
				"probe_max_ways": maxL3,
				"probe_max_mem":  maxMem,
			}).Warn("Post-probe allocation not kept (insufficient probe headroom)")
		}
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
		"allocations": func() int {
			if ap.runner != nil && ap.runner.Result() != nil {
				return len(ap.runner.Result().Allocations)
			}
			return 0
		}(),
	}).Info("Finished interference-aware probing")

	if s.config != nil && s.config.Reallocate {
		_ = s.relocateYoungestIfImprovesLocked("probe_finished")
	}

	s.lastProbeFinished = time.Now()
	s.probing = nil
	return nil
}

func (s *InterferenceAwareScheduler) relocateYoungestIfImprovesLocked(trigger string) error {
	if s.sockets <= 1 || s.cpuAllocator == nil {
		return nil
	}

	const eps = 0.02

	type cand struct {
		idx     int
		srcSock int
		dstSock int
		newObj  float64
		startAt time.Time
		ways    int
		mem     float64
	}

	capWays := s.totalWays
	if capWays <= 0 {
		capWays = 12
	}

	// Current loads and objective.
	loadWays := make([]int, s.sockets)
	loadMem := make([]float64, s.sockets)
	for idx, p := range s.profiles {
		if p == nil || p.pid == 0 {
			continue
		}
		sock := p.socket
		if sock < 0 || sock >= s.sockets {
			sock = s.currentSocketForContainerLocked(idx)
		}
		if sock < 0 || sock >= s.sockets {
			sock = 0
		}
		dw, dm, ok := s.estimatedDemandForIndexLocked(idx)
		if !ok {
			continue
		}
		loadWays[sock] += dw
		loadMem[sock] += dm
	}
	currentObj := 0.0
	for sock := 0; sock < s.sockets; sock++ {
		sc := socketInterferenceScore(loadWays[sock], loadMem[sock], capWays)
		if sc > currentObj {
			currentObj = sc
		}
	}

	// Enumerate improving single-container moves. Policy: execute at most one move, choosing the
	// youngest eligible container that yields an objective improvement.
	choices := make([]cand, 0)
	for idx, p := range s.profiles {
		if p == nil || p.pid == 0 || p.containerID == "" {
			continue
		}
		// Only move containers without a pinned allocation; moving allocations across sockets is
		// intentionally avoided to preserve stability.
		if p.l3Mask != 0 || p.memAlloc != 0 {
			continue
		}
		src := p.socket
		if src < 0 || src >= s.sockets {
			src = s.currentSocketForContainerLocked(idx)
		}
		if src < 0 || src >= s.sockets {
			src = 0
		}
		dw, dm, ok := s.estimatedDemandForIndexLocked(idx)
		if !ok {
			continue
		}
		for dst := 0; dst < s.sockets; dst++ {
			if dst == src {
				continue
			}
			tWays := append([]int(nil), loadWays...)
			tMem := append([]float64(nil), loadMem...)
			tWays[src] -= dw
			tMem[src] -= dm
			tWays[dst] += dw
			tMem[dst] += dm
			newObj := 0.0
			for sock := 0; sock < s.sockets; sock++ {
				sc := socketInterferenceScore(tWays[sock], tMem[sock], capWays)
				if sc > newObj {
					newObj = sc
				}
			}
			if newObj+eps < currentObj {
				choices = append(choices, cand{idx: idx, srcSock: src, dstSock: dst, newObj: newObj, startAt: p.startedAt, ways: dw, mem: dm})
			}
		}
	}
	if len(choices) == 0 {
		return nil
	}

	sort.Slice(choices, func(i, j int) bool {
		// youngest first
		if !choices[i].startAt.Equal(choices[j].startAt) {
			return choices[i].startAt.After(choices[j].startAt)
		}
		// then best objective
		if choices[i].newObj != choices[j].newObj {
			return choices[i].newObj < choices[j].newObj
		}
		return choices[i].idx < choices[j].idx
	})

	for _, c := range choices {
		p := s.profiles[c.idx]
		if p == nil || p.containerID == "" {
			continue
		}
		if _, err := s.cpuAllocator.Move(c.idx, p.containerID, c.dstSock); err != nil {
			continue
		}
		p.socket = c.dstSock
		if s.rdtAccountant != nil {
			_ = s.moveContainerCgroupLocked(c.idx, s.benchmarkClass)
		}
		s.schedulerLogger.WithFields(logrus.Fields{
			"trigger":        trigger,
			"container":      c.idx,
			"src_sock":       c.srcSock,
			"dst_sock":       c.dstSock,
			"demand_ways":    c.ways,
			"demand_mem":     c.mem,
			"current_obj":    currentObj,
			"planned_obj":    c.newObj,
			"moved_youngest": true,
		}).Info("Relocation decision")
		return nil
	}

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

func normalizeIPCEPercentThreshold(v float64) *float64 {
	if v == 0 {
		return nil
	}
	// Negative values are treated as "disabled" by the prober.
	if v < 0 {
		return &v
	}
	// IPCE is stored as a percentage in Perf.IPCEfficancy (0-100 typical).
	// Backwards-compat: if we provided a fraction (0-1), convert to percent e.g. in old benchmarks.
	if v > 0 && v <= 1 {
		v = v * 100
	}
	return &v
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
