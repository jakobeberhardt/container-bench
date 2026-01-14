package scheduler

import (
	"container-bench/internal/accounting"
	"container-bench/internal/config"
	"fmt"
	"sort"

	"github.com/sirupsen/logrus"
)

// DynamicSchedulerV3 isolates containers marked with priority: true using Intel RDT.
//
// It reuses the existing DynamicScheduler implementation (allocation-probe runner,
// guarantee search, and RDT defragmentation) but changes the admission and
// classification semantics:
//   - priority: true (and/or critical: true) is treated as "critical" for RDT isolation
//   - before admission, priority containers are placed onto the socket with fewer
//     other active priority containers (tie-break: fewer used cores)
//
// RDT repacking/defragmentation is handled by the embedded DynamicScheduler's
// consolidateRDTPartitionsLocked(), which is triggered on container stop.

type DynamicSchedulerV3 struct {
	*DynamicScheduler

	// explicitCPUPin marks containers that were explicitly pinned via YAML (core/cpu list).
	// We avoid moving these when trying to free cores for priority admission.
	explicitCPUPin map[int]bool
}

func NewDynamicSchedulerV3() *DynamicSchedulerV3 {
	base := NewDynamicScheduler()
	base.name = "dynamic-v3"
	base.version = "0.3.0"
	return &DynamicSchedulerV3{DynamicScheduler: base, explicitCPUPin: make(map[int]bool)}
}

func (s *DynamicSchedulerV3) Initialize(accountant *accounting.RDTAccountant, containers []ContainerInfo, schedulerConfig *config.SchedulerConfig) error {
	if err := s.DynamicScheduler.Initialize(accountant, containers, schedulerConfig); err != nil {
		return err
	}

	// Reclassify using priority:true (and keep critical:true as a synonym).
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range containers {
		p := s.profileLocked(c.Index)
		p.critical = isPriorityContainerConfig(c.Config)
		if c.Config != nil {
			s.explicitCPUPin[c.Index] = (c.Config.Core != "" || len(c.Config.CPUCores) > 0)
		}
	}
	return nil
}

func (s *DynamicSchedulerV3) OnContainerStart(info ContainerInfo) error {
	// Treat priority:true as critical:true for the embedded scheduler.
	if info.Config != nil && (info.Config.Priority || info.Config.Critical) {
		cfgCopy := *info.Config
		cfgCopy.Critical = true
		info.Config = &cfgCopy
	}
	return s.DynamicScheduler.OnContainerStart(info)
}

func (s *DynamicSchedulerV3) AssignCPUCores(containerIndex int) ([]int, error) {
	// If no allocator is wired, fall back.
	if s.cpuAllocator == nil {
		return s.DynamicScheduler.AssignCPUCores(containerIndex)
	}
	if s.hostConfig == nil {
		return s.DynamicScheduler.AssignCPUCores(containerIndex)
	}

	cfg := s.findContainerConfig(containerIndex)
	if cfg == nil {
		return s.DynamicScheduler.AssignCPUCores(containerIndex)
	}

	// Explicit request or pre-set: let allocator handle it.
	if cfg.Core != "" || len(cfg.CPUCores) > 0 {
		return s.DynamicScheduler.AssignCPUCores(containerIndex)
	}

	// Non-priority admission: preserve existing behavior.
	if !isPriorityContainerConfig(cfg) || s.hostConfig.Topology.Sockets <= 1 {
		return s.DynamicScheduler.AssignCPUCores(containerIndex)
	}

	requested := cfg.GetRequestedNumCores()

	// Pick a preferred socket for balancing, then try to allocate there.
	preferredSockets, err := s.rankSocketsForPriorityPlacement()
	if err != nil {
		return nil, err
	}

	// Try a couple of times to handle reserve races.
	for attempt := 0; attempt < 3; attempt++ {
		var lastErr error
		for _, targetSocket := range preferredSockets {
			cpus, err := s.pickFreeCPUsOnSocket(targetSocket, requested)
			if err != nil {
				// Best-effort: try to free cores by moving non-priority containers away.
				if s.hostConfig.Topology.Sockets == 2 {
					other := 1
					if targetSocket == 1 {
						other = 0
					}
					freed := s.tryEvictNonPriorityForCapacity(targetSocket, other, requested)
					if freed {
						cpus, err = s.pickFreeCPUsOnSocket(targetSocket, requested)
					}
				}
			}
			if err != nil {
				lastErr = err
				continue
			}

			if err := s.cpuAllocator.Reserve(containerIndex, cpus); err != nil {
				// Retry if another admission raced us.
				lastErr = err
				continue
			}

		cfg.CPUCores = append([]int(nil), cpus...)
		cfg.Core = config.FormatCPUSpec(cpus)

		assigned, err := s.cpuAllocator.EnsureAssigned(containerIndex, cfg)
		if err != nil {
			// Best-effort cleanup.
			s.cpuAllocator.Release(containerIndex)
			return nil, err
		}

		sock := 0
		if v, err := s.hostConfig.SocketOfPhysicalCPUs(assigned); err == nil {
			sock = v
		}

		s.mu.Lock()
		p := s.profileLocked(containerIndex)
		p.socket = sock
		s.mu.Unlock()

		s.schedulerLogger.WithFields(containerLogFields(s.containers, containerIndex, cfg)).WithFields(logrus.Fields{
			"cpus":                assigned,
			"cpuset":              cfg.Core,
			"requested_num_cores": requested,
			"target_socket":       sock,
			"source":              "dynamic_v3_priority_socket",
		}).Info("Assigned CPU cores")

		return assigned, nil
		}
		if lastErr != nil {
			// Next attempt.
			continue
		}
	}

	// If we couldn't reserve after retries, fall back to allocator default.
	return s.DynamicScheduler.AssignCPUCores(containerIndex)
}

func isPriorityContainerConfig(cfg *config.ContainerConfig) bool {
	if cfg == nil {
		return false
	}
	return cfg.Priority || cfg.Critical
}

func (s *DynamicSchedulerV3) findContainerConfig(containerIndex int) *config.ContainerConfig {
	// containers slice is maintained by orchestration; best-effort lookup.
	for i := range s.containers {
		if s.containers[i].Index == containerIndex {
			return s.containers[i].Config
		}
	}
	return nil
}

func (s *DynamicSchedulerV3) priorityAssumedDemand(containerIndex int) (ways int, mem float64) {
	// Default heuristic: until a priority container has a committed allocation, assume
	// it needs all resources (for balancing only).
	fullWays := s.totalWays
	if fullWays <= 0 {
		fullWays = 1
	}
	fullMem := 100.0

	s.mu.Lock()
	p := s.profiles[containerIndex]
	s.mu.Unlock()
	if p == nil {
		return fullWays, fullMem
	}

	// Only treat priority/critical profiles as priority load.
	if !p.critical {
		return 0, 0
	}

	// Use committed allocation after probing (floor*) if available; otherwise use current
	// (ways/mem) if already set. If none are set yet: keep full.
	useWays := 0
	useMem := 0.0
	if p.floorWays > 0 {
		useWays = p.floorWays
	} else if p.ways > 0 {
		useWays = p.ways
	}
	if p.floorMem > 0 {
		useMem = p.floorMem
	} else if p.mem > 0 {
		useMem = p.mem
	}
	if useWays <= 0 && useMem <= 0 {
		return fullWays, fullMem
	}
	if useWays <= 0 {
		useWays = fullWays
	}
	if useMem <= 0 {
		useMem = fullMem
	}
	return useWays, useMem
}

type socketCandidate struct {
	socket        int
	priorityCount int
	waysLoad      int
	memLoad       float64
	usedCores     int
}

func (s *DynamicSchedulerV3) rankSocketsForPriorityPlacement() ([]int, error) {
	bySocket, err := s.hostConfig.PhysicalCPUsBySocket()
	if err != nil {
		return nil, err
	}
	snapshot := s.cpuAllocator.Snapshot()

	usedPerSocket := make(map[int]int)
	for _, cpus := range snapshot {
		sock, err := s.hostConfig.SocketOfPhysicalCPUs(cpus)
		if err == nil {
			usedPerSocket[sock] += len(cpus)
		}
	}

	priorityPerSocket := make(map[int]int)
	waysPerSocket := make(map[int]int)
	memPerSocket := make(map[int]float64)
	for idx, cpus := range snapshot {
		cfg := s.findContainerConfig(idx)
		if !isPriorityContainerConfig(cfg) {
			continue
		}
		sock, err := s.hostConfig.SocketOfPhysicalCPUs(cpus)
		if err != nil {
			continue
		}
		assumedWays, assumedMem := s.priorityAssumedDemand(idx)
		priorityPerSocket[sock]++
		waysPerSocket[sock] += assumedWays
		memPerSocket[sock] += assumedMem
	}

	cands := make([]socketCandidate, 0, len(bySocket))
	for socket := range bySocket {
		cands = append(cands, socketCandidate{
			socket:        socket,
			priorityCount: priorityPerSocket[socket],
			waysLoad:      waysPerSocket[socket],
			memLoad:       memPerSocket[socket],
			usedCores:     usedPerSocket[socket],
		})
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].waysLoad != cands[j].waysLoad {
			return cands[i].waysLoad < cands[j].waysLoad
		}
		if cands[i].memLoad != cands[j].memLoad {
			return cands[i].memLoad < cands[j].memLoad
		}
		if cands[i].priorityCount != cands[j].priorityCount {
			return cands[i].priorityCount < cands[j].priorityCount
		}
		if cands[i].usedCores != cands[j].usedCores {
			return cands[i].usedCores < cands[j].usedCores
		}
		return cands[i].socket < cands[j].socket
	})

	out := make([]int, 0, len(cands))
	for _, c := range cands {
		out = append(out, c.socket)
	}
	return out, nil
}

func (s *DynamicSchedulerV3) pickFreeCPUsOnSocket(socket int, num int) ([]int, error) {
	bySocket, err := s.hostConfig.PhysicalCPUsBySocket()
	if err != nil {
		return nil, err
	}
	cpus, ok := bySocket[socket]
	if !ok {
		return nil, fmt.Errorf("unknown socket %d", socket)
	}

	snapshot := s.cpuAllocator.Snapshot()
	used := make(map[int]bool)
	for _, asg := range snapshot {
		for _, cpu := range asg {
			used[cpu] = true
		}
	}

	picked := make([]int, 0, num)
	for _, cpu := range cpus {
		if used[cpu] {
			continue
		}
		picked = append(picked, cpu)
		if len(picked) == num {
			return picked, nil
		}
	}
	return nil, fmt.Errorf("insufficient physical cores: requested %d", num)
}

func (s *DynamicSchedulerV3) tryEvictNonPriorityForCapacity(fromSocket int, toSocket int, neededCores int) bool {
	// Best-effort eviction: move smallest non-priority containers from fromSocket to toSocket
	// to free up capacity for a priority admission.
	if s.cpuAllocator == nil || s.hostConfig == nil {
		return false
	}

	// Snapshot candidates without holding the scheduler lock while moving.
	type cand struct {
		idx int
		id  string
		n   int
	}
	var cands []cand

	s.mu.Lock()
	for idx, p := range s.profiles {
		if p == nil || p.pid == 0 {
			continue
		}
		if p.critical {
			continue
		}
		if s.explicitCPUPin != nil && s.explicitCPUPin[idx] {
			continue
		}
		cid := p.containerID
		if cid == "" {
			continue
		}
		cpus, ok := s.cpuAllocator.Get(idx)
		if !ok || len(cpus) == 0 {
			continue
		}
		sock, err := s.hostConfig.SocketOfPhysicalCPUs(cpus)
		if err != nil || sock != fromSocket {
			continue
		}
		cands = append(cands, cand{idx: idx, id: cid, n: len(cpus)})
	}
	s.mu.Unlock()

	if len(cands) == 0 {
		return false
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].n != cands[j].n {
			return cands[i].n < cands[j].n
		}
		return cands[i].idx < cands[j].idx
	})

	freed := 0
	moved := 0
	for _, c := range cands {
		if freed >= neededCores {
			break
		}
		if _, err := s.cpuAllocator.Move(c.idx, c.id, toSocket); err != nil {
			continue
		}
		freed += c.n
		moved++
	}
	if moved > 0 {
		s.schedulerLogger.WithFields(logrus.Fields{
			"from_socket": fromSocket,
			"to_socket":   toSocket,
			"moved":       moved,
			"freed_cores": freed,
		}).Info("Evicted non-priority containers to free cores for priority admission")
		return true
	}
	return false
}

// NOTE: socket ranking and cpu picking live in rankSocketsForPriorityPlacement and pickFreeCPUsOnSocket.
