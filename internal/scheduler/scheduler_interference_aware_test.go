package scheduler

import (
	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"context"
	"fmt"
	"testing"
	"time"
)

type moveCall struct {
	idx int
	id  string
	dst int
}

type fakeCPUAllocator struct {
	assigned map[int][]int
	ensure   map[int][]int
	moves    []moveCall
}

func (f *fakeCPUAllocator) EnsureAssigned(containerIndex int, cfg *config.ContainerConfig) ([]int, error) {
	if f.assigned == nil {
		f.assigned = make(map[int][]int)
	}
	if prev, ok := f.assigned[containerIndex]; ok {
		return append([]int(nil), prev...), nil
	}
	if f.ensure != nil {
		if cpus, ok := f.ensure[containerIndex]; ok {
			f.assigned[containerIndex] = append([]int(nil), cpus...)
			if cfg != nil {
				cfg.CPUCores = append([]int(nil), cpus...)
				cfg.Core = fmt.Sprintf("%d", cpus[0])
			}
			return append([]int(nil), cpus...), nil
		}
	}
	// Default to a single CPU.
	f.assigned[containerIndex] = []int{0}
	if cfg != nil {
		cfg.CPUCores = []int{0}
		cfg.Core = "0"
	}
	return []int{0}, nil
}

func (f *fakeCPUAllocator) Reserve(containerIndex int, cpuIDs []int) error { return nil }
func (f *fakeCPUAllocator) Allocate(containerIndex int, num int) ([]int, error) {
	return nil, nil
}
func (f *fakeCPUAllocator) Release(containerIndex int) {}
func (f *fakeCPUAllocator) Get(containerIndex int) ([]int, bool) {
	if f.assigned == nil {
		return nil, false
	}
	cpus, ok := f.assigned[containerIndex]
	return append([]int(nil), cpus...), ok
}
func (f *fakeCPUAllocator) Snapshot() map[int][]int { return map[int][]int{} }
func (f *fakeCPUAllocator) Move(containerIndex int, containerID string, targetSocket int) ([]int, error) {
	if f.assigned == nil {
		f.assigned = make(map[int][]int)
	}
	f.moves = append(f.moves, moveCall{idx: containerIndex, id: containerID, dst: targetSocket})
	// Encode socket choice into CPU id for testing.
	cpus := []int{targetSocket}
	f.assigned[containerIndex] = cpus
	return append([]int(nil), cpus...), nil
}
func (f *fakeCPUAllocator) Swap(aIndex int, aContainerID string, bIndex int, bContainerID string) error {
	return nil
}

type noopCpusetApplier struct{}

func (noopCpusetApplier) UpdateCpuset(ctx context.Context, containerID string, cpuset string) error {
	return nil
}

func TestTakeContiguousFromMask_BestFit(t *testing.T) {
	// mask: bits 0-1 (len2) and bits 4-7 (len4)
	mask := uint64(0)
	mask |= (1 << 0) | (1 << 1)
	mask |= (1 << 4) | (1 << 5) | (1 << 6) | (1 << 7)

	taken, remaining, err := takeContiguousFromMask(mask, 2, 12)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if taken != ((1 << 0) | (1 << 1)) {
		t.Fatalf("expected taken=0b11, got 0x%x", taken)
	}
	if remaining != (mask &^ taken) {
		t.Fatalf("remaining mismatch")
	}
}

func TestBuildMemSteps(t *testing.T) {
	steps := buildMemSteps(30, 10)
	if len(steps) != 3 || steps[0] != 10 || steps[1] != 20 || steps[2] != 30 {
		t.Fatalf("unexpected steps: %#v", steps)
	}
}

func TestAverageIPCEfficiency_UsesIPCEfficancy(t *testing.T) {
	dfs := dataframe.NewDataFrames()
	cdf := dfs.AddContainer(0)

	v1 := 0.5
	v2 := 0.7
	cdf.AddStep(0, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v1}})
	cdf.AddStep(1, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v2}})

	eff := averageIPCEfficiency(dfs, 0, -1, 1)
	if eff < 0.59 || eff > 0.61 {
		t.Fatalf("expected ~0.6, got %v", eff)
	}
}

func TestAverageIPCEfficiency_DoesNotFallbackToIPCOverTheoreticalIPC(t *testing.T) {
	dfs := dataframe.NewDataFrames()
	cdf := dfs.AddContainer(0)

	ipc := 2.0
	theo := 4.0
	cdf.AddStep(1, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{InstructionsPerCycle: &ipc, TheoreticalIPC: &theo}})

	eff := averageIPCEfficiency(dfs, 0, 0, 1)
	if eff != -1 {
		t.Fatalf("expected -1 when IPCEfficancy missing, got %v", eff)
	}
}

func TestBreakCondition_TreatedAsPercent(t *testing.T) {
	// 80 means 80%.
	{
		thr := normalizeIPCEPercentThreshold(80)
		if thr == nil || *thr != 80 {
			t.Fatalf("expected 80 to remain 80, got %v", thr)
		}
	}

	// Backwards-compat: 0.8 means 80%.
	{
		thr := normalizeIPCEPercentThreshold(0.8)
		if thr == nil || *thr != 80 {
			t.Fatalf("expected 0.8 to map to 80, got %v", thr)
		}
	}
}

func TestPickBestSocket_AssumesMaxDemandUntilProbed(t *testing.T) {
	s := NewInterferenceAwareScheduler()
	s.sockets = 2
	s.totalWays = 10
	s.config = &config.SchedulerConfig{Prober: &config.ProberConfig{MaxL3Ways: 4, MaxMemBandwidth: 30}}

	// Existing running load: two unprobed containers on socket 0.
	s.profiles[1] = &containerProfile{index: 1, pid: 1001, socket: 0, containerID: "c1"}
	s.profiles[2] = &containerProfile{index: 2, pid: 1002, socket: 0, containerID: "c2"}
	// Candidate container (not started yet, but known to the benchmark).
	s.profiles[0] = &containerProfile{index: 0, pid: 0, socket: 0, containerID: "c0"}

	best, ok := s.pickBestSocketForContainerLocked(0)
	if !ok {
		t.Fatalf("expected ok")
	}
	if best != 1 {
		t.Fatalf("expected socket 1, got %d", best)
	}
}

func TestAssignCPUCores_AdmissionMovesToLeastInterferingSocket(t *testing.T) {
	s := NewInterferenceAwareScheduler()
	s.sockets = 2
	s.totalWays = 10
	s.config = &config.SchedulerConfig{Prober: &config.ProberConfig{MaxL3Ways: 4, MaxMemBandwidth: 30}}

	alloc := &fakeCPUAllocator{ensure: map[int][]int{0: {0}}}
	s.SetCPUAllocator(alloc)

	// One running container already on socket 0.
	s.profiles[1] = &containerProfile{index: 1, pid: 2001, socket: 0, containerID: "c1"}
	// Container 0 pending admission.
	s.profiles[0] = &containerProfile{index: 0, pid: 0, socket: 0, containerID: "c0"}

	cfg0 := &config.ContainerConfig{Index: 0, Name: "c0"}
	s.containers = []ContainerInfo{{Index: 0, Config: cfg0, PID: 0, ContainerID: "c0"}, {Index: 1, PID: 2001, ContainerID: "c1"}}

	_, err := s.AssignCPUCores(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(alloc.moves) != 1 {
		t.Fatalf("expected one Move call, got %d", len(alloc.moves))
	}
	if alloc.moves[0].idx != 0 || alloc.moves[0].dst != 1 {
		t.Fatalf("expected move of idx=0 to socket=1, got %#v", alloc.moves[0])
	}
}

func TestRelocationAfterProbe_MovesYoungestEligible(t *testing.T) {
	s := NewInterferenceAwareScheduler()
	s.sockets = 2
	s.totalWays = 10
	s.config = &config.SchedulerConfig{Reallocate: true, Prober: &config.ProberConfig{MaxL3Ways: 4, MaxMemBandwidth: 30}}

	alloc := &fakeCPUAllocator{}
	s.SetCPUAllocator(alloc)

	older := time.Now().Add(-10 * time.Second)
	younger := time.Now().Add(-1 * time.Second)

	// Two movable containers stacked on socket 0; moving the youngest improves balance.
	s.profiles[0] = &containerProfile{index: 0, pid: 3000, socket: 0, containerID: "old", startedAt: older}
	s.profiles[1] = &containerProfile{index: 1, pid: 3001, socket: 0, containerID: "young", startedAt: younger}

	if err := s.relocateYoungestIfImprovesLocked("probe_finished"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(alloc.moves) != 1 {
		t.Fatalf("expected one move, got %d", len(alloc.moves))
	}
	if alloc.moves[0].idx != 1 {
		t.Fatalf("expected youngest idx=1 to move, got idx=%d", alloc.moves[0].idx)
	}
	if alloc.moves[0].dst != 1 {
		t.Fatalf("expected move to socket 1, got %d", alloc.moves[0].dst)
	}
}

func TestOnContainerStop_TriggersYoungestRelocation(t *testing.T) {
	s := NewInterferenceAwareScheduler()
	s.sockets = 2
	s.totalWays = 10
	s.config = &config.SchedulerConfig{Reallocate: true, Prober: &config.ProberConfig{MaxL3Ways: 4, MaxMemBandwidth: 30}}

	alloc := &fakeCPUAllocator{}
	s.SetCPUAllocator(alloc)

	older := time.Now().Add(-10 * time.Second)
	younger := time.Now().Add(-1 * time.Second)

	// Two containers on socket 0, one container on socket 1 that will stop.
	s.profiles[0] = &containerProfile{index: 0, pid: 4000, socket: 0, containerID: "old", startedAt: older}
	s.profiles[1] = &containerProfile{index: 1, pid: 4001, socket: 0, containerID: "young", startedAt: younger}
	s.profiles[2] = &containerProfile{index: 2, pid: 4002, socket: 1, containerID: "stop", startedAt: older}
	s.containers = []ContainerInfo{{Index: 0, PID: 4000, ContainerID: "old"}, {Index: 1, PID: 4001, ContainerID: "young"}, {Index: 2, PID: 4002, ContainerID: "stop"}}

	if err := s.OnContainerStop(2); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(alloc.moves) != 1 {
		t.Fatalf("expected one move, got %d", len(alloc.moves))
	}
	if alloc.moves[0].idx != 1 || alloc.moves[0].dst != 1 {
		t.Fatalf("expected youngest idx=1 moved to socket 1, got %#v", alloc.moves[0])
	}
}
