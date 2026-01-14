package scheduler

import (
	"container-bench/internal/accounting"
	"container-bench/internal/config"
	"container-bench/internal/host"
	"testing"
)

type fakeAllocatorForDynamicV3 struct {
	assigned map[int][]int
}

func (f *fakeAllocatorForDynamicV3) EnsureAssigned(containerIndex int, cfg *config.ContainerConfig) ([]int, error) {
	if f.assigned == nil {
		f.assigned = make(map[int][]int)
	}
	if prev, ok := f.assigned[containerIndex]; ok {
		return append([]int(nil), prev...), nil
	}
	if cfg != nil && len(cfg.CPUCores) > 0 {
		f.assigned[containerIndex] = append([]int(nil), cfg.CPUCores...)
		return append([]int(nil), cfg.CPUCores...), nil
	}
	f.assigned[containerIndex] = []int{0}
	if cfg != nil {
		cfg.CPUCores = []int{0}
		cfg.Core = "0"
	}
	return []int{0}, nil
}

func (f *fakeAllocatorForDynamicV3) Reserve(containerIndex int, cpuIDs []int) error {
	if f.assigned == nil {
		f.assigned = make(map[int][]int)
	}
	// naive collision check
	used := make(map[int]int)
	for idx, cpus := range f.assigned {
		for _, c := range cpus {
			used[c] = idx
		}
	}
	for _, c := range cpuIDs {
		if other, ok := used[c]; ok && other != containerIndex {
			return errInsufficientCapacity("cpu already reserved")
		}
	}
	f.assigned[containerIndex] = append([]int(nil), cpuIDs...)
	return nil
}

func (f *fakeAllocatorForDynamicV3) Allocate(containerIndex int, num int) ([]int, error) { return nil, nil }
func (f *fakeAllocatorForDynamicV3) Release(containerIndex int)                          { delete(f.assigned, containerIndex) }
func (f *fakeAllocatorForDynamicV3) Get(containerIndex int) ([]int, bool) {
	cpus, ok := f.assigned[containerIndex]
	return append([]int(nil), cpus...), ok
}
func (f *fakeAllocatorForDynamicV3) Snapshot() map[int][]int {
	out := make(map[int][]int)
	for k, v := range f.assigned {
		out[k] = append([]int(nil), v...)
	}
	return out
}
func (f *fakeAllocatorForDynamicV3) Move(containerIndex int, containerID string, targetSocket int) ([]int, error) {
	return nil, nil
}
func (f *fakeAllocatorForDynamicV3) Swap(aIndex int, aContainerID string, bIndex int, bContainerID string) error {
	return nil
}

// errInsufficientCapacity is used to surface an error message that matches
// isInsufficientCPUCapacity() in cmd/main.go.
func errInsufficientCapacity(msg string) error {
	return &insufficientCapError{msg: msg}
}

type insufficientCapError struct{ msg string }

func (e *insufficientCapError) Error() string { return "insufficient capacity: " + e.msg }

func TestDynamicSchedulerV3_AssignCPUCores_PlacesPriorityOnLessPrioritySocket(t *testing.T) {
	// Build a minimal 2-socket topology: socket0 CPUs {0,1}, socket1 CPUs {2,3}
	hc := &host.HostConfig{}
	hc.Topology.Sockets = 2
	hc.Topology.CoreMap = map[int]host.CoreInfo{
		0: {LogicalID: 0, PhysicalID: 0, CoreID: 0, Siblings: []int{0}},
		1: {LogicalID: 1, PhysicalID: 0, CoreID: 1, Siblings: []int{1}},
		2: {LogicalID: 2, PhysicalID: 1, CoreID: 0, Siblings: []int{2}},
		3: {LogicalID: 3, PhysicalID: 1, CoreID: 1, Siblings: []int{3}},
	}
	hc.L3Cache.WaysPerCache = 12

	mockAlloc := &mockRDTAllocatorForScheduler{}
	acc, err := accounting.NewRDTAccountant(mockAlloc, hc)
	if err != nil {
		t.Fatalf("failed to create accountant: %v", err)
	}
	_ = acc.Initialize()

	cfg0 := &config.ContainerConfig{Index: 0, Image: "img", Priority: true, NumCores: 1}
	cfg1 := &config.ContainerConfig{Index: 1, Image: "img", Priority: true, NumCores: 1}

	s := NewDynamicSchedulerV3()
	s.SetHostConfig(hc)
	s.SetCPUAllocator(&fakeAllocatorForDynamicV3{})

	infos := []ContainerInfo{
		{Index: 0, Config: cfg0},
		{Index: 1, Config: cfg1},
	}
	if err := s.Initialize(acc, infos, &config.SchedulerConfig{Implementation: "dynamic-v3", RDT: true, Prober: &config.ProberConfig{Allocate: ptrBool(true)}}); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	cpus0, err := s.AssignCPUCores(0)
	if err != nil {
		t.Fatalf("assign 0 failed: %v", err)
	}
	if len(cpus0) != 1 {
		t.Fatalf("cpus0: %#v", cpus0)
	}
	// Do NOT start container 0 yet: v3 should still balance based on reserved priority.
	cpus1, err := s.AssignCPUCores(1)
	if err != nil {
		t.Fatalf("assign 1 failed: %v", err)
	}
	sock1, err := hc.SocketOfPhysicalCPUs(cpus1)
	if err != nil {
		t.Fatalf("sock1 failed: %v", err)
	}
	if sock1 != 1 {
		t.Fatalf("expected container1 on socket1, got socket %d (cpus=%v)", sock1, cpus1)
	}
}

func ptrBool(v bool) *bool { return &v }
