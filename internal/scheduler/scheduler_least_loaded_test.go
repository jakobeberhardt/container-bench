package scheduler

import (
	"strings"
	"testing"

	"container-bench/internal/config"
	"container-bench/internal/host"
)

type fakeCPUAllocatorLeastLoaded struct {
	snapshot map[int][]int
}

func (f *fakeCPUAllocatorLeastLoaded) EnsureAssigned(_ int, cfg *config.ContainerConfig) ([]int, error) {
	if cfg == nil {
		return nil, nil
	}
	return append([]int(nil), cfg.CPUCores...), nil
}

func (f *fakeCPUAllocatorLeastLoaded) Reserve(_ int, _ []int) error         { return nil }
func (f *fakeCPUAllocatorLeastLoaded) Allocate(_ int, _ int) ([]int, error) { return nil, nil }
func (f *fakeCPUAllocatorLeastLoaded) Release(_ int)                        {}
func (f *fakeCPUAllocatorLeastLoaded) Get(_ int) ([]int, bool)              { return nil, false }
func (f *fakeCPUAllocatorLeastLoaded) Snapshot() map[int][]int {
	out := make(map[int][]int, len(f.snapshot))
	for k, v := range f.snapshot {
		out[k] = append([]int(nil), v...)
	}
	return out
}
func (f *fakeCPUAllocatorLeastLoaded) Move(_ int, _ string, _ int) ([]int, error)  { return nil, nil }
func (f *fakeCPUAllocatorLeastLoaded) Swap(_ int, _ string, _ int, _ string) error { return nil }

func TestLeastLoadedScheduler_AssignCPUCores_NonBlockingOnInsufficientCapacity(t *testing.T) {
	ls := NewLeastLoadedScheduler()

	ls.hostConfig = &host.HostConfig{
		Topology: host.CPUTopology{
			Sockets:        2,
			ThreadsPerCore: 1,
			CoreMap: map[int]host.CoreInfo{
				0: {LogicalID: 0, PhysicalID: 0, CoreID: 0, Siblings: nil},
				1: {LogicalID: 1, PhysicalID: 0, CoreID: 1, Siblings: nil},
				2: {LogicalID: 2, PhysicalID: 1, CoreID: 0, Siblings: nil},
				3: {LogicalID: 3, PhysicalID: 1, CoreID: 1, Siblings: nil},
			},
		},
	}
	ls.cpuAllocator = &fakeCPUAllocatorLeastLoaded{snapshot: map[int][]int{1: {0, 1}, 2: {2, 3}}}

	cfg := &config.ContainerConfig{Index: 0, NumCores: 2}
	ls.containers = []ContainerInfo{{Index: 0, Config: cfg}}

	cpus, err := ls.AssignCPUCores(0)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if cpus != nil {
		t.Fatalf("expected nil cpus, got %v", cpus)
	}
	if !strings.Contains(err.Error(), "insufficient physical cores") {
		t.Fatalf("expected insufficient-cores error, got: %v", err)
	}
}
