package cpuallocator

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"container-bench/internal/config"
	"container-bench/internal/host"
)

type fakeCpusetApplier struct {
	mu      sync.Mutex
	updates []applied
	fail    bool
}

type applied struct {
	containerID string
	cpuset      string
}

func (f *fakeCpusetApplier) UpdateCpuset(_ context.Context, containerID string, cpuset string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return fmt.Errorf("boom")
	}
	f.updates = append(f.updates, applied{containerID: containerID, cpuset: cpuset})
	return nil
}

func (f *fakeCpusetApplier) calls() []applied {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]applied, len(f.updates))
	copy(out, f.updates)
	return out
}

func makeHostConfigEqualSockets(t *testing.T, sockets int, coresPerSocket int, threadsPerCore int) *host.HostConfig {
	t.Helper()
	if sockets <= 0 || coresPerSocket <= 0 || threadsPerCore <= 0 {
		t.Fatalf("invalid topology")
	}
	coreMap := make(map[int]host.CoreInfo)
	logical := 0
	for socket := 0; socket < sockets; socket++ {
		for core := 0; core < coresPerSocket; core++ {
			ids := make([]int, 0, threadsPerCore)
			for th := 0; th < threadsPerCore; th++ {
				ids = append(ids, logical+th)
			}
			for _, id := range ids {
				coreMap[id] = host.CoreInfo{
					LogicalID:  id,
					PhysicalID: socket,
					CoreID:     core,
					Siblings:   append([]int(nil), ids...),
				}
			}
			logical += threadsPerCore
		}
	}
	return &host.HostConfig{Topology: host.CPUTopology{
		PhysicalCores:  sockets * coresPerSocket,
		LogicalCores:   sockets * coresPerSocket * threadsPerCore,
		ThreadsPerCore: threadsPerCore,
		Sockets:        sockets,
		CoresPerSocket: coresPerSocket,
		CoreMap:        coreMap,
	}}
}

// socket0: cores 0,2 ; socket1: core 4 (single core)
func makeHostConfigUnevenSockets(t *testing.T) *host.HostConfig {
	t.Helper()
	coreMap := make(map[int]host.CoreInfo)
	// socket 0: 2 physical cores, 1 thread each
	for core := 0; core < 2; core++ {
		id := core
		coreMap[id] = host.CoreInfo{LogicalID: id, PhysicalID: 0, CoreID: core, Siblings: []int{id}}
	}
	// socket 1: 1 physical core, 1 thread
	coreMap[4] = host.CoreInfo{LogicalID: 4, PhysicalID: 1, CoreID: 0, Siblings: []int{4}}
	return &host.HostConfig{Topology: host.CPUTopology{
		PhysicalCores:  3,
		LogicalCores:   3,
		ThreadsPerCore: 1,
		Sockets:        2,
		CoresPerSocket: 0,
		CoreMap:        coreMap,
	}}
}

func TestPhysicalCoreAllocator_ReserveRejectsHTSibling(t *testing.T) {
	hc := makeHostConfigEqualSockets(t, 1, 2, 2) // physical CPUs are 0 and 2
	a, err := NewPhysicalCoreAllocator(hc, nil, nil)
	if err != nil {
		t.Fatalf("new allocator: %v", err)
	}
	if err := a.Reserve(0, []int{1}); err == nil {
		t.Fatalf("expected error reserving HT sibling")
	}
}

func TestPhysicalCoreAllocator_ReserveConflict(t *testing.T) {
	hc := makeHostConfigEqualSockets(t, 1, 2, 2)
	a, err := NewPhysicalCoreAllocator(hc, nil, nil)
	if err != nil {
		t.Fatalf("new allocator: %v", err)
	}
	if err := a.Reserve(0, []int{0}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := a.Reserve(1, []int{0}); err == nil {
		t.Fatalf("expected conflict error")
	}
}

func TestPhysicalCoreAllocator_AllocateInsufficient(t *testing.T) {
	hc := makeHostConfigEqualSockets(t, 1, 1, 2) // only one physical cpu
	a, err := NewPhysicalCoreAllocator(hc, nil, nil)
	if err != nil {
		t.Fatalf("new allocator: %v", err)
	}
	if _, err := a.Allocate(0, 2); err == nil {
		t.Fatalf("expected insufficient cores error")
	}
}

func TestPhysicalCoreAllocator_EnsureAssignedExplicit(t *testing.T) {
	hc := makeHostConfigEqualSockets(t, 1, 3, 2) // physical: 0,2,4
	a, err := NewPhysicalCoreAllocator(hc, nil, nil)
	if err != nil {
		t.Fatalf("new allocator: %v", err)
	}
	cfg := &config.ContainerConfig{Name: "c0", Core: "0,4"}
	cpus, err := a.EnsureAssigned(0, cfg)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if got := config.FormatCPUSpec(cpus); got != "0,4" {
		t.Fatalf("cpus=%v got %q", cpus, got)
	}
	if cfg.Core != "0,4" {
		t.Fatalf("cfg.Core got %q", cfg.Core)
	}
	if len(cfg.CPUCores) != 2 {
		t.Fatalf("cfg.CPUCores=%v", cfg.CPUCores)
	}
}

func TestPhysicalCoreAllocator_EnsureAssignedExplicitCPUCoresOnly(t *testing.T) {
	hc := makeHostConfigEqualSockets(t, 1, 3, 2) // physical: 0,2,4
	a, err := NewPhysicalCoreAllocator(hc, nil, nil)
	if err != nil {
		t.Fatalf("new allocator: %v", err)
	}

	// Caller provides CPUCores directly (Core string empty).
	cfg0 := &config.ContainerConfig{Name: "c0", CPUCores: []int{4, 0}}
	c0, err := a.EnsureAssigned(0, cfg0)
	if err != nil {
		t.Fatalf("ensure c0: %v", err)
	}
	if got := config.FormatCPUSpec(c0); got != "0,4" {
		t.Fatalf("c0 cpus=%v got %q", c0, got)
	}
	if cfg0.Core != "0,4" {
		t.Fatalf("cfg0.Core got %q", cfg0.Core)
	}
	if got, ok := a.Get(0); !ok || config.FormatCPUSpec(got) != "0,4" {
		t.Fatalf("allocator state=%v ok=%v", got, ok)
	}

	// Ensure allocator doesn't reuse reserved CPUs.
	cfg1 := &config.ContainerConfig{Name: "c1", NumCores: 1}
	c1, err := a.EnsureAssigned(1, cfg1)
	if err != nil {
		t.Fatalf("ensure c1: %v", err)
	}
	if got := config.FormatCPUSpec(c1); got != "2" {
		t.Fatalf("c1 cpus=%v got %q", c1, got)
	}
}

func TestPhysicalCoreAllocator_MoveSuccessAcrossSockets(t *testing.T) {
	hc := makeHostConfigEqualSockets(t, 2, 2, 2) // physical: socket0 {0,2}, socket1 {4,6}
	applier := &fakeCpusetApplier{}
	a, err := NewPhysicalCoreAllocator(hc, applier, nil)
	if err != nil {
		t.Fatalf("new allocator: %v", err)
	}
	cfg := &config.ContainerConfig{Name: "a", NumCores: 2}
	assigned, err := a.EnsureAssigned(0, cfg)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if config.FormatCPUSpec(assigned) != "0,2" {
		t.Fatalf("initial cpus=%v", assigned)
	}

	newCPUs, err := a.Move(0, "A", 1)
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	if config.FormatCPUSpec(newCPUs) != "4,6" {
		t.Fatalf("moved cpus=%v", newCPUs)
	}
	if cfg.Core != "4,6" {
		t.Fatalf("cfg.Core=%q", cfg.Core)
	}
	if got, ok := a.Get(0); !ok || config.FormatCPUSpec(got) != "4,6" {
		t.Fatalf("allocator state=%v ok=%v", got, ok)
	}
	calls := applier.calls()
	if len(calls) != 1 || calls[0].containerID != "A" || calls[0].cpuset != "4,6" {
		t.Fatalf("applier calls=%v", calls)
	}
}

func TestPhysicalCoreAllocator_MoveFailsIfApplierFails(t *testing.T) {
	hc := makeHostConfigEqualSockets(t, 2, 2, 2)
	applier := &fakeCpusetApplier{fail: true}
	a, err := NewPhysicalCoreAllocator(hc, applier, nil)
	if err != nil {
		t.Fatalf("new allocator: %v", err)
	}
	cfg := &config.ContainerConfig{Name: "a", NumCores: 1}
	assigned, err := a.EnsureAssigned(0, cfg)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	old := config.FormatCPUSpec(assigned)

	if _, err := a.Move(0, "A", 1); err == nil {
		t.Fatalf("expected move error")
	}
	if got, ok := a.Get(0); !ok || config.FormatCPUSpec(got) != old {
		t.Fatalf("expected rollback to %q, got %v", old, got)
	}
}

func TestPhysicalCoreAllocator_MoveFailsIfSocketFull(t *testing.T) {
	hc := makeHostConfigEqualSockets(t, 2, 2, 2)
	applier := &fakeCpusetApplier{}
	a, err := NewPhysicalCoreAllocator(hc, applier, nil)
	if err != nil {
		t.Fatalf("new allocator: %v", err)
	}
	// Fill socket 1
	cfg1 := &config.ContainerConfig{Name: "b", Core: "4,6"}
	if _, err := a.EnsureAssigned(1, cfg1); err != nil {
		t.Fatalf("ensure b: %v", err)
	}

	cfg0 := &config.ContainerConfig{Name: "a", NumCores: 2}
	if _, err := a.EnsureAssigned(0, cfg0); err != nil {
		t.Fatalf("ensure a: %v", err)
	}
	if _, err := a.Move(0, "A", 1); err == nil {
		t.Fatalf("expected insufficient capacity error")
	}
}

func TestPhysicalCoreAllocator_SwapAcrossSocketsSuccess(t *testing.T) {
	hc := makeHostConfigEqualSockets(t, 2, 2, 2)
	applier := &fakeCpusetApplier{}
	a, err := NewPhysicalCoreAllocator(hc, applier, nil)
	if err != nil {
		t.Fatalf("new allocator: %v", err)
	}
	// pin A on socket0, B on socket1 using explicit cores
	cfgA := &config.ContainerConfig{Name: "a", Core: "0"}
	cfgB := &config.ContainerConfig{Name: "b", Core: "4"}
	if _, err := a.EnsureAssigned(0, cfgA); err != nil {
		t.Fatalf("ensure a: %v", err)
	}
	if _, err := a.EnsureAssigned(1, cfgB); err != nil {
		t.Fatalf("ensure b: %v", err)
	}
	if err := a.Swap(0, "A", 1, "B"); err != nil {
		t.Fatalf("swap: %v", err)
	}
	if got, _ := a.Get(0); config.FormatCPUSpec(got) != "4" {
		t.Fatalf("A now %v", got)
	}
	if got, _ := a.Get(1); config.FormatCPUSpec(got) != "0" {
		t.Fatalf("B now %v", got)
	}
	calls := applier.calls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 applier calls, got %v", calls)
	}
}

func TestPhysicalCoreAllocator_SwapFailsIfDestinationSocketTooSmall(t *testing.T) {
	hc := makeHostConfigUnevenSockets(t)
	applier := &fakeCpusetApplier{}
	a, err := NewPhysicalCoreAllocator(hc, applier, nil)
	if err != nil {
		t.Fatalf("new allocator: %v", err)
	}
	// A uses 2 cores on socket0, B uses 1 on socket1
	cfgA := &config.ContainerConfig{Name: "a", Core: "0,1"}
	cfgB := &config.ContainerConfig{Name: "b", Core: "4"}
	if _, err := a.EnsureAssigned(0, cfgA); err != nil {
		t.Fatalf("ensure a: %v", err)
	}
	if _, err := a.EnsureAssigned(1, cfgB); err != nil {
		t.Fatalf("ensure b: %v", err)
	}
	oldA, _ := a.Get(0)
	oldB, _ := a.Get(1)
	if err := a.Swap(0, "A", 1, "B"); err == nil {
		t.Fatalf("expected swap failure")
	}
	gotA, _ := a.Get(0)
	gotB, _ := a.Get(1)
	if config.FormatCPUSpec(gotA) != config.FormatCPUSpec(oldA) || config.FormatCPUSpec(gotB) != config.FormatCPUSpec(oldB) {
		t.Fatalf("expected no change; A=%v B=%v", gotA, gotB)
	}
	if len(applier.calls()) != 0 {
		t.Fatalf("expected no docker updates")
	}
}

func TestPhysicalCoreAllocator_SwapSameSocketMovesFirst(t *testing.T) {
	hc := makeHostConfigEqualSockets(t, 2, 2, 2)
	applier := &fakeCpusetApplier{}
	a, err := NewPhysicalCoreAllocator(hc, applier, nil)
	if err != nil {
		t.Fatalf("new allocator: %v", err)
	}
	// Both on socket0
	cfgA := &config.ContainerConfig{Name: "a", Core: "0"}
	cfgB := &config.ContainerConfig{Name: "b", Core: "2"}
	if _, err := a.EnsureAssigned(0, cfgA); err != nil {
		t.Fatalf("ensure a: %v", err)
	}
	if _, err := a.EnsureAssigned(1, cfgB); err != nil {
		t.Fatalf("ensure b: %v", err)
	}
	if err := a.Swap(0, "A", 1, "B"); err != nil {
		t.Fatalf("swap: %v", err)
	}
	// B unchanged, A moved to socket1 (should become 4)
	if got, _ := a.Get(1); config.FormatCPUSpec(got) != "2" {
		t.Fatalf("B=%v", got)
	}
	if got, _ := a.Get(0); config.FormatCPUSpec(got) != "4" {
		t.Fatalf("A=%v", got)
	}
}
