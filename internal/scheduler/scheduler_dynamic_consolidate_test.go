package scheduler

import (
	"container-bench/internal/accounting"
	"container-bench/internal/allocation"
	"container-bench/internal/host"
	"fmt"
	"sync"
	"testing"
	"time"
)

type mockRDTAllocatorForScheduler struct {
	mu sync.Mutex
	// lastClasses captures the most recent class set applied via CreateAllRDTClasses.
	lastClasses map[string]struct {
		Socket0 *allocation.SocketAllocation
		Socket1 *allocation.SocketAllocation
	}
}

func (m *mockRDTAllocatorForScheduler) Initialize() error { return nil }

func (m *mockRDTAllocatorForScheduler) CreateRDTClass(className string, socket0, socket1 *allocation.SocketAllocation) error {
	return nil
}

func (m *mockRDTAllocatorForScheduler) UpdateRDTClass(className string, socket0, socket1 *allocation.SocketAllocation) error {
	return nil
}

func (m *mockRDTAllocatorForScheduler) CreateAllRDTClasses(classes map[string]struct {
	Socket0 *allocation.SocketAllocation
	Socket1 *allocation.SocketAllocation
}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastClasses = make(map[string]struct {
		Socket0 *allocation.SocketAllocation
		Socket1 *allocation.SocketAllocation
	}, len(classes))
	for k, v := range classes {
		m.lastClasses[k] = v
	}
	return nil
}

func (m *mockRDTAllocatorForScheduler) AssignContainerToClass(pid int, className string) error { return nil }
func (m *mockRDTAllocatorForScheduler) RemoveContainerFromClass(pid int) error            { return nil }
func (m *mockRDTAllocatorForScheduler) GetContainerClass(pid int) (string, error)        { return "", fmt.Errorf("not implemented") }
func (m *mockRDTAllocatorForScheduler) ListAvailableClasses() []string                   { return nil }
func (m *mockRDTAllocatorForScheduler) DeleteRDTClass(className string) error            { return nil }
func (m *mockRDTAllocatorForScheduler) Cleanup() error                                   { return nil }

func TestDynamicScheduler_IsSingleContiguousRunOfOnes(t *testing.T) {
	// 0xe0f = 1110 0000 1111 (two blocks of ones) => invalid for goresctrl.
	if isSingleContiguousRunOfOnes(0xe0f, 12) {
		t.Fatalf("expected non-contiguous run for 0xe0f")
	}
	if !isSingleContiguousRunOfOnes(0x03f, 12) { // 0000 0011 1111
		t.Fatalf("expected contiguous run for 0x03f")
	}
	if !isSingleContiguousRunOfOnes(0x000, 12) {
		t.Fatalf("expected contiguous run for 0x000")
	}
	if !isSingleContiguousRunOfOnes(0xfff, 12) {
		t.Fatalf("expected contiguous run for 0xfff")
	}
}

func TestDynamicScheduler_RepackCriticalMasksHigh(t *testing.T) {
	assigned, bench, err := repackCriticalMasksHigh(12, []int{2, 2, 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(assigned) != 3 {
		t.Fatalf("expected 3 assigned masks")
	}
	// Packed at the high end: 0xC00, 0x300, 0x0C0; benchmark = low 6 ways = 0x3F.
	wantAssigned := []uint64{0xC00, 0x300, 0x0C0}
	for i := range wantAssigned {
		if assigned[i] != wantAssigned[i] {
			t.Fatalf("assigned[%d]=0x%x, want 0x%x", i, assigned[i], wantAssigned[i])
		}
		if !isSingleContiguousRunOfOnes(assigned[i], 12) {
			t.Fatalf("assigned[%d] not contiguous: 0x%x", i, assigned[i])
		}
	}
	if bench != 0x3f {
		t.Fatalf("bench=0x%x, want 0x3f", bench)
	}
	if !isSingleContiguousRunOfOnes(bench, 12) {
		t.Fatalf("bench not contiguous: 0x%x", bench)
	}
}

func TestDynamicScheduler_TakeFromHighEndOfLowBlock(t *testing.T) {
	m, err := takeFromHighEndOfLowBlock(6, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// low block of 6 is 0b00_111111, take 2 from high end -> 0b00_110000 = 0x30
	if m != 0x30 {
		t.Fatalf("got 0x%x, want 0x30", m)
	}
}

func TestDynamicScheduler_ConsolidateRDTPartitionsLocked_DefragmentsBenchmark(t *testing.T) {
	mockAlloc := &mockRDTAllocatorForScheduler{}
	hostCfg := &host.HostConfig{L3Cache: host.L3CacheConfig{WaysPerCache: 12}}
	acc, err := accounting.NewRDTAccountant(mockAlloc, hostCfg)
	if err != nil {
		t.Fatalf("failed to create accountant: %v", err)
	}

	s := NewDynamicScheduler()
	s.rdtAccountant = acc
	s.totalWays = 12
	s.sockets = 1
	s.benchmarkClass = "system/default"

	// Start from a fragmented benchmark mask (similar to the log's 0xe0f case).
	s.benchmarkMask[0] = 0xe0f
	s.benchmarkMem[0] = 100

	now := time.Now()
	s.profiles = make(map[int]*dynamicContainerProfile)
	// Two active critical containers, 2 ways each.
	s.profiles[1] = &dynamicContainerProfile{
		index:       1,
		pid:         111,
		containerID: "c1",
		containerKey: "p1",
		startedAt:   now.Add(-10 * time.Second),
		critical:    true,
		socket:      0,
		className:   "dyn-p1",
		ways:        2,
		mem:         20,
	}
	s.profiles[2] = &dynamicContainerProfile{
		index:       2,
		pid:         222,
		containerID: "c2",
		containerKey: "p2",
		startedAt:   now.Add(-5 * time.Second),
		critical:    true,
		socket:      0,
		className:   "dyn-p2",
		ways:        2,
		mem:         10,
	}

	if err := s.consolidateRDTPartitionsLocked(); err != nil {
		t.Fatalf("consolidate failed: %v", err)
	}

	// totalWays=12, sumCriticalWays=4 => benchmarkWays=8 => 0xFF.
	if s.benchmarkMask[0] != 0xff {
		t.Fatalf("benchmarkMask=0x%x, want 0xff", s.benchmarkMask[0])
	}
	if !isSingleContiguousRunOfOnes(s.benchmarkMask[0], 12) {
		t.Fatalf("benchmark mask not contiguous: 0x%x", s.benchmarkMask[0])
	}

	mockAlloc.mu.Lock()
	classes := mockAlloc.lastClasses
	mockAlloc.mu.Unlock()
	if classes == nil {
		t.Fatalf("expected allocator to receive class set")
	}
	if _, ok := classes["system/default"]; !ok {
		t.Fatalf("expected system/default in class set")
	}
	if _, ok := classes["dyn-p1"]; !ok {
		t.Fatalf("expected dyn-p1 in class set")
	}
	if _, ok := classes["dyn-p2"]; !ok {
		t.Fatalf("expected dyn-p2 in class set")
	}

	// Ensure all class masks are contiguous (no fragmentation).
	for name, cfg := range classes {
		if cfg.Socket0 == nil {
			continue
		}
		bm := parseHexBitmask(cfg.Socket0.L3Bitmask)
		if bm == 0 && cfg.Socket0.L3Bitmask != "0x0" && cfg.Socket0.L3Bitmask != "" {
			// ignore; parseHexBitmask returns 0 on error too, but we only emit hex.
		}
		if !isSingleContiguousRunOfOnes(bm, 12) {
			t.Fatalf("class %s mask not contiguous: %s", name, cfg.Socket0.L3Bitmask)
		}
	}
}

func parseHexBitmask(s string) uint64 {
	var v uint64
	_, _ = fmt.Sscanf(s, "0x%x", &v)
	return v
}
