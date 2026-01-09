package scheduler

import (
	proberesources "container-bench/internal/probe/resources"
	"testing"
)

func TestDynamicScheduler_SelectDescGuarantee_LastGoodBeforeDrop(t *testing.T) {
	// Simulate desc probing where we cross below the guarantee; we should keep the previous.
	thr := 0.90
	allocs := []proberesources.AllocationResult{
		{L3Ways: 11, MemBandwidth: 90, IPCEfficiency: 0.95},
		{L3Ways: 10, MemBandwidth: 90, IPCEfficiency: 0.92},
		{L3Ways: 9, MemBandwidth: 90, IPCEfficiency: 0.89},
	}

	bestWays, bestMem := selectDescGuaranteeAllocation(allocs, thr)
	if bestWays != 10 || bestMem != 90 {
		t.Fatalf("expected last-good (10,90), got (%d,%.0f)", bestWays, bestMem)
	}
}

func TestDynamicScheduler_SelectAscGuarantee_FirstMeetingThreshold(t *testing.T) {
	thr := 0.90
	allocs := []proberesources.AllocationResult{
		{L3Ways: 1, MemBandwidth: 10, IPCEfficiency: 0.70},
		{L3Ways: 2, MemBandwidth: 10, IPCEfficiency: 0.91},
		{L3Ways: 3, MemBandwidth: 10, IPCEfficiency: 0.95},
	}

	bestWays, bestMem := selectAscGuaranteeAllocation(allocs, thr)
	if bestWays != 2 || bestMem != 10 {
		t.Fatalf("expected first meeting (2,10), got (%d,%.0f)", bestWays, bestMem)
	}
}

func TestDynamicScheduler_SelectAscGuarantee_NoneMeetKeepsLast(t *testing.T) {
	thr := 0.90
	allocs := []proberesources.AllocationResult{
		{L3Ways: 1, MemBandwidth: 10, IPCEfficiency: 0.70},
		{L3Ways: 2, MemBandwidth: 20, IPCEfficiency: 0.80},
		{L3Ways: 3, MemBandwidth: 30, IPCEfficiency: 0.85},
	}

	bestWays, bestMem := selectAscGuaranteeAllocation(allocs, thr)
	if bestWays != 3 || bestMem != 30 {
		t.Fatalf("expected last (3,30), got (%d,%.0f)", bestWays, bestMem)
	}
}

func TestDynamicScheduler_SelectDescGuarantee_NoCandidateMeets(t *testing.T) {
	// If the first (highest) allocation already fails, we keep it as best-effort.
	thr := 0.90
	allocs := []proberesources.AllocationResult{
		{L3Ways: 11, MemBandwidth: 90, IPCEfficiency: 0.85},
		{L3Ways: 10, MemBandwidth: 90, IPCEfficiency: 0.84},
	}

	bestWays, bestMem := selectDescGuaranteeAllocation(allocs, thr)
	if bestWays != 11 || bestMem != 90 {
		t.Fatalf("expected fallback (11,90), got (%d,%.0f)", bestWays, bestMem)
	}
}

func TestDynamicScheduler_SelectDescGuarantee_AllMeet(t *testing.T) {
	// If all candidates meet the guarantee, we keep the last (lowest allocation tested).
	thr := 0.90
	allocs := []proberesources.AllocationResult{
		{L3Ways: 11, MemBandwidth: 90, IPCEfficiency: 0.95},
		{L3Ways: 10, MemBandwidth: 80, IPCEfficiency: 0.93},
		{L3Ways: 9, MemBandwidth: 70, IPCEfficiency: 0.91},
	}

	bestWays, bestMem := selectDescGuaranteeAllocation(allocs, thr)
	if bestWays != 9 || bestMem != 70 {
		t.Fatalf("expected last (9,70), got (%d,%.0f)", bestWays, bestMem)
	}
}
