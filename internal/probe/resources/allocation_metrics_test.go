package resources

import (
	"container-bench/internal/dataframe"
	"testing"
	"time"
)

func TestComputeAllocationMetrics_UsesIPCEfficancy(t *testing.T) {
	dfs := dataframe.NewDataFrames()
	cdf := dfs.AddContainer(0)

	v1 := 0.5
	v2 := 0.7
	cdf.AddStep(0, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v1}})
	cdf.AddStep(1, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v2}})

	res := &AllocationResult{SocketID: 0}
	ComputeAllocationMetrics(res, dfs, 0, -1, 1)

	if res.IPCEfficiency < 0.59 || res.IPCEfficiency > 0.61 {
		t.Fatalf("expected ~0.6, got %v", res.IPCEfficiency)
	}
}

func TestAllocationProbeRunner_UsesIPCEfficancyWithoutTheoreticalIPC(t *testing.T) {
	dfs := dataframe.NewDataFrames()
	cdf := dfs.AddContainer(0)

	v := 0.8
	cdf.AddStep(1, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v}})

	calls := 0
	latest := func(_ *dataframe.DataFrames, _ int) int {
		calls++
		if calls == 1 {
			return 0
		}
		return 1
	}

	r := NewAllocationProbeRunner(
		AllocationProbeTarget{ContainerIndex: 0, ContainerName: "c0"},
		AllocationRange{SocketID: 0},
		[]AllocationSpec{{L3Ways: 1, MemBandwidth: 10}},
		0,
		AllocationProbeBreakPolicy{},
		AllocationProbeOptions{},
		AllocationProbeCallbacks{LatestStepNumber: latest},
	)

	if err := r.Start(dfs); err != nil {
		t.Fatalf("start: %v", err)
	}

	done, err := r.Step(dfs)
	if err != nil {
		t.Fatalf("step: %v", err)
	}
	if !done {
		t.Fatalf("expected done")
	}
	if r.BestEff() < 0.799 || r.BestEff() > 0.801 {
		t.Fatalf("expected best ~0.8, got %v", r.BestEff())
	}
}

func TestComputeAllocationMetrics_DoesNotFallbackToIPCOverTheoreticalIPC(t *testing.T) {
	dfs := dataframe.NewDataFrames()
	cdf := dfs.AddContainer(0)

	ipc := 2.0
	theo := 4.0
	cdf.AddStep(1, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{InstructionsPerCycle: &ipc, TheoreticalIPC: &theo}})

	res := &AllocationResult{SocketID: 0}
	ComputeAllocationMetrics(res, dfs, 0, 0, 1)

	if res.IPCEfficiency != -1 {
		t.Fatalf("expected IPCEfficiency=-1 when IPCEfficancy missing, got %v", res.IPCEfficiency)
	}
}

func TestAllocationProbeRunner_UnboundWhenCompletedEvenIfBestNotMax(t *testing.T) {
	dfs := dataframe.NewDataFrames()
	cdf := dfs.AddContainer(0)

	v1 := 90.0
	v2 := 50.0
	cdf.AddStep(1, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v1}})
	cdf.AddStep(2, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v2}})

	seq := []int{0, 1, 1, 2}
	pos := 0
	latest := func(_ *dataframe.DataFrames, _ int) int {
		if pos >= len(seq) {
			return seq[len(seq)-1]
		}
		v := seq[pos]
		pos++
		return v
	}

	r := NewAllocationProbeRunner(
		AllocationProbeTarget{ContainerIndex: 0, ContainerName: "c0"},
		AllocationRange{SocketID: 0, MaxL3Ways: 2, MaxMemBandwidth: 20},
		[]AllocationSpec{{L3Ways: 1, MemBandwidth: 10}, {L3Ways: 2, MemBandwidth: 10}},
		0,
		AllocationProbeBreakPolicy{},
		AllocationProbeOptions{},
		AllocationProbeCallbacks{LatestStepNumber: latest},
	)

	if err := r.Start(dfs); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Step 1: evaluates candidate 1
	done, err := r.Step(dfs)
	if err != nil {
		t.Fatalf("step1: %v", err)
	}
	if done {
		t.Fatalf("expected not done after first candidate")
	}

	// Step 2: evaluates candidate 2 and completes.
	done, err = r.Step(dfs)
	if err != nil {
		t.Fatalf("step2: %v", err)
	}
	if !done {
		t.Fatalf("expected done")
	}
	if r.StopReason() != "completed" {
		t.Fatalf("expected completed, got %q", r.StopReason())
	}
	if !r.Unbound() {
		t.Fatalf("expected unbound when completed")
	}
	// Best should still be the first candidate (ways=1), proving unbound is independent of best.
	if r.BestWays() != 1 {
		t.Fatalf("expected best ways=1, got %d", r.BestWays())
	}
}

func TestAllocationProbeRunner_NotUnboundWhenBreakConditionMet(t *testing.T) {
	dfs := dataframe.NewDataFrames()
	cdf := dfs.AddContainer(0)

	v1 := 90.0
	cdf.AddStep(1, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v1}})

	seq := []int{0, 1}
	pos := 0
	latest := func(_ *dataframe.DataFrames, _ int) int {
		if pos >= len(seq) {
			return seq[len(seq)-1]
		}
		v := seq[pos]
		pos++
		return v
	}

	thr := 80.0
	r := NewAllocationProbeRunner(
		AllocationProbeTarget{ContainerIndex: 0, ContainerName: "c0"},
		AllocationRange{SocketID: 0, MaxL3Ways: 2, MaxMemBandwidth: 20},
		[]AllocationSpec{{L3Ways: 1, MemBandwidth: 10}, {L3Ways: 2, MemBandwidth: 10}},
		0,
		AllocationProbeBreakPolicy{AcceptableIPCEfficiency: &thr},
		AllocationProbeOptions{},
		AllocationProbeCallbacks{LatestStepNumber: latest},
	)

	if err := r.Start(dfs); err != nil {
		t.Fatalf("start: %v", err)
	}

	done, err := r.Step(dfs)
	if err != nil {
		t.Fatalf("step: %v", err)
	}
	if !done {
		t.Fatalf("expected done")
	}
	if r.StopReason() != "break_condition" {
		t.Fatalf("expected break_condition, got %q", r.StopReason())
	}
	if r.Unbound() {
		t.Fatalf("expected not unbound when break condition met")
	}
}

func TestAllocationProbeRunner_DiminishingReturnsWithinWaysGroup(t *testing.T) {
	dfs := dataframe.NewDataFrames()
	cdf := dfs.AddContainer(0)

	v1 := 50.0
	v2 := 52.0
	cdf.AddStep(1, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v1}})
	cdf.AddStep(2, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v2}})

	// Produce step numbers for: start (0), end1 (1), start2 (1), end2 (2)
	seq := []int{0, 1, 1, 2}
	pos := 0
	latest := func(_ *dataframe.DataFrames, _ int) int {
		if pos >= len(seq) {
			return seq[len(seq)-1]
		}
		v := seq[pos]
		pos++
		return v
	}

	thr := 0.10 // 10% relative improvement threshold
	r := NewAllocationProbeRunner(
		AllocationProbeTarget{ContainerIndex: 0, ContainerName: "c0"},
		AllocationRange{SocketID: 0, MaxL3Ways: 2, MaxMemBandwidth: 20},
		[]AllocationSpec{{L3Ways: 1, MemBandwidth: 10}, {L3Ways: 1, MemBandwidth: 20}},
		0,
		AllocationProbeBreakPolicy{DiminishingReturnsThreshold: &thr},
		AllocationProbeOptions{},
		AllocationProbeCallbacks{LatestStepNumber: latest},
	)

	if err := r.Start(dfs); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Step 1: evaluates first mem point
	done, err := r.Step(dfs)
	if err != nil {
		t.Fatalf("step1: %v", err)
	}
	if done {
		t.Fatalf("expected not done after first candidate")
	}

	// Step 2: evaluates second mem point, triggers diminishing returns
	done, err = r.Step(dfs)
	if err != nil {
		t.Fatalf("step2: %v", err)
	}
	if !done {
		t.Fatalf("expected done")
	}
	if r.StopReason() != "diminishing_returns" {
		t.Fatalf("expected diminishing_returns, got %q", r.StopReason())
	}
	if r.Unbound() {
		t.Fatalf("expected not unbound when diminishing returns triggered")
	}
}

func TestAllocationProbeRunner_NonGreedyDoesNotStopEarlyAcrossSawtooth(t *testing.T) {
	dfs := dataframe.NewDataFrames()
	cdf := dfs.AddContainer(0)

	// Candidate efficiencies (percent):
	// ways1: 50 -> 51 (diminishing)
	// ways2: 80 -> 81 (diminishing)
	v1 := 50.0
	v2 := 51.0
	v3 := 80.0
	v4 := 81.0
	cdf.AddStep(1, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v1}})
	cdf.AddStep(2, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v2}})
	cdf.AddStep(3, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v3}})
	cdf.AddStep(4, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v4}})

	// start/end steps per candidate: (0->1), (1->2), (2->3), (3->4)
	seq := []int{0, 1, 1, 2, 2, 3, 3, 4}
	pos := 0
	latest := func(_ *dataframe.DataFrames, _ int) int {
		if pos >= len(seq) {
			return seq[len(seq)-1]
		}
		v := seq[pos]
		pos++
		return v
	}

	thr := 0.10
	r := NewAllocationProbeRunner(
		AllocationProbeTarget{ContainerIndex: 0, ContainerName: "c0"},
		AllocationRange{SocketID: 0, MaxL3Ways: 2, MaxMemBandwidth: 20},
		[]AllocationSpec{{L3Ways: 1, MemBandwidth: 10}, {L3Ways: 1, MemBandwidth: 20}, {L3Ways: 2, MemBandwidth: 10}, {L3Ways: 2, MemBandwidth: 20}},
		0,
		AllocationProbeBreakPolicy{DiminishingReturnsThreshold: &thr},
		AllocationProbeOptions{GreedyAllocation: false},
		AllocationProbeCallbacks{LatestStepNumber: latest},
	)

	if err := r.Start(dfs); err != nil {
		t.Fatalf("start: %v", err)
	}
	for i := 0; i < 3; i++ {
		done, err := r.Step(dfs)
		if err != nil {
			t.Fatalf("step %d: %v", i+1, err)
		}
		if done {
			t.Fatalf("expected not done after %d candidates", i+1)
		}
	}
	done, err := r.Step(dfs)
	if err != nil {
		t.Fatalf("step4: %v", err)
	}
	if !done {
		t.Fatalf("expected done")
	}
	if len(r.Result().Allocations) != 4 {
		t.Fatalf("expected 4 allocations, got %d", len(r.Result().Allocations))
	}
	if r.StopReason() != "diminishing_returns" {
		t.Fatalf("expected diminishing_returns (post-mortem), got %q", r.StopReason())
	}
	if r.Unbound() {
		t.Fatalf("expected not unbound when diminishing returns classified")
	}
}

func TestAllocationProbeRunner_GreedyStopsEarlyOnDiminishingReturns(t *testing.T) {
	dfs := dataframe.NewDataFrames()
	cdf := dfs.AddContainer(0)

	v1 := 50.0
	v2 := 51.0
	v3 := 80.0
	v4 := 81.0
	cdf.AddStep(1, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v1}})
	cdf.AddStep(2, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v2}})
	cdf.AddStep(3, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v3}})
	cdf.AddStep(4, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v4}})

	seq := []int{0, 1, 1, 2}
	pos := 0
	latest := func(_ *dataframe.DataFrames, _ int) int {
		if pos >= len(seq) {
			return seq[len(seq)-1]
		}
		v := seq[pos]
		pos++
		return v
	}

	thr := 0.10
	r := NewAllocationProbeRunner(
		AllocationProbeTarget{ContainerIndex: 0, ContainerName: "c0"},
		AllocationRange{SocketID: 0, MaxL3Ways: 2, MaxMemBandwidth: 20},
		[]AllocationSpec{{L3Ways: 1, MemBandwidth: 10}, {L3Ways: 1, MemBandwidth: 20}, {L3Ways: 2, MemBandwidth: 10}, {L3Ways: 2, MemBandwidth: 20}},
		0,
		AllocationProbeBreakPolicy{DiminishingReturnsThreshold: &thr},
		AllocationProbeOptions{GreedyAllocation: true},
		AllocationProbeCallbacks{LatestStepNumber: latest},
	)

	if err := r.Start(dfs); err != nil {
		t.Fatalf("start: %v", err)
	}
	_, err := r.Step(dfs)
	if err != nil {
		t.Fatalf("step1: %v", err)
	}
	done, err := r.Step(dfs)
	if err != nil {
		t.Fatalf("step2: %v", err)
	}
	if !done {
		t.Fatalf("expected done")
	}
	if len(r.Result().Allocations) != 2 {
		t.Fatalf("expected 2 allocations (stopped early), got %d", len(r.Result().Allocations))
	}
	if r.StopReason() != "diminishing_returns" {
		t.Fatalf("expected diminishing_returns, got %q", r.StopReason())
	}
}
