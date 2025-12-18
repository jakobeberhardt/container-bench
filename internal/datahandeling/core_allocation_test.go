package datahandeling

import (
	"testing"
	"time"

	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
)

func TestCoreAllocationAggregationCountsPerSocket(t *testing.T) {
	hc := &host.HostConfig{Topology: host.CPUTopology{Sockets: 2, CoreMap: map[int]host.CoreInfo{
		0: {LogicalID: 0, PhysicalID: 0, CoreID: 0, Siblings: []int{0}},
		1: {LogicalID: 1, PhysicalID: 0, CoreID: 1, Siblings: []int{1}},
		4: {LogicalID: 4, PhysicalID: 1, CoreID: 0, Siblings: []int{4}},
		5: {LogicalID: 5, PhysicalID: 1, CoreID: 1, Siblings: []int{5}},
	}}}

	h := NewDefaultDataHandler(hc)
	dfs := dataframe.NewDataFrames()

	ts := time.Unix(100, 0)
	// container 0 uses socket0: 0,1
	c0 := dfs.AddContainer(0)
	c0.AddStep(0, &dataframe.SamplingStep{Timestamp: ts, Docker: &dataframe.DockerMetrics{AssignedCoresCSV: strPtr("0,1")}})
	// container 1 uses socket1: 4
	c1 := dfs.AddContainer(1)
	c1.AddStep(0, &dataframe.SamplingStep{Timestamp: ts, Docker: &dataframe.DockerMetrics{AssignedCoresCSV: strPtr("4")}})

	benchCfg := &config.BenchmarkConfig{Containers: map[string]config.ContainerConfig{
		"c0": {Index: 0},
		"c1": {Index: 1},
	}}
	out, err := h.ProcessDataFrames(1, benchCfg, dfs, ts, ts)
	if err != nil {
		t.Fatalf("ProcessDataFrames: %v", err)
	}
	if len(out.CoreAllocationSteps) != 1 {
		t.Fatalf("expected 1 core allocation step, got %d", len(out.CoreAllocationSteps))
	}
	step := out.CoreAllocationSteps[0]
	if step.CoresAllocatedSocketZero != 2 {
		t.Fatalf("socket0 expected 2, got %d", step.CoresAllocatedSocketZero)
	}
	if step.CoresAllocatedSocketOne != 1 {
		t.Fatalf("socket1 expected 1, got %d", step.CoresAllocatedSocketOne)
	}
}

func TestCoreAllocationAggregationIgnoresHTSiblingsAndUnknownCPUs(t *testing.T) {
	// socket0: physical cpu 0, HT sibling 2
	hc := &host.HostConfig{Topology: host.CPUTopology{Sockets: 2, CoreMap: map[int]host.CoreInfo{
		0: {LogicalID: 0, PhysicalID: 0, CoreID: 0, Siblings: []int{0, 2}},
		2: {LogicalID: 2, PhysicalID: 0, CoreID: 0, Siblings: []int{0, 2}},
		4: {LogicalID: 4, PhysicalID: 1, CoreID: 0, Siblings: []int{4}},
	}}}
	h := NewDefaultDataHandler(hc)
	dfs := dataframe.NewDataFrames()

	ts := time.Unix(200, 0)
	c0 := dfs.AddContainer(0)
	// includes HT sibling 2 (ignored) and unknown cpu 99 (ignored)
	c0.AddStep(0, &dataframe.SamplingStep{Timestamp: ts, Docker: &dataframe.DockerMetrics{AssignedCoresCSV: strPtr("0,2,4,99")}})

	benchCfg := &config.BenchmarkConfig{Containers: map[string]config.ContainerConfig{
		"c0": {Index: 0},
	}}
	out, err := h.ProcessDataFrames(1, benchCfg, dfs, ts, ts)
	if err != nil {
		t.Fatalf("ProcessDataFrames: %v", err)
	}
	if len(out.CoreAllocationSteps) != 1 {
		t.Fatalf("expected 1 core allocation step, got %d", len(out.CoreAllocationSteps))
	}
	step := out.CoreAllocationSteps[0]
	// counts cpu0 on socket0 and cpu4 on socket1, ignores cpu2 (HT) and 99
	if step.CoresAllocatedSocketZero != 1 {
		t.Fatalf("socket0 expected 1, got %d", step.CoresAllocatedSocketZero)
	}
	if step.CoresAllocatedSocketOne != 1 {
		t.Fatalf("socket1 expected 1, got %d", step.CoresAllocatedSocketOne)
	}
}

func TestCoreAllocationAggregationGroupsByStepNumberNotTimestamp(t *testing.T) {
	hc := &host.HostConfig{Topology: host.CPUTopology{Sockets: 2, CoreMap: map[int]host.CoreInfo{
		0: {LogicalID: 0, PhysicalID: 0, CoreID: 0, Siblings: []int{0}},
		1: {LogicalID: 1, PhysicalID: 0, CoreID: 1, Siblings: []int{1}},
		4: {LogicalID: 4, PhysicalID: 1, CoreID: 0, Siblings: []int{4}},
	}}}
	h := NewDefaultDataHandler(hc)
	dfs := dataframe.NewDataFrames()

	// Same stepNumber, different timestamps (skew across containers).
	t0 := time.Unix(300, 0)
	t1 := time.Unix(301, 0)

	c0 := dfs.AddContainer(0)
	c0.AddStep(7, &dataframe.SamplingStep{Timestamp: t0, Docker: &dataframe.DockerMetrics{AssignedCoresCSV: strPtr("0,1")}})
	c1 := dfs.AddContainer(1)
	c1.AddStep(7, &dataframe.SamplingStep{Timestamp: t1, Docker: &dataframe.DockerMetrics{AssignedCoresCSV: strPtr("4")}})

	benchCfg := &config.BenchmarkConfig{Containers: map[string]config.ContainerConfig{
		"c0": {Index: 0},
		"c1": {Index: 1},
	}}

	out, err := h.ProcessDataFrames(1, benchCfg, dfs, t0, t1)
	if err != nil {
		t.Fatalf("ProcessDataFrames: %v", err)
	}
	if len(out.CoreAllocationSteps) != 1 {
		t.Fatalf("expected 1 aggregated step, got %d", len(out.CoreAllocationSteps))
	}
	step := out.CoreAllocationSteps[0]
	if step.StepNumber != 7 {
		t.Fatalf("expected step_number=7, got %d", step.StepNumber)
	}
	if step.CoresAllocatedSocketZero != 2 || step.CoresAllocatedSocketOne != 1 {
		t.Fatalf("unexpected counts: s0=%d s1=%d", step.CoresAllocatedSocketZero, step.CoresAllocatedSocketOne)
	}
}

func strPtr(s string) *string { return &s }
