package datahandeling

import (
	"testing"
	"time"

	"container-bench/internal/config"
)

func TestComputePriorityQoSMetFractions_IPCE_TimeWeighted(t *testing.T) {
	ipce := 0.80
	cfg := &config.BenchmarkConfig{}
	cfg.Containers = map[string]config.ContainerConfig{
		"c0": {Index: 0, Priority: true, IPCEfficiency: &ipce},
	}

	base := time.Unix(1000, 0)
	vLow := 0.70
	vHigh := 0.90

	m := &BenchmarkMetrics{ContainerMetrics: []ContainerMetrics{{
		ContainerIndex: 0,
		Steps: []MetricStep{
			{StepNumber: 0, Timestamp: base.Add(0 * time.Second), PerfIPCEfficancy: &vLow},
			{StepNumber: 1, Timestamp: base.Add(10 * time.Second), PerfIPCEfficancy: &vHigh},
			{StepNumber: 2, Timestamp: base.Add(20 * time.Second), PerfIPCEfficancy: &vHigh},
		},
	}}}

	got := ComputePriorityQoSMetFractions(cfg, m)
	res, ok := got[0]
	if !ok {
		t.Fatalf("expected result for container 0")
	}
	// intervals: [0..10] low (fail), [10..20] high (meet) => fraction 0.5
	if res.Metric != QoSMetricIPCE {
		t.Fatalf("expected metric ipce, got %q", res.Metric)
	}
	if res.FractionMet < 0.49 || res.FractionMet > 0.51 {
		t.Fatalf("expected ~0.5, got %v", res.FractionMet)
	}
	if res.Guarantee != ipce {
		t.Fatalf("expected guarantee %v, got %v", ipce, res.Guarantee)
	}
}
