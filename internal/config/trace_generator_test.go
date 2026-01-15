package config

import "testing"

func TestExpandGeneratedTrace_Deterministic(t *testing.T) {
	ipc := 3.2
	ipce := 0.8
	cfg := &BenchmarkConfig{
		Benchmark: BenchmarkInfo{
			Name: "generated",
			MaxT: 100,
			Data: DataConfig{DB: DatabaseConfig{Host: "h", Name: "n", User: "u", Password: "p", Org: "o"}},
		},
		Arrival: &ArrivalConfig{
			Seed:          1337,
			Mean:          10,
			Sigma:         2,
			Length:        &NormalDistConfig{Mean: 20, Sigma: 5, Min: 1},
			Split:         WeightedSet{Random: true},
			Sensitivities: WeightedSet{Random: true},
		},
		Data: &CollectorConfig{Frequency: 2000, Perf: map[string]bool{"instructions": true}, Docker: map[string]bool{"cpu_usage_total": true}, RDT: true},
		Workloads: map[string]WorkloadConfig{
			"w1": {Image: "img1", Command: "cmd1 --timeout 0", NumCores: 1, Kind: "single-thread", Sensitivity: "low", Critical: true, IPC: &ipc, IPCEfficiency: &ipce},
			"w2": {Image: "img2", Command: "cmd2 --timeout 0", NumCores: 2, Kind: "multi-thread", Sensitivity: "high"},
		},
	}

	c1, o1, err := expandGeneratedTrace(cfg)
	if err != nil {
		t.Fatalf("expand1: %v", err)
	}
	c2, o2, err := expandGeneratedTrace(cfg)
	if err != nil {
		t.Fatalf("expand2: %v", err)
	}

	if len(o1) == 0 {
		t.Fatalf("expected jobs")
	}
	if len(o1) != len(o2) {
		t.Fatalf("expected same job count, got %d vs %d", len(o1), len(o2))
	}
	for i := range o1 {
		if o1[i] != o2[i] {
			t.Fatalf("order differs at %d: %q vs %q", i, o1[i], o2[i])
		}
		j1 := c1[o1[i]]
		j2 := c2[o2[i]]
		if j1.Image != j2.Image || j1.Command != j2.Command || j1.NumCores != j2.NumCores {
			t.Fatalf("job %q differs", o1[i])
		}

		// Scheduling hint fields should also be deterministic and preserved.
		if j1.Critical != j2.Critical {
			t.Fatalf("job %q critical differs", o1[i])
		}
		if (j1.IPC == nil) != (j2.IPC == nil) || (j1.IPCEfficiency == nil) != (j2.IPCEfficiency == nil) {
			t.Fatalf("job %q ipc/ipce nilness differs", o1[i])
		}
		if j1.IPC != nil && j2.IPC != nil && *j1.IPC != *j2.IPC {
			t.Fatalf("job %q ipc differs", o1[i])
		}
		if j1.IPCEfficiency != nil && j2.IPCEfficiency != nil && *j1.IPCEfficiency != *j2.IPCEfficiency {
			t.Fatalf("job %q ipce differs", o1[i])
		}

		st := j1.GetStartSeconds()
		sp := j1.GetStopSeconds(cfg.Benchmark.MaxT)
		if st < 0 || sp <= st || sp > cfg.Benchmark.MaxT {
			t.Fatalf("invalid window for %q: start=%d stop=%d", o1[i], st, sp)
		}
		if j1.Data.Frequency != cfg.Data.Frequency {
			t.Fatalf("expected data defaults applied")
		}
	}
}

func TestGeneratedTraceArrivalStartTimesStrictlyIncreasing(t *testing.T) {
	cfg := &BenchmarkConfig{
		Benchmark: BenchmarkInfo{
			Name: "trace-test",
			MaxT: 500,
			Data: DataConfig{DB: DatabaseConfig{Host: "h", Name: "n", User: "u", Password: "p", Org: "o"}},
		},
		Arrival: &ArrivalConfig{
			Seed:          1,
			Mean:          30,
			Sigma:         10,
			Length:        &NormalDistConfig{Mean: 50, Sigma: 0, Min: 10},
			Split:         WeightedSet{Random: true},
			Sensitivities: WeightedSet{Random: true},
		},
		Data: &CollectorConfig{Frequency: 2000, Perf: map[string]bool{"instructions": true}, Docker: map[string]bool{"cpu_usage_total": true}, RDT: true},
		Workloads: map[string]WorkloadConfig{
			"cache-l1": {Image: "alpine:latest", Command: "sh -lc 'sleep 999999'", NumCores: 1},
		},
	}

	containers, order, err := expandGeneratedTrace(cfg)
	if err != nil {
		t.Fatalf("expandGeneratedTrace: %v", err)
	}
	if len(order) < 2 {
		t.Fatalf("expected at least 2 containers, got %d", len(order))
	}

	first := containers[order[0]]
	prev := first.GetStartSeconds()
	for i := 1; i < len(order); i++ {
		curC := containers[order[i]]
		cur := curC.GetStartSeconds()
		if cur <= prev {
			t.Fatalf("start_t not strictly increasing at i=%d: prev=%d cur=%d", i, prev, cur)
		}
		prev = cur
	}
}

func TestExpandGeneratedTrace_PrioritySplitSelectsTemplates(t *testing.T) {
	baseCfg := &BenchmarkConfig{
		Benchmark: BenchmarkInfo{
			Name: "trace-priority-split",
			MaxT: 120,
			Data: DataConfig{DB: DatabaseConfig{Host: "h", Name: "n", User: "u", Password: "p", Org: "o"}},
		},
		Arrival: &ArrivalConfig{
			Seed:          42,
			Mean:          10,
			Sigma:         0,
			Length:        &NormalDistConfig{Mean: 30, Sigma: 0, Min: 10},
			Split:         WeightedSet{Random: true},
			Sensitivities: WeightedSet{Random: true},
		},
		Data: &CollectorConfig{Frequency: 2000, Perf: map[string]bool{"instructions": true}, Docker: map[string]bool{"cpu_usage_total": true}, RDT: true},
		Workloads: map[string]WorkloadConfig{
			"prio":    {Image: "img", Command: "cmd-prio --timeout 0", NumCores: 1, Critical: false, Priority: true},
			"nonprio": {Image: "img", Command: "cmd-nonprio --timeout 0", NumCores: 1, Critical: false, Priority: false},
		},
	}

	// Force all generated jobs to be priority.
	cfgP := *baseCfg
	arrP := *baseCfg.Arrival
	cfgP.Arrival = &arrP
	cfgP.Arrival.PrioritySplit = WeightedSet{Random: false, Weights: map[string]float64{"priority": 1, "nonpriority": 0}}
	containers, order, err := expandGeneratedTrace(&cfgP)
	if err != nil {
		t.Fatalf("expandGeneratedTrace priority: %v", err)
	}
	if len(order) == 0 {
		t.Fatalf("expected jobs")
	}
	for _, k := range order {
		c := containers[k]
		if !c.Priority {
			t.Fatalf("expected generated container %q to be priority", k)
		}
		if c.Command != "cmd-prio --timeout 0" {
			t.Fatalf("expected priority template for %q, got command=%q", k, c.Command)
		}
	}

	// Force all generated jobs to be non-priority.
	cfgN := *baseCfg
	arrN := *baseCfg.Arrival
	cfgN.Arrival = &arrN
	cfgN.Arrival.PrioritySplit = WeightedSet{Random: false, Weights: map[string]float64{"priority": 0, "nonpriority": 1}}
	containers2, order2, err := expandGeneratedTrace(&cfgN)
	if err != nil {
		t.Fatalf("expandGeneratedTrace nonpriority: %v", err)
	}
	if len(order2) == 0 {
		t.Fatalf("expected jobs")
	}
	for _, k := range order2 {
		c := containers2[k]
		if c.Priority {
			t.Fatalf("expected generated container %q to be nonpriority", k)
		}
		if c.Command != "cmd-nonprio --timeout 0" {
			t.Fatalf("expected nonpriority template for %q, got command=%q", k, c.Command)
		}
	}
}

func TestExpandGeneratedTrace_PrioritySplitRelaxesKindBeforePriority(t *testing.T) {
	cfg := &BenchmarkConfig{
		Benchmark: BenchmarkInfo{
			Name: "trace-priority-split-relax",
			MaxT: 60,
			Data: DataConfig{DB: DatabaseConfig{Host: "h", Name: "n", User: "u", Password: "p", Org: "o"}},
		},
		Arrival: &ArrivalConfig{
			Seed:  1,
			Mean:  10,
			Sigma: 0,
			Length: &NormalDistConfig{Mean: 20, Sigma: 0, Min: 10},
			// Always pick single-thread kind.
			Split: WeightedSet{Random: false, Weights: map[string]float64{"single-thread": 1}},
			Sensitivities: WeightedSet{Random: true},
			// Always request nonpriority.
			PrioritySplit: WeightedSet{Random: false, Weights: map[string]float64{"priority": 0, "nonpriority": 1}},
		},
		Data: &CollectorConfig{Frequency: 2000, Perf: map[string]bool{"instructions": true}, Docker: map[string]bool{"cpu_usage_total": true}, RDT: true},
		Workloads: map[string]WorkloadConfig{
			// Priority matches the chosen kind.
			"prio-st": {Image: "img", Command: "cmd-prio --timeout 0", NumCores: 1, Priority: true, Kind: "single-thread"},
			// Nonpriority is multi-thread only.
			"nonprio-mt": {Image: "img", Command: "cmd-nonprio --timeout 0", NumCores: 1, Priority: false, Kind: "multi-thread"},
		},
	}

	containers, order, err := expandGeneratedTrace(cfg)
	if err != nil {
		t.Fatalf("expandGeneratedTrace: %v", err)
	}
	if len(order) == 0 {
		t.Fatalf("expected jobs")
	}
	for _, k := range order {
		c := containers[k]
		if c.Priority {
			t.Fatalf("expected %q to be nonpriority (relax kind before priority)", k)
		}
		if c.Command != "cmd-nonprio --timeout 0" {
			t.Fatalf("expected nonpriority template for %q, got command=%q", k, c.Command)
		}
	}
}
