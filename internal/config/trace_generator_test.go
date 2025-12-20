package config

import "testing"

func TestExpandGeneratedTrace_Deterministic(t *testing.T) {
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
			"w1": {Image: "img1", Command: "cmd1 --timeout 0", NumCores: 1, Kind: "single-thread", Sensitivity: "low"},
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
