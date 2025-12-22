package config

import "testing"

func TestTraceChecksum_DeterministicAcrossMapOrder(t *testing.T) {
	maxT := 60
	cfg1 := &BenchmarkConfig{Benchmark: BenchmarkInfo{Name: "t", MaxT: maxT}}
	cfg1.Containers = map[string]ContainerConfig{
		"b": {Index: 1, Image: "img2", Command: "cmd2", NumCores: 2},
		"a": {Index: 0, Image: "img1", Command: "cmd1", NumCores: 1},
	}
	st0 := 0
	sp0 := 10
	st1 := 5
	sp1 := 20
	c0 := cfg1.Containers["a"]
	c0.StartT = &st0
	c0.StopT = &sp0
	cfg1.Containers["a"] = c0
	c1 := cfg1.Containers["b"]
	c1.StartT = &st1
	c1.StopT = &sp1
	cfg1.Containers["b"] = c1

	cfg2 := &BenchmarkConfig{Benchmark: BenchmarkInfo{Name: "t", MaxT: maxT}}
	// Same containers but inserted in opposite order.
	cfg2.Containers = map[string]ContainerConfig{
		"a": cfg1.Containers["a"],
		"b": cfg1.Containers["b"],
	}

	s1, err := TraceChecksum(cfg1)
	if err != nil {
		t.Fatalf("TraceChecksum(cfg1): %v", err)
	}
	s2, err := TraceChecksum(cfg2)
	if err != nil {
		t.Fatalf("TraceChecksum(cfg2): %v", err)
	}
	if s1 != s2 {
		t.Fatalf("expected same checksum, got %q vs %q", s1, s2)
	}
	if len(s1) != 6 {
		t.Fatalf("expected 6-char checksum, got %q (len=%d)", s1, len(s1))
	}
}

func TestTraceChecksum_ChangesWhenTraceChanges(t *testing.T) {
	maxT := 60
	cfg := &BenchmarkConfig{Benchmark: BenchmarkInfo{Name: "t", MaxT: maxT}}
	cfg.Containers = map[string]ContainerConfig{
		"a": {Index: 0, Image: "img1", Command: "cmd1", NumCores: 1},
	}
	s1, err := TraceChecksum(cfg)
	if err != nil {
		t.Fatalf("TraceChecksum: %v", err)
	}

	c := cfg.Containers["a"]
	c.Command = "cmd1 --changed"
	cfg.Containers["a"] = c

	s2, err := TraceChecksum(cfg)
	if err != nil {
		t.Fatalf("TraceChecksum after change: %v", err)
	}
	if s1 == s2 {
		t.Fatalf("expected checksum to change, got %q", s1)
	}
}
