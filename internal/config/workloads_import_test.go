package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWorkloadsInputImport_Directory(t *testing.T) {
	tmp := t.TempDir()
	jobsDir := filepath.Join(tmp, "jobs")
	if err := os.MkdirAll(filepath.Join(jobsDir, "stress-ng"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Two workload templates in separate files.
	if err := os.WriteFile(filepath.Join(jobsDir, "stress-ng", "tsearch.yml"), []byte(`
tsearch-8:
  image: img
  command: "stress-ng --tsearch 1 --tsearch-size 8 --timeout 0"
  num_cores: 1
  kind: single-thread
  sensitivity: low

tsearch-16:
  image: img
  command: "stress-ng --tsearch 1 --tsearch-size 16 --timeout 0"
  num_cores: 1
  kind: single-thread
  sensitivity: low
`), 0o644); err != nil {
		t.Fatalf("write tsearch.yml: %v", err)
	}

	if err := os.WriteFile(filepath.Join(jobsDir, "compression.yml"), []byte(`
7z-large-1:
  image: img2
  command: "sh -lc 'echo hi'"
  num_cores: 1
  kind: single-thread
  sensitivity: low
`), 0o644); err != nil {
		t.Fatalf("write compression.yml: %v", err)
	}

	tracePath := filepath.Join(tmp, "trace.yml")
	trace := `
benchmark:
  name: trace-test
  max_t: 60
  data:
    db:
      host: h
      name: n
      user: u
      password: p
      org: o

arrival:
  seed: 1
  mean: 10
  sigma: 0
  length:
    mean: 20
    sigma: 0
    min: 5

data:
  frequency: 2000
  perf:
    instructions: true
  docker:
    cpu_usage_total: true
  rdt: true

workloads:
  input: ./jobs
`
	if err := os.WriteFile(tracePath, []byte(trace), 0o644); err != nil {
		t.Fatalf("write trace.yml: %v", err)
	}

	cfg, _, err := LoadConfigWithContent(tracePath)
	if err != nil {
		t.Fatalf("LoadConfigWithContent: %v", err)
	}

	if cfg.Workloads == nil {
		t.Fatalf("expected workloads map")
	}
	if _, ok := cfg.Workloads["tsearch-8"]; !ok {
		t.Fatalf("expected imported workload tsearch-8")
	}
	if _, ok := cfg.Workloads["tsearch-16"]; !ok {
		t.Fatalf("expected imported workload tsearch-16")
	}
	if _, ok := cfg.Workloads["7z-large-1"]; !ok {
		t.Fatalf("expected imported workload 7z-large-1")
	}
	if _, ok := cfg.Workloads["input"]; ok {
		t.Fatalf("did not expect workloads to contain key 'input'")
	}

	if len(cfg.Containers) == 0 {
		t.Fatalf("expected generated containers")
	}
}
