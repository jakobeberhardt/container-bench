package database

import (
	"compress/gzip"
	"container-bench/internal/config"
	"container-bench/internal/datahandeling"
	"container-bench/internal/probe"
	"container-bench/internal/probe/resources"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type SpoolArtifact struct {
	Version int `json:"version"`

	CreatedAt time.Time `json:"created_at"`

	BenchmarkID   int    `json:"benchmark_id"`
	BenchmarkName string `json:"benchmark_name"`
	TraceChecksum string `json:"trace_checksum"`

	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`

	ConfigContent string `json:"config_content"`

	Metrics                 *datahandeling.BenchmarkMetrics    `json:"metrics"`
	Metadata                *BenchmarkMetadata                 `json:"metadata"`
	ContainerTimingMetadata []*ContainerTimingMetadata         `json:"container_timing_metadata"`
	ProbeResults            []*probe.ProbeResult               `json:"probe_results,omitempty"`
	AllocationProbeResults  []*resources.AllocationProbeResult `json:"allocation_probe_results,omitempty"`
}

func DefaultSpoolDir() string {
	if v := strings.TrimSpace(os.Getenv("CONTAINER_BENCH_SPOOL_DIR")); v != "" {
		return v
	}
	return "spool"
}

// WriteSpoolArtifact writes a gzip-compressed JSON artifact to disk atomically.
// It returns the final file path.
func WriteSpoolArtifact(dir string, artifact *SpoolArtifact) (string, error) {
	if artifact == nil {
		return "", fmt.Errorf("spool artifact is nil")
	}
	if dir == "" {
		dir = DefaultSpoolDir()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}

	checksum := artifact.TraceChecksum
	if checksum == "" {
		checksum = "nocsum"
	}
	name := fmt.Sprintf(
		"benchmark_%d_%s_%s.json.gz",
		artifact.BenchmarkID,
		artifact.CreatedAt.UTC().Format("20060102T150405Z"),
		checksum,
	)
	finalPath := filepath.Join(dir, name)

	tmp, err := os.CreateTemp(dir, name+".tmp.*")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()

	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpPath)
		}
	}()

	gz := gzip.NewWriter(tmp)
	enc := json.NewEncoder(gz)
	enc.SetIndent("", "  ")
	if err := enc.Encode(artifact); err != nil {
		_ = gz.Close()
		return "", err
	}
	if err := gz.Close(); err != nil {
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return "", err
	}
	ok = true
	return finalPath, nil
}

// BuildSpoolArtifact constructs a spool artifact from the in-memory benchmark results.
func BuildSpoolArtifact(
	benchmarkID int,
	benchmarkCfg *config.BenchmarkConfig,
	configContent string,
	metrics *datahandeling.BenchmarkMetrics,
	metadata *BenchmarkMetadata,
	containerMeta []*ContainerTimingMetadata,
	probeResults []*probe.ProbeResult,
	allocProbeResults []*resources.AllocationProbeResult,
	startTime, endTime time.Time,
) *SpoolArtifact {
	name := ""
	checksum := ""
	if benchmarkCfg != nil {
		name = benchmarkCfg.Benchmark.Name
		if cs, err := config.TraceChecksum(benchmarkCfg); err == nil {
			checksum = cs
		}
	}
	if metadata != nil {
		if checksum == "" {
			checksum = metadata.TraceChecksum
		}
		if name == "" {
			name = metadata.BenchmarkName
		}
	}

	return &SpoolArtifact{
		Version:                 1,
		CreatedAt:               time.Now(),
		BenchmarkID:             benchmarkID,
		BenchmarkName:           name,
		TraceChecksum:           checksum,
		StartTime:               startTime,
		EndTime:                 endTime,
		ConfigContent:           configContent,
		Metrics:                 metrics,
		Metadata:                metadata,
		ContainerTimingMetadata: containerMeta,
		ProbeResults:            probeResults,
		AllocationProbeResults:  allocProbeResults,
	}
}
