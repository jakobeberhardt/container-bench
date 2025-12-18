package scheduler

import (
	"container-bench/internal/dataframe"
	"testing"
	"time"
)

func TestTakeContiguousFromMask_BestFit(t *testing.T) {
	// mask: bits 0-1 (len2) and bits 4-7 (len4)
	mask := uint64(0)
	mask |= (1 << 0) | (1 << 1)
	mask |= (1 << 4) | (1 << 5) | (1 << 6) | (1 << 7)

	taken, remaining, err := takeContiguousFromMask(mask, 2, 12)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if taken != ((1 << 0) | (1 << 1)) {
		t.Fatalf("expected taken=0b11, got 0x%x", taken)
	}
	if remaining != (mask &^ taken) {
		t.Fatalf("remaining mismatch")
	}
}

func TestBuildMemSteps(t *testing.T) {
	steps := buildMemSteps(30, 10)
	if len(steps) != 3 || steps[0] != 10 || steps[1] != 20 || steps[2] != 30 {
		t.Fatalf("unexpected steps: %#v", steps)
	}
}

func TestAverageIPCEfficiency_UsesIPCEfficancy(t *testing.T) {
	dfs := dataframe.NewDataFrames()
	cdf := dfs.AddContainer(0)

	v1 := 0.5
	v2 := 0.7
	cdf.AddStep(0, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v1}})
	cdf.AddStep(1, &dataframe.SamplingStep{Timestamp: time.Now(), Perf: &dataframe.PerfMetrics{IPCEfficancy: &v2}})

	eff := averageIPCEfficiency(dfs, 0, -1, 1)
	if eff < 0.59 || eff > 0.61 {
		t.Fatalf("expected ~0.6, got %v", eff)
	}
}
