package allocation

import (
	"container-bench/internal/plot/database"
	"math"
	"testing"
)

func TestNormalizePercentMetric_FractionToPercent(t *testing.T) {
	got := normalizePercentMetric(0.9485)
	want := 94.85
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestNormalizePercentMetric_AlreadyPercent(t *testing.T) {
	got := normalizePercentMetric(94.85)
	want := 94.85
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestAllocationPlotGenerator_getMetricValue_IPCEfficiency_NoDoubleScale(t *testing.T) {
	g := &AllocationPlotGenerator{}

	alloc := database.AllocationData{IPCEfficiency: 94.85}
	v := g.getMetricValue(alloc, "ipc_efficiency")
	if v == nil {
		t.Fatalf("expected value")
	}
	if math.Abs(*v-94.85) > 1e-9 {
		t.Fatalf("expected 94.85, got %v", *v)
	}
}

func TestAllocationPlotGenerator_getMetricValue_IPCEfficiency_FractionScales(t *testing.T) {
	g := &AllocationPlotGenerator{}

	alloc := database.AllocationData{IPCEfficiency: 0.9485}
	v := g.getMetricValue(alloc, "ipc_efficiency")
	if v == nil {
		t.Fatalf("expected value")
	}
	if math.Abs(*v-94.85) > 1e-6 {
		t.Fatalf("expected 94.85, got %v", *v)
	}
}
