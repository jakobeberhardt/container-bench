package main

import "testing"

func TestNormalizeSchedulerImplementation_AllowsHyphens(t *testing.T) {
	got, err := normalizeSchedulerImplementation("interference-aware")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "interference_aware" {
		t.Fatalf("got %q, want %q", got, "interference_aware")
	}
}

func TestNormalizeSchedulerImplementation_AllowsUnderscores(t *testing.T) {
	got, err := normalizeSchedulerImplementation("least_loaded")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "least_loaded" {
		t.Fatalf("got %q, want %q", got, "least_loaded")
	}
}

func TestNormalizeSchedulerImplementation_RejectsUnknown(t *testing.T) {
	_, err := normalizeSchedulerImplementation("nope")
	if err == nil {
		t.Fatalf("expected error")
	}
}
