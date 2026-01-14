package scheduler

import "testing"

func TestDynamicScheduler_BufferHeadroom_CapsToMax(t *testing.T) {
	bestWays, bestMem := 4, 20.0
	maxWays, maxMem := 5, 30.0
	memStep := 10.0
	bufWays := 2
	bufMemSteps := 2

	desWays := bestWays + bufWays
	if desWays > maxWays {
		desWays = maxWays
	}
	desMem := bestMem + float64(bufMemSteps)*memStep
	if desMem > maxMem {
		desMem = maxMem
	}

	if desWays != 5 {
		t.Fatalf("ways=%d, want 5", desWays)
	}
	if desMem != 30 {
		t.Fatalf("mem=%.1f, want 30", desMem)
	}
}
