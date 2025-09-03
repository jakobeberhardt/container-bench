package scheduler

import (
	"context"
	"fmt"

	"github.com/jakobeberhardt/container-bench/internal/config"
)

// Scheduler defines the interface for container scheduling strategies
type Scheduler interface {
	Initialize(ctx context.Context) error
	ScheduleContainer(ctx context.Context, containerID string, config config.ContainerConfig) error
	Monitor(ctx context.Context, containerIDs map[string]string) error
	Finalize(ctx context.Context) error
	Name() string
}

// NewScheduler creates a new scheduler based on the implementation name
func NewScheduler(implementation string, rdtEnabled bool) (Scheduler, error) {
	switch implementation {
	case "default", "":
		return NewDefaultScheduler(rdtEnabled), nil
	case "rdt":
		if !rdtEnabled {
			return nil, fmt.Errorf("RDT scheduler requested but RDT is not enabled")
		}
		return NewRDTScheduler(), nil
	case "adaptive":
		return NewAdaptiveScheduler(rdtEnabled), nil
	default:
		return nil, fmt.Errorf("unknown scheduler implementation: %s", implementation)
	}
}
