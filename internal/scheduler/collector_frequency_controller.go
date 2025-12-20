package scheduler

import "time"

// CollectorFrequencyController allows schedulers/probers to temporarily override
// a container's data-collection frequency at runtime (e.g., during allocation probing).
//
// Implementations should return a restore function that reverts the previous setting.
// Callers must call restore exactly once when done.
type CollectorFrequencyController interface {
	OverrideContainerFrequency(containerIndex int, freq time.Duration) (restore func(), err error)
}

// CollectorFrequencyControllerSetter is an optional interface that schedulers can implement
// to receive a frequency controller from the orchestration layer.
type CollectorFrequencyControllerSetter interface {
	SetCollectorFrequencyController(controller CollectorFrequencyController)
}
