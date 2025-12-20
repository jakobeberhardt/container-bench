package main

import (
	"fmt"
	"time"

	"container-bench/internal/scheduler"
)

type containerCollectorFrequencyController struct {
	bench *ContainerBench
}

func (c containerCollectorFrequencyController) OverrideContainerFrequency(containerIndex int, freq time.Duration) (func(), error) {
	if c.bench == nil {
		return nil, fmt.Errorf("bench is nil")
	}
	collector := c.bench.collectors[containerIndex]
	if collector == nil {
		return nil, fmt.Errorf("collector for container %d not running", containerIndex)
	}
	return collector.OverrideFrequency(freq)
}

var _ scheduler.CollectorFrequencyController = containerCollectorFrequencyController{}
