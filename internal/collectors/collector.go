package collectors

import (
	"context"
	"time"

	"container-bench/internal/dataframe"
)

type Collector interface {
	Start(ctx context.Context) error
	Stop() error
}

type ContainerCollector struct {
	containerIndex int
	ContainerID    string
	containerPID   int
	cgroupPath     string
	config         CollectorConfig
	dataFrame      *dataframe.ContainerDataFrame
	
	perfCollector   *PerfCollector
	dockerCollector *DockerCollector
	rdtCollector    *RDTCollector
	
	stopChan chan struct{}
}

type CollectorConfig struct {
	Frequency time.Duration
	EnablePerf   bool
	EnableDocker bool
	EnableRDT    bool
}

func NewContainerCollector(containerIndex int, containerID string, config CollectorConfig, df *dataframe.ContainerDataFrame) *ContainerCollector {
	return &ContainerCollector{
		containerIndex: containerIndex,
		ContainerID:    containerID,
		config:         config,
		dataFrame:      df,
		stopChan:       make(chan struct{}),
	}
}

func (cc *ContainerCollector) SetContainerInfo(pid int, cgroupPath string) {
	cc.containerPID = pid
	cc.cgroupPath = cgroupPath
}

func (cc *ContainerCollector) Start(ctx context.Context) error {
	// Initialize collectors based on config
	var err error
	
	if cc.config.EnablePerf {
		cc.perfCollector, err = NewPerfCollector(cc.containerPID, cc.cgroupPath)
		if err != nil {
			return err
		}
	}
	
	if cc.config.EnableDocker {
		cc.dockerCollector, err = NewDockerCollector(cc.ContainerID)
		if err != nil {
			return err
		}
	}
	
	if cc.config.EnableRDT {
		cc.rdtCollector, err = NewRDTCollector(cc.containerPID)
		if err != nil {
			return err
		}
	}
	
	// Start collection goroutine
	go cc.collect(ctx)
	
	return nil
}

func (cc *ContainerCollector) collect(ctx context.Context) {
	ticker := time.NewTicker(cc.config.Frequency)
	defer ticker.Stop()
	
	stepCounter := 0
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-cc.stopChan:
			return
		case <-ticker.C:
			step := &dataframe.SamplingStep{
				Timestamp: time.Now(),
			}
			
			// Collect metrics from enabled collectors
			if cc.perfCollector != nil {
				step.Perf = cc.perfCollector.Collect()
			}
			
			if cc.dockerCollector != nil {
				step.Docker = cc.dockerCollector.Collect()
			}
			
			if cc.rdtCollector != nil {
				step.RDT = cc.rdtCollector.Collect()
			}
			
			// Store in data frame
			cc.dataFrame.AddStep(stepCounter, step)
			stepCounter++
		}
	}
}

func (cc *ContainerCollector) Stop() error {
	close(cc.stopChan)
	
	if cc.perfCollector != nil {
		cc.perfCollector.Close()
	}
	
	if cc.dockerCollector != nil {
		cc.dockerCollector.Close()
	}
	
	if cc.rdtCollector != nil {
		cc.rdtCollector.Close()
	}
	
	return nil
}
