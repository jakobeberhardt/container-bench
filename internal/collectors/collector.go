package collectors

import (
	"context"
	"time"

	"container-bench/internal/dataframe"
	"container-bench/internal/logging"

	"github.com/sirupsen/logrus"
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
	cpuCores       []int
	config         CollectorConfig
	dataFrame      *dataframe.ContainerDataFrame

	perfCollector   *PerfCollector
	dockerCollector *DockerCollector
	rdtCollector    *RDTCollector

	stopChan chan struct{}
	stopped  bool
}

type CollectorConfig struct {
	Frequency    time.Duration
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

func (cc *ContainerCollector) SetContainerInfo(pid int, cgroupPath string, cpuCores []int) {
	cc.containerPID = pid
	cc.cgroupPath = cgroupPath
	cc.cpuCores = cpuCores
}

func (cc *ContainerCollector) Start(ctx context.Context) error {
	var err error

	if cc.config.EnablePerf {
		cc.perfCollector, err = NewPerfCollector(cc.containerPID, cc.cgroupPath, cc.cpuCores)
		if err != nil {
			// Log warning but don't fail the entire collector
			logger := logging.GetLogger()
			logger.WithFields(logrus.Fields{
				"container_index": cc.containerIndex,
			}).WithError(err).Warn("Failed to enable perf monitoring, continuing without perf metrics")
			cc.perfCollector = nil
		}
	}

	if cc.config.EnableDocker {
		cc.dockerCollector, err = NewDockerCollector(cc.ContainerID, cc.containerIndex)
		if err != nil {
			return err
		}
	}

	if cc.config.EnableRDT {
		cc.rdtCollector, err = NewRDTCollector(cc.containerPID)
		if err != nil {
			// Log warning but don't fail the entire collector
			logger := logging.GetLogger()
			logger.WithFields(logrus.Fields{
				"container_index": cc.containerIndex,
			}).WithError(err).Warn("Failed to enable RDT monitoring, continuing without RDT metrics")
			cc.rdtCollector = nil
		}
	}

	// Start the collector process
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

			// Collect metrics from all enabled collectors
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
	if !cc.stopped {
		close(cc.stopChan)
		cc.stopped = true
	}

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
