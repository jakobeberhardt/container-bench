package collectors

import (
	"context"
	"fmt"
	"sync"
	"time"

	"container-bench/internal/config"
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

	frequencyMu      sync.Mutex
	currentFrequency time.Duration
	freqUpdateChan   chan time.Duration

	perfCollector   *PerfCollector
	dockerCollector *DockerCollector
	rdtCollector    *RDTCollector

	stopChan chan struct{}
	stopped  bool
}

type CollectorConfig struct {
	Frequency       time.Duration
	PIDSyncInterval time.Duration
	PerfConfig      *config.PerfConfig
	DockerConfig    *config.DockerMetricsConfig
	RDTConfig       *config.RDTConfig
}

func NewContainerCollector(containerIndex int, containerID string, config CollectorConfig, df *dataframe.ContainerDataFrame) *ContainerCollector {
	return &ContainerCollector{
		containerIndex:   containerIndex,
		ContainerID:      containerID,
		config:           config,
		dataFrame:        df,
		currentFrequency: config.Frequency,
		freqUpdateChan:   make(chan time.Duration, 1),
		stopChan:         make(chan struct{}),
	}
}

func (cc *ContainerCollector) setFrequencyLocked(freq time.Duration) {
	cc.currentFrequency = freq
	// Ensure the latest value wins even if a previous update is still buffered.
	select {
	case cc.freqUpdateChan <- freq:
	default:
		select {
		case <-cc.freqUpdateChan:
		default:
		}
		cc.freqUpdateChan <- freq
	}
}

// OverrideFrequency temporarily overrides the collector sampling frequency.
// It returns a restore function that resets the previous frequency.
func (cc *ContainerCollector) OverrideFrequency(freq time.Duration) (restore func(), err error) {
	if freq <= 0 {
		return nil, fmt.Errorf("frequency must be > 0")
	}

	cc.frequencyMu.Lock()
	prev := cc.currentFrequency
	cc.setFrequencyLocked(freq)
	cc.frequencyMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			cc.frequencyMu.Lock()
			cc.setFrequencyLocked(prev)
			cc.frequencyMu.Unlock()
		})
	}, nil
}

func (cc *ContainerCollector) SetContainerInfo(pid int, cgroupPath string, cpuCores []int) {
	cc.containerPID = pid
	cc.cgroupPath = cgroupPath
	cc.cpuCores = cpuCores
}

func (cc *ContainerCollector) Start(ctx context.Context) error {
	var err error

	if cc.config.PerfConfig != nil && cc.config.PerfConfig.Enabled {
		cc.perfCollector, err = NewPerfCollector(cc.containerPID, cc.cgroupPath, cc.cpuCores, cc.config.PerfConfig)
		if err != nil {
			// Log warning but don't fail the entire collector
			logger := logging.GetLogger()
			logger.WithFields(logrus.Fields{
				"container_index": cc.containerIndex,
			}).WithError(err).Warn("Failed to enable perf monitoring, continuing without perf metrics")
			cc.perfCollector = nil
		}
	}

	if cc.config.DockerConfig != nil && cc.config.DockerConfig.Enabled {
		cc.dockerCollector, err = NewDockerCollector(cc.ContainerID, cc.containerIndex, cc.config.DockerConfig)
		if err != nil {
			return err
		}
	}

	if cc.config.RDTConfig != nil && cc.config.RDTConfig.Enabled {
		cc.rdtCollector, err = NewRDTCollector(cc.containerPID, cc.cgroupPath, cc.config.RDTConfig, cc.config.PIDSyncInterval)
		if err != nil {
			// Log warning but don't fail the entire collector
			logger := logging.GetLogger()
			logger.WithFields(logrus.Fields{
				"container_index": cc.containerIndex,
			}).WithError(err).Warn("Failed to enable RDT monitoring, continuing without RDT metrics")
			cc.rdtCollector = nil
		} else {
			// Start the PID sync ticker
			cc.rdtCollector.StartPIDSyncTicker()
		}
	}

	// Start the collector process
	go cc.collect(ctx)

	return nil
}

func (cc *ContainerCollector) collect(ctx context.Context) {
	cc.frequencyMu.Lock()
	freq := cc.currentFrequency
	cc.frequencyMu.Unlock()
	ticker := time.NewTicker(freq)
	defer ticker.Stop()

	stepCounter := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-cc.stopChan:
			return
		case newFreq := <-cc.freqUpdateChan:
			if newFreq > 0 {
				ticker.Reset(newFreq)
			}
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

// SyncRDTPIDs syncs PIDs from the container's cgroup to the RDT monitoring group
// This should be called after docker exec commands to ensure new processes are monitored
func (cc *ContainerCollector) SyncRDTPIDs() error {
	if cc.rdtCollector != nil {
		return cc.rdtCollector.SyncPIDs()
	}
	return nil
}
