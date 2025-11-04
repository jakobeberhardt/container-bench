package scheduler

import (
	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/probe"

	"github.com/sirupsen/logrus"
)

// ContainerInfo holds complete information about a container including its PID
type ContainerInfo struct {
	Index  int
	Config *config.ContainerConfig
	PID    int
}

type Scheduler interface {
	Initialize(allocator RDTAllocator, containers []ContainerInfo) error
	ProcessDataFrames(dataframes *dataframe.DataFrames) error
	Shutdown() error
	GetVersion() string
	SetLogLevel(level string) error

	// Host configuration for scheduler decisions
	SetHostConfig(hostConfig *host.HostConfig)

	// Probe injection for sensitivity analysis
	SetProbe(prober *probe.Probe)
}

type DefaultScheduler struct {
	name            string
	version         string
	schedulerLogger *logrus.Logger
	hostConfig      *host.HostConfig
	containers      []ContainerInfo
	rdtAllocator    RDTAllocator
	prober          *probe.Probe
}

func NewDefaultScheduler() *DefaultScheduler {
	return &DefaultScheduler{
		name:            "probe",
		version:         "1.0.0",
		schedulerLogger: logging.GetSchedulerLogger(),
	}
}

func (ds *DefaultScheduler) Initialize(allocator RDTAllocator, containers []ContainerInfo) error {
	ds.rdtAllocator = allocator
	ds.containers = containers

	ds.schedulerLogger.WithField("containers", len(containers)).Info("Default scheduler initialized")
	return nil
}

func (ds *DefaultScheduler) ProcessDataFrames(dataframes *dataframe.DataFrames) error {
	containers := dataframes.GetAllContainers()

	for containerIndex, containerDF := range containers {
		latest := containerDF.GetLatestStep()
		if latest == nil {
			continue
		}

		// Simple and lightweight: just print current CPU and cache miss rate
		if latest.Perf != nil && latest.Perf.CacheMissRate != nil {
			ds.schedulerLogger.WithFields(logrus.Fields{
				"container":       containerIndex,
				"cache_miss_rate": *latest.Perf.CacheMissRate,
			}).Info("Cache miss rate")
		}

		if latest.Docker != nil && latest.Docker.CPUUsagePercent != nil {
			ds.schedulerLogger.WithFields(logrus.Fields{
				"container":   containerIndex,
				"cpu_percent": *latest.Docker.CPUUsagePercent,
			}).Info("CPU usage")
		}
	}

	return nil
}

func (ds *DefaultScheduler) Shutdown() error {
	// Default scheduler doesn't need cleanup
	return nil
}

func (ds *DefaultScheduler) GetVersion() string {
	return ds.version
}

func (ds *DefaultScheduler) SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	ds.schedulerLogger.SetLevel(logLevel)
	return nil
}

func (ds *DefaultScheduler) SetHostConfig(hostConfig *host.HostConfig) {
	ds.hostConfig = hostConfig
}

func (ds *DefaultScheduler) SetProbe(prober *probe.Probe) {
	ds.prober = prober
	ds.schedulerLogger.Debug("Probe injected into scheduler")
}
