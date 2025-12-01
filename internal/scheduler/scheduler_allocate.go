package scheduler

import (
	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/probe"

	"github.com/sirupsen/logrus"
)

type AllocationScheduler struct {
	name            string
	version         string
	schedulerLogger *logrus.Logger
	hostConfig      *host.HostConfig
	containers      []ContainerInfo
	rdtAllocator    RDTAllocator
	prober          *probe.Probe
	config          *config.SchedulerConfig
}

func NewAllocationScheduler() *AllocationScheduler {
	return &AllocationScheduler{
		name:            "allocation",
		version:         "1.0.0",
		schedulerLogger: logging.GetSchedulerLogger(),
	}
}

func (ds *AllocationScheduler) Initialize(allocator RDTAllocator, containers []ContainerInfo, schedulerConfig *config.SchedulerConfig) error {
	ds.rdtAllocator = allocator
	ds.containers = containers
	ds.config = schedulerConfig

	ds.schedulerLogger.WithField("containers", len(containers)).Info("Allocation scheduler initialized")
	return nil
}

func (ds *AllocationScheduler) ProcessDataFrames(dataframes *dataframe.DataFrames) error {
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

func (ds *AllocationScheduler) Shutdown() error {
	// TODO: Implement Cleanup
	return nil
}

func (ds *AllocationScheduler) GetVersion() string {
	return ds.version
}

func (ds *AllocationScheduler) SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	ds.schedulerLogger.SetLevel(logLevel)
	return nil
}

func (ds *AllocationScheduler) SetHostConfig(hostConfig *host.HostConfig) {
	ds.hostConfig = hostConfig
}

func (ds *AllocationScheduler) SetProbe(prober *probe.Probe) {
	ds.prober = prober
	ds.schedulerLogger.Debug("Probe injected into scheduler")
}
