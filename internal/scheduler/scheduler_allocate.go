package scheduler

import (
	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/probe"
	"slices"

	"time"

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

	warmupCompleteTime time.Time
}

func NewAllocationScheduler() *AllocationScheduler {
	return &AllocationScheduler{
		name:            "allocation",
		version:         "1.0.0",
		schedulerLogger: logging.GetSchedulerLogger(),
	}
}

func (as *AllocationScheduler) Initialize(allocator RDTAllocator, containers []ContainerInfo, schedulerConfig *config.SchedulerConfig) error {
	as.rdtAllocator = allocator
	as.containers = containers
	as.config = schedulerConfig

	warmupSeconds := 0
	if as.config != nil && as.config.Allocator != nil && as.config.Allocator.WarmupT > 0 {
		warmupSeconds = as.config.Allocator.WarmupT
		as.schedulerLogger.WithField("seconds", as.config.Allocator.WarmupT).Info("Warming up benchmark")
	}

	as.warmupCompleteTime = time.Now().Add(time.Duration(warmupSeconds) * time.Second)

	as.schedulerLogger.WithFields(logrus.Fields{
		"containers":     len(containers),
		"warmup_seconds": warmupSeconds,
	}).Info("Allocation scheduler initialized")
	return nil
}

func (as *AllocationScheduler) ProcessDataFrames(dataframes *dataframe.DataFrames) error {
	if time.Now().Before(as.warmupCompleteTime) {
		return nil
	}

	containers := dataframes.GetAllContainers()

	for containerIndex, containerDF := range containers {
		latest := containerDF.GetLatestStep()
		if latest == nil {
			continue
		}

		// Simple and lightweight: just print current CPU and cache miss rate
		if latest.Perf != nil && latest.Perf.CacheMissRate != nil {
			as.schedulerLogger.WithFields(logrus.Fields{
				"container":       containerIndex,
				"cache_miss_rate": *latest.Perf.CacheMissRate,
			}).Info("Cache miss rate")
		}

		if latest.Docker != nil && latest.Docker.CPUUsagePercent != nil {
			as.schedulerLogger.WithFields(logrus.Fields{
				"container":   containerIndex,
				"cpu_percent": *latest.Docker.CPUUsagePercent,
			}).Info("CPU usage")
		}

		pid := as.containers[containerIndex].PID

		as.schedulerLogger.WithFields(logrus.Fields{
			"container": containerIndex,
			"pid":       pid,
		}).Info("Found PID")

		class, err := as.rdtAllocator.GetContainerClass(pid)
		if err != nil {
			as.schedulerLogger.WithError(err).Error("Could not find class!")
		} else {
			as.schedulerLogger.WithFields(logrus.Fields{
				"pid":   pid,
				"class": class,
			}).Info("Found class for PID")
		}

		classes := as.rdtAllocator.ListAvailableClasses()

		exists := slices.Contains(classes, "victim-test")

		if exists == false {
			as.rdtAllocator.CreateRDTClass("victim-test", 0.3, 0.3)
		}

	}

	return nil
}

func (as *AllocationScheduler) Shutdown() error {
	// TODO: Implement Cleanup
	return nil
}

func (as *AllocationScheduler) GetVersion() string {
	return as.version
}

func (as *AllocationScheduler) SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	as.schedulerLogger.SetLevel(logLevel)
	return nil
}

func (as *AllocationScheduler) SetHostConfig(hostConfig *host.HostConfig) {
	as.hostConfig = hostConfig
}

func (as *AllocationScheduler) SetProbe(prober *probe.Probe) {
	as.prober = prober
	as.schedulerLogger.Debug("Probe injected into scheduler")
}
