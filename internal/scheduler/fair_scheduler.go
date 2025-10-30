package scheduler

import (
	"fmt"

	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"

	"github.com/sirupsen/logrus"
)

type FairScheduler struct {
	name            string
	version         string
	schedulerLogger *logrus.Logger
	hostConfig      *host.HostConfig
	containers      []ContainerInfo
	rdtAllocator    RDTAllocator
	initialized     bool
}

func NewFairScheduler() *FairScheduler {
	return &FairScheduler{
		name:            "fair",
		version:         "1.0.0",
		schedulerLogger: logging.GetSchedulerLogger(),
	}
}

func (fs *FairScheduler) Initialize(allocator RDTAllocator, containers []ContainerInfo) error {
	fs.rdtAllocator = allocator
	fs.containers = containers

	if fs.rdtAllocator == nil {
		fs.schedulerLogger.Warn("No RDT allocator provided, scheduler will run in monitoring-only mode")
		fs.initialized = true
		return nil
	}

	if fs.hostConfig == nil {
		return fmt.Errorf("host config not set, call SetHostConfig before Initialize")
	}

	if !fs.hostConfig.RDT.Supported || !fs.hostConfig.RDT.AllocationSupported {
		fs.schedulerLogger.Warn("RDT allocation not supported, scheduler will run in monitoring-only mode")
		fs.initialized = true
		return nil
	}

	// Apply fair allocation immediately
	if err := fs.applyFairAllocation(); err != nil {
		return fmt.Errorf("failed to apply initial fair allocation: %v", err)
	}

	fs.schedulerLogger.WithFields(logrus.Fields{
		"containers":  len(containers),
		"total_ways":  fs.hostConfig.L3Cache.WaysPerCache,
		"rdt_enabled": fs.rdtAllocator != nil,
	}).Info("Fair scheduler initialized with equal L3 cache allocation")

	fs.initialized = true
	return nil
}

func (fs *FairScheduler) applyFairAllocation() error {
	if fs.rdtAllocator == nil || fs.hostConfig == nil {
		return nil
	}

	numContainers := len(fs.containers)
	if numContainers == 0 {
		return fmt.Errorf("no containers to allocate")
	}

	totalWays := fs.hostConfig.L3Cache.WaysPerCache
	if totalWays == 0 {
		return fmt.Errorf("no cache ways available")
	}

	// Calculate base allocation (ways per container)
	baseWays := totalWays / numContainers
	remainderWays := totalWays % numContainers

	fs.schedulerLogger.WithFields(logrus.Fields{
		"total_ways":     totalWays,
		"num_containers": numContainers,
		"base_ways":      baseWays,
		"remainder_ways": remainderWays,
	}).Info("Calculating fair L3 cache allocation")

	// Allocate cache to each container
	for i, container := range fs.containers {
		ways := baseWays

		// Distribute remainder ways to first few containers
		if i < remainderWays {
			ways++
		}

		percentage := float64(ways) / float64(totalWays)

		if err := fs.rdtAllocator.AllocateL3CacheWays(container.Index, ways); err != nil {
			fs.schedulerLogger.WithError(err).WithFields(logrus.Fields{
				"container_index": container.Index,
				"ways":            ways,
			}).Error("Failed to allocate L3 cache")
			return fmt.Errorf("failed to allocate cache to container %d: %v", container.Index, err)
		}

		fs.schedulerLogger.WithFields(logrus.Fields{
			"container_index": container.Index,
			"ways":            ways,
			"percentage":      fmt.Sprintf("%.1f%%", percentage*100),
		}).Info("Allocated L3 cache to container")
	}

	return nil
}

func (fs *FairScheduler) ProcessDataFrames(dataframes *dataframe.DataFrames) error {
	if !fs.initialized {
		return fmt.Errorf("scheduler not initialized")
	}

	// Fair scheduler maintains static allocation, so we just log metrics
	containers := dataframes.GetAllContainers()

	for containerIndex, containerDF := range containers {
		latest := containerDF.GetLatestStep()
		if latest == nil {
			continue
		}

		logFields := logrus.Fields{
			"container": containerIndex,
		}

		// Log performance metrics
		if latest.Perf != nil {
			if latest.Perf.CacheMissRate != nil {
				logFields["cache_miss_rate"] = fmt.Sprintf("%.2f%%", *latest.Perf.CacheMissRate)
			}
			if latest.Perf.InstructionsPerCycle != nil {
				logFields["ipc"] = fmt.Sprintf("%.2f", *latest.Perf.InstructionsPerCycle)
			}
		}

		if latest.Docker != nil && latest.Docker.CPUUsagePercent != nil {
			logFields["cpu_percent"] = fmt.Sprintf("%.1f%%", *latest.Docker.CPUUsagePercent)
		}

		// Log RDT metrics if available
		if latest.RDT != nil {
			if latest.RDT.L3CacheAllocation != nil {
				logFields["l3_allocated_ways"] = *latest.RDT.L3CacheAllocation
			}
			if latest.RDT.L3CacheOccupancy != nil {
				logFields["l3_occupancy_mb"] = fmt.Sprintf("%.2f", float64(*latest.RDT.L3CacheOccupancy)/(1024*1024))
			}
		}

		if len(logFields) > 1 {
			fs.schedulerLogger.WithFields(logFields).Debug("Container metrics")
		}
	}

	return nil
}

func (fs *FairScheduler) Shutdown() error {
	if fs.rdtAllocator != nil {
		fs.schedulerLogger.Info("Fair scheduler shutting down")
		// Reset allocations to default
		if err := fs.rdtAllocator.Reset(); err != nil {
			fs.schedulerLogger.WithError(err).Warn("Failed to reset allocations during shutdown")
		}
	}
	return nil
}

func (fs *FairScheduler) GetVersion() string {
	return fs.version
}

func (fs *FairScheduler) SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	fs.schedulerLogger.SetLevel(logLevel)
	return nil
}

func (fs *FairScheduler) SetHostConfig(hostConfig *host.HostConfig) {
	fs.hostConfig = hostConfig
}
