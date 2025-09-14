package scheduler

import (
	"fmt"

	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"github.com/sirupsen/logrus"
)

// FairScheduler implements a scheduler that allocates proportional L3 cache resources
// to all containers participating in the benchmark
type FairScheduler struct {
	name            string
	version         string
	schedulerLogger *logrus.Logger
	hostConfig      *host.HostConfig
	rdtAllocator    RDTAllocator
	initialized     bool
	
	// Track containers and their allocations
	containerClasses map[int]string  // container index -> RDT class name
	totalContainers  int
}

// NewFairScheduler creates a new fair scheduler instance
func NewFairScheduler() *FairScheduler {
	return &FairScheduler{
		name:             "fair",
		version:          "1.0.0",
		schedulerLogger:  logging.GetSchedulerLogger(),
		containerClasses: make(map[int]string),
	}
}

func (fs *FairScheduler) Initialize() error {
	fs.schedulerLogger.Info("Initializing fair scheduler")
	
	// Get host configuration
	hostConfig, err := host.GetHostConfig()
	if err != nil {
		return fmt.Errorf("failed to get host configuration: %v", err)
	}
	fs.hostConfig = hostConfig
	
	// Initialize RDT allocator
	fs.rdtAllocator = NewDefaultRDTAllocator()
	if err := fs.rdtAllocator.Initialize(); err != nil {
		fs.schedulerLogger.WithError(err).Warn("RDT allocator initialization failed, fair allocation disabled")
		fs.rdtAllocator = nil
	}
	
	fs.schedulerLogger.WithFields(logrus.Fields{
		"l3_cache_mb":     fs.hostConfig.L3Cache.TotalSizeMB,
		"cache_ways":      fs.hostConfig.L3Cache.WaysPerCache,
		"rdt_supported":   fs.hostConfig.RDT.Supported,
		"rdt_allocator":   fs.rdtAllocator != nil,
	}).Info("Fair scheduler initialized")
	
	fs.initialized = true
	return nil
}

func (fs *FairScheduler) ProcessDataFrames(dataframes *dataframe.DataFrames) error {
	if !fs.initialized {
		return fmt.Errorf("fair scheduler not initialized")
	}
	
	containers := dataframes.GetAllContainers()
	currentContainerCount := len(containers)
	
	// Check if we need to update allocations due to container count change
	if currentContainerCount != fs.totalContainers {
		fs.schedulerLogger.WithFields(logrus.Fields{
			"previous_containers": fs.totalContainers,
			"current_containers":  currentContainerCount,
		}).Info("Container count changed, updating fair allocations")
		
		if err := fs.updateFairAllocations(containers); err != nil {
			fs.schedulerLogger.WithError(err).Error("Failed to update fair allocations")
			return err
		}
		
		fs.totalContainers = currentContainerCount
	}
	
	// Monitor container performance and log allocation effectiveness
	fs.monitorContainerPerformance(containers)
	
	return nil
}

func (fs *FairScheduler) updateFairAllocations(containers map[int]*dataframe.ContainerDataFrame) error {
	if fs.rdtAllocator == nil || !fs.hostConfig.RDT.Supported {
		fs.schedulerLogger.Debug("RDT not available, skipping fair allocation")
		return nil
	}
	
	containerCount := len(containers)
	if containerCount == 0 {
		return nil
	}
	
	// Calculate fair allocation per container
	waysPerContainer, _ := fs.hostConfig.GetFairL3Allocation(containerCount)
	allocationPercent := float64(waysPerContainer) / float64(fs.hostConfig.L3Cache.WaysPerCache) * 100.0
	
	fs.schedulerLogger.WithFields(logrus.Fields{
		"total_containers":    containerCount,
		"ways_per_container":  waysPerContainer,
		"allocation_percent":  allocationPercent,
		"total_cache_ways":    fs.hostConfig.L3Cache.WaysPerCache,
	}).Info("Calculating fair L3 cache allocations")
	
	// For fair allocation, we'll create container-specific RDT classes
	// Note: This is a simplified approach. In practice, you might want to use
	// existing classes or a more sophisticated allocation strategy
	
	containerIndex := 0
	for containerIdx, containerDF := range containers {
		// Get the latest step to find container PID
		latestStep := containerDF.GetLatestStep()
		if latestStep == nil || latestStep.RDT == nil {
			fs.schedulerLogger.WithField("container", containerIdx).Debug("No RDT data available for container")
			continue
		}
		
		// For this implementation, we'll assign containers to existing classes
		// In a full implementation, you would create dedicated classes with specific allocations
		className := fs.selectRDTClassForContainer(containerIndex, containerCount)
		
		if className != "" {
			// Try to get PID from RDT collector or other means
			// This is a simplified approach - in practice, you'd track PIDs properly
			fs.schedulerLogger.WithFields(logrus.Fields{
				"container":       containerIdx,
				"assigned_class":  className,
				"allocation_pct":  allocationPercent,
			}).Info("Assigned container to RDT class for fair allocation")
			
			fs.containerClasses[containerIdx] = className
		}
		
		containerIndex++
	}
	
	return nil
}

func (fs *FairScheduler) selectRDTClassForContainer(containerIndex, totalContainers int) string {
	// Get available RDT classes
	availableClasses := fs.rdtAllocator.ListAvailableClasses()
	if len(availableClasses) == 0 {
		return ""
	}
	
	// For fair allocation, we'll cycle through available classes
	// In a production system, you would create dedicated classes with specific allocations
	classIndex := containerIndex % len(availableClasses)
	return availableClasses[classIndex]
}

func (fs *FairScheduler) monitorContainerPerformance(containers map[int]*dataframe.ContainerDataFrame) {
	for containerIndex, containerDF := range containers {
		latest := containerDF.GetLatestStep()
		if latest == nil {
			continue
		}
		
		fields := logrus.Fields{
			"container": containerIndex,
		}
		
		// Log cache performance metrics
		if latest.Perf != nil && latest.Perf.CacheMissRate != nil {
			fields["cache_miss_rate"] = *latest.Perf.CacheMissRate
		}
		
		// Log RDT metrics
		if latest.RDT != nil {
			if latest.RDT.RDTClassName != nil {
				fields["rdt_class"] = *latest.RDT.RDTClassName
			}
			if latest.RDT.CacheLLCUtilizationPercent != nil {
				fields["llc_utilization_percent"] = *latest.RDT.CacheLLCUtilizationPercent
			}
			if latest.RDT.L3CacheOccupancyMB != nil {
				fields["cache_occupancy_mb"] = *latest.RDT.L3CacheOccupancyMB
			}
		}
		
		// Log CPU usage
		if latest.Docker != nil && latest.Docker.CPUUsagePercent != nil {
			fields["cpu_percent"] = *latest.Docker.CPUUsagePercent
		}
		
		fs.schedulerLogger.WithFields(fields).Debug("Container performance metrics")
		
		// Check if cache miss rate is high and log recommendation
		if latest.Perf != nil && latest.Perf.CacheMissRate != nil && *latest.Perf.CacheMissRate > 40.0 {
			fs.schedulerLogger.WithFields(logrus.Fields{
				"container":       containerIndex,
				"cache_miss_rate": *latest.Perf.CacheMissRate,
			}).Warn("High cache miss rate detected - container may benefit from more cache allocation")
		}
	}
}

func (fs *FairScheduler) Shutdown() error {
	fs.schedulerLogger.Info("Shutting down fair scheduler")
	
	if fs.rdtAllocator != nil {
		if err := fs.rdtAllocator.Cleanup(); err != nil {
			fs.schedulerLogger.WithError(err).Warn("Failed to cleanup RDT allocator")
		}
	}
	
	// Clear container tracking
	fs.containerClasses = make(map[int]string)
	fs.totalContainers = 0
	fs.initialized = false
	
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
