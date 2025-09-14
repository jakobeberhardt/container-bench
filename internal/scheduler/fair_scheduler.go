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
	
	// Container information provided by orchestration
	containers       []ContainerInfo
	containerClasses map[int]string  // container index -> RDT class name
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

func (fs *FairScheduler) Initialize(allocator RDTAllocator, containers []ContainerInfo) error {
	fs.schedulerLogger.WithField("containers", len(containers)).Info("Initializing fair scheduler")
	
	fs.rdtAllocator = allocator
	fs.containers = containers
	
	if fs.rdtAllocator != nil {
		if err := fs.rdtAllocator.Initialize(); err != nil {
			fs.schedulerLogger.WithError(err).Warn("RDT allocator initialization failed, fair allocation disabled")
			fs.rdtAllocator = nil
		}
	}
	
	// Perform initial fair allocation setup
	if err := fs.updateFairAllocations(); err != nil {
		return fmt.Errorf("failed to setup fair allocations: %w", err)
	}
	
	fs.schedulerLogger.WithFields(logrus.Fields{
		"scheduler":       fs.name,
		"version":         fs.version,
		"containers":      len(fs.containers),
		"rdt_allocator":   fs.rdtAllocator != nil,
	}).Info("Fair scheduler initialized")
	
	fs.initialized = true
	return nil
}

func (fs *FairScheduler) ProcessDataFrames(dataframes *dataframe.DataFrames) error {
	if !fs.initialized {
		return fmt.Errorf("fair scheduler not initialized")
	}
	
	// Fair scheduler uses allocator for resource allocation - no dynamic reallocation needed
	// Container allocation was done during initialization
	
	// Log current container metrics for monitoring
	containers := dataframes.GetAllContainers()
	for containerIndex, containerDF := range containers {
		latest := containerDF.GetLatestStep()
		if latest == nil {
			continue
		}
		
		if latest.Perf != nil && latest.Perf.CacheMissRate != nil {
			fs.schedulerLogger.WithFields(logrus.Fields{
				"container":        containerIndex,
				"cache_miss_rate": *latest.Perf.CacheMissRate,
				"rdt_class":       fs.containerClasses[containerIndex],
			}).Debug("Container cache performance")
		}
	}
	
	return nil
}

func (fs *FairScheduler) updateFairAllocations() error {
	if fs.rdtAllocator == nil || fs.hostConfig == nil || !fs.hostConfig.RDT.Supported {
		fs.schedulerLogger.Debug("RDT not available, skipping fair allocation")
		return nil
	}
	
	containerCount := len(fs.containers)
	if containerCount == 0 {
		return nil
	}
	
	// For fair allocation: create dedicated RDT class for each container
	// Calculate exclusive cache bit allocation per container
	totalCacheWays := 12 // From /sys/fs/resctrl/info/L3/cbm_mask = fff (12 bits)
	waysPerContainer := totalCacheWays / containerCount
	
	fs.schedulerLogger.WithFields(logrus.Fields{
		"total_containers":    containerCount,
		"total_cache_ways":    totalCacheWays,
		"ways_per_container":  waysPerContainer,
	}).Info("Creating fair RDT allocations with dedicated classes and exclusive cache ways")
	
	// Prepare all classes for batch creation with exclusive cache bit ranges
	classes := make(map[string]struct{
		L3CachePercent    float64
		MemBandwidthPercent float64
		CacheBitMask       string
	})
	
	for i, container := range fs.containers {
		className := fmt.Sprintf("fair_container_%d", container.Index)
		
		// Calculate exclusive cache bit range for this container
		startWay := i * waysPerContainer
		endWay := startWay + waysPerContainer - 1
		
		// Ensure we don't exceed available cache ways
		if endWay >= totalCacheWays {
			endWay = totalCacheWays - 1
		}
		
		// Create bit mask for this container's exclusive cache ways
		var bitMask uint32 = 0
		for way := startWay; way <= endWay; way++ {
			bitMask |= (1 << way)
		}
		cacheBitMask := fmt.Sprintf("0x%x", bitMask)
		
		classes[className] = struct{
			L3CachePercent    float64
			MemBandwidthPercent float64
			CacheBitMask       string
		}{
			L3CachePercent:    0, // Not used when bit mask is specified
			MemBandwidthPercent: 0, // No memory bandwidth allocation for now
			CacheBitMask:       cacheBitMask,
		}
		
		fs.schedulerLogger.WithFields(logrus.Fields{
			"container":       container.Index,
			"class":          className,
			"start_way":      startWay,
			"end_way":        endWay,
			"cache_bit_mask": cacheBitMask,
		}).Info("Assigned exclusive cache ways to container")
	}
	
	// Create all RDT classes at once
	if err := fs.rdtAllocator.CreateAllRDTClasses(classes); err != nil {
		return fmt.Errorf("failed to create fair RDT classes: %w", err)
	}
	
	// Assign each container to its dedicated class
	for _, container := range fs.containers {
		className := fmt.Sprintf("fair_container_%d", container.Index)
		
		// Assign container PID to its dedicated class
		if err := fs.rdtAllocator.AssignContainerToClass(container.PID, className); err != nil {
			fs.schedulerLogger.WithError(err).WithFields(logrus.Fields{
				"container": container.Index,
				"pid":       container.PID,
				"class":     className,
			}).Error("Failed to assign container to its dedicated RDT class")
			continue
		}
		
		// Track the assignment
		fs.containerClasses[container.Index] = className
		
		fs.schedulerLogger.WithFields(logrus.Fields{
			"container":     container.Index,
			"pid":           container.PID,
			"class":         className,
			"cache_ways":    waysPerContainer,
		}).Info("Assigned container to dedicated fair RDT class")
	}
	
	return nil
}

func (fs *FairScheduler) Shutdown() error {
	fs.schedulerLogger.Info("Shutting down fair scheduler")
	
	// Clear container tracking
	fs.containerClasses = make(map[int]string)
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

func (fs *FairScheduler) SetHostConfig(hostConfig *host.HostConfig) {
	fs.hostConfig = hostConfig
}
