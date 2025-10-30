package manager

import (
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
	"sync"

	"container-bench/internal/host"
	"container-bench/internal/logging"

	"github.com/intel/goresctrl/pkg/rdt"
	"github.com/sirupsen/logrus"
)

type RDTAllocator interface {
	Initialize() error
	RegisterContainer(containerIndex int, pid int, cgroupPath string) error
	AllocateL3Cache(containerIndex int, percentage float64) error
	AllocateL3CacheWays(containerIndex int, ways int) error
	Reset() error
	Shutdown() error
}

type ContainerAllocation struct {
	Index      int
	PID        int
	CGroupPath string
	ClassName  string
	Ways       int
	Percentage float64
}

type ResourceManager struct {
	hostConfig    *host.HostConfig
	logger        *logrus.Logger
	containers    map[int]*ContainerAllocation
	mu            sync.RWMutex
	initialized   bool
	currentConfig *rdt.Config
	partitionName string
}

func NewResourceManager(hostConfig *host.HostConfig) *ResourceManager {
	return &ResourceManager{
		hostConfig:    hostConfig,
		logger:        logging.GetLogger(),
		containers:    make(map[int]*ContainerAllocation),
		partitionName: "container-bench",
	}
}

func (rm *ResourceManager) Initialize() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.initialized {
		return nil
	}

	if rm.hostConfig == nil {
		return fmt.Errorf("host config not set")
	}

	if !rm.hostConfig.RDT.Supported {
		return fmt.Errorf("RDT not supported on this system")
	}

	if !rm.hostConfig.RDT.AllocationSupported {
		return fmt.Errorf("RDT allocation not supported on this system")
	}

	if err := rdt.Initialize(""); err != nil {
		return fmt.Errorf("failed to initialize RDT: %v", err)
	}

	rm.currentConfig = &rdt.Config{
		Partitions: make(map[string]struct {
			L2Allocation rdt.CatConfig `json:"l2Allocation"`
			L3Allocation rdt.CatConfig `json:"l3Allocation"`
			MBAllocation rdt.MbaConfig `json:"mbAllocation"`
			Classes      map[string]struct {
				L2Allocation rdt.CatConfig         `json:"l2Allocation"`
				L3Allocation rdt.CatConfig         `json:"l3Allocation"`
				MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
				Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
			} `json:"classes"`
		}),
	}

	rm.logger.WithFields(logrus.Fields{
		"total_ways":  rm.hostConfig.L3Cache.WaysPerCache,
		"max_closids": rm.hostConfig.RDT.MaxCLOSIDs,
	}).Info("ResourceManager initialized")

	rm.initialized = true
	return nil
}

func (rm *ResourceManager) RegisterContainer(containerIndex int, pid int, cgroupPath string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.initialized {
		return fmt.Errorf("ResourceManager not initialized")
	}

	allocation := &ContainerAllocation{
		Index:      containerIndex,
		PID:        pid,
		CGroupPath: cgroupPath,
	}

	rm.containers[containerIndex] = allocation

	rm.logger.WithFields(logrus.Fields{
		"container_index": containerIndex,
		"pid":             pid,
		"cgroup_path":     cgroupPath,
	}).Debug("Container registered with ResourceManager")

	return nil
}

func (rm *ResourceManager) AllocateL3Cache(containerIndex int, percentage float64) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.initialized {
		return fmt.Errorf("ResourceManager not initialized")
	}

	if percentage < 0 || percentage > 1.0 {
		return fmt.Errorf("percentage must be between 0.0 and 1.0, got: %f", percentage)
	}

	totalWays := rm.hostConfig.L3Cache.WaysPerCache
	ways := int(float64(totalWays) * percentage)

	if ways == 0 && percentage > 0 {
		ways = 1
	}

	if ways > totalWays {
		ways = totalWays
	}

	return rm.allocateL3CacheWaysInternal(containerIndex, ways)
}

func (rm *ResourceManager) AllocateL3CacheWays(containerIndex int, ways int) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.initialized {
		return fmt.Errorf("ResourceManager not initialized")
	}

	return rm.allocateL3CacheWaysInternal(containerIndex, ways)
}

func (rm *ResourceManager) allocateL3CacheWaysInternal(containerIndex int, ways int) error {
	allocation, exists := rm.containers[containerIndex]
	if !exists {
		return fmt.Errorf("container %d not registered", containerIndex)
	}

	totalWays := rm.hostConfig.L3Cache.WaysPerCache
	if ways <= 0 || ways > totalWays {
		return fmt.Errorf("ways must be between 1 and %d, got: %d", totalWays, ways)
	}

	className := fmt.Sprintf("container-%d", containerIndex)

	allocation.ClassName = className
	allocation.Ways = ways
	allocation.Percentage = float64(ways) / float64(totalWays)

	if err := rm.applyRDTConfig(); err != nil {
		return fmt.Errorf("failed to apply RDT configuration: %v", err)
	}

	// After applying config, try to get the class (it's created at root level, not under partition)
	ctrlGroup, found := rdt.GetClass(className)
	if !found {
		// List all available classes for debugging
		allClasses := rdt.GetClasses()
		classNames := make([]string, 0, len(allClasses))
		for _, c := range allClasses {
			classNames = append(classNames, c.Name())
		}
		rm.logger.WithFields(logrus.Fields{
			"expected_class":    className,
			"available_classes": classNames,
		}).Warn("Could not find created RDT class")
	} else {
		if err := rm.syncContainerPIDs(allocation, ctrlGroup); err != nil {
			rm.logger.WithError(err).Warn("Failed to sync PIDs to RDT class")
		}
	}

	rm.logger.WithFields(logrus.Fields{
		"container_index": containerIndex,
		"class_name":      className,
		"ways":            ways,
		"percentage":      allocation.Percentage * 100,
	}).Info("L3 cache allocated to container")

	return nil
}

func (rm *ResourceManager) applyRDTConfig() error {
	config := &rdt.Config{
		Partitions: make(map[string]struct {
			L2Allocation rdt.CatConfig `json:"l2Allocation"`
			L3Allocation rdt.CatConfig `json:"l3Allocation"`
			MBAllocation rdt.MbaConfig `json:"mbAllocation"`
			Classes      map[string]struct {
				L2Allocation rdt.CatConfig         `json:"l2Allocation"`
				L3Allocation rdt.CatConfig         `json:"l3Allocation"`
				MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
				Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
			} `json:"classes"`
		}),
	}

	partition := config.Partitions[rm.partitionName]
	partition.Classes = make(map[string]struct {
		L2Allocation rdt.CatConfig         `json:"l2Allocation"`
		L3Allocation rdt.CatConfig         `json:"l3Allocation"`
		MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
		Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
	})

	// Set partition-level L3 allocation (100% - required when classes have L3 allocations)
	partitionL3Config := make(rdt.CatConfig)
	for _, cacheID := range rm.hostConfig.L3Cache.CacheIDs {
		partitionL3Config[fmt.Sprintf("%d", cacheID)] = rdt.CacheIdCatConfig{
			Unified: rdt.CacheProportion("100%"),
		}
	}
	partition.L3Allocation = partitionL3Config

	// Sort containers by index to ensure consistent allocation
	sortedContainers := make([]*ContainerAllocation, 0, len(rm.containers))
	for _, allocation := range rm.containers {
		if allocation.ClassName != "" {
			sortedContainers = append(sortedContainers, allocation)
		}
	}
	sort.Slice(sortedContainers, func(i, j int) bool {
		return sortedContainers[i].Index < sortedContainers[j].Index
	})

	// Create non-overlapping bitmasks for each container
	startWay := 0
	for _, allocation := range sortedContainers {
		// Create bitmask with 'ways' consecutive bits starting at 'startWay'
		bitmask := rm.generateNonOverlappingBitmask(allocation.Ways, startWay)
		bitmaskHex := fmt.Sprintf("0x%x", bitmask)

		l3Config := make(rdt.CatConfig)
		for _, cacheID := range rm.hostConfig.L3Cache.CacheIDs {
			l3Config[fmt.Sprintf("%d", cacheID)] = rdt.CacheIdCatConfig{
				Unified: rdt.CacheProportion(bitmaskHex),
			}
		}

		class := partition.Classes[allocation.ClassName]
		class.L3Allocation = l3Config
		partition.Classes[allocation.ClassName] = class

		// Move to next starting position
		startWay += allocation.Ways
	}

	config.Partitions[rm.partitionName] = partition

	if err := rdt.SetConfig(config, false); err != nil {
		return fmt.Errorf("failed to set RDT configuration: %v", err)
	}

	rm.currentConfig = config
	return nil
}

// generateNonOverlappingBitmask creates a bitmask with 'ways' consecutive bits set, starting at 'startBit'
// For example: ways=4, startBit=0 -> 0x000f (bits 0-3)
//
//	ways=4, startBit=4 -> 0x00f0 (bits 4-7)
func (rm *ResourceManager) generateNonOverlappingBitmask(ways int, startBit int) uint64 {
	if ways <= 0 {
		return 0
	}

	var mask uint64 = 0
	for i := 0; i < ways; i++ {
		mask |= (1 << (startBit + i))
	}

	// Ensure the mask doesn't exceed the maximum
	if mask > rm.hostConfig.L3Cache.MaxBitmask {
		rm.logger.WithFields(logrus.Fields{
			"requested_mask": fmt.Sprintf("0x%x", mask),
			"max_mask":       fmt.Sprintf("0x%x", rm.hostConfig.L3Cache.MaxBitmask),
		}).Warn("Generated bitmask exceeds maximum, capping")
		mask = mask & rm.hostConfig.L3Cache.MaxBitmask
	}

	return mask
}

func (rm *ResourceManager) syncContainerPIDs(allocation *ContainerAllocation, ctrlGroup rdt.CtrlGroup) error {
	if allocation.CGroupPath == "" {
		return fmt.Errorf("cgroup path not set for container %d", allocation.Index)
	}

	cgroupProcsPath := fmt.Sprintf("%s/cgroup.procs", allocation.CGroupPath)
	data, err := ioutil.ReadFile(cgroupProcsPath)
	if err != nil {
		return fmt.Errorf("failed to read cgroup.procs from %s: %v", cgroupProcsPath, err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	pids := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			pids = append(pids, line)
		}
	}

	if len(pids) == 0 {
		rm.logger.WithField("container_index", allocation.Index).Debug("No PIDs found in cgroup")
		return nil
	}

	if err := ctrlGroup.AddPids(pids...); err != nil {
		return fmt.Errorf("failed to add PIDs to RDT control group: %v", err)
	}

	rm.logger.WithFields(logrus.Fields{
		"container_index": allocation.Index,
		"class_name":      allocation.ClassName,
		"num_pids":        len(pids),
	}).Debug("Synced PIDs to RDT control group")

	return nil
}

func (rm *ResourceManager) Reset() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.initialized {
		return fmt.Errorf("ResourceManager not initialized")
	}

	rm.logger.Info("Resetting RDT configuration to default")

	// First, remove all PIDs from the RDT classes
	for _, allocation := range rm.containers {
		if allocation.ClassName == "" {
			continue
		}

		if ctrlGroup, exists := rdt.GetClass(allocation.ClassName); exists {
			// Get current PIDs
			pids, err := ctrlGroup.GetPids()
			if err == nil && len(pids) > 0 {
				rm.logger.WithFields(logrus.Fields{
					"class_name": allocation.ClassName,
					"num_pids":   len(pids),
				}).Debug("Removing PIDs from RDT class before reset")
			}
		}
	}

	// Reset to default (empty) configuration - use force=true to remove non-empty groups
	defaultConfig := &rdt.Config{}
	if err := rdt.SetConfig(defaultConfig, true); err != nil {
		return fmt.Errorf("failed to reset RDT configuration: %v", err)
	}

	rm.logger.Info("RDT configuration reset to default (all allocations removed)")
	return nil
}

func (rm *ResourceManager) Shutdown() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.initialized {
		return nil
	}

	rm.logger.Info("Shutting down ResourceManager")

	defaultConfig := &rdt.Config{}
	if err := rdt.SetConfig(defaultConfig, false); err != nil {
		rm.logger.WithError(err).Warn("Failed to reset RDT to default configuration")
	}

	rm.containers = make(map[int]*ContainerAllocation)
	rm.currentConfig = nil
	rm.initialized = false

	return nil
}

func (rm *ResourceManager) GetContainerAllocation(containerIndex int) (*ContainerAllocation, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	allocation, exists := rm.containers[containerIndex]
	if !exists {
		return nil, fmt.Errorf("container %d not registered", containerIndex)
	}

	return allocation, nil
}
