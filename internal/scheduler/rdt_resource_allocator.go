package scheduler

import (
	"fmt"
	"os"
	"sync"

	"container-bench/internal/host"
	"container-bench/internal/logging"

	"github.com/intel/goresctrl/pkg/rdt"
	"github.com/sirupsen/logrus"
)

// ResourceAllocator provides dynamic RDT resource allocation with accounting
// This is a singleton that maintains central accounting of allocated resources
type ResourceAllocator interface {
	// Initialize sets up the allocator and reads system capabilities
	Initialize() error

	// CreateGroup creates a new RDT class with specified resource allocation
	// Returns error if resources are not available or invalid
	CreateGroup(name string, config GroupConfig) error

	// DeleteGroup removes an RDT class
	DeleteGroup(name string) error

	// AssignContainer moves a container (by PID) to a specific RDT group
	AssignContainer(pid int, groupName string) error

	// GetContainerGroup returns the current group name for a container PID
	GetContainerGroup(pid int) (string, error)

	// GetAvailableResources returns currently available (unallocated) resources per socket
	GetAvailableResources() map[int]*SocketResources

	// ResetToDefault moves all containers back to default group and deletes all custom groups
	ResetToDefault() error

	// Close cleans up resources
	Close() error
}

// GroupConfig defines the resources to allocate for an RDT group
type GroupConfig struct {
	// Per-socket L3 cache allocation
	L3Allocation map[int]L3Config // socket -> L3 config

	// Per-socket memory bandwidth allocation
	MBAllocation map[int]MBConfig // socket -> MB config
}

// L3Config defines L3 cache allocation for a socket
type L3Config struct {
	// Number of cache ways to allocate (will be converted to contiguous bitmask)
	Ways int

	// Optional: explicit cache way range (e.g., "0-2" for ways 0, 1, 2)
	// If set, Ways is ignored
	WayRange string

	// Optional: explicit bitmask (e.g., "0x7" for ways 0, 1, 2)
	// If set, Ways and WayRange are ignored
	BitMask string
}

// MBConfig defines memory bandwidth allocation for a socket
type MBConfig struct {
	// Percentage of memory bandwidth (0-100)
	Percentage int
}

// SocketResources tracks available resources per socket
type SocketResources struct {
	Socket int

	// L3 cache
	TotalCacheWays     int
	AvailableCacheWays int
	AllocatedWaysMask  uint64 // Bitmask of allocated cache ways

	// Memory bandwidth
	TotalMBW     int // Always 100%
	AvailableMBW int // Remaining percentage
}

// DefaultResourceAllocator implements ResourceAllocator with central accounting
type DefaultResourceAllocator struct {
	logger     *logrus.Logger
	hostConfig *host.HostConfig

	mu sync.RWMutex // Protects all internal state

	// System capabilities
	sockets        []int        // Available sockets
	cacheWaysTotal int          // Total cache ways per socket
	socketCount    int          // Number of sockets
	socketMap      map[int]bool // Quick socket lookup

	// Resource accounting per socket
	resources map[int]*SocketResources

	// Group management
	groups       map[string]rdt.CtrlGroup // group name -> RDT control group
	groupConfigs map[string]GroupConfig   // group name -> original config

	// PID tracking
	pidToGroup map[int]string // PID -> group name

	initialized bool
}

// NewDefaultResourceAllocator creates a new resource allocator singleton
func NewDefaultResourceAllocator() *DefaultResourceAllocator {
	return &DefaultResourceAllocator{
		logger:       logging.GetSchedulerLogger(),
		resources:    make(map[int]*SocketResources),
		groups:       make(map[string]rdt.CtrlGroup),
		groupConfigs: make(map[string]GroupConfig),
		pidToGroup:   make(map[int]string),
		socketMap:    make(map[int]bool),
	}
}

func (a *DefaultResourceAllocator) Initialize() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.initialized {
		return nil
	}

	a.logger.Info("Initializing RDT Resource Allocator")

	// Check if RDT is supported
	if !rdt.MonSupported() {
		return fmt.Errorf("RDT not supported on this system")
	}

	// Read system capabilities
	if err := a.readSystemCapabilities(); err != nil {
		return fmt.Errorf("failed to read system capabilities: %w", err)
	}

	// Initialize resource tracking
	a.initializeResourceTracking()

	a.initialized = true
	a.logger.WithFields(logrus.Fields{
		"sockets":    a.sockets,
		"cache_ways": a.cacheWaysTotal,
	}).Info("RDT Resource Allocator initialized")

	return nil
}

func (a *DefaultResourceAllocator) readSystemCapabilities() error {
	// Read L3 cache ways from resctrl
	cbmMaskPath := "/sys/fs/resctrl/info/L3/cbm_mask"
	data, err := os.ReadFile(cbmMaskPath)
	if err != nil {
		return fmt.Errorf("failed to read L3 CBM mask: %w", err)
	}

	var cbmMask uint64
	if _, err := fmt.Sscanf(string(data), "%x", &cbmMask); err != nil {
		return fmt.Errorf("failed to parse CBM mask: %w", err)
	}

	// Count cache ways
	ways := 0
	mask := cbmMask
	for mask > 0 {
		ways += int(mask & 1)
		mask >>= 1
	}
	a.cacheWaysTotal = ways

	// Get host configuration for socket information
	hostCfg, err := host.GetHostConfig()
	if err != nil {
		// Fall back to single socket if host config not available
		a.logger.Warn("Could not get host config, assuming single socket system")
		a.sockets = []int{0}
		a.socketCount = 1
		a.socketMap[0] = true
		return nil
	}

	a.hostConfig = hostCfg

	// Get sockets from host config
	if hostCfg.Topology.Sockets > 0 {
		a.socketCount = hostCfg.Topology.Sockets
		a.sockets = make([]int, a.socketCount)
		for i := 0; i < a.socketCount; i++ {
			a.sockets[i] = i
			a.socketMap[i] = true
		}
	} else {
		a.sockets = []int{0}
		a.socketCount = 1
		a.socketMap[0] = true
	}

	return nil
}

func (a *DefaultResourceAllocator) initializeResourceTracking() {
	for _, socket := range a.sockets {
		a.resources[socket] = &SocketResources{
			Socket:             socket,
			TotalCacheWays:     a.cacheWaysTotal,
			AvailableCacheWays: a.cacheWaysTotal,
			AllocatedWaysMask:  0,
			TotalMBW:           100,
			AvailableMBW:       100,
		}
	}
}

func (a *DefaultResourceAllocator) CreateGroup(name string, config GroupConfig) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.initialized {
		return fmt.Errorf("allocator not initialized")
	}

	// Check if group already exists
	if _, exists := a.groups[name]; exists {
		return fmt.Errorf("group %s already exists", name)
	}

	a.logger.WithField("group", name).Info("Creating RDT group")

	// Validate and prepare configuration
	rdtConfig, err := a.buildRDTConfig(name, config)
	if err != nil {
		return fmt.Errorf("failed to build RDT config: %w", err)
	}

	// Apply configuration
	if err := rdt.SetConfig(rdtConfig, false); err != nil {
		return fmt.Errorf("failed to set RDT config: %w", err)
	}

	// Get the created class
	cls, ok := rdt.GetClass(name)
	if !ok {
		return fmt.Errorf("group %s was not created", name)
	}

	// Update accounting
	if err := a.updateResourceAccounting(name, config, true); err != nil {
		// Rollback
		_ = a.deleteGroupInternal(name)
		return fmt.Errorf("failed to update resource accounting: %w", err)
	}

	a.groups[name] = cls
	a.groupConfigs[name] = config

	a.logger.WithField("group", name).Info("RDT group created successfully")
	return nil
}

// buildRDTConfig constructs the RDT configuration for creating a new group
func (a *DefaultResourceAllocator) buildRDTConfig(groupName string, config GroupConfig) (*rdt.Config, error) {
	// Start with current config by getting all existing classes
	rdtConfig := &rdt.Config{
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

	partition := struct {
		L2Allocation rdt.CatConfig `json:"l2Allocation"`
		L3Allocation rdt.CatConfig `json:"l3Allocation"`
		MBAllocation rdt.MbaConfig `json:"mbAllocation"`
		Classes      map[string]struct {
			L2Allocation rdt.CatConfig         `json:"l2Allocation"`
			L3Allocation rdt.CatConfig         `json:"l3Allocation"`
			MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
			Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
		} `json:"classes"`
	}{
		L3Allocation: rdt.CatConfig{
			"all": rdt.CacheIdCatConfig{
				Unified: "0xfff", // Full allocation at partition level
			},
		},
		MBAllocation: rdt.MbaConfig{
			"all": []rdt.MbProportion{"100%"},
		},
		Classes: make(map[string]struct {
			L2Allocation rdt.CatConfig         `json:"l2Allocation"`
			L3Allocation rdt.CatConfig         `json:"l3Allocation"`
			MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
			Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
		}),
	}

	// Add existing groups to maintain them
	for existingName, existingConfig := range a.groupConfigs {
		class, err := a.buildClassConfig(existingConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to rebuild config for existing group %s: %w", existingName, err)
		}
		partition.Classes[existingName] = class
	}

	// Add the new group
	newClass, err := a.buildClassConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to build config for new group: %w", err)
	}
	partition.Classes[groupName] = newClass

	rdtConfig.Partitions["default"] = partition

	return rdtConfig, nil
}

// buildClassConfig builds the RDT class configuration from GroupConfig
func (a *DefaultResourceAllocator) buildClassConfig(config GroupConfig) (struct {
	L2Allocation rdt.CatConfig         `json:"l2Allocation"`
	L3Allocation rdt.CatConfig         `json:"l3Allocation"`
	MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
	Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
}, error) {
	class := struct {
		L2Allocation rdt.CatConfig         `json:"l2Allocation"`
		L3Allocation rdt.CatConfig         `json:"l3Allocation"`
		MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
		Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
	}{
		L3Allocation: make(rdt.CatConfig),
		MBAllocation: make(rdt.MbaConfig),
	}

	// Build L3 allocation
	for socket, l3cfg := range config.L3Allocation {
		var bitmask string
		var err error

		if l3cfg.BitMask != "" {
			// Explicit bitmask provided
			bitmask = l3cfg.BitMask
		} else if l3cfg.WayRange != "" {
			// Way range provided (e.g., "0-2")
			bitmask, err = a.parseCacheWayRange(l3cfg.WayRange)
			if err != nil {
				return class, fmt.Errorf("invalid way range for socket %d: %w", socket, err)
			}
		} else {
			// Number of ways provided - allocate from bottom
			bitmask, err = a.allocateCacheWays(socket, l3cfg.Ways)
			if err != nil {
				return class, fmt.Errorf("failed to allocate cache ways for socket %d: %w", socket, err)
			}
		}

		// Validate bitmask is contiguous
		if !a.isContiguousMask(bitmask) {
			return class, fmt.Errorf("bitmask %s is not contiguous", bitmask)
		}

		cacheID := fmt.Sprintf("%d", socket)
		if a.socketCount == 1 {
			cacheID = "all"
		}

		class.L3Allocation[cacheID] = rdt.CacheIdCatConfig{
			Unified: rdt.CacheProportion(bitmask),
		}
	}

	// Build MB allocation
	for socket, mbcfg := range config.MBAllocation {
		cacheID := fmt.Sprintf("%d", socket)
		if a.socketCount == 1 {
			cacheID = "all"
		}

		class.MBAllocation[cacheID] = []rdt.MbProportion{
			rdt.MbProportion(fmt.Sprintf("%d%%", mbcfg.Percentage)),
		}
	}

	return class, nil
}

// allocateCacheWays finds contiguous cache ways and returns bitmask
func (a *DefaultResourceAllocator) allocateCacheWays(socket, ways int) (string, error) {
	res, ok := a.resources[socket]
	if !ok {
		return "", fmt.Errorf("socket %d not found", socket)
	}

	if ways > res.AvailableCacheWays {
		return "", fmt.Errorf("insufficient cache ways on socket %d: requested %d, available %d",
			socket, ways, res.AvailableCacheWays)
	}

	// Find contiguous unallocated ways
	mask := uint64(0)
	found := 0
	startBit := -1

	for bit := 0; bit < a.cacheWaysTotal && found < ways; bit++ {
		if res.AllocatedWaysMask&(1<<bit) == 0 {
			// This bit is available
			if startBit == -1 {
				startBit = bit
			}
			mask |= (1 << bit)
			found++
		} else {
			// Hit an allocated bit, reset
			if found < ways {
				mask = 0
				found = 0
				startBit = -1
			}
		}
	}

	if found < ways {
		return "", fmt.Errorf("could not find %d contiguous cache ways on socket %d", ways, socket)
	}

	return fmt.Sprintf("0x%x", mask), nil
}

// parseCacheWayRange converts a range like "0-2" to a bitmask
func (a *DefaultResourceAllocator) parseCacheWayRange(wayRange string) (string, error) {
	var start, end int
	n, err := fmt.Sscanf(wayRange, "%d-%d", &start, &end)
	if err != nil || n != 2 {
		return "", fmt.Errorf("invalid cache way range format '%s', expected 'start-end'", wayRange)
	}

	if start < 0 || end >= a.cacheWaysTotal || start > end {
		return "", fmt.Errorf("invalid cache way range %d-%d, must be within 0-%d",
			start, end, a.cacheWaysTotal-1)
	}

	mask := uint64(0)
	for i := start; i <= end; i++ {
		mask |= (1 << i)
	}

	return fmt.Sprintf("0x%x", mask), nil
}

// isContiguousMask checks if a bitmask represents contiguous bits
func (a *DefaultResourceAllocator) isContiguousMask(maskStr string) bool {
	var mask uint64
	// Handle both "0x123" and "123" formats
	if _, err := fmt.Sscanf(maskStr, "0x%x", &mask); err != nil {
		if _, err := fmt.Sscanf(maskStr, "%x", &mask); err != nil {
			return false
		}
	}

	if mask == 0 {
		return false
	}

	// Find first set bit
	firstBit := -1
	for i := 0; i < 64; i++ {
		if mask&(1<<i) != 0 {
			firstBit = i
			break
		}
	}

	if firstBit == -1 {
		return false
	}

	// Check all bits from firstBit are contiguous
	for i := firstBit; i < 64; i++ {
		if mask&(1<<i) == 0 {
			// Found a zero, check if there are any more ones after it
			for j := i + 1; j < 64; j++ {
				if mask&(1<<j) != 0 {
					return false // Non-contiguous
				}
			}
			break
		}
	}

	return true
}

// updateResourceAccounting updates the resource tracking when creating/deleting groups
func (a *DefaultResourceAllocator) updateResourceAccounting(groupName string, config GroupConfig, allocate bool) error {
	for socket, l3cfg := range config.L3Allocation {
		res, ok := a.resources[socket]
		if !ok {
			return fmt.Errorf("socket %d not found", socket)
		}

		var mask uint64
		var maskStr string

		if l3cfg.BitMask != "" {
			maskStr = l3cfg.BitMask
		} else if l3cfg.WayRange != "" {
			var err error
			maskStr, err = a.parseCacheWayRange(l3cfg.WayRange)
			if err != nil {
				return err
			}
		} else {
			var err error
			maskStr, err = a.allocateCacheWays(socket, l3cfg.Ways)
			if err != nil {
				return err
			}
		}

		if _, err := fmt.Sscanf(maskStr, "%x", &mask); err != nil {
			return fmt.Errorf("invalid bitmask: %w", err)
		}

		if allocate {
			// Check for overlap
			if res.AllocatedWaysMask&mask != 0 {
				return fmt.Errorf("cache ways overlap with existing allocation on socket %d", socket)
			}
			res.AllocatedWaysMask |= mask
		} else {
			res.AllocatedWaysMask &^= mask
		}

		// Recalculate available ways
		available := 0
		for i := 0; i < a.cacheWaysTotal; i++ {
			if res.AllocatedWaysMask&(1<<i) == 0 {
				available++
			}
		}
		res.AvailableCacheWays = available
	}

	// Update memory bandwidth accounting
	for socket, mbcfg := range config.MBAllocation {
		res, ok := a.resources[socket]
		if !ok {
			return fmt.Errorf("socket %d not found", socket)
		}

		if allocate {
			if mbcfg.Percentage > res.AvailableMBW {
				return fmt.Errorf("insufficient memory bandwidth on socket %d: requested %d%%, available %d%%",
					socket, mbcfg.Percentage, res.AvailableMBW)
			}
			res.AvailableMBW -= mbcfg.Percentage
		} else {
			res.AvailableMBW += mbcfg.Percentage
			if res.AvailableMBW > 100 {
				res.AvailableMBW = 100
			}
		}
	}

	return nil
}

func (a *DefaultResourceAllocator) DeleteGroup(name string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.deleteGroupInternal(name)
}

func (a *DefaultResourceAllocator) deleteGroupInternal(name string) error {
	if !a.initialized {
		return fmt.Errorf("allocator not initialized")
	}

	// Check if group exists
	config, exists := a.groupConfigs[name]
	if !exists {
		return fmt.Errorf("group %s does not exist", name)
	}

	a.logger.WithField("group", name).Info("Deleting RDT group")

	// Move all PIDs in this group back to default
	for pid, groupName := range a.pidToGroup {
		if groupName == name {
			delete(a.pidToGroup, pid)
		}
	}

	// Update resource accounting
	if err := a.updateResourceAccounting(name, config, false); err != nil {
		a.logger.WithError(err).Warn("Failed to update resource accounting during delete")
	}

	// Remove from tracking
	delete(a.groups, name)
	delete(a.groupConfigs, name)

	// Rebuild and apply RDT configuration without this group
	rdtConfig, err := a.buildCurrentConfig()
	if err != nil {
		return fmt.Errorf("failed to build RDT config: %w", err)
	}

	if err := rdt.SetConfig(rdtConfig, false); err != nil {
		return fmt.Errorf("failed to set RDT config: %w", err)
	}

	a.logger.WithField("group", name).Info("RDT group deleted successfully")
	return nil
}

// buildCurrentConfig builds RDT config from currently tracked groups
func (a *DefaultResourceAllocator) buildCurrentConfig() (*rdt.Config, error) {
	rdtConfig := &rdt.Config{
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

	partition := struct {
		L2Allocation rdt.CatConfig `json:"l2Allocation"`
		L3Allocation rdt.CatConfig `json:"l3Allocation"`
		MBAllocation rdt.MbaConfig `json:"mbAllocation"`
		Classes      map[string]struct {
			L2Allocation rdt.CatConfig         `json:"l2Allocation"`
			L3Allocation rdt.CatConfig         `json:"l3Allocation"`
			MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
			Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
		} `json:"classes"`
	}{
		L3Allocation: rdt.CatConfig{
			"all": rdt.CacheIdCatConfig{
				Unified: "0xfff",
			},
		},
		MBAllocation: rdt.MbaConfig{
			"all": []rdt.MbProportion{"100%"},
		},
		Classes: make(map[string]struct {
			L2Allocation rdt.CatConfig         `json:"l2Allocation"`
			L3Allocation rdt.CatConfig         `json:"l3Allocation"`
			MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
			Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
		}),
	}

	// Add all existing groups
	for groupName, config := range a.groupConfigs {
		class, err := a.buildClassConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failed to build config for group %s: %w", groupName, err)
		}
		partition.Classes[groupName] = class
	}

	rdtConfig.Partitions["default"] = partition
	return rdtConfig, nil
}

func (a *DefaultResourceAllocator) AssignContainer(pid int, groupName string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.initialized {
		return fmt.Errorf("allocator not initialized")
	}

	// Check if group exists
	group, exists := a.groups[groupName]
	if !exists {
		return fmt.Errorf("group %s does not exist", groupName)
	}

	// Assign PID to group
	pidStr := fmt.Sprintf("%d", pid)
	if err := group.AddPids(pidStr); err != nil {
		return fmt.Errorf("failed to assign PID %d to group %s: %w", pid, groupName, err)
	}

	// Update tracking
	a.pidToGroup[pid] = groupName

	a.logger.WithFields(logrus.Fields{
		"pid":   pid,
		"group": groupName,
	}).Debug("Container assigned to RDT group")

	return nil
}

func (a *DefaultResourceAllocator) GetContainerGroup(pid int) (string, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	groupName, exists := a.pidToGroup[pid]
	if !exists {
		return "", fmt.Errorf("PID %d not tracked", pid)
	}

	return groupName, nil
}

func (a *DefaultResourceAllocator) GetAvailableResources() map[int]*SocketResources {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Return copy to prevent external modification
	result := make(map[int]*SocketResources)
	for socket, res := range a.resources {
		resCopy := *res
		result[socket] = &resCopy
	}

	return result
}

func (a *DefaultResourceAllocator) ResetToDefault() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.initialized {
		return fmt.Errorf("allocator not initialized")
	}

	a.logger.Info("Resetting all RDT allocations to default")

	// Clear all groups
	for name := range a.groupConfigs {
		a.logger.WithField("group", name).Debug("Deleting group")
	}

	// Reset to default configuration
	defaultConfig := &rdt.Config{
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

	partition := struct {
		L2Allocation rdt.CatConfig `json:"l2Allocation"`
		L3Allocation rdt.CatConfig `json:"l3Allocation"`
		MBAllocation rdt.MbaConfig `json:"mbAllocation"`
		Classes      map[string]struct {
			L2Allocation rdt.CatConfig         `json:"l2Allocation"`
			L3Allocation rdt.CatConfig         `json:"l3Allocation"`
			MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
			Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
		} `json:"classes"`
	}{
		L3Allocation: rdt.CatConfig{
			"all": rdt.CacheIdCatConfig{
				Unified: "0xfff",
			},
		},
		MBAllocation: rdt.MbaConfig{
			"all": []rdt.MbProportion{"100%"},
		},
		Classes: make(map[string]struct {
			L2Allocation rdt.CatConfig         `json:"l2Allocation"`
			L3Allocation rdt.CatConfig         `json:"l3Allocation"`
			MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
			Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
		}),
	}

	defaultConfig.Partitions["default"] = partition

	if err := rdt.SetConfig(defaultConfig, true); err != nil {
		return fmt.Errorf("failed to reset RDT config: %w", err)
	}

	// Clear internal state
	a.groups = make(map[string]rdt.CtrlGroup)
	a.groupConfigs = make(map[string]GroupConfig)
	a.pidToGroup = make(map[int]string)
	a.initializeResourceTracking()

	a.logger.Info("RDT allocations reset to default successfully")
	return nil
}

func (a *DefaultResourceAllocator) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.initialized {
		return nil
	}

	a.logger.Info("Closing RDT Resource Allocator")

	// Reset to default on close
	a.initialized = false
	return nil
}
