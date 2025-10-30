package scheduler

import (
	"fmt"
	"strconv"

	"container-bench/internal/host"
	"container-bench/internal/logging"
	"github.com/intel/goresctrl/pkg/rdt"
	"github.com/sirupsen/logrus"
)

// RDTManager provides centralized RDT resource management for schedulers
// This is the single source of truth for creating, configuring, and managing RDT classes
type RDTManager interface {
	// Initialize sets up the RDT manager with host configuration
	Initialize(hostConfig *host.HostConfig) error
	
	// CreateClass creates a new RDT class with specified L3 cache and memory bandwidth allocations
	CreateClass(className string, l3CachePercent float64, memBandwidthPercent float64) error
	
	// UpdateClassAllocation updates the allocation for an existing class
	UpdateClassAllocation(className string, l3CachePercent float64, memBandwidthPercent float64) error
	
	// AssignPIDToClass assigns a process ID to an RDT class
	AssignPIDToClass(pid int, className string) error
	
	// RemovePIDFromClass removes a process ID from its current RDT class
	RemovePIDFromClass(pid int) error
	
	// GetClassForPID returns the current RDT class name for a process ID
	GetClassForPID(pid int) (string, error)
	
	// ListClasses returns all available RDT classes
	ListClasses() []string
	
	// DeleteClass removes an RDT class (only if empty)
	DeleteClass(className string) error
	
	// GetClassAllocation returns the current allocation percentages for a class
	GetClassAllocation(className string) (l3CachePercent float64, memBandwidthPercent float64, error error)
	
	// Cleanup performs cleanup operations
	Cleanup() error
}

// DefaultRDTManager implements the RDTManager interface using goresctrl
type DefaultRDTManager struct {
	logger     *logrus.Logger
	hostConfig *host.HostConfig
	
	// Track PID to class assignments
	pidToClass map[int]string
	
	// Track managed classes and their configurations
	managedClasses map[string]*ClassConfig
}

// ClassConfig stores the configuration for an RDT class
type ClassConfig struct {
	L3CachePercent     float64
	MemBandwidthPercent float64
}

// NewDefaultRDTManager creates a new RDT manager instance
func NewDefaultRDTManager() *DefaultRDTManager {
	return &DefaultRDTManager{
		logger:         logging.GetSchedulerLogger(),
		pidToClass:     make(map[int]string),
		managedClasses: make(map[string]*ClassConfig),
	}
}

func (rm *DefaultRDTManager) Initialize(hostConfig *host.HostConfig) error {
	rm.logger.Info("Initializing RDT manager")
	
	if hostConfig == nil {
		return fmt.Errorf("host configuration is required")
	}
	
	rm.hostConfig = hostConfig
	
	if !hostConfig.RDT.Supported {
		return fmt.Errorf("RDT is not supported on this system")
	}
	
	rm.logger.WithFields(logrus.Fields{
		"rdt_supported":      hostConfig.RDT.Supported,
		"l3_cache_mb":        hostConfig.L3Cache.SizeMB,
		"cache_ways":         hostConfig.L3Cache.WaysPerCache,
	}).Info("RDT manager initialized successfully")
	
	return nil
}

func (rm *DefaultRDTManager) CreateClass(className string, l3CachePercent float64, memBandwidthPercent float64) error {
	if rm.hostConfig == nil {
		return fmt.Errorf("RDT manager not initialized")
	}
	
	// Validate inputs
	if l3CachePercent < 0 || l3CachePercent > 100 {
		return fmt.Errorf("L3 cache percentage must be between 0 and 100")
	}
	if memBandwidthPercent < 0 || memBandwidthPercent > 100 {
		return fmt.Errorf("memory bandwidth percentage must be between 0 and 100")
	}
	
	// Check if class already exists
	if _, exists := rm.managedClasses[className]; exists {
		return fmt.Errorf("class %s already exists", className)
	}
	
	rm.logger.WithFields(logrus.Fields{
		"class_name":           className,
		"l3_cache_percent":     l3CachePercent,
		"mem_bandwidth_percent": memBandwidthPercent,
	}).Info("Creating RDT class")
	
	// Create the RDT configuration
	if err := rm.applyRDTConfiguration(className, l3CachePercent, memBandwidthPercent); err != nil {
		return fmt.Errorf("failed to apply RDT configuration: %v", err)
	}
	
	// Store the class configuration
	rm.managedClasses[className] = &ClassConfig{
		L3CachePercent:      l3CachePercent,
		MemBandwidthPercent: memBandwidthPercent,
	}
	
	rm.logger.WithField("class_name", className).Info("RDT class created successfully")
	return nil
}

func (rm *DefaultRDTManager) UpdateClassAllocation(className string, l3CachePercent float64, memBandwidthPercent float64) error {
	if rm.hostConfig == nil {
		return fmt.Errorf("RDT manager not initialized")
	}
	
	// Check if class exists
	if _, exists := rm.managedClasses[className]; !exists {
		return fmt.Errorf("class %s does not exist", className)
	}
	
	rm.logger.WithFields(logrus.Fields{
		"class_name":           className,
		"l3_cache_percent":     l3CachePercent,
		"mem_bandwidth_percent": memBandwidthPercent,
	}).Info("Updating RDT class allocation")
	
	// Apply the updated configuration
	if err := rm.applyRDTConfiguration(className, l3CachePercent, memBandwidthPercent); err != nil {
		return fmt.Errorf("failed to update RDT configuration: %v", err)
	}
	
	// Update stored configuration
	rm.managedClasses[className].L3CachePercent = l3CachePercent
	rm.managedClasses[className].MemBandwidthPercent = memBandwidthPercent
	
	return nil
}

func (rm *DefaultRDTManager) applyRDTConfiguration(className string, l3CachePercent float64, memBandwidthPercent float64) error {
	// Calculate actual cache ways from percentage
	totalWays := rm.hostConfig.L3Cache.WaysPerCache
	allocatedWays := int(float64(totalWays) * l3CachePercent / 100.0)
	if allocatedWays < 1 {
		allocatedWays = 1 // Ensure at least 1 way
	}
	if allocatedWays > int(totalWays-1) {
		allocatedWays = int(totalWays - 1) // Leave at least 1 way for others
	}
	
	// Create a bitmask for the allocated ways
	// For example, if allocatedWays = 1, create mask "1" (first way)
	// If allocatedWays = 2, create mask "3" (first two ways: 11 in binary)
	waysMask := (1 << allocatedWays) - 1
	waysMaskHex := fmt.Sprintf("%x", waysMask)
	
	rm.logger.WithFields(logrus.Fields{
		"class_name":      className,
		"total_ways":      totalWays,
		"allocated_ways":  allocatedWays,
		"ways_mask_hex":   waysMaskHex,
		"cache_percent":   l3CachePercent,
	}).Info("Creating RDT class with cache way allocation")
	
	// Build a simple RDT configuration that just creates the class
	config := &rdt.Config{
		Partitions: map[string]struct {
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
			"default": {
				L3Allocation: make(rdt.CatConfig),
				MBAllocation: make(rdt.MbaConfig),
				Classes: map[string]struct {
					L2Allocation rdt.CatConfig         `json:"l2Allocation"`
					L3Allocation rdt.CatConfig         `json:"l3Allocation"`
					MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
					Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
				}{
					className: {
						L3Allocation: make(rdt.CatConfig),
						MBAllocation: make(rdt.MbaConfig),
					},
				},
			},
		},
	}
	
	// Configure L3 cache allocation for each cache ID using hex bitmask
	for _, cacheID := range rm.hostConfig.L3Cache.CacheIDs {
		cacheIDStr := strconv.FormatUint(cacheID, 10)
		
		// For the class, use the calculated ways mask
		config.Partitions["default"].Classes[className].L3Allocation[cacheIDStr] = rdt.CacheIdCatConfig{
			Unified: rdt.CacheProportion(waysMaskHex),
		}
		
		// Set memory bandwidth allocation
		mbPercent := int(memBandwidthPercent)
		if mbPercent < 10 {
			mbPercent = 10 // Minimum 10%
		}
		if mbPercent > 100 {
			mbPercent = 100
		}
		
		config.Partitions["default"].Classes[className].MBAllocation[cacheIDStr] = rdt.CacheIdMbaConfig{
			rdt.MbProportion(fmt.Sprintf("%d%%", mbPercent)),
		}
	}
	
	// Apply the configuration
	if err := rdt.SetConfig(config, false); err != nil {
		return fmt.Errorf("failed to set RDT configuration: %w", err)
	}
	
	rm.logger.WithFields(logrus.Fields{
		"class_name":               className,
		"l3_cache_ways":            allocatedWays,
		"memory_bandwidth_percent": int(memBandwidthPercent),
	}).Info("Successfully applied RDT configuration")
	
	return nil
}

func (rm *DefaultRDTManager) AssignPIDToClass(pid int, className string) error {
	if rm.hostConfig == nil {
		return fmt.Errorf("RDT manager not initialized")
	}
	
	// Check if class exists
	if _, exists := rm.managedClasses[className]; !exists {
		return fmt.Errorf("class %s does not exist", className)
	}
	
	// Get the RDT control group
	ctrlGroup, exists := rdt.GetClass(className)
	if !exists {
		return fmt.Errorf("RDT class %s not found in system", className)
	}
	
	// Assign PID to the class
	pidStr := strconv.Itoa(pid)
	if err := ctrlGroup.AddPids(pidStr); err != nil {
		return fmt.Errorf("failed to assign PID %d to class %s: %v", pid, className, err)
	}
	
	// Track the assignment
	rm.pidToClass[pid] = className
	
	rm.logger.WithFields(logrus.Fields{
		"pid":        pid,
		"class_name": className,
	}).Info("Assigned PID to RDT class")
	
	return nil
}

func (rm *DefaultRDTManager) RemovePIDFromClass(pid int) error {
	className, exists := rm.pidToClass[pid]
	if !exists {
		return fmt.Errorf("PID %d is not assigned to any class", pid)
	}
	
	// Get the RDT control group
	if _, exists := rdt.GetClass(className); !exists {
		// Clean up our tracking even if class doesn't exist
		delete(rm.pidToClass, pid)
		return nil
	}
	
	// Remove PID from the class (assign to default class)
	if defaultClass, exists := rdt.GetClass("system/default"); exists {
		pidStr := strconv.Itoa(pid)
		if err := defaultClass.AddPids(pidStr); err != nil {
			rm.logger.WithError(err).WithFields(logrus.Fields{
				"pid":        pid,
				"class_name": className,
			}).Warn("Failed to move PID to default class")
		}
	}
	
	// Remove from tracking
	delete(rm.pidToClass, pid)
	
	rm.logger.WithFields(logrus.Fields{
		"pid":        pid,
		"class_name": className,
	}).Info("Removed PID from RDT class")
	
	return nil
}

func (rm *DefaultRDTManager) GetClassForPID(pid int) (string, error) {
	if className, exists := rm.pidToClass[pid]; exists {
		return className, nil
	}
	return "", fmt.Errorf("PID %d is not assigned to any managed class", pid)
}

func (rm *DefaultRDTManager) ListClasses() []string {
	classes := make([]string, 0, len(rm.managedClasses))
	for className := range rm.managedClasses {
		classes = append(classes, className)
	}
	return classes
}

func (rm *DefaultRDTManager) DeleteClass(className string) error {
	// Check if class exists
	config, exists := rm.managedClasses[className]
	if !exists {
		return fmt.Errorf("class %s does not exist", className)
	}
	
	// Check if any PIDs are still assigned
	for pid, assignedClass := range rm.pidToClass {
		if assignedClass == className {
			return fmt.Errorf("cannot delete class %s: PID %d is still assigned", className, pid)
		}
	}
	
	// Remove from managed classes
	delete(rm.managedClasses, className)
	
	rm.logger.WithFields(logrus.Fields{
		"class_name":           className,
		"l3_cache_percent":     config.L3CachePercent,
		"mem_bandwidth_percent": config.MemBandwidthPercent,
	}).Info("Deleted RDT class")
	
	return nil
}

func (rm *DefaultRDTManager) GetClassAllocation(className string) (float64, float64, error) {
	config, exists := rm.managedClasses[className]
	if !exists {
		return 0, 0, fmt.Errorf("class %s does not exist", className)
	}
	
	return config.L3CachePercent, config.MemBandwidthPercent, nil
}

func (rm *DefaultRDTManager) Cleanup() error {
	rm.logger.Info("Cleaning up RDT manager")
	
	// Move all managed PIDs back to default class
	for pid := range rm.pidToClass {
		if err := rm.RemovePIDFromClass(pid); err != nil {
			rm.logger.WithError(err).WithField("pid", pid).Warn("Failed to remove PID during cleanup")
		}
	}
	
	// Clear managed classes
	for className := range rm.managedClasses {
		delete(rm.managedClasses, className)
	}
	
	rm.logger.Info("RDT manager cleanup completed")
	return nil
}
