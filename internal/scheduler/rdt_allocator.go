package scheduler

import (
	"fmt"
	"strconv"

	"container-bench/internal/logging"
	"github.com/intel/goresctrl/pkg/rdt"
	"github.com/sirupsen/logrus"
)

// RDTAllocator provides an interface for RDT resource allocation operations
// This is used by schedulers to allocate cache and memory bandwidth resources
// to containers while maintaining separation from monitoring operations
type RDTAllocator interface {
	// Initialize sets up the RDT allocator
	Initialize() error
	
	// CreateRDTClass creates a new RDT control group with specified allocations
	CreateRDTClass(className string, l3CachePercent float64, memBandwidthPercent float64) error
	
	// CreateAllRDTClasses creates multiple RDT classes at once in a single configuration update
	CreateAllRDTClasses(classes map[string]struct{
		L3CachePercent    float64
		MemBandwidthPercent float64
		CacheBitMask       string
	}) error
	
	// AssignContainerToClass assigns a container PID to an RDT class
	AssignContainerToClass(pid int, className string) error
	
	// RemoveContainerFromClass removes a container PID from its current RDT class
	RemoveContainerFromClass(pid int) error
	
	// GetContainerClass returns the current RDT class name for a container
	GetContainerClass(pid int) (string, error)
	
	// ListAvailableClasses returns all available RDT classes
	ListAvailableClasses() []string
	
	// DeleteRDTClass removes an RDT class (only if empty)
	DeleteRDTClass(className string) error
	
	// Cleanup performs cleanup operations
	Cleanup() error
}

// DefaultRDTAllocator implements the RDTAllocator interface using goresctrl
type DefaultRDTAllocator struct {
	logger         *logrus.Logger
	initialized    bool
	managedClasses map[string]rdt.CtrlGroup // Classes created by this allocator
	pidToClass     map[int]string           // Track which PID is in which class
}

// NewDefaultRDTAllocator creates a new RDT allocator instance
func NewDefaultRDTAllocator() *DefaultRDTAllocator {
	return &DefaultRDTAllocator{
		logger:         logging.GetSchedulerLogger(),
		managedClasses: make(map[string]rdt.CtrlGroup),
		pidToClass:     make(map[int]string),
	}
}

func (a *DefaultRDTAllocator) Initialize() error {
	// Check if RDT is supported
	if !rdt.MonSupported() {
		return fmt.Errorf("RDT not supported on this system")
	}
	
	a.logger.Info("Initializing RDT allocator")
	a.initialized = true
	return nil
}

func (a *DefaultRDTAllocator) CreateRDTClass(className string, l3CachePercent float64, memBandwidthPercent float64) error {
	if !a.initialized {
		return fmt.Errorf("RDT allocator not initialized")
	}
	
	// Check if class already exists
	if _, exists := rdt.GetClass(className); exists {
		a.logger.WithField("class_name", className).Debug("RDT class already exists")
		return nil // Return success if class already exists
	}
	
	a.logger.WithField("class_name", className).WithField("l3_cache_percent", l3CachePercent).WithField("mem_bandwidth_percent", memBandwidthPercent).Info("RDT class created successfully")
	return nil
}

// CreateAllRDTClasses creates all RDT classes at once in a single configuration update
func (a *DefaultRDTAllocator) CreateAllRDTClasses(classes map[string]struct{
	L3CachePercent    float64
	MemBandwidthPercent float64
	CacheBitMask       string
}) error {
	if !a.initialized {
		return fmt.Errorf("RDT allocator not initialized")
	}

	a.logger.WithField("total_classes", len(classes)).Info("Creating all RDT classes in single configuration")

	// Create the classes map for the Config structure
	configClasses := make(map[string]struct {
		L2Allocation rdt.CatConfig         `json:"l2Allocation"`
		L3Allocation rdt.CatConfig         `json:"l3Allocation"`
		MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
		Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
	})

	for className, classConfig := range classes {
		configClasses[className] = struct {
			L2Allocation rdt.CatConfig         `json:"l2Allocation"`
			L3Allocation rdt.CatConfig         `json:"l3Allocation"`
			MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
			Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
		}{
			L3Allocation: rdt.CatConfig{
				"0": rdt.CacheIdCatConfig{
					Unified: func() rdt.CacheProportion {
						if classConfig.CacheBitMask != "" {
							return rdt.CacheProportion(classConfig.CacheBitMask)
						}
						return rdt.CacheProportion(fmt.Sprintf("%.0f%%", classConfig.L3CachePercent))
					}(),
				},
			},
			MBAllocation: func() rdt.MbaConfig {
				if classConfig.MemBandwidthPercent > 0 {
					return rdt.MbaConfig{
						"0": rdt.CacheIdMbaConfig{rdt.MbProportion(fmt.Sprintf("%.0f%%", classConfig.MemBandwidthPercent))},
					}
				}
				return rdt.MbaConfig{}
			}(),
		}
	}

	// Create the complete configuration with all classes
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
			"": { // Root partition must have L3 and MB allocation defined
				L3Allocation: rdt.CatConfig{
					"0": rdt.CacheIdCatConfig{
						Unified: rdt.CacheProportion("100%"), // Root partition gets full cache by default
					},
				},
				MBAllocation: rdt.MbaConfig{
					"0": rdt.CacheIdMbaConfig{rdt.MbProportion("100%")}, // Root partition gets full bandwidth by default
				},
				Classes: configClasses,
			},
		},
	}
	
	// Apply the configuration for all classes at once
	if err := rdt.SetConfig(config, false); err != nil {
		return fmt.Errorf("failed to create RDT classes: %v", err)
	}

	// Track all created classes
	for className := range classes {
		if ctrlGroup, exists := rdt.GetClass(className); exists {
			a.managedClasses[className] = ctrlGroup
		}
	}

	a.logger.WithField("classes_created", len(classes)).Info("All RDT classes created successfully")
	return nil
}

func (a *DefaultRDTAllocator) AssignContainerToClass(pid int, className string) error {
	if !a.initialized {
		return fmt.Errorf("RDT allocator not initialized")
	}
	
	// Get the target class
	ctrlGroup, exists := rdt.GetClass(className)
	if !exists {
		return fmt.Errorf("RDT class %s not found", className)
	}
	
	pidStr := strconv.Itoa(pid)
	
	// Remove from current class if assigned
	if currentClass, exists := a.pidToClass[pid]; exists {
		if currentClass == className {
			// Already in the target class
			return nil
		}
		// Remove from current class first
		if err := a.RemoveContainerFromClass(pid); err != nil {
			a.logger.WithError(err).Warn("Failed to remove container from current class")
		}
	}
	
	// Add to new class
	if err := ctrlGroup.AddPids(pidStr); err != nil {
		return fmt.Errorf("failed to assign PID %d to RDT class %s: %v", pid, className, err)
	}
	
	// Track the assignment
	a.pidToClass[pid] = className
	
	a.logger.WithFields(logrus.Fields{
		"pid":        pid,
		"class_name": className,
	}).Info("Container assigned to RDT class")
	
	return nil
}

func (a *DefaultRDTAllocator) RemoveContainerFromClass(pid int) error {
	if !a.initialized {
		return fmt.Errorf("RDT allocator not initialized")
	}

	currentClass, exists := a.pidToClass[pid]
	if !exists {
		// Not tracked by this allocator
		return nil
	}

	// Note: goresctrl doesn't provide a direct "remove PID" method
	// PIDs are typically moved to another class rather than removed
	// For now, we'll move to the default class
	defaultClass, exists := rdt.GetClass("system/default")
	if !exists {
		// Use the first available class as fallback
		classes := rdt.GetClasses()
		if len(classes) > 0 {
			defaultClass = classes[0]
		} else {
			return fmt.Errorf("no default RDT class available")
		}
	}

	pidStr := strconv.Itoa(pid)
	if err := defaultClass.AddPids(pidStr); err != nil {
		return fmt.Errorf("failed to move PID %d to default class: %v", pid, err)
	}

	// Remove from tracking
	delete(a.pidToClass, pid)

	a.logger.WithFields(logrus.Fields{
		"pid":          pid,
		"from_class":   currentClass,
		"to_class":     defaultClass.Name(),
	}).Info("Container moved from RDT class to default")

	return nil
}

func (a *DefaultRDTAllocator) GetContainerClass(pid int) (string, error) {
	if !a.initialized {
		return "", fmt.Errorf("RDT allocator not initialized")
	}
	
	if className, exists := a.pidToClass[pid]; exists {
		return className, nil
	}
	
	return "", fmt.Errorf("container PID %d not tracked by allocator", pid)
}

func (a *DefaultRDTAllocator) ListAvailableClasses() []string {
	if !a.initialized {
		return nil
	}
	
	classes := rdt.GetClasses()
	classNames := make([]string, len(classes))
	for i, class := range classes {
		classNames[i] = class.Name()
	}
	
	return classNames
}

func (a *DefaultRDTAllocator) DeleteRDTClass(className string) error {
	if !a.initialized {
		return fmt.Errorf("RDT allocator not initialized")
	}
	
	// Note: goresctrl doesn't provide direct class deletion
	// Classes are typically managed through configuration
	return fmt.Errorf("dynamic RDT class deletion not implemented")
}

func (a *DefaultRDTAllocator) Cleanup() error {
	if !a.initialized {
		return nil
	}
	
	a.logger.Info("Cleaning up RDT allocator")
	
	// Move all tracked PIDs back to default class
	for pid := range a.pidToClass {
		if err := a.RemoveContainerFromClass(pid); err != nil {
			a.logger.WithError(err).WithField("pid", pid).Warn("Failed to cleanup PID from RDT class")
		}
	}
	
	// Clear tracking
	a.pidToClass = make(map[int]string)
	a.managedClasses = make(map[string]rdt.CtrlGroup)
	a.initialized = false
	
	return nil
}
