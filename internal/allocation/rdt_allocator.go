package allocation

import (
	"fmt"
	"strconv"
	"sync"

	"container-bench/internal/logging"

	"github.com/intel/goresctrl/pkg/rdt"
	"github.com/sirupsen/logrus"
)

// DefaultRDTAllocator implements the RDTAllocator interface using goresctrl
// Uses RDT system, no internal PID tracking
type DefaultRDTAllocator struct {
	logger         *logrus.Logger
	initialized    bool
	managedClasses map[string]rdt.CtrlGroup // Classes created by this allocator
	managedConfigs map[string]struct {
		Socket0 *SocketAllocation
		Socket1 *SocketAllocation
	}
	mu sync.Mutex
}

// NewDefaultRDTAllocator creates a new RDT allocator instance
func NewDefaultRDTAllocator() *DefaultRDTAllocator {
	return &DefaultRDTAllocator{
		logger:         logging.GetAccountantLogger(),
		managedClasses: make(map[string]rdt.CtrlGroup),
		managedConfigs: make(map[string]struct {
			Socket0 *SocketAllocation
			Socket1 *SocketAllocation
		}),
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

// Helper: resolve SocketAllocation to CacheProportion (bitmask only)
func (a *DefaultRDTAllocator) resolveL3Allocation(alloc *SocketAllocation) (rdt.CacheProportion, error) {
	if alloc == nil || alloc.L3Bitmask == "" {
		return "", nil
	}
	return rdt.CacheProportion(alloc.L3Bitmask), nil
}

// Helper: resolve memory bandwidth allocation
func resolveMBAllocation(alloc *SocketAllocation) rdt.MbProportion {
	if alloc == nil || alloc.MemBandwidth <= 0 {
		return ""
	}
	return rdt.MbProportion(fmt.Sprintf("%.0f%%", alloc.MemBandwidth))
}

func (a *DefaultRDTAllocator) CreateRDTClass(className string, socket0, socket1 *SocketAllocation) error {
	if !a.initialized {
		return fmt.Errorf("RDT allocator not initialized")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Treat create as upsert: goresctrl SetConfig will create/update.
	a.managedConfigs[className] = struct {
		Socket0 *SocketAllocation
		Socket1 *SocketAllocation
	}{Socket0: socket0, Socket1: socket1}

	if err := a.applyManagedConfigLocked(false); err != nil {
		return fmt.Errorf("failed to create RDT class %s: %w", className, err)
	}

	if ctrlGroup, exists := rdt.GetClass(className); exists {
		a.managedClasses[className] = ctrlGroup
	}

	a.logger.WithField("class_name", className).Debug("RDT class created successfully")
	return nil
}

func (a *DefaultRDTAllocator) UpdateRDTClass(className string, socket0, socket1 *SocketAllocation) error {
	if !a.initialized {
		return fmt.Errorf("RDT allocator not initialized")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Treat update as upsert as well.
	a.managedConfigs[className] = struct {
		Socket0 *SocketAllocation
		Socket1 *SocketAllocation
	}{Socket0: socket0, Socket1: socket1}

	if err := a.applyManagedConfigLocked(false); err != nil {
		return fmt.Errorf("failed to update RDT class %s: %w", className, err)
	}

	if ctrlGroup, exists := rdt.GetClass(className); exists {
		a.managedClasses[className] = ctrlGroup
	}

	a.logger.WithField("class_name", className).Debug("RDT class updated successfully")
	return nil
}

// CreateAllRDTClasses creates all RDT classes at once in a single configuration update
func (a *DefaultRDTAllocator) CreateAllRDTClasses(classes map[string]struct {
	Socket0 *SocketAllocation
	Socket1 *SocketAllocation
}) error {
	if !a.initialized {
		return fmt.Errorf("RDT allocator not initialized")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Replace the managed config set.
	a.managedConfigs = make(map[string]struct {
		Socket0 *SocketAllocation
		Socket1 *SocketAllocation
	}, len(classes))
	for name, cfg := range classes {
		a.managedConfigs[name] = struct {
			Socket0 *SocketAllocation
			Socket1 *SocketAllocation
		}{Socket0: cfg.Socket0, Socket1: cfg.Socket1}
	}

	if err := a.applyManagedConfigLocked(false); err != nil {
		return err
	}

	// Refresh managed class handles.
	a.managedClasses = make(map[string]rdt.CtrlGroup)
	for className := range a.managedConfigs {
		if ctrlGroup, exists := rdt.GetClass(className); exists {
			a.managedClasses[className] = ctrlGroup
		}
	}

	a.logger.WithField("classes_created", len(classes)).Debug("All RDT classes created successfully")
	return nil
	/*
		NOTE: This method historically applied the provided classes directly.
		It now replaces the allocator's managed class set to avoid partial SetConfig updates.
	*/
}

func (a *DefaultRDTAllocator) applyManagedConfigLocked(force bool) error {
	a.logger.WithField("total_classes", len(a.managedConfigs)).Debug("Creating all RDT classes in single configuration")

	configClasses := make(map[string]struct {
		L2Allocation rdt.CatConfig         `json:"l2Allocation"`
		L3Allocation rdt.CatConfig         `json:"l3Allocation"`
		MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
		Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
	}, len(a.managedConfigs))

	for className, classConfig := range a.managedConfigs {
		l3Config := rdt.CatConfig{}
		if classConfig.Socket0 != nil {
			l3Alloc, err := a.resolveL3Allocation(classConfig.Socket0)
			if err != nil {
				return fmt.Errorf("failed to resolve L3 allocation for %s socket0: %w", className, err)
			}
			if l3Alloc != "" {
				l3Config["0"] = rdt.CacheIdCatConfig{Unified: l3Alloc}
			}
		}
		if classConfig.Socket1 != nil {
			l3Alloc, err := a.resolveL3Allocation(classConfig.Socket1)
			if err != nil {
				return fmt.Errorf("failed to resolve L3 allocation for %s socket1: %w", className, err)
			}
			if l3Alloc != "" {
				l3Config["1"] = rdt.CacheIdCatConfig{Unified: l3Alloc}
			}
		}

		mbConfig := rdt.MbaConfig{}
		if classConfig.Socket0 != nil {
			if mbAlloc := resolveMBAllocation(classConfig.Socket0); mbAlloc != "" {
				mbConfig["0"] = rdt.CacheIdMbaConfig{mbAlloc}
			}
		}
		if classConfig.Socket1 != nil {
			if mbAlloc := resolveMBAllocation(classConfig.Socket1); mbAlloc != "" {
				mbConfig["1"] = rdt.CacheIdMbaConfig{mbAlloc}
			}
		}

		configClasses[className] = struct {
			L2Allocation rdt.CatConfig         `json:"l2Allocation"`
			L3Allocation rdt.CatConfig         `json:"l3Allocation"`
			MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
			Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
		}{
			L3Allocation: l3Config,
			MBAllocation: mbConfig,
		}
	}

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
			"": {
				L3Allocation: rdt.CatConfig{
					rdt.CacheIdAll: rdt.CacheIdCatConfig{Unified: rdt.CacheProportion("100%")},
				},
				MBAllocation: rdt.MbaConfig{
					rdt.CacheIdAll: rdt.CacheIdMbaConfig{rdt.MbProportion("100%")},
				},
				Classes: configClasses,
			},
		},
	}

	if err := rdt.SetConfig(config, force); err != nil {
		return fmt.Errorf("failed to create RDT classes: %v", err)
	}

	a.logger.WithField("classes_created", len(a.managedConfigs)).Debug("All RDT classes created successfully")
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

	// Check if PID is already in the target class (query RDT system)
	currentClass, err := a.GetContainerClass(pid)
	if err == nil && currentClass == className {
		// Already in the target class
		a.logger.WithFields(logrus.Fields{
			"pid":   pid,
			"class": className,
		}).Trace("Container already in target RDT class")
		return nil
	}

	// Add to new class (this implicitly moves it from old class)
	if err := ctrlGroup.AddPids(pidStr); err != nil {
		return fmt.Errorf("failed to assign PID %d to RDT class %s: %v", pid, className, err)
	}

	a.logger.WithFields(logrus.Fields{
		"pid":        pid,
		"class_name": className,
		"prev_class": currentClass,
	}).Debug("Container assigned to RDT class")

	return nil
}

func (a *DefaultRDTAllocator) RemoveContainerFromClass(pid int) error {
	if !a.initialized {
		return fmt.Errorf("RDT allocator not initialized")
	}

	// Query RDT system to find current class
	currentClass, err := a.GetContainerClass(pid)
	if err != nil {
		// PID not in any class, nothing to remove
		a.logger.WithField("pid", pid).Trace("PID not in any RDT class, nothing to remove")
		return nil
	}

	// Note: goresctrl doesn't provide a direct "remove PID" method
	// PIDs are typically moved to another class rather than removed
	// Move to the default class
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

	a.logger.WithFields(logrus.Fields{
		"pid":        pid,
		"from_class": currentClass,
		"to_class":   defaultClass.Name(),
	}).Debug("Container moved from RDT class to default")

	return nil
}

func (a *DefaultRDTAllocator) GetContainerClass(pid int) (string, error) {
	if !a.initialized {
		return "", fmt.Errorf("RDT allocator not initialized")
	}

	// Query the RDT system
	pidStr := strconv.Itoa(pid)
	classes := rdt.GetClasses()

	for _, class := range classes {
		pids, err := class.GetPids()
		if err != nil {
			continue // Skip this class if we can't get PIDs
		}

		// Check if our PID is in this class
		for _, classPid := range pids {
			if classPid == pidStr {
				return class.Name(), nil
			}
		}
	}

	return "", fmt.Errorf("container PID %d not found in any RDT class", pid)
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

	a.mu.Lock()
	defer a.mu.Unlock()

	// First, move all PIDs from this class to the default class
	ctrlGroup, exists := rdt.GetClass(className)
	if !exists {
		a.logger.WithField("class", className).Debug("RDT class not found, nothing to delete")
		return nil
	}

	pids, err := ctrlGroup.GetPids()
	if err != nil {
		a.logger.WithError(err).WithField("class", className).Warn("Failed to get PIDs from class")
	} else {
		a.logger.WithFields(logrus.Fields{
			"class": className,
			"pids":  len(pids),
		}).Debug("Moving PIDs out of class before deletion")

		for _, pidStr := range pids {
			pid, err := strconv.Atoi(pidStr)
			if err != nil {
				continue
			}
			if err := a.RemoveContainerFromClass(pid); err != nil {
				a.logger.WithError(err).WithFields(logrus.Fields{
					"pid":   pid,
					"class": className,
				}).Warn("Failed to move PID from class")
			}
		}
	}

	// Remove from managed configs and re-apply config. Use force=true so the deleted class disappears.
	delete(a.managedConfigs, className)
	if err := a.applyManagedConfigLocked(true); err != nil {
		return fmt.Errorf("failed to delete RDT class %s: %w", className, err)
	}
	delete(a.managedClasses, className)

	a.logger.WithField("class", className).Debug("RDT class deleted successfully")
	return nil
}

func (a *DefaultRDTAllocator) Cleanup() error {
	if !a.initialized {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.logger.Debug("Cleaning up RDT allocations")

	// Get list of class names before deleting (to avoid modifying map during iteration)
	classNames := make([]string, 0, len(a.managedConfigs))
	for className := range a.managedConfigs {
		classNames = append(classNames, className)
	}

	// Move all PIDs from managed classes to default
	for _, className := range classNames {
		ctrlGroup, exists := rdt.GetClass(className)
		if !exists {
			continue
		}
		pids, err := ctrlGroup.GetPids()
		if err != nil {
			continue
		}
		for _, pidStr := range pids {
			pid, err := strconv.Atoi(pidStr)
			if err != nil {
				continue
			}
			_ = a.RemoveContainerFromClass(pid)
		}
	}

	// Reset RDT configuration to default (empty custom classes)
	a.managedConfigs = make(map[string]struct {
		Socket0 *SocketAllocation
		Socket1 *SocketAllocation
	})
	if err := a.applyManagedConfigLocked(true); err != nil {
		a.logger.WithError(err).Warn("Failed to reset RDT configuration to default")
		return fmt.Errorf("failed to reset RDT config: %w", err)
	}

	// Clear managed classes
	a.managedClasses = make(map[string]rdt.CtrlGroup)
	a.initialized = false

	a.logger.Debug("RDT allocator cleanup completed successfully")
	return nil
}
