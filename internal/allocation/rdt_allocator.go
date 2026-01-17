package allocation

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"container-bench/internal/logging"
	"container-bench/internal/rdtguard"

	"github.com/intel/goresctrl/pkg/rdt"
	"github.com/sirupsen/logrus"
)

// Implements the RDTAllocator interface using goresctrl.
// Uses the RDT system and does not track PIDs internally.
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

// Creates a new RDT allocator instance.
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
	rdtguard.Lock()
	supported := rdt.MonSupported()
	rdtguard.Unlock()
	if !supported {
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

	rdtguard.Lock()
	ctrlGroup, exists := rdt.GetClass(className)
	rdtguard.Unlock()
	if exists {
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

	rdtguard.Lock()
	ctrlGroup, exists := rdt.GetClass(className)
	rdtguard.Unlock()
	if exists {
		a.managedClasses[className] = ctrlGroup
	}

	a.logger.WithField("class_name", className).Debug("RDT class updated successfully")
	return nil
}

// Creates all RDT classes at once in a single configuration update.
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
		rdtguard.Lock()
		ctrlGroup, exists := rdt.GetClass(className)
		rdtguard.Unlock()
		if exists {
			a.managedClasses[className] = ctrlGroup
		}
	}

	a.logger.WithField("classes_created", len(classes)).Debug("All RDT classes created successfully")
	return nil
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

	rdtguard.Lock()
	err := rdt.SetConfig(config, force)
	rdtguard.Unlock()
	if err != nil {
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
	rdtguard.Lock()
	ctrlGroup, exists := rdt.GetClass(className)
	rdtguard.Unlock()
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

	// Add to new class (this implicitly moves it from old class).
	// IMPORTANT: a docker container has multiple PIDs (entrypoint + exec'ed processes).
	// Moving only the init PID leaves the real workload in the old class (making
	// allocation probes appear to have identical results). We therefore move all
	// PIDs in the container's cgroup where possible.
	pidsToMove := []string{pidStr}
	if cgroupPIDs, err := getCgroupPIDsForPID(pid); err == nil && len(cgroupPIDs) > 0 {
		pidsToMove = cgroupPIDs
	} else if err != nil {
		a.logger.WithError(err).WithField("pid", pid).Trace("Failed to enumerate cgroup PIDs; falling back to single PID")
	}

	var firstErr error
	for _, taskPID := range pidsToMove {
		rdtguard.Lock()
		err = ctrlGroup.AddPids(taskPID)
		rdtguard.Unlock()
		if err != nil {
			// Best-effort: tasks can exit between reading cgroup.procs and moving.
			msg := strings.ToLower(err.Error())
			if strings.Contains(msg, "no such process") || strings.Contains(msg, "not found") {
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
			a.logger.WithError(err).WithFields(logrus.Fields{
				"container_pid": pid,
				"task_pid":      taskPID,
				"class":         className,
			}).Warn("Failed to move task PID into RDT class")
		}
	}
	if firstErr != nil {
		return fmt.Errorf("failed to fully assign PID %d to RDT class %s: %v", pid, className, firstErr)
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
	rdtguard.Lock()
	defaultClass, exists := rdt.GetClass("system/default")
	rdtguard.Unlock()
	if !exists {
		// Use the first available class as fallback
		rdtguard.Lock()
		classes := rdt.GetClasses()
		rdtguard.Unlock()
		if len(classes) > 0 {
			defaultClass = classes[0]
		} else {
			return fmt.Errorf("no default RDT class available")
		}
	}

	pidStr := strconv.Itoa(pid)
	// See AssignContainerToClass for rationale: move all tasks from the container cgroup
	// so future allocations don't get stuck split across classes.
	pidsToMove := []string{pidStr}
	if cgroupPIDs, err := getCgroupPIDsForPID(pid); err == nil && len(cgroupPIDs) > 0 {
		pidsToMove = cgroupPIDs
	} else if err != nil {
		a.logger.WithError(err).WithField("pid", pid).Trace("Failed to enumerate cgroup PIDs for removal; falling back to single PID")
	}

	var firstErr error
	for _, taskPID := range pidsToMove {
		rdtguard.Lock()
		err = defaultClass.AddPids(taskPID)
		rdtguard.Unlock()
		if err != nil {
			msg := strings.ToLower(err.Error())
			if strings.Contains(msg, "no such process") || strings.Contains(msg, "not found") {
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
			a.logger.WithError(err).WithFields(logrus.Fields{
				"container_pid": pid,
				"task_pid":      taskPID,
				"to_class":      defaultClass.Name(),
			}).Warn("Failed to move task PID to default RDT class")
		}
	}
	if firstErr != nil {
		return fmt.Errorf("failed to move PID %d to default class: %v", pid, firstErr)
	}

	a.logger.WithFields(logrus.Fields{
		"pid":        pid,
		"from_class": currentClass,
		"to_class":   defaultClass.Name(),
	}).Debug("Container moved from RDT class to default")

	return nil
}

func getCgroupPIDsForPID(pid int) ([]string, error) {
	// This implementation targets cgroup v2 (unified hierarchy), where /proc/<pid>/cgroup
	// contains a line like: "0::/system.slice/docker-<id>.scope".
	// If we cannot resolve the cgroup path (e.g., cgroup v1), return an error so callers
	// can fall back to moving only the init PID.
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return nil, err
	}

	var relPath string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// cgroup v2: 0::<path>
		if strings.HasPrefix(line, "0::") {
			parts := strings.SplitN(line, ":", 3)
			if len(parts) == 3 {
				relPath = strings.TrimSpace(parts[2])
				break
			}
		}
	}
	if relPath == "" {
		return nil, fmt.Errorf("could not find unified (0::) cgroup path for pid %d", pid)
	}

	// relPath is absolute within the cgroup mount. Ensure we don't accidentally drop the root.
	cgroupDir := filepath.Clean(filepath.Join("/sys/fs/cgroup", relPath))
	procsPath := filepath.Join(cgroupDir, "cgroup.procs")
	procsData, err := os.ReadFile(procsPath)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(string(procsData)), "\n")
	pids := make([]string, 0, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		pids = append(pids, l)
	}
	return pids, nil
}

func (a *DefaultRDTAllocator) GetContainerClass(pid int) (string, error) {
	if !a.initialized {
		return "", fmt.Errorf("RDT allocator not initialized")
	}

	// Query the RDT system
	pidStr := strconv.Itoa(pid)
	rdtguard.Lock()
	classes := rdt.GetClasses()
	rdtguard.Unlock()

	for _, class := range classes {
		rdtguard.Lock()
		pids, err := class.GetPids()
		rdtguard.Unlock()
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

	rdtguard.Lock()
	classes := rdt.GetClasses()
	rdtguard.Unlock()
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
	rdtguard.Lock()
	ctrlGroup, exists := rdt.GetClass(className)
	rdtguard.Unlock()
	if !exists {
		a.logger.WithField("class", className).Debug("RDT class not found, nothing to delete")
		return nil
	}

	rdtguard.Lock()
	pids, err := ctrlGroup.GetPids()
	rdtguard.Unlock()
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
		rdtguard.Lock()
		ctrlGroup, exists := rdt.GetClass(className)
		rdtguard.Unlock()
		if !exists {
			continue
		}
		rdtguard.Lock()
		pids, err := ctrlGroup.GetPids()
		rdtguard.Unlock()
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
