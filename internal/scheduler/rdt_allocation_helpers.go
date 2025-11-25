package scheduler

import (
	"fmt"

	"github.com/intel/goresctrl/pkg/rdt"
	"github.com/sirupsen/logrus"
)

// ProbeIsolationConfig defines the configuration for isolating a probe
type ProbeIsolationConfig struct {
	// Name prefix for the groups (will create <prefix>_probe and <prefix>_others)
	GroupPrefix string

	// Per-socket configuration for probe group
	ProbeL3Ways map[int]int // socket -> number of cache ways
	ProbeMBW    map[int]int // socket -> memory bandwidth percentage

	// Per-socket configuration for others group (if not specified, uses remaining resources)
	OthersL3Ways map[int]int // socket -> number of cache ways (optional)
	OthersMBW    map[int]int // socket -> memory bandwidth percentage (optional)

	// PIDs to assign
	ProbePID  int   // PID of the probe container
	OtherPIDs []int // PIDs of other containers to isolate from probe
}

// CreateProbeIsolation creates two complementary RDT groups for probe isolation:
// 1. <prefix>_probe: exclusive resources for the probe
// 2. <prefix>_others: remaining resources for other containers
// This ensures complete resource partitioning with no overlap
func (a *DefaultResourceAllocator) CreateProbeIsolation(config ProbeIsolationConfig) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.initialized {
		return fmt.Errorf("allocator not initialized")
	}

	probeGroupName := fmt.Sprintf("%s_probe", config.GroupPrefix)
	othersGroupName := fmt.Sprintf("%s_others", config.GroupPrefix)

	a.logger.WithFields(logrus.Fields{
		"probe_group":  probeGroupName,
		"others_group": othersGroupName,
	}).Info("Creating probe isolation groups")

	// Build probe group configuration
	probeConfig := GroupConfig{
		L3Allocation: make(map[int]L3Config),
		MBAllocation: make(map[int]MBConfig),
	}

	for socket, ways := range config.ProbeL3Ways {
		probeConfig.L3Allocation[socket] = L3Config{Ways: ways}
	}

	for socket, mbw := range config.ProbeMBW {
		probeConfig.MBAllocation[socket] = MBConfig{Percentage: mbw}
	}

	// Create probe group first to allocate its resources
	if err := a.createGroupInternal(probeGroupName, probeConfig); err != nil {
		return fmt.Errorf("failed to create probe group: %w", err)
	}

	// Build others group configuration using remaining resources
	othersConfig := GroupConfig{
		L3Allocation: make(map[int]L3Config),
		MBAllocation: make(map[int]MBConfig),
	}

	// For each socket, allocate remaining cache ways
	for socket := range config.ProbeL3Ways {
		res, ok := a.resources[socket]
		if !ok {
			// Rollback probe group
			_ = a.deleteGroupInternal(probeGroupName)
			return fmt.Errorf("socket %d not found", socket)
		}

		// Check if explicit others allocation was specified
		if othersWays, hasExplicit := config.OthersL3Ways[socket]; hasExplicit {
			othersConfig.L3Allocation[socket] = L3Config{Ways: othersWays}
		} else {
			// Use all remaining cache ways
			othersConfig.L3Allocation[socket] = L3Config{Ways: res.AvailableCacheWays}
		}
	}

	// For each socket, allocate remaining memory bandwidth
	for socket := range config.ProbeMBW {
		res, ok := a.resources[socket]
		if !ok {
			// Rollback probe group
			_ = a.deleteGroupInternal(probeGroupName)
			return fmt.Errorf("socket %d not found", socket)
		}

		// Check if explicit others allocation was specified
		if othersMBW, hasExplicit := config.OthersMBW[socket]; hasExplicit {
			othersConfig.MBAllocation[socket] = MBConfig{Percentage: othersMBW}
		} else {
			// Use all remaining bandwidth
			othersConfig.MBAllocation[socket] = MBConfig{Percentage: res.AvailableMBW}
		}
	}

	// Create others group
	if err := a.createGroupInternal(othersGroupName, othersConfig); err != nil {
		// Rollback probe group
		_ = a.deleteGroupInternal(probeGroupName)
		return fmt.Errorf("failed to create others group: %w", err)
	}

	a.logger.WithFields(logrus.Fields{
		"probe_group":  probeGroupName,
		"others_group": othersGroupName,
	}).Info("Probe isolation groups created successfully")

	// Assign PIDs if provided
	if config.ProbePID > 0 {
		if err := a.assignContainerInternal(config.ProbePID, probeGroupName); err != nil {
			a.logger.WithError(err).Warn("Failed to assign probe PID to group")
		}
	}

	for _, pid := range config.OtherPIDs {
		if err := a.assignContainerInternal(pid, othersGroupName); err != nil {
			a.logger.WithError(err).WithField("pid", pid).Warn("Failed to assign other PID to group")
		}
	}

	return nil
}

// DeleteProbeIsolation removes both probe and others groups created by CreateProbeIsolation
func (a *DefaultResourceAllocator) DeleteProbeIsolation(groupPrefix string) error {
	probeGroupName := fmt.Sprintf("%s_probe", groupPrefix)
	othersGroupName := fmt.Sprintf("%s_others", groupPrefix)

	a.logger.WithFields(logrus.Fields{
		"probe_group":  probeGroupName,
		"others_group": othersGroupName,
	}).Info("Deleting probe isolation groups")

	var firstErr error

	// Delete both groups
	if err := a.DeleteGroup(probeGroupName); err != nil {
		a.logger.WithError(err).Warn("Failed to delete probe group")
		firstErr = err
	}

	if err := a.DeleteGroup(othersGroupName); err != nil {
		a.logger.WithError(err).Warn("Failed to delete others group")
		if firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return fmt.Errorf("failed to delete probe isolation groups: %w", firstErr)
	}

	a.logger.Info("Probe isolation groups deleted successfully")
	return nil
}

// createGroupInternal creates a group without acquiring the lock (internal use)
func (a *DefaultResourceAllocator) createGroupInternal(name string, config GroupConfig) error {
	// Check if group already exists
	if _, exists := a.groups[name]; exists {
		return fmt.Errorf("group %s already exists", name)
	}

	a.logger.WithField("group", name).Debug("Creating RDT group (internal)")

	// Validate and prepare configuration
	rdtConfig, err := a.buildRDTConfig(name, config)
	if err != nil {
		return fmt.Errorf("failed to build RDT config: %w", err)
	}

	// Apply configuration
	if err := a.SetConfig(rdtConfig, false); err != nil {
		return fmt.Errorf("failed to set RDT config: %w", err)
	}

	// Get the created class
	cls, ok := a.GetClass(name)
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

	return nil
}

// assignContainerInternal assigns a container without acquiring the lock (internal use)
func (a *DefaultResourceAllocator) assignContainerInternal(pid int, groupName string) error {
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
	}).Debug("Container assigned to RDT group (internal)")

	return nil
}

// GetClass wraps rdt.GetClass for easier mocking in tests
func (a *DefaultResourceAllocator) GetClass(name string) (rdt.CtrlGroup, bool) {
	return rdt.GetClass(name)
}

// SetConfig wraps rdt.SetConfig for easier mocking in tests
func (a *DefaultResourceAllocator) SetConfig(config *rdt.Config, force bool) error {
	return rdt.SetConfig(config, force)
}
