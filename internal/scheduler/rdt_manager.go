package scheduler

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

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
	L3CachePercent      float64
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
		"rdt_supported": hostConfig.RDT.Supported,
		"l3_cache_mb":   hostConfig.L3Cache.SizeMB,
		"cache_ways":    hostConfig.L3Cache.WaysPerCache,
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
		"class_name":            className,
		"l3_cache_percent":      l3CachePercent,
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
		"class_name":            className,
		"l3_cache_percent":      l3CachePercent,
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
		"class_name":     className,
		"total_ways":     totalWays,
		"allocated_ways": allocatedWays,
		"ways_mask_hex":  waysMaskHex,
		"cache_percent":  l3CachePercent,
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
		"class_name":            className,
		"l3_cache_percent":      config.L3CachePercent,
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

// Helper functions for probe isolation

type resctrlInfo struct {
	L3Ways int
}

func readResctrlInfo() (*resctrlInfo, error) {
	cbmMaskPath := "/sys/fs/resctrl/info/L3/cbm_mask"
	data, err := os.ReadFile(cbmMaskPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read L3 CBM mask: %w", err)
	}

	var cbmMask uint64
	if _, err := fmt.Sscanf(string(data), "%x", &cbmMask); err != nil {
		return nil, fmt.Errorf("failed to parse CBM mask: %w", err)
	}

	ways := 0
	for cbmMask > 0 {
		ways += int(cbmMask & 1)
		cbmMask >>= 1
	}

	return &resctrlInfo{L3Ways: ways}, nil
}

// parseCacheWayRange converts a way range string (e.g., "0-2") to a hex bitmask
func parseCacheWayRange(wayRange string, totalWays int) (string, error) {
	var start, end int
	n, err := fmt.Sscanf(wayRange, "%d-%d", &start, &end)
	if err != nil || n != 2 {
		return "", fmt.Errorf("invalid cache way range format '%s', expected 'start-end' (e.g., '0-5')", wayRange)
	}

	if start < 0 || end >= totalWays || start > end {
		return "", fmt.Errorf("invalid cache way range %d-%d, must be within 0-%d and start <= end", start, end, totalWays-1)
	}

	// Create contiguous bitmask
	mask := uint64(0)
	for i := start; i <= end; i++ {
		mask |= (1 << i)
	}

	return fmt.Sprintf("0x%x", mask), nil
}

// calculateComplementaryWays calculates the complementary way range for "others" group
func calculateComplementaryWays(probeWays string, totalWays int) (string, error) {
	var probeStart, probeEnd int
	n, err := fmt.Sscanf(probeWays, "%d-%d", &probeStart, &probeEnd)
	if err != nil || n != 2 {
		return "", fmt.Errorf("invalid probe way range: %s", probeWays)
	}

	if probeEnd >= totalWays-1 {
		return "", fmt.Errorf("probe ways leave no space for others")
	}

	othersStart := probeEnd + 1
	othersEnd := totalWays - 1

	return fmt.Sprintf("%d-%d", othersStart, othersEnd), nil
}

// ProbeIsolationManager handles RDT isolation for probe operations
type ProbeIsolationManager struct {
	logger           *logrus.Logger
	probeClassName   string
	othersClassName  string
	isActive         bool
	probeCgroupPaths []string // Cgroup paths for containers in probe class
	otherCgroupPaths []string // Cgroup paths for containers in others class
	syncTicker       *time.Ticker
	syncStopChan     chan struct{}
}

func NewProbeIsolationManager(logger *logrus.Logger) *ProbeIsolationManager {
	return &ProbeIsolationManager{
		logger:           logger,
		probeClassName:   "probe",
		othersClassName:  "others",
		isActive:         false,
		probeCgroupPaths: make([]string, 0),
		otherCgroupPaths: make([]string, 0),
	}
}

// SetupProbeIsolation creates RDT classes for probe isolation
// probeContainerIDs: container IDs for containers in probe class (target + probe container)
// otherContainerIDs: container IDs for containers in others class
func (m *ProbeIsolationManager) SetupProbeIsolation(probeWays string, probeMemBW int, probePIDs []int, otherPIDs []int, probeContainerIDs []string, otherContainerIDs []string) error {
	if m.isActive {
		return fmt.Errorf("probe isolation already active")
	}

	m.logger.WithFields(logrus.Fields{
		"probe_ways":       probeWays,
		"probe_mem_bw":     probeMemBW,
		"probe_pids":       len(probePIDs),
		"other_pids":       len(otherPIDs),
		"probe_containers": len(probeContainerIDs),
		"other_containers": len(otherContainerIDs),
	}).Info("Setting up RDT probe isolation")

	// Build cgroup paths for containers
	m.probeCgroupPaths = make([]string, 0, len(probeContainerIDs))
	for _, containerID := range probeContainerIDs {
		cgroupPath := fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope", containerID)
		m.probeCgroupPaths = append(m.probeCgroupPaths, cgroupPath)
	}

	m.otherCgroupPaths = make([]string, 0, len(otherContainerIDs))
	for _, containerID := range otherContainerIDs {
		cgroupPath := fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope", containerID)
		m.otherCgroupPaths = append(m.otherCgroupPaths, cgroupPath)
	}

	info, err := readResctrlInfo()
	if err != nil {
		return fmt.Errorf("failed to read resctrl info: %w", err)
	}

	m.logger.WithField("total_l3_ways", info.L3Ways).Debug("Detected cache configuration")

	probeMask, err := parseCacheWayRange(probeWays, info.L3Ways)
	if err != nil {
		return fmt.Errorf("failed to parse probe ways: %w", err)
	}

	othersWays, err := calculateComplementaryWays(probeWays, info.L3Ways)
	if err != nil {
		return fmt.Errorf("failed to calculate complementary ways: %w", err)
	}

	othersMask, err := parseCacheWayRange(othersWays, info.L3Ways)
	if err != nil {
		return fmt.Errorf("failed to parse others ways: %w", err)
	}

	othersMemBW := 100 - probeMemBW
	if othersMemBW < 10 {
		othersMemBW = 10
	}

	m.logger.WithFields(logrus.Fields{
		"probe_mask":    probeMask,
		"probe_mem_bw":  probeMemBW,
		"others_ways":   othersWays,
		"others_mask":   othersMask,
		"others_mem_bw": othersMemBW,
	}).Info("Calculated RDT allocation masks")

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
				L3Allocation: rdt.CatConfig{
					"all": rdt.CacheIdCatConfig{
						Unified: "0xfff",
					},
				},
				MBAllocation: rdt.MbaConfig{
					"all": []rdt.MbProportion{"100%"},
				},
				Classes: map[string]struct {
					L2Allocation rdt.CatConfig         `json:"l2Allocation"`
					L3Allocation rdt.CatConfig         `json:"l3Allocation"`
					MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
					Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
				}{
					m.probeClassName: {
						L3Allocation: rdt.CatConfig{
							"all": rdt.CacheIdCatConfig{
								Unified: rdt.CacheProportion(probeMask),
							},
						},
						MBAllocation: rdt.MbaConfig{
							"all": []rdt.MbProportion{
								rdt.MbProportion(fmt.Sprintf("%d%%", probeMemBW)),
							},
						},
					},
					m.othersClassName: {
						L3Allocation: rdt.CatConfig{
							"all": rdt.CacheIdCatConfig{
								Unified: rdt.CacheProportion(othersMask),
							},
						},
						MBAllocation: rdt.MbaConfig{
							"all": []rdt.MbProportion{
								rdt.MbProportion(fmt.Sprintf("%d%%", othersMemBW)),
							},
						},
					},
				},
			},
		},
	}

	if err := rdt.SetConfig(config, false); err != nil {
		return fmt.Errorf("failed to set RDT config for probe isolation: %w", err)
	}

	m.logger.Info("RDT classes created for probe isolation")

	probeClass, ok := rdt.GetClass(m.probeClassName)
	if !ok {
		return fmt.Errorf("probe class not found after creation")
	}

	othersClass, ok := rdt.GetClass(m.othersClassName)
	if !ok {
		return fmt.Errorf("others class not found after creation")
	}

	if len(probePIDs) > 0 {
		probePIDStrs := make([]string, len(probePIDs))
		for i, pid := range probePIDs {
			probePIDStrs[i] = fmt.Sprintf("%d", pid)
		}
		if err := probeClass.AddPids(probePIDStrs...); err != nil {
			return fmt.Errorf("failed to assign PIDs to probe class: %w", err)
		}
		m.logger.WithField("pids", probePIDs).Info("Assigned PIDs to probe RDT class")
	}

	if len(otherPIDs) > 0 {
		otherPIDStrs := make([]string, len(otherPIDs))
		for i, pid := range otherPIDs {
			otherPIDStrs[i] = fmt.Sprintf("%d", pid)
		}
		if err := othersClass.AddPids(otherPIDStrs...); err != nil {
			return fmt.Errorf("failed to assign PIDs to others class: %w", err)
		}
		m.logger.WithField("pids", len(otherPIDs)).Info("Assigned PIDs to others RDT class")
	}

	// Start PID sync ticker to keep RDT classes updated as containers spawn new processes
	m.startPIDSyncTicker()

	m.isActive = true
	return nil
}

// TeardownProbeIsolation restores default RDT configuration
func (m *ProbeIsolationManager) TeardownProbeIsolation() error {
	if !m.isActive {
		return nil
	}

	m.logger.Info("Tearing down RDT probe isolation")

	// Stop PID sync ticker
	m.stopPIDSyncTicker()

	defaultConfig := &rdt.Config{
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
				L3Allocation: rdt.CatConfig{
					"all": rdt.CacheIdCatConfig{
						Unified: "0xfff",
					},
				},
				MBAllocation: rdt.MbaConfig{
					"all": []rdt.MbProportion{"100%"},
				},
				Classes: map[string]struct {
					L2Allocation rdt.CatConfig         `json:"l2Allocation"`
					L3Allocation rdt.CatConfig         `json:"l3Allocation"`
					MBAllocation rdt.MbaConfig         `json:"mbAllocation"`
					Kubernetes   rdt.KubernetesOptions `json:"kubernetes"`
				}{},
			},
		},
	}

	if err := rdt.SetConfig(defaultConfig, true); err != nil {
		return fmt.Errorf("failed to reset RDT config: %w", err)
	}

	m.logger.Info("RDT configuration reset to default")
	m.isActive = false
	return nil
}

// startPIDSyncTicker starts periodic syncing of PIDs from cgroups to RDT classes
func (m *ProbeIsolationManager) startPIDSyncTicker() {
	m.syncStopChan = make(chan struct{})
	m.syncTicker = time.NewTicker(100 * time.Millisecond)

	go func() {
		m.logger.Debug("RDT PID sync ticker started for probe isolation")

		for {
			select {
			case <-m.syncTicker.C:
				if !m.isActive {
					continue
				}
				m.syncPIDsToClasses()
			case <-m.syncStopChan:
				m.logger.Debug("RDT PID sync ticker stopped")
				return
			}
		}
	}()
}

// stopPIDSyncTicker stops the PID sync ticker
func (m *ProbeIsolationManager) stopPIDSyncTicker() {
	if m.syncTicker != nil {
		m.syncTicker.Stop()
		m.syncTicker = nil
	}
	if m.syncStopChan != nil {
		close(m.syncStopChan)
		m.syncStopChan = nil
	}
}

// syncPIDsToClasses reads PIDs from cgroups and syncs them to RDT classes
func (m *ProbeIsolationManager) syncPIDsToClasses() {
	// Sync probe containers
	probeClass, ok := rdt.GetClass(m.probeClassName)
	if ok {
		var allPIDs []string
		for _, cgroupPath := range m.probeCgroupPaths {
			pids := m.readCGroupPIDs(cgroupPath)
			allPIDs = append(allPIDs, pids...)
		}
		if len(allPIDs) > 0 {
			if err := probeClass.AddPids(allPIDs...); err != nil {
				m.logger.WithError(err).Trace("Failed to sync PIDs to probe class")
			}
		}
	}

	// Sync others containers
	othersClass, ok := rdt.GetClass(m.othersClassName)
	if ok {
		var allPIDs []string
		for _, cgroupPath := range m.otherCgroupPaths {
			pids := m.readCGroupPIDs(cgroupPath)
			allPIDs = append(allPIDs, pids...)
		}
		if len(allPIDs) > 0 {
			if err := othersClass.AddPids(allPIDs...); err != nil {
				m.logger.WithError(err).Trace("Failed to sync PIDs to others class")
			}
		}
	}
}

// readCGroupPIDs reads all PIDs from a cgroup
func (m *ProbeIsolationManager) readCGroupPIDs(cgroupPath string) []string {
	cgroupProcsPath := fmt.Sprintf("%s/cgroup.procs", cgroupPath)
	data, err := ioutil.ReadFile(cgroupProcsPath)
	if err != nil {
		m.logger.WithError(err).WithField("path", cgroupProcsPath).Trace("Failed to read cgroup.procs")
		return nil
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	pids := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			pids = append(pids, line)
		}
	}

	return pids
}

func (m *ProbeIsolationManager) IsActive() bool {
	return m.isActive
}

// AddProbeCgroupPath adds a cgroup path to the probe class for PID syncing
func (m *ProbeIsolationManager) AddProbeCgroupPath(cgroupPath string) {
	m.probeCgroupPaths = append(m.probeCgroupPaths, cgroupPath)
	m.logger.WithField("cgroup_path", cgroupPath).Debug("Added cgroup path to probe class")
}
