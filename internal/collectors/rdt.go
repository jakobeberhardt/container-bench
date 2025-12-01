package collectors

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"

	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"

	"github.com/intel/goresctrl/pkg/rdt"
	"github.com/sirupsen/logrus"
)

type RDTCollector struct {
	pid          int
	pidStr       string
	cgroupPath   string // Path to container's cgroup
	monGroupName string
	className    string
	rdtEnabled   bool
	config       *config.RDTConfig
	logger       *logrus.Logger
	hostConfig   *host.HostConfig

	// RDT monitoring group for this container
	monGroup  rdt.MonGroup
	ctrlGroup rdt.CtrlGroup

	// PID sync ticker
	syncInterval time.Duration
	syncTicker   *time.Ticker
	syncStopChan chan struct{}
	isClosing    bool // Flag to indicate shutdown in progress

	// Mutex for thread-safe access to monGroup, ctrlGroup, className
	mu sync.RWMutex

	lastBandwidthTotal uint64
	lastBandwidthLocal uint64
	firstCollection    bool
}

// newRDTCollectorBase creates the base RDT collector (internal function)
func newRDTCollectorBase(pid int, cgroupPath string) (*RDTCollector, error) {
	logger := logging.GetLogger()

	// Check if RDT is supported and initialized
	if !rdt.MonSupported() {
		return nil, fmt.Errorf("RDT monitoring not supported on this system")
	}

	// Get host configuration
	// TODO: Check the host info implementation
	hostConfig, err := host.GetHostConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get host configuration: %v", err)
	}

	pidStr := strconv.Itoa(pid)

	// Create unique monitoring group name for this container
	monGroupName := fmt.Sprintf("container-bench-mon-%d", pid)

	// Find which RDT class this PID belongs to
	ctrlGroup, className, err := findRDTClassForPID(pidStr)
	if err != nil {
		logger.WithError(err).WithField("pid", pid).Warn("Could not find RDT class for PID, using default")
		// Fall back to default class
		ctrlGroup, exists := rdt.GetClass("system/default")
		if !exists {
			// Try to get any available control group
			classes := rdt.GetClasses()
			if len(classes) == 0 {
				return nil, fmt.Errorf("no RDT control groups available")
			}
			ctrlGroup = classes[0]
			className = ctrlGroup.Name()
			logger.WithField("class", className).Warn("Using first available RDT class instead of system/default")
		} else {
			className = "system/default"
		}
	}

	// Create monitoring group for this container
	monGroup, err := ctrlGroup.CreateMonGroup(monGroupName, map[string]string{
		"container-bench": "true",
		"pid":             pidStr,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create RDT monitoring group: %v", err)
	}

	logger.WithFields(logrus.Fields{
		"pid":         pid,
		"mon_group":   monGroupName,
		"ctrl_group":  className,
		"cgroup_path": cgroupPath,
	}).Debug("RDT monitoring initialized for container")

	collector := &RDTCollector{
		pid:             pid,
		pidStr:          pidStr,
		cgroupPath:      cgroupPath,
		monGroupName:    monGroupName,
		className:       className,
		rdtEnabled:      true,
		logger:          logger,
		hostConfig:      hostConfig,
		monGroup:        monGroup,
		ctrlGroup:       ctrlGroup,
		firstCollection: true,
	}

	// Sync all PIDs from the container's cgroup to the monitoring group
	if err := collector.syncCGroupPIDs(); err != nil {
		logger.WithError(err).WithField("pid", pid).Warn("Initial cgroup PID sync failed")
	}

	return collector, nil
}

// searches all RDT classes to find which one contains the given PID
func findRDTClassForPID(pidStr string) (rdt.CtrlGroup, string, error) {
	classes := rdt.GetClasses()

	for _, class := range classes {
		pids, err := class.GetPids()
		if err != nil {
			continue // Skip this class if we cant get PIDs
		}

		// Check if our PID is in this class
		for _, classPid := range pids {
			if classPid == pidStr {
				return class, class.Name(), nil
			}
		}
	}

	return nil, "", fmt.Errorf("PID %s not found in any RDT class", pidStr)
}

// allocationInfo holds parsed allocation data from resctrl schemata
type allocationInfo struct {
	l3BitmaskPerSocket  map[int]string
	l3WaysPerSocket     map[int]uint64
	mbaPercentPerSocket map[int]uint64
	l3AllocationString  string
	mbaAllocationString string
}

// Reads allocation information directly from the resctrl filesystem
func (rc *RDTCollector) readAllocationFromResctrl() (*allocationInfo, error) {
	className := rc.ctrlGroup.Name()
	// Construct path to schemata file for this class
	var schemataPath string
	if className == "system/default" {
		schemataPath = "/sys/fs/resctrl/schemata"
	} else {
		schemataPath = fmt.Sprintf("/sys/fs/resctrl/%s/schemata", className)
	}

	// Read the schemata file
	data, err := ioutil.ReadFile(schemataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schemata file %s: %v", schemataPath, err)
	}

	info := &allocationInfo{
		l3BitmaskPerSocket:  make(map[int]string),
		l3WaysPerSocket:     make(map[int]uint64),
		mbaPercentPerSocket: make(map[int]uint64),
	}

	// Parse allocation information from schemata
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "L3:") {
			info.l3AllocationString = line
			if err := rc.parseL3Allocation(line, info); err != nil {
				rc.logger.WithError(err).Debug("Failed to parse L3 allocation")
			}
		} else if strings.HasPrefix(line, "MB:") {
			info.mbaAllocationString = line
			if err := rc.parseMBAAllocation(line, info); err != nil {
				rc.logger.WithError(err).Debug("Failed to parse MBA allocation")
			}
		}
	}

	if len(info.l3BitmaskPerSocket) == 0 && len(info.mbaPercentPerSocket) == 0 {
		return nil, fmt.Errorf("no L3 or MBA allocation found in schemata")
	}

	return info, nil
}

// parses the L3 allocation line and populates per-socket allocation info
// Format: "L3:0=fff;1=fff" where 0,1 are socket IDs and fff is hex bitmask
func (rc *RDTCollector) parseL3Allocation(l3Line string, info *allocationInfo) error {
	// Remove "L3:" prefix
	allocStr := strings.TrimPrefix(l3Line, "L3:")

	// Split by cache domains: "0=fff;1=fff"
	domains := strings.Split(allocStr, ";")
	if len(domains) == 0 {
		return fmt.Errorf("no cache domains found")
	}

	for _, domain := range domains {
		domain = strings.TrimSpace(domain)
		if domain == "" {
			continue
		}

		// Parse "0=fff" format
		parts := strings.Split(domain, "=")
		if len(parts) != 2 {
			rc.logger.WithField("domain", domain).Debug("Skipping invalid domain format")
			continue
		}

		// Parse socket ID
		socketID, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			rc.logger.WithError(err).WithField("socket_id", parts[0]).Debug("Failed to parse socket ID")
			continue
		}

		// Parse the bitmask
		bitmaskStr := strings.TrimSpace(parts[1])
		maskValue, err := strconv.ParseUint(bitmaskStr, 16, 64)
		if err != nil {
			rc.logger.WithError(err).WithField("bitmask", bitmaskStr).Debug("Failed to parse bitmask")
			continue
		}

		// Store bitmask and calculate ways
		info.l3BitmaskPerSocket[socketID] = bitmaskStr
		info.l3WaysPerSocket[socketID] = countSetBits(maskValue)
	}

	if len(info.l3BitmaskPerSocket) == 0 {
		return fmt.Errorf("no valid L3 allocations parsed")
	}

	return nil
}

// parses the MBA (Memory Bandwidth Allocation) line and populates per-socket MBA info
// Format: "MB:0=100;1=100" where 0,1 are socket IDs and 100 is throttle percentage
func (rc *RDTCollector) parseMBAAllocation(mbaLine string, info *allocationInfo) error {
	// Remove "MB:" prefix
	allocStr := strings.TrimPrefix(mbaLine, "MB:")

	// Split by domains: "0=100;1=100"
	domains := strings.Split(allocStr, ";")
	if len(domains) == 0 {
		return fmt.Errorf("no MBA domains found")
	}

	for _, domain := range domains {
		domain = strings.TrimSpace(domain)
		if domain == "" {
			continue
		}

		// Parse "0=100" format
		parts := strings.Split(domain, "=")
		if len(parts) != 2 {
			rc.logger.WithField("domain", domain).Debug("Skipping invalid MBA domain format")
			continue
		}

		// Parse socket ID
		socketID, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			rc.logger.WithError(err).WithField("socket_id", parts[0]).Debug("Failed to parse MBA socket ID")
			continue
		}

		// Parse the throttle percentage
		throttleStr := strings.TrimSpace(parts[1])
		throttle, err := strconv.ParseUint(throttleStr, 10, 64)
		if err != nil {
			rc.logger.WithError(err).WithField("throttle", throttleStr).Debug("Failed to parse MBA throttle")
			continue
		}

		// Store MBA throttle percentage
		info.mbaPercentPerSocket[socketID] = throttle
	}

	if len(info.mbaPercentPerSocket) == 0 {
		return fmt.Errorf("no valid MBA allocations parsed")
	}

	return nil
}

// countSetBits counts the number of set bits in a uint64
func countSetBits(mask uint64) uint64 {
	count := uint64(0)
	for mask != 0 {
		count += mask & 1
		mask >>= 1
	}
	return count
}

// syncCGroupPIDs reads all PIDs from the container's cgroup and adds them to the RDT monitoring group
func (rc *RDTCollector) syncCGroupPIDs() error {
	// Check if we're shutting down
	rc.mu.RLock()
	if rc.isClosing {
		rc.mu.RUnlock()
		return fmt.Errorf("collector is closing")
	}
	rc.mu.RUnlock()

	if rc.cgroupPath == "" {
		return fmt.Errorf("cgroup path not set")
	}

	rc.mu.RLock()
	monGroup := rc.monGroup
	ctrlGroup := rc.ctrlGroup
	rc.mu.RUnlock()

	if monGroup == nil {
		return fmt.Errorf("monitoring group not initialized")
	}

	if ctrlGroup == nil {
		return fmt.Errorf("control group not initialized")
	}

	pidsBefore, err := monGroup.GetPids()
	if err != nil {
		rc.logger.WithError(err).Trace("Could not get PIDs from monitoring group before sync")
		pidsBefore = []string{} // Continue anyway
	}

	// Read PIDs from the cgroup.procs file
	cgroupProcsPath := fmt.Sprintf("%s/cgroup.procs", rc.cgroupPath)
	data, err := ioutil.ReadFile(cgroupProcsPath)
	if err != nil {
		return fmt.Errorf("failed to read cgroup.procs from %s: %v", cgroupProcsPath, err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 {
		rc.logger.WithField("cgroup_path", rc.cgroupPath).Trace("No PIDs found in cgroup")
		return nil
	}

	// Filter out empty lines and prepare PID list
	pids := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			pids = append(pids, line)
		}
	}

	if len(pids) == 0 {
		return nil
	}

	rc.mu.RLock()
	if rc.isClosing {
		rc.mu.RUnlock()
		return fmt.Errorf("collector is closing")
	}
	rc.mu.RUnlock()

	// PIDs must be added to the control group BEFORE adding to monitoring group
	// The monitoring group is a subdivision within the control group
	// Step 1: Add all PIDs to the parent control group first
	if err := ctrlGroup.AddPids(pids...); err != nil {
		return fmt.Errorf("failed to add PIDs to RDT control group %s: %v", ctrlGroup.Name(), err)
	}

	rc.logger.WithFields(logrus.Fields{
		"pid":        rc.pid,
		"ctrl_group": ctrlGroup.Name(),
		"pids_count": len(pids),
	}).Trace("Added PIDs to RDT control group")

	// Step 2: Now add PIDs to the monitoring group (which is within the control group)
	if err := monGroup.AddPids(pids...); err != nil {
		return fmt.Errorf("failed to add PIDs to RDT monitoring group: %v", err)
	}

	// Get PIDs after sync
	pidsAfter, err := monGroup.GetPids()
	if err != nil {
		rc.logger.WithError(err).Error("Could not get PIDs from monitoring group after sync")
		pidsAfter = []string{}
	}

	rc.logger.WithFields(logrus.Fields{
		"pid":            rc.pid,
		"cgroup_path":    rc.cgroupPath,
		"ctrl_group":     ctrlGroup.Name(),
		"mon_group":      monGroup.Name(),
		"pids_before":    len(pidsBefore),
		"pids_after":     len(pidsAfter),
		"pids_in_cgroup": len(pids),
	}).Trace("Synced cgroup PIDs to RDT monitoring group")

	return nil
}

func (rc *RDTCollector) SyncPIDs() error {
	return rc.syncCGroupPIDs()
}

func (rc *RDTCollector) StartPIDSyncTicker() {
	if rc.syncInterval == 0 {
		rc.logger.WithField("pid", rc.pid).Debug("PID sync ticker disabled (interval is 0)")
		return
	}

	rc.syncStopChan = make(chan struct{})
	rc.syncTicker = time.NewTicker(rc.syncInterval)

	go func() {
		rc.logger.WithFields(logrus.Fields{
			"pid":      rc.pid,
			"interval": rc.syncInterval,
		}).Info("RDT PID sync ticker started")

		defer func() {
			if r := recover(); r != nil {
				rc.logger.WithFields(logrus.Fields{
					"pid":   rc.pid,
					"panic": r,
				}).Error("RDT PID sync ticker panicked, recovering")
			}
		}()

		for {
			select {
			case <-rc.syncTicker.C:
				// Check if we're shutting down before syncing
				rc.mu.RLock()
				isClosing := rc.isClosing
				rc.mu.RUnlock()

				if isClosing {
					rc.logger.WithField("pid", rc.pid).Trace("Skipping PID sync, collector is closing")
					continue
				}

				// Detect and handle class migration before syncing PIDs
				// This ensures child processes are added to the correct RDT class
				// if a scheduler has moved the container to a different class
				if err := rc.detectAndHandleClassMigration(); err != nil {
					rc.mu.RLock()
					isClosing := rc.isClosing
					rc.mu.RUnlock()

					if !isClosing {
						rc.logger.WithError(err).WithField("pid", rc.pid).Trace("Failed to handle class migration in ticker")
					}
				}

				// Now sync all PIDs to the (potentially new) monitoring group
				if err := rc.syncCGroupPIDs(); err != nil {
					rc.mu.RLock()
					isClosing := rc.isClosing
					rc.mu.RUnlock()

					if !isClosing {
						rc.logger.WithError(err).WithField("pid", rc.pid).Trace("Failed to sync PIDs in ticker")
					}
				}
			case <-rc.syncStopChan:
				rc.logger.WithField("pid", rc.pid).Debug("RDT PID sync ticker stopped")
				return
			}
		}
	}()
}

// StopPIDSyncTicker stops the PID sync ticker goroutine
func (rc *RDTCollector) StopPIDSyncTicker() {
	if rc.syncTicker != nil {
		rc.syncTicker.Stop()

		if rc.syncStopChan != nil {
			close(rc.syncStopChan)
		}

		time.Sleep(10 * time.Millisecond)

		rc.syncTicker = nil
		rc.syncStopChan = nil
	}
}

func NewRDTCollector(pid int, cgroupPath string, rdtConfig *config.RDTConfig, syncInterval time.Duration) (*RDTCollector, error) {
	// Create using the base constructor
	// TODO: Make it selective
	collector, err := newRDTCollectorBase(pid, cgroupPath)
	if err != nil {
		return nil, err
	}

	// Set the config for potential future selective collection
	// For now, RDT collects all available metrics regardless of config
	collector.config = rdtConfig
	collector.syncInterval = syncInterval

	return collector, nil
}

// checks if the container has been moved to a different RDT class
func (rc *RDTCollector) detectAndHandleClassMigration() error {
	// Find the current RDT class for this PID
	currentCtrlGroup, currentClassName, err := findRDTClassForPID(rc.pidStr)
	if err != nil {
		// PID might not be in any class (container terminated or not yet assigned)
		rc.logger.WithError(err).WithField("pid", rc.pid).Trace("Could not find RDT class for PID during migration check")
		return nil
	}

	rc.mu.RLock()
	oldClassName := rc.className
	rc.mu.RUnlock()

	if currentClassName == oldClassName {
		// No migration detected
		return nil
	}

	rc.logger.WithFields(logrus.Fields{
		"pid":       rc.pid,
		"old_class": oldClassName,
		"new_class": currentClassName,
		"mon_group": rc.monGroupName,
	}).Info("Migrating monitoring group")

	// Acquire write lock for migration
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Delete the old monitoring group from the old class
	if rc.ctrlGroup != nil && rc.monGroupName != "" {
		if err := rc.ctrlGroup.DeleteMonGroup(rc.monGroupName); err != nil {
			rc.logger.WithError(err).WithFields(logrus.Fields{
				"pid":       rc.pid,
				"old_class": oldClassName,
				"mon_group": rc.monGroupName,
			}).Warn("Failed to delete old monitoring group during migration")
			// Continue anyway - we'll create the new one
		} else {
			rc.logger.WithFields(logrus.Fields{
				"pid":       rc.pid,
				"old_class": oldClassName,
				"mon_group": rc.monGroupName,
			}).Debug("Deleted old monitoring group")
		}
	}

	// Create new monitoring group in the new class
	newMonGroup, err := currentCtrlGroup.CreateMonGroup(rc.monGroupName, map[string]string{
		"container-bench": "true",
		"pid":             rc.pidStr,
	})
	if err != nil {
		rc.logger.WithError(err).WithFields(logrus.Fields{
			"pid":       rc.pid,
			"new_class": currentClassName,
			"mon_group": rc.monGroupName,
		}).Error("Failed to create new monitoring group after migration")
		// Set monGroup to nil to prevent collection attempts
		rc.monGroup = nil
		return fmt.Errorf("failed to create monitoring group in new class %s: %v", currentClassName, err)
	}

	// Update the collector's state
	rc.ctrlGroup = currentCtrlGroup
	rc.className = currentClassName
	rc.monGroup = newMonGroup

	// Reset bandwidth tracking state since we're now in a new monitoring group
	rc.lastBandwidthTotal = 0
	rc.lastBandwidthLocal = 0
	rc.firstCollection = true

	rc.logger.WithFields(logrus.Fields{
		"pid":       rc.pid,
		"new_class": currentClassName,
		"mon_group": rc.monGroupName,
	}).Info("RDT monitoring group successfully migrated to new class")

	// Sync PIDs to the new monitoring group
	rc.mu.Unlock()
	if err := rc.syncCGroupPIDs(); err != nil {
		rc.logger.WithError(err).WithFields(logrus.Fields{
			"pid":       rc.pid,
			"new_class": currentClassName,
		}).Warn("Failed to sync cgroup PIDs to new monitoring group")
	}
	rc.mu.Lock()

	return nil
}

func (rc *RDTCollector) Collect() *dataframe.RDTMetrics {
	if !rc.rdtEnabled {
		return nil
	}

	// Thread-safe read of monGroup
	rc.mu.RLock()
	monGroup := rc.monGroup
	rc.mu.RUnlock()

	if monGroup == nil {
		return nil
	}

	// Check if container has been moved to a different RDT class
	if err := rc.detectAndHandleClassMigration(); err != nil {
		rc.logger.WithError(err).WithField("pid", rc.pid).Warn("Failed to handle RDT class migration")
		// Continue with collection using current monitoring group
	}

	// Re-read monGroup after potential migration
	rc.mu.RLock()
	monGroup = rc.monGroup
	rc.mu.RUnlock()

	// Verify monitoring group is still valid after potential migration
	if monGroup == nil {
		rc.logger.WithField("pid", rc.pid).Warn("No valid monitoring group available after migration check")
		return nil
	}

	// Get monitoring data
	monData := monGroup.GetMonData()
	if monData.L3 == nil {
		rc.logger.WithField("pid", rc.pid).Debug("No L3 monitoring data available")
		return nil
	}

	metrics := &dataframe.RDTMetrics{}

	// Process L3 monitoring data
	rc.processL3MonitoringData(&monData, metrics)

	// Add RDT class information
	className := rc.className
	metrics.RDTClassName = &className
	monGroupName := rc.monGroupName
	metrics.MonGroupName = &monGroupName

	rc.calculateDerivedMetrics(metrics)

	rc.getAllocationInfo(metrics)

	return metrics
}

func (rc *RDTCollector) processL3MonitoringData(monData *rdt.MonData, metrics *dataframe.RDTMetrics) {
	// Initialize per-socket maps
	metrics.L3OccupancyPerSocket = make(map[int]uint64)
	metrics.MemoryBandwidthTotalPerSocket = make(map[int]uint64)
	metrics.MemoryBandwidthLocalPerSocket = make(map[int]uint64)

	// Track totals for delta calculation
	var totalBandwidthTotal uint64
	var totalBandwidthLocal uint64
	socketBandwidthTotal := make(map[int]uint64)
	socketBandwidthLocal := make(map[int]uint64)

	// Iterate through L3 data for all cache IDs (socket IDs)
	for cacheID, leafData := range monData.L3 {
		socketID := int(cacheID)
		rc.logger.WithFields(logrus.Fields{
			"pid":       rc.pid,
			"socket_id": socketID,
		}).Trace("Processing L3 monitoring data")

		if occupancy, exists := leafData["llc_occupancy"]; exists {
			metrics.L3OccupancyPerSocket[socketID] = occupancy

			// Debug log when we see non-zero occupancy
			if occupancy > 0 {
				rc.logger.WithFields(logrus.Fields{
					"pid":       rc.pid,
					"socket_id": socketID,
					"occupancy": occupancy,
				}).Trace("RDT: Non-zero LLC occupancy detected")
			}
		}

		// Memory bandwidth monitoring (cumulative counters)
		if mbmTotal, exists := leafData["mbm_total_bytes"]; exists {
			totalBandwidthTotal += mbmTotal
			socketBandwidthTotal[socketID] = mbmTotal
		}

		if mbmLocal, exists := leafData["mbm_local_bytes"]; exists {
			totalBandwidthLocal += mbmLocal
			socketBandwidthLocal[socketID] = mbmLocal
		}
	}

	if !rc.firstCollection {
		// Compute delta from last collection - total bandwidth has already been aggregated
		// We can't split it back to per-socket, so we only store if there's a single socket
		// For per-socket bandwidth, we need to track per-socket cumulative values
		if totalBandwidthTotal >= rc.lastBandwidthTotal {
			delta := totalBandwidthTotal - rc.lastBandwidthTotal
			if delta > 0 && len(socketBandwidthTotal) == 1 {
				// Single socket - attribute to that socket
				for socketID := range socketBandwidthTotal {
					metrics.MemoryBandwidthTotalPerSocket[socketID] = delta
				}
			}
		}

		if totalBandwidthLocal >= rc.lastBandwidthLocal {
			delta := totalBandwidthLocal - rc.lastBandwidthLocal
			if delta > 0 && len(socketBandwidthLocal) == 1 {
				// Single socket - attribute to that socket
				for socketID := range socketBandwidthLocal {
					metrics.MemoryBandwidthLocalPerSocket[socketID] = delta
				}
			}
		}
	}

	// Update last values for next delta calculation
	rc.lastBandwidthTotal = totalBandwidthTotal
	rc.lastBandwidthLocal = totalBandwidthLocal
	rc.firstCollection = false
}

func (rc *RDTCollector) calculateDerivedMetrics(metrics *dataframe.RDTMetrics) {
	if rc.hostConfig == nil {
		return
	}

	// Calculate per-socket L3 cache utilization percentage
	if len(metrics.L3OccupancyPerSocket) > 0 {
		metrics.L3UtilizationPctPerSocket = make(map[int]float64)
		for socketID, occupancy := range metrics.L3OccupancyPerSocket {
			if occupancy > 0 {
				utilizationPercent := rc.hostConfig.GetL3CacheUtilizationPercent(occupancy)
				metrics.L3UtilizationPctPerSocket[socketID] = utilizationPercent
			}
		}
	}

	// TODO: Calculate per-socket memory bandwidth in MB/s
	// This requires knowing the sampling interval, which we don't have here
	// For now, we'll leave this empty and can add it later if needed
}

// gets allocation information for the container
func (rc *RDTCollector) getAllocationInfo(metrics *dataframe.RDTMetrics) {
	if rc.ctrlGroup == nil {
		return
	}

	// Set the RDT class name
	className := rc.ctrlGroup.Name()
	metrics.RDTClassName = &className

	// Set the monitoring group name
	metrics.MonGroupName = &rc.monGroupName

	// Try to get allocation information from the resctrl filesystem
	if allocInfo, err := rc.readAllocationFromResctrl(); err == nil {
		// Store per-socket allocation details
		if len(allocInfo.l3BitmaskPerSocket) > 0 {
			metrics.L3BitmaskPerSocket = allocInfo.l3BitmaskPerSocket
			metrics.L3WaysPerSocket = allocInfo.l3WaysPerSocket

			// Calculate percentage for each socket
			if rc.hostConfig != nil && rc.hostConfig.L3Cache.WaysPerCache > 0 {
				metrics.L3AllocationPctPerSocket = make(map[int]float64)
				for socketID, ways := range allocInfo.l3WaysPerSocket {
					percentage := float64(ways) / float64(rc.hostConfig.L3Cache.WaysPerCache) * 100.0
					metrics.L3AllocationPctPerSocket[socketID] = percentage
				}
			}
		}

		if len(allocInfo.mbaPercentPerSocket) > 0 {
			metrics.MBAPercentPerSocket = allocInfo.mbaPercentPerSocket
		}

		// Store full allocation strings
		if allocInfo.l3AllocationString != "" {
			metrics.L3AllocationString = &allocInfo.l3AllocationString
		}
		if allocInfo.mbaAllocationString != "" {
			metrics.MBAAllocationString = &allocInfo.mbaAllocationString
		}

		// Set MBA throttle from socket 0
		if throttle, ok := allocInfo.mbaPercentPerSocket[0]; ok {
			metrics.MBAThrottle = &throttle
		}

		rc.logger.WithFields(logrus.Fields{
			"pid":            rc.pid,
			"rdt_class":      className,
			"l3_sockets":     len(allocInfo.l3WaysPerSocket),
			"mba_sockets":    len(allocInfo.mbaPercentPerSocket),
			"l3_allocation":  allocInfo.l3AllocationString,
			"mba_allocation": allocInfo.mbaAllocationString,
		}).Trace("Retrieved RDT allocation information")
	} else {
		rc.logger.WithFields(logrus.Fields{
			"pid":       rc.pid,
			"rdt_class": className,
			"error":     err,
		}).Debug("Could not retrieve allocation information")
	}
}

func (rc *RDTCollector) GetMonGroup() rdt.MonGroup {
	return rc.monGroup
}

func (rc *RDTCollector) GetCtrlGroup() rdt.CtrlGroup {
	return rc.ctrlGroup
}

func (rc *RDTCollector) GetPID() int {
	return rc.pid
}

func (rc *RDTCollector) Close() {
	// Set closing flag first to stop any ongoing operations
	rc.mu.Lock()
	rc.isClosing = true
	rc.mu.Unlock()

	// Stop the PID sync ticker
	rc.StopPIDSyncTicker()

	if rc.rdtEnabled && rc.ctrlGroup != nil && rc.monGroupName != "" {
		// Remove PID from monitoring group
		if rc.monGroup != nil {
			// TODO: Make sure this happens after the scheduler finished
		}

		// Delete the monitoring group
		if err := rc.ctrlGroup.DeleteMonGroup(rc.monGroupName); err != nil {
			rc.logger.WithFields(logrus.Fields{
				"pid":       rc.pid,
				"mon_group": rc.monGroupName,
			}).WithError(err).Warn("Failed to cleanup RDT monitoring group")
		} else {
			rc.logger.WithFields(logrus.Fields{
				"pid":       rc.pid,
				"mon_group": rc.monGroupName,
			}).Info("RDT monitoring group cleaned up")
		}

		rc.rdtEnabled = false

		rc.mu.Lock()
		rc.monGroup = nil
		rc.mu.Unlock()
	}
}
