package collectors

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/rdtguard"

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
	rdtguard.Lock()
	supported := rdt.MonSupported()
	rdtguard.Unlock()
	if !supported {
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
		rdtguard.Lock()
		ctrlGroup, exists := rdt.GetClass("system/default")
		rdtguard.Unlock()
		if !exists {
			// Try to get any available control group
			rdtguard.Lock()
			classes := rdt.GetClasses()
			rdtguard.Unlock()
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
	rdtguard.Lock()
	monGroup, err := ctrlGroup.CreateMonGroup(monGroupName, map[string]string{
		"container-bench": "true",
		"pid":             pidStr,
	})
	rdtguard.Unlock()
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
	rdtguard.Lock()
	classes := rdt.GetClasses()
	rdtguard.Unlock()

	for _, class := range classes {
		rdtguard.Lock()
		pids, err := class.GetPids()
		rdtguard.Unlock()
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
	if rc.cgroupPath == "" {
		return fmt.Errorf("cgroup path not set")
	}

	// Read PIDs from the cgroup.procs file first (no need to hold the RDT lock).
	// We keep monGroup/ctrlGroup interactions protected by rc.mu to avoid races with
	// class migration (which can delete/recreate mon_groups).

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

	// From here on, hold the RLock so the monitoring group cannot be deleted/recreated
	// concurrently by a class migration (which takes the write lock).
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	if rc.isClosing {
		return fmt.Errorf("collector is closing")
	}
	monGroup := rc.monGroup
	ctrlGroup := rc.ctrlGroup
	if monGroup == nil {
		return fmt.Errorf("monitoring group not initialized")
	}
	if ctrlGroup == nil {
		return fmt.Errorf("control group not initialized")
	}
	ctrlName := ctrlGroup.Name()
	monName := monGroup.Name()

	rdtguard.Lock()
	pidsBefore, err := monGroup.GetPids()
	rdtguard.Unlock()
	if err != nil {
		rc.logger.WithError(err).WithFields(logrus.Fields{
			"pid":        rc.pid,
			"ctrl_group": ctrlName,
			"mon_group":  monName,
		}).Trace("Could not get PIDs from monitoring group before sync")
		pidsBefore = []string{} // Continue anyway
	}

	// PIDs must be added to the control group BEFORE adding to monitoring group
	// The monitoring group is a subdivision within the control group
	// Step 1: Add all PIDs to the parent control group first
	rdtguard.Lock()
	err = ctrlGroup.AddPids(pids...)
	rdtguard.Unlock()
	if err != nil {
		return fmt.Errorf("failed to add PIDs to RDT control group %s: %v", ctrlName, err)
	}

	rc.logger.WithFields(logrus.Fields{
		"pid":        rc.pid,
		"ctrl_group": ctrlName,
		"pids_count": len(pids),
	}).Trace("Added PIDs to RDT control group")

	// Step 2: Now add PIDs to the monitoring group (which is within the control group)
	rdtguard.Lock()
	err = monGroup.AddPids(pids...)
	rdtguard.Unlock()
	if err != nil {
		return fmt.Errorf("failed to add PIDs to RDT monitoring group: %v", err)
	}

	// Get PIDs after sync
	rdtguard.Lock()
	pidsAfter, err := monGroup.GetPids()
	rdtguard.Unlock()
	if err != nil {
		// This can happen benignly if the underlying mon_group disappears (e.g., external
		// cleanup or class churn). Avoid spamming Error logs.
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "no such file") || strings.Contains(msg, "not found") {
			rc.logger.WithError(err).WithFields(logrus.Fields{
				"pid":        rc.pid,
				"ctrl_group": ctrlName,
				"mon_group":  monName,
			}).Debug("Could not get PIDs from monitoring group after sync")
		} else {
			rc.logger.WithError(err).WithFields(logrus.Fields{
				"pid":        rc.pid,
				"ctrl_group": ctrlName,
				"mon_group":  monName,
			}).Warn("Could not get PIDs from monitoring group after sync")
		}
		pidsAfter = []string{}
	}

	rc.logger.WithFields(logrus.Fields{
		"pid":            rc.pid,
		"cgroup_path":    rc.cgroupPath,
		"ctrl_group":     ctrlName,
		"mon_group":      monName,
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
		rdtguard.Lock()
		err := rc.ctrlGroup.DeleteMonGroup(rc.monGroupName)
		rdtguard.Unlock()
		if err != nil {
			msg := strings.ToLower(err.Error())
			fields := logrus.Fields{
				"pid":       rc.pid,
				"old_class": oldClassName,
				"mon_group": rc.monGroupName,
			}
			// Benign race: old class/mon_group may already be gone if the scheduler deleted
			// the old class or another sync cycle already cleaned it up.
			if strings.Contains(msg, "resctrl group not found") || strings.Contains(msg, "no such file") || strings.Contains(msg, "not found") {
				rc.logger.WithError(err).WithFields(fields).Debug("Old monitoring group already gone during migration")
			} else {
				rc.logger.WithError(err).WithFields(fields).Warn("Failed to delete old monitoring group during migration")
			}
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
	rdtguard.Lock()
	newMonGroup, err := currentCtrlGroup.CreateMonGroup(rc.monGroupName, map[string]string{
		"container-bench": "true",
		"pid":             rc.pidStr,
	})
	rdtguard.Unlock()
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
	}).Debug("RDT monitoring group successfully migrated to new class")

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

	// Check if container has been moved to a different RDT class BEFORE reading monGroup
	// This handles the case where the class was updated externally (e.g., during allocation probe)
	if err := rc.detectAndHandleClassMigration(); err != nil {
		rc.logger.WithError(err).WithField("pid", rc.pid).Debug("Failed to handle RDT class migration during collect")
		// Don't return - try to collect anyway with current monitoring group
	}

	// Thread-safe read of state after migration check
	rc.mu.RLock()
	monGroup := rc.monGroup
	className := rc.className
	monGroupName := rc.monGroupName
	rc.mu.RUnlock()

	// Verify monitoring group is valid
	if monGroup == nil {
		rc.logger.WithField("pid", rc.pid).Trace("No valid monitoring group available for collection")
		return nil
	}
	if className != "" && monGroupName != "" {
		monDataPath := filepath.Join("/sys/fs/resctrl", className, "mon_groups", monGroupName, "mon_data")
		if _, err := os.Stat(monDataPath); err != nil {
			if os.IsNotExist(err) {
				rc.logger.WithFields(logrus.Fields{
					"pid":       rc.pid,
					"class":     className,
					"mon_group": monGroupName,
					"mon_data":  monDataPath,
				}).Debug("mon_data missing; recreating monitoring group")
				if rerr := rc.recreateMonitoringGroup(); rerr != nil {
					rc.logger.WithError(rerr).WithField("pid", rc.pid).Debug("Failed to recreate monitoring group")
				}
				// Re-read after recreation attempt
				rc.mu.RLock()
				monGroup = rc.monGroup
				rc.mu.RUnlock()
				if monGroup == nil {
					return nil
				}
			}
		}
	}

	// Hold the lock during GetMonData so the PID-sync ticker can't migrate/delete
	// the monitoring group concurrently (which can cause transient mon_data ENOENT).
	rc.mu.RLock()
	monData := monGroup.GetMonData()
	rc.mu.RUnlock()

	// If we got no data, check if this is due to monitoring group being deleted
	// This can happen during RDT class updates in allocation probes
	if monData.L3 == nil {
		// Try migration detection one more time in case the group was deleted
		rc.logger.WithField("pid", rc.pid).Trace("No L3 monitoring data, checking for class changes")
		if err := rc.detectAndHandleClassMigration(); err != nil {
			rc.logger.WithError(err).WithField("pid", rc.pid).Debug("Migration check failed after empty data")
		}
		// Re-read after potential migration
		rc.mu.RLock()
		monGroup = rc.monGroup
		rc.mu.RUnlock()

		if monGroup != nil {
			monData = monGroup.GetMonData()
		}

		// If still no data, return nil
		if monData.L3 == nil {
			rc.logger.WithField("pid", rc.pid).Trace("No L3 monitoring data available after migration check")
			return nil
		}
	}

	metrics := &dataframe.RDTMetrics{}

	// Process L3 monitoring data
	rc.processL3MonitoringData(&monData, metrics)

	// Add RDT class information
	className = rc.className
	metrics.RDTClassName = &className
	monGroupName = rc.monGroupName
	metrics.MonGroupName = &monGroupName

	rc.calculateDerivedMetrics(metrics)

	rc.getAllocationInfo(metrics)

	return metrics
}

// recreateMonitoringGroup recreates (or re-attaches to) the monitoring group under the
// container's current RDT class. This is a best-effort repair for transient races when
// classes are recreated/deleted while collectors are running.
func (rc *RDTCollector) recreateMonitoringGroup() error {
	currentCtrlGroup, currentClassName, err := findRDTClassForPID(rc.pidStr)
	if err != nil {
		return err
	}

	rc.mu.Lock()
	if rc.isClosing {
		rc.mu.Unlock()
		return fmt.Errorf("collector is closing")
	}
	monGroupName := rc.monGroupName
	rc.mu.Unlock()

	if monGroupName == "" {
		return fmt.Errorf("monitoring group name not set")
	}

	// Attach to existing mon group if it exists, otherwise create it.
	rdtguard.Lock()
	mg, ok := currentCtrlGroup.GetMonGroup(monGroupName)
	rdtguard.Unlock()
	if ok {
		rc.mu.Lock()
		rc.ctrlGroup = currentCtrlGroup
		rc.className = currentClassName
		rc.monGroup = mg
		rc.mu.Unlock()
	} else {
		rdtguard.Lock()
		mg, err := currentCtrlGroup.CreateMonGroup(monGroupName, map[string]string{
			"container-bench": "true",
			"pid":             rc.pidStr,
		})
		rdtguard.Unlock()
		if err != nil {
			return err
		}
		rc.mu.Lock()
		rc.ctrlGroup = currentCtrlGroup
		rc.className = currentClassName
		rc.monGroup = mg
		rc.mu.Unlock()
	}

	// Best-effort: re-sync cgroup PIDs into ctrl+mon groups.
	if err := rc.syncCGroupPIDs(); err != nil {
		return err
	}

	return nil
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
		// We can't split it back to per-socket, so we only store if there's a single socket
		// For per-socket bandwidth, we need to track per-socket cumulative values
		if totalBandwidthTotal >= rc.lastBandwidthTotal {
			delta := totalBandwidthTotal - rc.lastBandwidthTotal
			if delta > 0 && len(socketBandwidthTotal) == 1 {
				for socketID := range socketBandwidthTotal {
					metrics.MemoryBandwidthTotalPerSocket[socketID] = delta
				}
			}
		}

		if totalBandwidthLocal >= rc.lastBandwidthLocal {
			delta := totalBandwidthLocal - rc.lastBandwidthLocal
			if delta > 0 && len(socketBandwidthLocal) == 1 {
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
		rdtguard.Lock()
		err := rc.ctrlGroup.DeleteMonGroup(rc.monGroupName)
		rdtguard.Unlock()
		if err != nil {
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
