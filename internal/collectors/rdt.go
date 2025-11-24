package collectors

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"

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

// Reads allocation information directly from the resctrl filesystem
// TODO: We should do this using gorestctl
func (rc *RDTCollector) readAllocationFromResctrl() (uint64, float64, error) {
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
		return 0, 0, fmt.Errorf("failed to read schemata file %s: %v", schemataPath, err)
	}

	// Parse L3 cache allocation from schemata
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "L3:") {
			// Parse L3 line: "L3:0=fff;1=fff"
			return rc.parseL3Allocation(line)
		}
	}

	return 0, 0, fmt.Errorf("no L3 allocation found in schemata")
}

// parses the L3 allocation line and calculates ways and percentage
func (rc *RDTCollector) parseL3Allocation(l3Line string) (uint64, float64, error) {
	// Remove "L3:" prefix
	allocStr := strings.TrimPrefix(l3Line, "L3:")

	// Split by cache domains: "0=fff;1=fff"
	domains := strings.Split(allocStr, ";")
	if len(domains) == 0 {
		return 0, 0, fmt.Errorf("no cache domains found")
	}

	// Take the first domain for calculation
	firstDomain := strings.TrimSpace(domains[0])
	parts := strings.Split(firstDomain, "=")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid domain format: %s", firstDomain)
	}

	// Parse the bitmask
	bitmask := strings.TrimSpace(parts[1])
	maskValue, err := strconv.ParseUint(bitmask, 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse bitmask %s: %v", bitmask, err)
	}

	// Count the number of set bits (cache ways)
	ways := countSetBits(maskValue)

	// Calculate percentage based on total cache ways
	// TODO: check the host info
	var percentage float64
	if rc.hostConfig != nil && rc.hostConfig.L3Cache.WaysPerCache > 0 {
		percentage = float64(ways) / float64(rc.hostConfig.L3Cache.WaysPerCache) * 100.0
	}

	return ways, percentage, nil
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

	// Read PIDs from the cgroup.procs file
	cgroupProcsPath := fmt.Sprintf("%s/cgroup.procs", rc.cgroupPath)
	data, err := ioutil.ReadFile(cgroupProcsPath)
	if err != nil {
		return fmt.Errorf("failed to read cgroup.procs from %s: %v", cgroupProcsPath, err)
	}

	// Parse PIDs
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 {
		rc.logger.WithField("cgroup_path", rc.cgroupPath).Debug("No PIDs found in cgroup")
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

	if err := rc.monGroup.AddPids(pids...); err != nil {
		return fmt.Errorf("failed to add PIDs to RDT monitoring group: %v", err)
	}

	rc.logger.WithFields(logrus.Fields{
		"pid":         rc.pid,
		"cgroup_path": rc.cgroupPath,
		"num_pids":    len(pids),
	}).Trace("Synced cgroup PIDs to RDT monitoring group")

	return nil
}

// SyncPIDs syncs all PIDs from the container's cgroup to the RDT monitoring group
// This should be called after docker exec
func (rc *RDTCollector) SyncPIDs() error {
	return rc.syncCGroupPIDs()
}

func NewRDTCollector(pid int, cgroupPath string, rdtConfig *config.RDTConfig) (*RDTCollector, error) {
	// Create using the base constructor
	// TODO: Make it selective
	collector, err := newRDTCollectorBase(pid, cgroupPath)
	if err != nil {
		return nil, err
	}

	// Set the config for potential future selective collection
	// For now, RDT collects all available metrics regardless of config
	collector.config = rdtConfig

	return collector, nil
}

// detectAndHandleClassMigration checks if the container has been moved to a different RDT class
// and handles the migration by deleting the old monitoring group and creating a new one
func (rc *RDTCollector) detectAndHandleClassMigration() error {
	// Find the current RDT class for this PID
	currentCtrlGroup, currentClassName, err := findRDTClassForPID(rc.pidStr)
	if err != nil {
		// PID might not be in any class (container terminated or not yet assigned)
		rc.logger.WithError(err).WithField("pid", rc.pid).Trace("Could not find RDT class for PID during migration check")
		return nil // Don't treat this as an error - container might be stopping
	}

	// Check if the class has changed
	if currentClassName == rc.className {
		// No migration detected
		return nil
	}

	rc.logger.WithFields(logrus.Fields{
		"pid":        rc.pid,
		"old_class":  rc.className,
		"new_class":  currentClassName,
		"mon_group":  rc.monGroupName,
	}).Info("RDT class migration detected - migrating monitoring group")

	// Delete the old monitoring group from the old class
	if rc.ctrlGroup != nil && rc.monGroupName != "" {
		if err := rc.ctrlGroup.DeleteMonGroup(rc.monGroupName); err != nil {
			rc.logger.WithError(err).WithFields(logrus.Fields{
				"pid":       rc.pid,
				"old_class": rc.className,
				"mon_group": rc.monGroupName,
			}).Warn("Failed to delete old monitoring group during migration")
			// Continue anyway - we'll create the new one
		} else {
			rc.logger.WithFields(logrus.Fields{
				"pid":       rc.pid,
				"old_class": rc.className,
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

	// Sync PIDs from cgroup to the new monitoring group
	if err := rc.syncCGroupPIDs(); err != nil {
		rc.logger.WithError(err).WithFields(logrus.Fields{
			"pid":       rc.pid,
			"new_class": currentClassName,
		}).Warn("Failed to sync cgroup PIDs to new monitoring group")
	}

	rc.logger.WithFields(logrus.Fields{
		"pid":        rc.pid,
		"new_class":  currentClassName,
		"mon_group":  rc.monGroupName,
	}).Info("RDT monitoring group successfully migrated to new class")

	return nil
}

func (rc *RDTCollector) Collect() *dataframe.RDTMetrics {
	if !rc.rdtEnabled || rc.monGroup == nil {
		return nil
	}

	// Check if container has been moved to a different RDT class
	if err := rc.detectAndHandleClassMigration(); err != nil {
		rc.logger.WithError(err).WithField("pid", rc.pid).Warn("Failed to handle RDT class migration")
		// Continue with collection using current monitoring group
	}

	// Verify monitoring group is still valid after potential migration
	if rc.monGroup == nil {
		rc.logger.WithField("pid", rc.pid).Warn("No valid monitoring group available after migration check")
		return nil
	}

	// Get monitoring data
	monData := rc.monGroup.GetMonData()
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
	var totalBandwidthTotal uint64
	var totalBandwidthLocal uint64

	// Iterate through L3 data for all cache IDs
	for cacheID, leafData := range monData.L3 {
		rc.logger.WithFields(logrus.Fields{
			"pid":      rc.pid,
			"cache_id": cacheID,
		}).Trace("Processing L3 monitoring data")

		if occupancy, exists := leafData["llc_occupancy"]; exists {
			if metrics.L3CacheOccupancy == nil {
				metrics.L3CacheOccupancy = &occupancy
			} else {
				*metrics.L3CacheOccupancy += occupancy
			}

			// Debug log when we see non-zero occupancy
			if occupancy > 0 {
				rc.logger.WithFields(logrus.Fields{
					"pid":       rc.pid,
					"cache_id":  cacheID,
					"occupancy": occupancy,
				}).Trace("RDT: Non-zero LLC occupancy detected")
			}
		}

		// Memory bandwidth monitoring (cumulative counters - aggregate first)
		if mbmTotal, exists := leafData["mbm_total_bytes"]; exists {
			totalBandwidthTotal += mbmTotal
		}

		if mbmLocal, exists := leafData["mbm_local_bytes"]; exists {
			totalBandwidthLocal += mbmLocal
		}
	}

	if !rc.firstCollection {
		// Compute delta from last collection
		if totalBandwidthTotal >= rc.lastBandwidthTotal {
			deltaBandwidthTotal := totalBandwidthTotal - rc.lastBandwidthTotal
			if deltaBandwidthTotal > 0 {
				metrics.MemoryBandwidthTotal = &deltaBandwidthTotal
			}
		}

		if totalBandwidthLocal >= rc.lastBandwidthLocal {
			deltaBandwidthLocal := totalBandwidthLocal - rc.lastBandwidthLocal
			if deltaBandwidthLocal > 0 {
				metrics.MemoryBandwidthLocal = &deltaBandwidthLocal
			}
		}
	}

	// Update last values for next delta calculation
	rc.lastBandwidthTotal = totalBandwidthTotal
	rc.lastBandwidthLocal = totalBandwidthLocal
	rc.firstCollection = false
}

func (rc *RDTCollector) calculateDerivedMetrics(metrics *dataframe.RDTMetrics) {
	// Calculate L3 cache utilization percentage
	if metrics.L3CacheOccupancy != nil && rc.hostConfig != nil {
		utilizationPercent := rc.hostConfig.GetL3CacheUtilizationPercent(*metrics.L3CacheOccupancy)
		metrics.CacheLLCUtilizationPercent = &utilizationPercent
	}

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
	if allocation, percentage, err := rc.readAllocationFromResctrl(); err == nil {
		metrics.L3CacheAllocation = &allocation
		metrics.L3CacheAllocationPct = &percentage

		rc.logger.WithFields(logrus.Fields{
			"pid":                   rc.pid,
			"rdt_class":             className,
			"l3_allocation_ways":    allocation,
			"l3_allocation_percent": percentage,
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
		rc.monGroup = nil
	}
}
