package collectors

import (
	"fmt"
	"strconv"

	"container-bench/internal/dataframe"
	"container-bench/internal/logging"
	"github.com/intel/goresctrl/pkg/rdt"
	"github.com/sirupsen/logrus"
)

type RDTCollector struct {
	pid          int
	pidStr       string
	monGroupName string
	className    string
	rdtEnabled   bool
	logger       *logrus.Logger
	
	// RDT monitoring group for this container
	monGroup     rdt.MonGroup
	ctrlGroup    rdt.CtrlGroup
}

func NewRDTCollector(pid int) (*RDTCollector, error) {
	logger := logging.GetLogger()
	
	// Check if RDT is supported and initialized
	if !rdt.MonSupported() {
		return nil, fmt.Errorf("RDT monitoring not supported on this system")
	}
	
	pidStr := strconv.Itoa(pid)
	
	// Create unique monitoring group name for this container
	monGroupName := fmt.Sprintf("container-bench-mon-%d", pid)
	
	// Get or create the default control group
	ctrlGroup, exists := rdt.GetClass("system/default")
	if !exists {
		// Try to get any available control group
		classes := rdt.GetClasses()
		if len(classes) == 0 {
			return nil, fmt.Errorf("no RDT control groups available")
		}
		ctrlGroup = classes[0]
		logger.WithField("class", ctrlGroup.Name()).Warn("Using first available RDT class instead of system/default")
	}
	
	// Create monitoring group for this container
	monGroup, err := ctrlGroup.CreateMonGroup(monGroupName, map[string]string{
		"container-bench": "true",
		"pid":            pidStr,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create RDT monitoring group: %v", err)
	}
	
	// Add the container PID to the monitoring group
	if err := monGroup.AddPids(pidStr); err != nil {
		// Cleanup on failure
		ctrlGroup.DeleteMonGroup(monGroupName)
		return nil, fmt.Errorf("failed to add PID %d to RDT monitoring group: %v", pid, err)
	}
	
	logger.WithFields(logrus.Fields{
		"pid":            pid,
		"mon_group":      monGroupName,
		"ctrl_group":     ctrlGroup.Name(),
	}).Debug("RDT monitoring initialized for container")
	
	return &RDTCollector{
		pid:          pid,
		pidStr:       pidStr,
		monGroupName: monGroupName,
		className:    ctrlGroup.Name(),
		rdtEnabled:   true,
		logger:       logger,
		monGroup:     monGroup,
		ctrlGroup:    ctrlGroup,
	}, nil
}

func (rc *RDTCollector) Collect() *dataframe.RDTMetrics {
	if !rc.rdtEnabled || rc.monGroup == nil {
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
	
	// Get allocation information if available
	rc.processAllocationData(metrics)
	
	// Calculate derived metrics
	rc.calculateDerivedMetrics(metrics)
	
	return metrics
}

func (rc *RDTCollector) processL3MonitoringData(monData *rdt.MonData, metrics *dataframe.RDTMetrics) {
	// Iterate through L3 data for all cache IDs
	for cacheID, leafData := range monData.L3 {
		rc.logger.WithFields(logrus.Fields{
			"pid":      rc.pid,
			"cache_id": cacheID,
		}).Trace("Processing L3 monitoring data")
		
		// L3 cache occupancy (usually in bytes)
		if occupancy, exists := leafData["llc_occupancy"]; exists {
			if metrics.L3CacheOccupancy == nil {
				metrics.L3CacheOccupancy = &occupancy
			} else {
				*metrics.L3CacheOccupancy += occupancy
			}
		}
		
		// Memory bandwidth monitoring
		if mbmTotal, exists := leafData["mbm_total_bytes"]; exists {
			if metrics.MemoryBandwidthTotal == nil {
				metrics.MemoryBandwidthTotal = &mbmTotal
			} else {
				*metrics.MemoryBandwidthTotal += mbmTotal
			}
			
			// Convert to MB/s (assuming measurement interval)
			bandwidthMBps := float64(mbmTotal) / (1024.0 * 1024.0)
			if metrics.MemoryBandwidthMBps == nil {
				metrics.MemoryBandwidthMBps = &bandwidthMBps
			} else {
				*metrics.MemoryBandwidthMBps += bandwidthMBps
			}
		}
		
		if mbmLocal, exists := leafData["mbm_local_bytes"]; exists {
			if metrics.MemoryBandwidthLocal == nil {
				metrics.MemoryBandwidthLocal = &mbmLocal
			} else {
				*metrics.MemoryBandwidthLocal += mbmLocal
			}
		}
	}
}

func (rc *RDTCollector) processAllocationData(metrics *dataframe.RDTMetrics) {
	// For now, we don't have allocation data in monitoring mode
	// This would be filled when the scheduler makes allocations
	// and we can query the actual allocation from the control group
}

func (rc *RDTCollector) calculateDerivedMetrics(metrics *dataframe.RDTMetrics) {
	// Cache utilization percentage (would need total cache size for accurate calculation)
	// For now, we can't calculate this without knowing the total L3 cache size
	
	// Bandwidth utilization percentage (would need system max bandwidth)
	// For now, we can't calculate this without knowing the system's max memory bandwidth
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
			// Note: We don't remove the PID here as it might be handled by the scheduler
			// or other components that need the PID in the group
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
