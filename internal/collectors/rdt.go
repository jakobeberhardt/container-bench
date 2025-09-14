package collectors

import (
	"fmt"
	"strconv"

	"container-bench/internal/dataframe"
	"container-bench/internal/host"
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
	hostConfig   *host.HostConfig
	
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
	
	// Get host configuration
	hostConfig, err := host.GetHostConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get host configuration: %v", err)
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
		hostConfig:   hostConfig,
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
	
	// Calculate derived metrics using host configuration
	rc.calculateDerivedMetrics(metrics)
	
	// Get allocation information if available
	rc.getAllocationInfo(metrics)
	
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
		
		// Memory bandwidth monitoring - total bytes
		if mbmTotal, exists := leafData["mbm_total_bytes"]; exists {
			if metrics.MemoryBandwidthTotal == nil {
				metrics.MemoryBandwidthTotal = &mbmTotal
			} else {
				*metrics.MemoryBandwidthTotal += mbmTotal
			}
		}
		
		// Memory bandwidth monitoring - local bytes
		if mbmLocal, exists := leafData["mbm_local_bytes"]; exists {
			if metrics.MemoryBandwidthLocal == nil {
				metrics.MemoryBandwidthLocal = &mbmLocal
			} else {
				*metrics.MemoryBandwidthLocal += mbmLocal
			}
		}
	}
}

// calculateDerivedMetrics calculates derived metrics using host configuration
func (rc *RDTCollector) calculateDerivedMetrics(metrics *dataframe.RDTMetrics) {
	// Calculate L3 cache utilization percentage
	if metrics.L3CacheOccupancy != nil && rc.hostConfig != nil {
		utilizationPercent := rc.hostConfig.GetL3CacheUtilizationPercent(*metrics.L3CacheOccupancy)
		metrics.CacheLLCUtilizationPercent = &utilizationPercent
	}
	
	// Calculate memory bandwidth utilization percentage
	if metrics.MemoryBandwidthTotal != nil {
		bandwidthMBps := float64(*metrics.MemoryBandwidthTotal) / (1024.0 * 1024.0)
		
		// Calculate bandwidth utilization percentage
		if rc.hostConfig != nil {
			bandwidthUtilization := rc.hostConfig.GetMemoryBandwidthUtilizationPercent(bandwidthMBps)
			metrics.BandwidthUtilizationPercent = &bandwidthUtilization
		}
	}
}

// getAllocationInfo gets allocation information for the container
func (rc *RDTCollector) getAllocationInfo(metrics *dataframe.RDTMetrics) {
	// Try to get allocation information from the control group
	// Note: This might require additional RDT API calls that aren't available in goresctrl
	// For now, we'll attempt to get basic allocation information
	
	// The allocation information is typically managed by the scheduler
	// and not directly accessible through monitoring APIs
	// We'll leave these fields for the scheduler to populate if needed
	
	// If we have host config, we can provide some context
	if rc.hostConfig != nil {
		// Store cache allocation percentage if available
		// This would typically be set by the scheduler when allocating resources
		
		// For debugging, log the available cache information
		rc.logger.WithFields(logrus.Fields{
			"pid":                 rc.pid,
			"total_l3_cache_mb":   rc.hostConfig.L3Cache.TotalSizeMB,
			"cache_ways":          rc.hostConfig.L3Cache.WaysPerCache,
		}).Trace("Cache configuration available")
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
