package collectors

import (
	"fmt"
	"strconv"

	"container-bench/internal/dataframe"
	"github.com/intel/goresctrl/pkg/rdt"
)

type RDTCollector struct {
	pid        int
	className  string
	rdtEnabled bool
}

func NewRDTCollector(pid int) (*RDTCollector, error) {
	collector := &RDTCollector{
		pid:        pid,
		className:  fmt.Sprintf("container-%d", pid),
		rdtEnabled: false,
	}

	// Check if RDT is available and initialize if needed
	if err := rdt.Initialize(""); err != nil {
		// RDT not available, but we can still create the collector
		// It will return nil metrics
		return collector, nil
	}

	collector.rdtEnabled = true

	// Create RDT class for this container if it doesn't exist
	if _, exists := rdt.GetClass(collector.className); !exists {
		// For now, we just assign to a default monitoring group
		// In a real scheduler implementation, this would be more sophisticated
		if defaultClass, exists := rdt.GetClass("default"); exists {
			pidStr := strconv.Itoa(pid)
			if err := defaultClass.AddPids(pidStr); err != nil {
				return nil, fmt.Errorf("failed to add PID %d to RDT class: %w", pid, err)
			}
		}
	}

	return collector, nil
}

func (rc *RDTCollector) Collect() *dataframe.RDTMetrics {
	if !rc.rdtEnabled {
		return nil
	}

	metrics := &dataframe.RDTMetrics{}

	// Try to get the class this PID belongs to
	if class, exists := rdt.GetClass(rc.className); exists {
		// Get monitoring data
		monData := class.GetMonData()
		
		// Extract L3 cache usage - check if we have data for any cache ID
		for cacheID, l3Data := range monData.L3 {
			if llcOccupancy, exists := l3Data["llc_occupancy"]; exists {
				metrics.L3CacheUsage = &llcOccupancy
				break // Use first available cache data
			}
			// Use cache ID for reference
			_ = cacheID
		}

		// Get the class name
		className := rc.className
		metrics.CLOSGroup = &className
	}

	return metrics
}

func (rc *RDTCollector) Close() {
	if !rc.rdtEnabled {
		return
	}

	// For now, we don't remove PIDs explicitly as the default implementation
	// doesn't have a RemovePids method. This would be handled when the container stops.
}
