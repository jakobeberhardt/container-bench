package scheduler

import (
	"container-bench/internal/dataframe"
	"container-bench/internal/logging"
	"github.com/sirupsen/logrus"
)

type Scheduler interface {
	Initialize() error
	ProcessDataFrames(dataframes *dataframe.DataFrames) error
	Shutdown() error
	GetVersion() string
}

type DefaultScheduler struct {
	name    string
	version string
	logCounter int // Counter to control logging frequency
}

func NewDefaultScheduler() *DefaultScheduler {
	return &DefaultScheduler{
		name:    "default",
		version: "1.0.0",
	}
}

func (ds *DefaultScheduler) Initialize() error {
	// Default scheduler doesn't need initialization
	return nil
}

func (ds *DefaultScheduler) ProcessDataFrames(dataframes *dataframe.DataFrames) error {
	logger := logging.GetLogger()
	
	containers := dataframes.GetAllContainers()
	
	// Increment counter and log detailed metrics every 5 seconds to avoid spam
	ds.logCounter++
	logDetailed := (ds.logCounter % 5 == 0)
	
	if logDetailed {
		logger.WithField("total_containers", len(containers)).Info("Scheduler: Processing dataframes (detailed logging)")
	}
	
	for containerIndex, containerDF := range containers {
		latest := containerDF.GetLatestStep()
		if latest != nil {
			if logDetailed {
				// Create structured log entry with real-time metrics
				logFields := logrus.Fields{
					"container_index": containerIndex,
					"timestamp":       latest.Timestamp.Format("15:04:05.000"),
				}
				
				// Add Perf metrics if available
				if latest.Perf != nil {
					if latest.Perf.Instructions != nil {
						logFields["perf_instructions"] = *latest.Perf.Instructions
					}
					if latest.Perf.CacheMisses != nil {
						logFields["perf_cache_misses"] = *latest.Perf.CacheMisses
					}
					if latest.Perf.CacheMissRate != nil {
						logFields["perf_cache_miss_rate"] = *latest.Perf.CacheMissRate
					}
					if latest.Perf.InstructionsPerCycle != nil {
						logFields["perf_ipc"] = *latest.Perf.InstructionsPerCycle
					}
				}
				
				// Add Docker metrics if available
				if latest.Docker != nil {
					if latest.Docker.CPUUsagePercent != nil {
						logFields["docker_cpu_percent"] = *latest.Docker.CPUUsagePercent
					}
					if latest.Docker.MemoryUsagePercent != nil {
						logFields["docker_memory_percent"] = *latest.Docker.MemoryUsagePercent
					}
					if latest.Docker.MemoryUsage != nil {
						// Convert to MB for readability
						logFields["docker_memory_mb"] = *latest.Docker.MemoryUsage / (1024 * 1024)
					}
				}
				
				// Add RDT metrics if available
				if latest.RDT != nil {
					if latest.RDT.L3CacheUsage != nil {
						logFields["rdt_l3_cache_usage"] = *latest.RDT.L3CacheUsage
					}
					if latest.RDT.MemoryBandwidth != nil {
						logFields["rdt_memory_bandwidth"] = *latest.RDT.MemoryBandwidth
					}
				}
				
				// Log the real-time data
				logger.WithFields(logFields).Info("Scheduler: Real-time container metrics")
			}
			
			// Always check for scheduling decisions (but log less frequently)
			if latest.Perf != nil && latest.Perf.CacheMissRate != nil {
				if *latest.Perf.CacheMissRate > 0.1 { // 10% cache miss rate
					logger.WithFields(logrus.Fields{
						"container_index": containerIndex,
						"cache_miss_rate": *latest.Perf.CacheMissRate,
					}).Warn("Scheduler: High cache miss rate detected - would trigger resource reallocation")
				}
			}
			
			if latest.Docker != nil && latest.Docker.CPUUsagePercent != nil {
				if *latest.Docker.CPUUsagePercent > 80.0 { // 80% CPU usage
					logger.WithFields(logrus.Fields{
						"container_index": containerIndex,
						"cpu_percent":     *latest.Docker.CPUUsagePercent,
					}).Warn("Scheduler: High CPU usage detected - would consider migration")
				}
			}
			
			if latest.Docker != nil && latest.Docker.MemoryUsagePercent != nil {
				if *latest.Docker.MemoryUsagePercent > 90.0 { // 90% memory usage
					logger.WithFields(logrus.Fields{
						"container_index":   containerIndex,
						"memory_percent":    *latest.Docker.MemoryUsagePercent,
					}).Warn("Scheduler: High memory usage detected - would consider scaling")
				}
			}
		} else if logDetailed {
			logger.WithField("container_index", containerIndex).Debug("Scheduler: No data available for container")
		}
	}
	
	return nil
}

func (ds *DefaultScheduler) Shutdown() error {
	// Default scheduler doesn't need cleanup
	return nil
}

func (ds *DefaultScheduler) GetVersion() string {
	return ds.version
}
