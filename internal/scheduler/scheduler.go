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
	SetLogLevel(level string) error
}

type DefaultScheduler struct {
	name    string
	version string
	logCounter int // Counter to control logging frequency
	schedulerLogger *logrus.Logger
}

func NewDefaultScheduler() *DefaultScheduler {
	return &DefaultScheduler{
		name:            "default",
		version:         "1.0.0",
		schedulerLogger: logging.GetSchedulerLogger(),
	}
}

func (ds *DefaultScheduler) Initialize() error {
	// Default scheduler doesn't need initialization
	return nil
}

func (ds *DefaultScheduler) ProcessDataFrames(dataframes *dataframe.DataFrames) error {
	containers := dataframes.GetAllContainers()
	
	// Increment counter for periodic logging
	ds.logCounter++
	
	// Log at different levels based on scheduler log level
	currentLevel := ds.schedulerLogger.GetLevel()
	
	// DEBUG level: Log full dataframe details every 2 seconds
	if currentLevel <= logrus.DebugLevel && (ds.logCounter % 2 == 0) {
		ds.schedulerLogger.WithField("total_containers", len(containers)).Debug("SCHEDULER: Processing dataframes (full details)")
	}
	
	for containerIndex, containerDF := range containers {
		latest := containerDF.GetLatestStep()
		if latest != nil {
			
			// DEBUG level: Log all available metrics for each container
			if currentLevel <= logrus.DebugLevel && (ds.logCounter % 2 == 0) {
				logFields := logrus.Fields{
					"container_index": containerIndex,
					"timestamp":       latest.Timestamp.Format("15:04:05.000"),
				}
				
				// Add ALL Perf metrics if available
				if latest.Perf != nil {
					if latest.Perf.Instructions != nil {
						logFields["perf_instructions"] = *latest.Perf.Instructions
					}
					if latest.Perf.Cycles != nil {
						logFields["perf_cycles"] = *latest.Perf.Cycles
					}
					if latest.Perf.CacheMisses != nil {
						logFields["perf_cache_misses"] = *latest.Perf.CacheMisses
					}
					if latest.Perf.CacheReferences != nil {
						logFields["perf_cache_references"] = *latest.Perf.CacheReferences
					}
					if latest.Perf.CacheMissRate != nil {
						logFields["perf_cache_miss_rate"] = *latest.Perf.CacheMissRate
					}
					if latest.Perf.InstructionsPerCycle != nil {
						logFields["perf_ipc"] = *latest.Perf.InstructionsPerCycle
					}
					if latest.Perf.BranchInstructions != nil {
						logFields["perf_branch_instructions"] = *latest.Perf.BranchInstructions
					}
					if latest.Perf.BranchMisses != nil {
						logFields["perf_branch_misses"] = *latest.Perf.BranchMisses
					}
					if latest.Perf.BranchMissRate != nil {
						logFields["perf_branch_miss_rate"] = *latest.Perf.BranchMissRate
					}
				}
				
				// Add ALL Docker metrics if available
				if latest.Docker != nil {
					if latest.Docker.CPUUsagePercent != nil {
						logFields["docker_cpu_percent"] = *latest.Docker.CPUUsagePercent
					}
					if latest.Docker.CPUUsageTotal != nil {
						logFields["docker_cpu_total"] = *latest.Docker.CPUUsageTotal
					}
					if latest.Docker.CPUUsageKernel != nil {
						logFields["docker_cpu_kernel"] = *latest.Docker.CPUUsageKernel
					}
					if latest.Docker.CPUUsageUser != nil {
						logFields["docker_cpu_user"] = *latest.Docker.CPUUsageUser
					}
					if latest.Docker.MemoryUsagePercent != nil {
						logFields["docker_memory_percent"] = *latest.Docker.MemoryUsagePercent
					}
					if latest.Docker.MemoryUsage != nil {
						logFields["docker_memory_mb"] = *latest.Docker.MemoryUsage / (1024 * 1024)
					}
					if latest.Docker.MemoryLimit != nil {
						logFields["docker_memory_limit_mb"] = *latest.Docker.MemoryLimit / (1024 * 1024)
					}
					if latest.Docker.NetworkRxBytes != nil {
						logFields["docker_net_rx_bytes"] = *latest.Docker.NetworkRxBytes
					}
					if latest.Docker.NetworkTxBytes != nil {
						logFields["docker_net_tx_bytes"] = *latest.Docker.NetworkTxBytes
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
					if latest.RDT.CLOSGroup != nil {
						logFields["rdt_clos_group"] = *latest.RDT.CLOSGroup
					}
				}
				
				ds.schedulerLogger.WithFields(logFields).Debug("SCHEDULER: Complete container metrics")
			}
			
			// WARN level: Always log high resource usage alerts
			if latest.Perf != nil && latest.Perf.CacheMissRate != nil {
				if *latest.Perf.CacheMissRate > 0.1 { // 10% cache miss rate
					ds.schedulerLogger.WithFields(logrus.Fields{
						"container_index": containerIndex,
						"cache_miss_rate": *latest.Perf.CacheMissRate,
						"threshold":       0.1,
					}).Warn("SCHEDULER: High cache miss rate detected - resource reallocation recommended")
				}
			}
			
			if latest.Docker != nil && latest.Docker.CPUUsagePercent != nil {
				if *latest.Docker.CPUUsagePercent > 80.0 { // 80% CPU usage
					ds.schedulerLogger.WithFields(logrus.Fields{
						"container_index": containerIndex,
						"cpu_percent":     *latest.Docker.CPUUsagePercent,
						"threshold":       80.0,
					}).Warn("SCHEDULER: High CPU usage detected - migration consideration triggered")
				}
			}
			
			if latest.Docker != nil && latest.Docker.MemoryUsagePercent != nil {
				if *latest.Docker.MemoryUsagePercent > 90.0 { // 90% memory usage
					ds.schedulerLogger.WithFields(logrus.Fields{
						"container_index": containerIndex,
						"memory_percent":  *latest.Docker.MemoryUsagePercent,
						"threshold":       90.0,
					}).Warn("SCHEDULER: High memory usage detected - scaling consideration triggered")
				}
			}
			
			// INFO level: Log scheduling decisions and significant events every 5 seconds
			if currentLevel <= logrus.InfoLevel && (ds.logCounter % 5 == 0) {
				// Example scheduling decisions
				performanceScore := ds.calculatePerformanceScore(latest)
				ds.schedulerLogger.WithFields(logrus.Fields{
					"container_index":    containerIndex,
					"performance_score":  performanceScore,
					"timestamp":         latest.Timestamp.Format("15:04:05"),
				}).Info("SCHEDULER: Container performance evaluation completed")
				
				// Simulate scheduling decisions
				if performanceScore < 0.5 {
					ds.schedulerLogger.WithFields(logrus.Fields{
						"container_index":   containerIndex,
						"performance_score": performanceScore,
						"action":           "resource_reallocation",
					}).Info("SCHEDULER: Low performance detected - initiating resource reallocation")
				}
			}
			
		} else if currentLevel <= logrus.DebugLevel {
			ds.schedulerLogger.WithField("container_index", containerIndex).Debug("SCHEDULER: No data available for container")
		}
	}
	
	return nil
}

// calculatePerformanceScore calculates a simple performance score based on available metrics
func (ds *DefaultScheduler) calculatePerformanceScore(step *dataframe.SamplingStep) float64 {
	score := 1.0 // Start with perfect score
	
	// Reduce score based on cache miss rate
	if step.Perf != nil && step.Perf.CacheMissRate != nil {
		score -= *step.Perf.CacheMissRate // Higher miss rate = lower score
	}
	
	// Reduce score based on high CPU usage
	if step.Docker != nil && step.Docker.CPUUsagePercent != nil {
		if *step.Docker.CPUUsagePercent > 80 {
			score -= 0.3 // Penalize high CPU usage
		}
	}
	
	// Reduce score based on high memory usage
	if step.Docker != nil && step.Docker.MemoryUsagePercent != nil {
		if *step.Docker.MemoryUsagePercent > 90 {
			score -= 0.4 // Penalize high memory usage
		}
	}
	
	// Ensure score is between 0 and 1
	if score < 0 {
		score = 0
	}
	if score > 1 {
		score = 1
	}
	
	return score
}

func (ds *DefaultScheduler) Shutdown() error {
	// Default scheduler doesn't need cleanup
	return nil
}

func (ds *DefaultScheduler) GetVersion() string {
	return ds.version
}

func (ds *DefaultScheduler) SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	ds.schedulerLogger.SetLevel(logLevel)
	return nil
}
