package scheduler

import (
	"fmt"
	"sync"

	"container-bench/internal/dataframe"
	"container-bench/internal/logging"
	"github.com/sirupsen/logrus"
)

// CacheAwareScheduler implements a scheduler that monitors cache miss rates
// and allocates containers to different RDT classes based on performance thresholds
type CacheAwareScheduler struct {
	name               string
	version            string
	schedulerLogger    *logrus.Logger
	rdtAllocator       RDTAllocator
	
	// Configuration
	cacheMissThreshold float64 // Cache miss rate threshold (e.g., 0.4 for 40%)
	highMissClassName  string  // RDT class for high cache miss containers
	lowMissClassName   string  // RDT class for low cache miss containers
	
	// State tracking
	containerStates    map[int]*ContainerState
	stateMutex         sync.RWMutex
	initialized        bool
}

// ContainerState tracks the state of a container for scheduling decisions
type ContainerState struct {
	PID                    int
	ContainerIndex         int
	CurrentRDTClass        string
	CacheMissRateHistory   []float64 // Track recent cache miss rates
	ConsecutiveHighMisses  int       // Count of consecutive high miss rates
	ConsecutiveLowMisses   int       // Count of consecutive low miss rates
	AllocationDecisionsMade int      // Track how many times we've moved this container
}

const (
	// Number of consecutive high/low miss rates before taking action
	ConsecutiveThreshold = 3
	// Maximum number of allocation decisions per container to prevent thrashing
	MaxAllocationDecisions = 5
	// History length for cache miss rates
	CacheMissHistoryLength = 10
)

func NewCacheAwareScheduler() *CacheAwareScheduler {
	return NewCacheAwareSchedulerWithRDT(false)
}

func NewCacheAwareSchedulerWithRDT(enableRDTAllocation bool) *CacheAwareScheduler {
	var rdtAllocator RDTAllocator
	if enableRDTAllocation {
		rdtAllocator = NewDefaultRDTAllocator()
	}
	
	return &CacheAwareScheduler{
		name:               "cache-aware",
		version:            "1.0.0",
		schedulerLogger:    logging.GetSchedulerLogger(),
		rdtAllocator:       rdtAllocator,
		cacheMissThreshold: 0.4, // 40% cache miss rate threshold
		highMissClassName:  "system/default", // High miss containers go to default class
		lowMissClassName:   "system/default", // For now, use same class (would be separate in production)
		containerStates:    make(map[int]*ContainerState),
	}
}

func (s *CacheAwareScheduler) Initialize() error {
	s.schedulerLogger.Info("Initializing cache-aware scheduler")
	
	// Initialize RDT allocator only if enabled
	if s.rdtAllocator != nil {
		if err := s.rdtAllocator.Initialize(); err != nil {
			s.schedulerLogger.WithError(err).Error("Failed to initialize RDT allocator")
			return fmt.Errorf("failed to initialize RDT allocator: %v", err)
		}
		
		// Log available RDT classes
		availableClasses := s.rdtAllocator.ListAvailableClasses()
		s.schedulerLogger.WithField("available_classes", availableClasses).Info("Available RDT classes")
		
		// For demonstration, we'll work with existing classes
		// In production, you would create specific classes for high/low cache miss containers
		if len(availableClasses) >= 2 {
			s.highMissClassName = availableClasses[0]
			s.lowMissClassName = availableClasses[0] // Use same for now
		}
		
		s.schedulerLogger.WithFields(logrus.Fields{
			"cache_miss_threshold": s.cacheMissThreshold,
			"high_miss_class":      s.highMissClassName,
			"low_miss_class":       s.lowMissClassName,
			"rdt_allocation":       "enabled",
		}).Info("Cache-aware scheduler configuration")
	} else {
		s.schedulerLogger.WithFields(logrus.Fields{
			"cache_miss_threshold": s.cacheMissThreshold,
			"rdt_allocation":       "disabled (monitoring only)",
		}).Info("Cache-aware scheduler configuration")
	}
	
	s.initialized = true
	return nil
}

func (s *CacheAwareScheduler) ProcessDataFrames(dataframes *dataframe.DataFrames) error {
	if !s.initialized {
		return fmt.Errorf("scheduler not initialized")
	}
	
	containers := dataframes.GetAllContainers()
	
	for containerIndex, containerDF := range containers {
		latest := containerDF.GetLatestStep()
		if latest == nil {
			continue
		}
		
		// Get or create container state
		state := s.getOrCreateContainerState(containerIndex)
		
		// Process performance metrics
		if latest.Perf != nil && latest.Perf.CacheMissRate != nil {
			cacheMissRate := *latest.Perf.CacheMissRate
			
			// Update container state
			s.updateContainerState(state, cacheMissRate)
			
			// Make scheduling decisions
			if err := s.makeSchedulingDecision(state); err != nil {
				s.schedulerLogger.WithError(err).WithField("container", containerIndex).Warn("Failed to make scheduling decision")
			}
			
			// Log current status
			s.schedulerLogger.WithFields(logrus.Fields{
				"container":           containerIndex,
				"cache_miss_rate":     cacheMissRate,
				"current_rdt_class":   state.CurrentRDTClass,
				"consecutive_high":    state.ConsecutiveHighMisses,
				"consecutive_low":     state.ConsecutiveLowMisses,
				"allocation_decisions": state.AllocationDecisionsMade,
			}).Debug("Container performance status")
		}
		
		// Also log other metrics for visibility
		if latest.Docker != nil && latest.Docker.CPUUsagePercent != nil {
			s.schedulerLogger.WithFields(logrus.Fields{
				"container":   containerIndex,
				"cpu_percent": *latest.Docker.CPUUsagePercent,
			}).Debug("Container CPU usage")
		}
		
		if latest.RDT != nil {
			s.logRDTMetrics(containerIndex, latest.RDT)
		}
	}
	
	return nil
}

func (s *CacheAwareScheduler) getOrCreateContainerState(containerIndex int) *ContainerState {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()
	
	if state, exists := s.containerStates[containerIndex]; exists {
		return state
	}
	
	// Create new state
	state := &ContainerState{
		ContainerIndex:         containerIndex,
		CurrentRDTClass:        s.lowMissClassName, // Start with low miss class
		CacheMissRateHistory:   make([]float64, 0, CacheMissHistoryLength),
		ConsecutiveHighMisses:  0,
		ConsecutiveLowMisses:   0,
		AllocationDecisionsMade: 0,
	}
	
	s.containerStates[containerIndex] = state
	return state
}

func (s *CacheAwareScheduler) updateContainerState(state *ContainerState, cacheMissRate float64) {
	// Add to history
	state.CacheMissRateHistory = append(state.CacheMissRateHistory, cacheMissRate)
	if len(state.CacheMissRateHistory) > CacheMissHistoryLength {
		state.CacheMissRateHistory = state.CacheMissRateHistory[1:]
	}
	
	// Update consecutive counters
	if cacheMissRate > s.cacheMissThreshold {
		state.ConsecutiveHighMisses++
		state.ConsecutiveLowMisses = 0
	} else {
		state.ConsecutiveLowMisses++
		state.ConsecutiveHighMisses = 0
	}
}

func (s *CacheAwareScheduler) makeSchedulingDecision(state *ContainerState) error {
	// Skip allocation decisions if RDT allocator is not available (monitoring-only mode)
	if s.rdtAllocator == nil {
		// Just log the cache behavior for monitoring purposes
		if state.ConsecutiveHighMisses >= ConsecutiveThreshold {
			s.schedulerLogger.WithFields(logrus.Fields{
				"container":           state.ContainerIndex,
				"consecutive_high":    state.ConsecutiveHighMisses,
				"cache_miss_threshold": s.cacheMissThreshold,
			}).Info("Container has high cache miss rate (monitoring only)")
		} else if state.ConsecutiveLowMisses >= ConsecutiveThreshold {
			s.schedulerLogger.WithFields(logrus.Fields{
				"container":           state.ContainerIndex,
				"consecutive_low":     state.ConsecutiveLowMisses,
				"cache_miss_threshold": s.cacheMissThreshold,
			}).Info("Container has low cache miss rate (monitoring only)")
		}
		return nil
	}
	
	// Prevent thrashing by limiting allocation decisions
	if state.AllocationDecisionsMade >= MaxAllocationDecisions {
		return nil
	}
	
	var targetClass string
	var shouldMove bool
	var reason string
	
	// Decision logic: move to high miss class if consistently high miss rate
	if state.ConsecutiveHighMisses >= ConsecutiveThreshold && state.CurrentRDTClass != s.highMissClassName {
		targetClass = s.highMissClassName
		shouldMove = true
		reason = fmt.Sprintf("high cache miss rate (%d consecutive)", state.ConsecutiveHighMisses)
	} else if state.ConsecutiveLowMisses >= ConsecutiveThreshold && state.CurrentRDTClass != s.lowMissClassName {
		targetClass = s.lowMissClassName
		shouldMove = true
		reason = fmt.Sprintf("low cache miss rate (%d consecutive)", state.ConsecutiveLowMisses)
	}
	
	if shouldMove && state.PID > 0 {
		if err := s.rdtAllocator.AssignContainerToClass(state.PID, targetClass); err != nil {
			return fmt.Errorf("failed to assign container to RDT class %s: %v", targetClass, err)
		}
		
		s.schedulerLogger.WithFields(logrus.Fields{
			"container":     state.ContainerIndex,
			"pid":           state.PID,
			"from_class":    state.CurrentRDTClass,
			"to_class":      targetClass,
			"reason":        reason,
		}).Info("Container moved to different RDT class")
		
		state.CurrentRDTClass = targetClass
		state.AllocationDecisionsMade++
		
		// Reset consecutive counters after making a decision
		state.ConsecutiveHighMisses = 0
		state.ConsecutiveLowMisses = 0
	}
	
	return nil
}

func (s *CacheAwareScheduler) logRDTMetrics(containerIndex int, rdtMetrics *dataframe.RDTMetrics) {
	fields := logrus.Fields{
		"container": containerIndex,
	}
	
	if rdtMetrics.L3CacheOccupancyMB != nil {
		fields["l3_cache_mb"] = *rdtMetrics.L3CacheOccupancyMB
	}
	if rdtMetrics.MemoryBandwidthMBps != nil {
		fields["memory_bw_mbps"] = *rdtMetrics.MemoryBandwidthMBps
	}
	if rdtMetrics.RDTClassName != nil {
		fields["rdt_class"] = *rdtMetrics.RDTClassName
	}
	
	s.schedulerLogger.WithFields(fields).Debug("RDT metrics")
}

// SetContainerPID allows the main orchestrator to provide PID information
// This is called when containers are started and PIDs become available
func (s *CacheAwareScheduler) SetContainerPID(containerIndex int, pid int) {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()
	
	if state, exists := s.containerStates[containerIndex]; exists {
		state.PID = pid
	} else {
		// Create state if it doesn't exist
		state := &ContainerState{
			PID:                     pid,
			ContainerIndex:          containerIndex,
			CurrentRDTClass:         s.lowMissClassName,
			CacheMissRateHistory:    make([]float64, 0, CacheMissHistoryLength),
			ConsecutiveHighMisses:   0,
			ConsecutiveLowMisses:    0,
			AllocationDecisionsMade: 0,
		}
		s.containerStates[containerIndex] = state
	}
	
	s.schedulerLogger.WithFields(logrus.Fields{
		"container": containerIndex,
		"pid":       pid,
	}).Info("Container PID registered with cache-aware scheduler")
}

func (s *CacheAwareScheduler) Shutdown() error {
	if !s.initialized {
		return nil
	}
	
	s.schedulerLogger.Info("Shutting down cache-aware scheduler")
	
	// Clean up RDT allocator if it exists
	if s.rdtAllocator != nil {
		if err := s.rdtAllocator.Cleanup(); err != nil {
			s.schedulerLogger.WithError(err).Warn("Failed to cleanup RDT allocator")
		}
	}
	
	s.stateMutex.Lock()
	s.containerStates = make(map[int]*ContainerState)
	s.stateMutex.Unlock()
	
	s.initialized = false
	return nil
}

func (s *CacheAwareScheduler) GetVersion() string {
	return s.version
}

func (s *CacheAwareScheduler) SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	s.schedulerLogger.SetLevel(logLevel)
	return nil
}

// GetRDTAllocator returns the RDT allocator for external use if needed
func (s *CacheAwareScheduler) GetRDTAllocator() RDTAllocator {
	return s.rdtAllocator
}
