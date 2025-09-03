package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/jakobeberhardt/container-bench/internal/config"
	log "github.com/sirupsen/logrus"
)

// AdaptiveScheduler implements an adaptive scheduling strategy that learns from performance data
type AdaptiveScheduler struct {
	rdtEnabled       bool
	defaultScheduler *DefaultScheduler
	rdtScheduler     *RDTScheduler
	performanceData  map[string]*ContainerPerformance
	mutex            sync.RWMutex
}

// ContainerPerformance tracks performance metrics for a container
type ContainerPerformance struct {
	CacheMisses      float64
	MemoryBandwidth  float64
	CPUUtilization   float64
	LastUpdate       time.Time
	PerformanceScore float64
}

// NewAdaptiveScheduler creates a new adaptive scheduler
func NewAdaptiveScheduler(rdtEnabled bool) *AdaptiveScheduler {
	as := &AdaptiveScheduler{
		rdtEnabled:       rdtEnabled,
		defaultScheduler: NewDefaultScheduler(rdtEnabled),
		performanceData:  make(map[string]*ContainerPerformance),
	}

	if rdtEnabled {
		as.rdtScheduler = NewRDTScheduler()
	}

	return as
}

// Initialize initializes the adaptive scheduler
func (as *AdaptiveScheduler) Initialize(ctx context.Context) error {
	log.Info("Initializing adaptive scheduler")

	if err := as.defaultScheduler.Initialize(ctx); err != nil {
		return err
	}

	if as.rdtEnabled && as.rdtScheduler != nil {
		if err := as.rdtScheduler.Initialize(ctx); err != nil {
			log.WithError(err).Warn("Failed to initialize RDT scheduler, falling back to default")
			as.rdtScheduler = nil
		}
	}

	return nil
}

// ScheduleContainer applies adaptive scheduling policies to a container
func (as *AdaptiveScheduler) ScheduleContainer(ctx context.Context, containerID string, cfg config.ContainerConfig) error {
	log.WithFields(log.Fields{
		"container_id": containerID[:12],
		"strategy":     "adaptive",
	}).Info("Applying adaptive scheduling policy")

	// Initialize performance tracking for this container
	as.mutex.Lock()
	as.performanceData[containerID] = &ContainerPerformance{
		LastUpdate:       time.Now(),
		PerformanceScore: 1.0, // Start with neutral score
	}
	as.mutex.Unlock()

	// Start with default scheduling
	if err := as.defaultScheduler.ScheduleContainer(ctx, containerID, cfg); err != nil {
		return err
	}

	// Apply RDT scheduling if available and beneficial
	if as.rdtScheduler != nil {
		if err := as.rdtScheduler.ScheduleContainer(ctx, containerID, cfg); err != nil {
			log.WithError(err).Warn("Failed to apply RDT scheduling, using default only")
		}
	}

	return nil
}

// Monitor monitors containers and adapts scheduling strategies based on performance
func (as *AdaptiveScheduler) Monitor(ctx context.Context, containerIDs map[string]string) error {
	log.Info("Starting adaptive scheduler monitoring")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("Adaptive scheduler monitoring stopped")
			return ctx.Err()
		case <-ticker.C:
			as.adaptScheduling(ctx, containerIDs)
		}
	}
}

// adaptScheduling analyzes performance and adapts scheduling strategies
func (as *AdaptiveScheduler) adaptScheduling(ctx context.Context, containerIDs map[string]string) {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	// Analyze performance patterns
	highInterferenceContainers := as.detectInterference(containerIDs)
	
	if len(highInterferenceContainers) > 0 {
		log.WithField("containers", highInterferenceContainers).Info("Detected high interference containers")
		
		// Apply mitigation strategies
		as.applyInterferenceMitigation(ctx, highInterferenceContainers)
	}

	// Update performance scores
	as.updatePerformanceScores()
}

// detectInterference detects busy neighbor interference patterns
func (as *AdaptiveScheduler) detectInterference(containerIDs map[string]string) []string {
	var highInterference []string
	
	// This is a simplified interference detection algorithm
	// In a real implementation, you would analyze:
	// - Cache miss rates correlation between containers
	// - Memory bandwidth competition
	// - CPU utilization patterns
	// - Performance degradation over time
	
	for containerID, perf := range as.performanceData {
		if perf.PerformanceScore < 0.7 { // Performance degraded significantly
			if time.Since(perf.LastUpdate) < time.Minute {
				highInterference = append(highInterference, containerID)
			}
		}
	}
	
	return highInterference
}

// applyInterferenceMitigation applies strategies to mitigate busy neighbor effects
func (as *AdaptiveScheduler) applyInterferenceMitigation(ctx context.Context, interferingContainers []string) {
	for _, containerID := range interferingContainers {
		log.WithField("container_id", containerID[:12]).Info("Applying interference mitigation")
		
		// Strategy 1: Migrate to different resource group (if RDT available)
		if as.rdtScheduler != nil {
			// This would involve moving the container to a less contended resource group
			log.WithField("container_id", containerID[:12]).Debug("Considering RDT resource group migration")
		}
		
		// Strategy 2: Adjust CPU affinity to reduce cache conflicts
		// This could involve moving containers to different NUMA nodes or cache domains
		log.WithField("container_id", containerID[:12]).Debug("Considering CPU affinity adjustment")
		
		// Strategy 3: Implement memory bandwidth throttling
		// This would limit the memory bandwidth usage of interfering containers
		log.WithField("container_id", containerID[:12]).Debug("Considering memory bandwidth throttling")
	}
}

// updatePerformanceScores updates performance scores based on collected metrics
func (as *AdaptiveScheduler) updatePerformanceScores() {
	now := time.Now()
	
	for containerID, perf := range as.performanceData {
		// Simple performance score calculation
		// In a real implementation, this would use actual metrics from profiler
		baseScore := 1.0
		
		// Degrade score based on cache misses
		if perf.CacheMisses > 0.1 { // 10% cache miss rate threshold
			baseScore *= (1.0 - perf.CacheMisses)
		}
		
		// Degrade score based on memory bandwidth saturation
		if perf.MemoryBandwidth > 0.8 { // 80% bandwidth utilization threshold
			baseScore *= (1.0 - (perf.MemoryBandwidth - 0.8) * 2)
		}
		
		perf.PerformanceScore = baseScore
		perf.LastUpdate = now
		
		log.WithFields(log.Fields{
			"container_id":       containerID[:12],
			"performance_score":  perf.PerformanceScore,
			"cache_misses":       perf.CacheMisses,
			"memory_bandwidth":   perf.MemoryBandwidth,
		}).Debug("Updated container performance score")
	}
}

// UpdateContainerMetrics updates performance metrics for a container (called by profiler)
func (as *AdaptiveScheduler) UpdateContainerMetrics(containerID string, cacheMisses, memoryBandwidth, cpuUtil float64) {
	as.mutex.Lock()
	defer as.mutex.Unlock()
	
	if perf, exists := as.performanceData[containerID]; exists {
		perf.CacheMisses = cacheMisses
		perf.MemoryBandwidth = memoryBandwidth
		perf.CPUUtilization = cpuUtil
		perf.LastUpdate = time.Now()
	}
}

// Finalize cleans up adaptive scheduler resources
func (as *AdaptiveScheduler) Finalize(ctx context.Context) error {
	log.Info("Finalizing adaptive scheduler")

	if as.rdtScheduler != nil {
		if err := as.rdtScheduler.Finalize(ctx); err != nil {
			log.WithError(err).Warn("Error finalizing RDT scheduler")
		}
	}

	return as.defaultScheduler.Finalize(ctx)
}

// Name returns the scheduler name
func (as *AdaptiveScheduler) Name() string {
	return "adaptive"
}
