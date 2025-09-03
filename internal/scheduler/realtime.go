package scheduler

import (
	"context"
	"time"

	"github.com/jakobeberhardt/container-bench/internal/storage"
)

// MetricsConsumer defines the interface for schedulers to receive real-time metrics
type MetricsConsumer interface {
	// OnMetrics is called whenever new comprehensive metrics are available
	OnMetrics(ctx context.Context, metrics *storage.BenchmarkMetrics) error
	
	// GetSchedulingDecisions returns any scheduling decisions to be applied
	GetSchedulingDecisions(ctx context.Context) ([]SchedulingDecision, error)
}

// SchedulingDecision represents a scheduling action to be taken
type SchedulingDecision struct {
	ContainerName string                 `json:"container_name"`
	Action        SchedulingAction       `json:"action"`
	Parameters    map[string]interface{} `json:"parameters"`
	Timestamp     time.Time              `json:"timestamp"`
	Reason        string                 `json:"reason"`
}

// SchedulingAction represents the type of scheduling action
type SchedulingAction string

const (
	ActionCPUPin        SchedulingAction = "cpu_pin"           // Pin container to specific CPU core(s)
	ActionCacheAllocate SchedulingAction = "cache_allocate"   // Allocate cache resources via RDT CAT
	ActionMemBWLimit    SchedulingAction = "membw_limit"      // Limit memory bandwidth via RDT MBA
	ActionCPUQuota      SchedulingAction = "cpu_quota"        // Adjust CPU quota/shares
	ActionMemoryLimit   SchedulingAction = "memory_limit"     // Adjust memory limits
	ActionPriority      SchedulingAction = "priority"         // Change container priority
	ActionThrottle      SchedulingAction = "throttle"         // Throttle container resources
	ActionMigrate       SchedulingAction = "migrate"          // Migrate container to different NUMA node
)

// RealTimeDataFeed provides real-time metrics to schedulers
type RealTimeDataFeed struct {
	subscribers []MetricsConsumer
	buffer      []*storage.BenchmarkMetrics
	maxBuffer   int
}

// NewRealTimeDataFeed creates a new real-time data feed
func NewRealTimeDataFeed(maxBuffer int) *RealTimeDataFeed {
	return &RealTimeDataFeed{
		subscribers: make([]MetricsConsumer, 0),
		buffer:      make([]*storage.BenchmarkMetrics, 0, maxBuffer),
		maxBuffer:   maxBuffer,
	}
}

// Subscribe adds a metrics consumer to receive real-time data
func (feed *RealTimeDataFeed) Subscribe(consumer MetricsConsumer) {
	feed.subscribers = append(feed.subscribers, consumer)
}

// PublishMetrics sends metrics to all subscribers and maintains a buffer
func (feed *RealTimeDataFeed) PublishMetrics(ctx context.Context, metrics *storage.BenchmarkMetrics) error {
	// Add to buffer (ring buffer behavior)
	feed.buffer = append(feed.buffer, metrics)
	if len(feed.buffer) > feed.maxBuffer {
		feed.buffer = feed.buffer[1:] // Remove oldest entry
	}

	// Send to all subscribers
	for _, subscriber := range feed.subscribers {
		if err := subscriber.OnMetrics(ctx, metrics); err != nil {
			// Log error but continue with other subscribers
			continue
		}
	}

	return nil
}

// GetRecentMetrics returns recent metrics from the buffer
func (feed *RealTimeDataFeed) GetRecentMetrics(count int) []*storage.BenchmarkMetrics {
	if count > len(feed.buffer) {
		count = len(feed.buffer)
	}
	if count == 0 {
		return []*storage.BenchmarkMetrics{}
	}

	// Return last 'count' metrics
	start := len(feed.buffer) - count
	result := make([]*storage.BenchmarkMetrics, count)
	copy(result, feed.buffer[start:])
	return result
}

// GetMetricsSince returns metrics since a specific time
func (feed *RealTimeDataFeed) GetMetricsSince(since time.Time) []*storage.BenchmarkMetrics {
	var result []*storage.BenchmarkMetrics
	for _, metrics := range feed.buffer {
		if metrics.UTCTimestamp.After(since) {
			result = append(result, metrics)
		}
	}
	return result
}

// GetMetricsForContainer returns metrics for a specific container
func (feed *RealTimeDataFeed) GetMetricsForContainer(containerName string, count int) []*storage.BenchmarkMetrics {
	var result []*storage.BenchmarkMetrics
	added := 0
	
	// Iterate backwards through buffer to get most recent first
	for i := len(feed.buffer) - 1; i >= 0 && added < count; i-- {
		if feed.buffer[i].ContainerName == containerName {
			result = append([]*storage.BenchmarkMetrics{feed.buffer[i]}, result...)
			added++
		}
	}
	
	return result
}
