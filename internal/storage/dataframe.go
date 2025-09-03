package storage

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// DataFrameEntry represents a single sampling step for a container
type DataFrameEntry struct {
	// Timing information
	SamplingStep     int64     `json:"sampling_step"`
	UTCTimestamp     time.Time `json:"utc_timestamp"`
	RelativeTime     int64     `json:"relative_time_ms"` // milliseconds since benchmark start

	// Performance data from different sources
	PerfMetrics   *PerfData   `json:"perf_metrics,omitempty"`
	DockerMetrics *DockerData `json:"docker_metrics,omitempty"`
	RDTMetrics    *RDTData    `json:"rdt_metrics,omitempty"`

	// Additional context
	CPUExecutedOn    int `json:"cpu_executed_on"`
}

// ContainerDataFrame represents all profiling data for a single container
type ContainerDataFrame struct {
	// Container metadata
	ContainerName  string `json:"container_name"`
	ContainerIndex int    `json:"container_index"`
	ContainerImage string `json:"container_image"`
	ContainerCore  int    `json:"container_core"`
	
	// Time series data - one entry per sampling step
	Entries []DataFrameEntry `json:"entries"`
	
	// Thread safety
	mutex sync.RWMutex
}

// BenchmarkDataFrames holds all container DataFrames for a benchmark
type BenchmarkDataFrames struct {
	// Benchmark-level metadata
	BenchmarkID       int64     `json:"benchmark_id"`
	BenchmarkStarted  time.Time `json:"benchmark_started"`
	BenchmarkFinished time.Time `json:"benchmark_finished,omitempty"`
	SamplingFrequency int       `json:"sampling_frequency_ms"`
	UsedScheduler     string    `json:"used_scheduler"`

	// Container DataFrames - one per container
	Containers map[string]*ContainerDataFrame `json:"containers"`
	
	// Thread safety
	mutex sync.RWMutex
}

// NewContainerDataFrame creates a new DataFrame for a container
func NewContainerDataFrame(containerName string, containerIndex int, containerImage string, containerCore int) *ContainerDataFrame {
	return &ContainerDataFrame{
		ContainerName:  containerName,
		ContainerIndex: containerIndex,
		ContainerImage: containerImage,
		ContainerCore:  containerCore,
		Entries:        make([]DataFrameEntry, 0),
	}
}

// NewBenchmarkDataFrames creates a new BenchmarkDataFrames structure
func NewBenchmarkDataFrames(benchmarkID int64, benchmarkStarted time.Time, samplingFrequency int, usedScheduler string) *BenchmarkDataFrames {
	return &BenchmarkDataFrames{
		BenchmarkID:       benchmarkID,
		BenchmarkStarted:  benchmarkStarted,
		SamplingFrequency: samplingFrequency,
		UsedScheduler:     usedScheduler,
		Containers:        make(map[string]*ContainerDataFrame),
	}
}

// AddContainer adds a new container DataFrame
func (bdf *BenchmarkDataFrames) AddContainer(containerName string, containerIndex int, containerImage string, containerCore int) {
	bdf.mutex.Lock()
	defer bdf.mutex.Unlock()
	
	bdf.Containers[containerName] = NewContainerDataFrame(containerName, containerIndex, containerImage, containerCore)
	
	log.WithFields(log.Fields{
		"container_name":  containerName,
		"container_index": containerIndex,
		"container_image": containerImage,
		"container_core":  containerCore,
	}).Debug("Added container DataFrame")
}

// AddDataPoint adds a new data point to a container's DataFrame
func (bdf *BenchmarkDataFrames) AddDataPoint(
	containerName string,
	samplingStep int64,
	timestamp time.Time,
	relativeTime int64,
	cpuExecutedOn int,
	perfMetrics *PerfData,
	dockerMetrics *DockerData,
	rdtMetrics *RDTData,
) {
	bdf.mutex.RLock()
	containerDF, exists := bdf.Containers[containerName]
	bdf.mutex.RUnlock()
	
	if !exists {
		log.WithField("container_name", containerName).Warn("Container DataFrame not found, skipping data point")
		return
	}

	entry := DataFrameEntry{
		SamplingStep:     samplingStep,
		UTCTimestamp:     timestamp,
		RelativeTime:     relativeTime,
		CPUExecutedOn:    cpuExecutedOn,
		PerfMetrics:      perfMetrics,
		DockerMetrics:    dockerMetrics,
		RDTMetrics:       rdtMetrics,
	}

	containerDF.mutex.Lock()
	containerDF.Entries = append(containerDF.Entries, entry)
	containerDF.mutex.Unlock()

	log.WithFields(log.Fields{
		"container":     containerName,
		"sampling_step": samplingStep,
		"entries_count": len(containerDF.Entries),
	}).Debug("Added data point to container DataFrame")
}

// GetContainerCount returns the number of containers being tracked
func (bdf *BenchmarkDataFrames) GetContainerCount() int {
	bdf.mutex.RLock()
	defer bdf.mutex.RUnlock()
	
	return len(bdf.Containers)
}

// GetTotalDataPoints returns the total number of data points across all containers
func (bdf *BenchmarkDataFrames) GetTotalDataPoints() int {
	bdf.mutex.RLock()
	defer bdf.mutex.RUnlock()
	
	total := 0
	for _, containerDF := range bdf.Containers {
		containerDF.mutex.RLock()
		total += len(containerDF.Entries)
		containerDF.mutex.RUnlock()
	}
	
	return total
}

// GetSummary returns a summary of the DataFrames for logging
func (bdf *BenchmarkDataFrames) GetSummary() map[string]interface{} {
	bdf.mutex.RLock()
	defer bdf.mutex.RUnlock()
	
	summary := map[string]interface{}{
		"benchmark_id":       bdf.BenchmarkID,
		"benchmark_started":  bdf.BenchmarkStarted,
		"sampling_frequency": bdf.SamplingFrequency,
		"used_scheduler":     bdf.UsedScheduler,
		"container_count":    len(bdf.Containers),
		"total_data_points":  bdf.GetTotalDataPoints(),
		"containers":         make(map[string]interface{}),
	}

	containers := summary["containers"].(map[string]interface{})
	for name, containerDF := range bdf.Containers {
		containerDF.mutex.RLock()
		containers[name] = map[string]interface{}{
			"container_name":  containerDF.ContainerName,
			"container_index": containerDF.ContainerIndex,
			"container_image": containerDF.ContainerImage,
			"container_core":  containerDF.ContainerCore,
			"entries_count":   len(containerDF.Entries),
		}
		containerDF.mutex.RUnlock()
	}

	return summary
}

// SetBenchmarkFinished sets the benchmark finished time
func (bdf *BenchmarkDataFrames) SetBenchmarkFinished(finishedTime time.Time) {
	bdf.mutex.Lock()
	defer bdf.mutex.Unlock()
	
	bdf.BenchmarkFinished = finishedTime
}

// LogDataFramesSummary logs a comprehensive summary of the DataFrames
func (bdf *BenchmarkDataFrames) LogDataFramesSummary() {
	summary := bdf.GetSummary()
	
	log.WithFields(log.Fields{
		"benchmark_id":         summary["benchmark_id"],
		"benchmark_started":    summary["benchmark_started"],
		"benchmark_finished":   bdf.BenchmarkFinished,
		"sampling_frequency":   summary["sampling_frequency"],
		"used_scheduler":       summary["used_scheduler"],
		"container_count":      summary["container_count"],
		"total_data_points":    summary["total_data_points"],
		"duration_seconds":     func() float64 {
			if !bdf.BenchmarkFinished.IsZero() {
				return bdf.BenchmarkFinished.Sub(bdf.BenchmarkStarted).Seconds()
			}
			return time.Since(bdf.BenchmarkStarted).Seconds()
		}(),
	}).Info("Benchmark DataFrames Summary")

	// Log per-container details
	bdf.mutex.RLock()
	defer bdf.mutex.RUnlock()
	
	for name, containerDF := range bdf.Containers {
		containerDF.mutex.RLock()
		entriesCount := len(containerDF.Entries)
		
		// Get first and last timestamp if we have entries
		var firstTimestamp, lastTimestamp time.Time
		if entriesCount > 0 {
			firstTimestamp = containerDF.Entries[0].UTCTimestamp
			lastTimestamp = containerDF.Entries[entriesCount-1].UTCTimestamp
		}
		
		log.WithFields(log.Fields{
			"container_name":    name,
			"container_index":   containerDF.ContainerIndex,
			"container_image":   containerDF.ContainerImage,
			"container_core":    containerDF.ContainerCore,
			"entries_count":     entriesCount,
			"first_timestamp":   firstTimestamp,
			"last_timestamp":    lastTimestamp,
			"duration_seconds":  func() float64 {
				if !firstTimestamp.IsZero() && !lastTimestamp.IsZero() {
					return lastTimestamp.Sub(firstTimestamp).Seconds()
				}
				return 0
			}(),
		}).Info("Container DataFrame Summary")
		
		containerDF.mutex.RUnlock()
	}
}

// ConvertToStorableMetrics converts all DataFrame entries to BenchmarkMetrics for database storage
func (bdf *BenchmarkDataFrames) ConvertToStorableMetrics() []*BenchmarkMetrics {
	var metrics []*BenchmarkMetrics

	bdf.mutex.RLock()
	defer bdf.mutex.RUnlock()

	for _, containerDF := range bdf.Containers {
		containerDF.mutex.RLock()
		
		for _, entry := range containerDF.Entries {
			metric := &BenchmarkMetrics{
				// Benchmark-level metadata
				BenchmarkID:       bdf.BenchmarkID,
				BenchmarkStarted:  bdf.BenchmarkStarted,
				BenchmarkFinished: bdf.BenchmarkFinished,
				SamplingFrequency: bdf.SamplingFrequency,
				UsedScheduler:     bdf.UsedScheduler,

				// Container-level metadata
				ContainerName:  containerDF.ContainerName,
				ContainerIndex: containerDF.ContainerIndex,
				ContainerImage: containerDF.ContainerImage,
				ContainerCore:  containerDF.ContainerCore,

				// Timing information
				UTCTimestamp:     entry.UTCTimestamp,
				RelativeTime:     entry.RelativeTime,
				SamplingStep:     entry.SamplingStep,
				CPUExecutedOn:    entry.CPUExecutedOn,

				// Performance data
				PerfMetrics:   entry.PerfMetrics,
				DockerMetrics: entry.DockerMetrics,
				RDTMetrics:    entry.RDTMetrics,
			}
			
			metrics = append(metrics, metric)
		}
		
		containerDF.mutex.RUnlock()
	}

	log.WithFields(log.Fields{
		"total_metrics": len(metrics),
		"containers":    len(bdf.Containers),
	}).Info("Converted DataFrames to storable metrics")

	return metrics
}
