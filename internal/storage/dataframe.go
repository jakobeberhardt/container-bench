package storage

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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

// ExportToCSV exports the benchmark DataFrames to CSV files
func (bdf *BenchmarkDataFrames) ExportToCSV(exportPath string, benchmarkName string) error {
	bdf.mutex.RLock()
	defer bdf.mutex.RUnlock()

	// Create export directory if it doesn't exist
	if err := os.MkdirAll(exportPath, 0755); err != nil {
		return fmt.Errorf("failed to create export directory: %w", err)
	}

	// Create a timestamp for the file names
	timestamp := bdf.BenchmarkStarted.Format("20060102_150405")
	
	// Export benchmark metadata
	metadataFile := filepath.Join(exportPath, fmt.Sprintf("%s_%s_metadata.csv", benchmarkName, timestamp))
	if err := bdf.exportMetadata(metadataFile); err != nil {
		return fmt.Errorf("failed to export metadata: %w", err)
	}

	// Export data for each container
	for containerName, containerDF := range bdf.Containers {
		filename := filepath.Join(exportPath, fmt.Sprintf("%s_%s_%s.csv", benchmarkName, timestamp, containerName))
		if err := containerDF.ExportToCSV(filename); err != nil {
			return fmt.Errorf("failed to export container %s: %w", containerName, err)
		}
		
		log.WithFields(log.Fields{
			"container": containerName,
			"filename":  filename,
			"entries":   len(containerDF.Entries),
		}).Info("Exported container DataFrame to CSV")
	}

	log.WithFields(log.Fields{
		"export_path":    exportPath,
		"benchmark_name": benchmarkName,
		"containers":     len(bdf.Containers),
	}).Info("Successfully exported all DataFrames to CSV")

	return nil
}

// exportMetadata exports benchmark metadata to a CSV file
func (bdf *BenchmarkDataFrames) exportMetadata(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"Property", "Value"}); err != nil {
		return err
	}

	// Write metadata
	metadata := [][]string{
		{"benchmark_id", strconv.FormatInt(bdf.BenchmarkID, 10)},
		{"benchmark_started", bdf.BenchmarkStarted.Format(time.RFC3339)},
		{"benchmark_finished", bdf.BenchmarkFinished.Format(time.RFC3339)},
		{"sampling_frequency_ms", strconv.Itoa(bdf.SamplingFrequency)},
		{"used_scheduler", bdf.UsedScheduler},
		{"container_count", strconv.Itoa(len(bdf.Containers))},
	}

	for _, row := range metadata {
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

// ExportToCSV exports a single container's DataFrame to CSV
func (cdf *ContainerDataFrame) ExportToCSV(filename string) error {
	cdf.mutex.RLock()
	defer cdf.mutex.RUnlock()

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"container_name", "container_index", "container_image", "container_core",
		"sampling_step", "utc_timestamp", "relative_time_ms", "cpu_executed_on",
		// Perf metrics
		"perf_cpu_cycles", "perf_instructions", "perf_cache_references", "perf_cache_misses",
		"perf_branch_instructions", "perf_branch_misses", "perf_ipc", "perf_cache_miss_rate",
		// Docker metrics  
		"docker_cpu_usage_percent", "docker_memory_usage", "docker_memory_limit",
		"docker_network_rx_bytes", "docker_network_tx_bytes", "docker_disk_read_bytes", "docker_disk_write_bytes",
		// RDT metrics
		"rdt_llc_occupancy", "rdt_local_mem_bw", "rdt_remote_mem_bw", "rdt_total_mem_bw",
	}

	if err := writer.Write(header); err != nil {
		return err
	}

	// Write data rows
	for _, entry := range cdf.Entries {
		row := []string{
			cdf.ContainerName,
			strconv.Itoa(cdf.ContainerIndex),
			cdf.ContainerImage,
			strconv.Itoa(cdf.ContainerCore),
			strconv.FormatInt(entry.SamplingStep, 10),
			entry.UTCTimestamp.Format(time.RFC3339Nano),
			strconv.FormatInt(entry.RelativeTime, 10),
			strconv.Itoa(entry.CPUExecutedOn),
		}

		// Add Perf metrics
		if entry.PerfMetrics != nil {
			row = append(row, []string{
				strconv.FormatUint(entry.PerfMetrics.CPUCycles, 10),
				strconv.FormatUint(entry.PerfMetrics.Instructions, 10),
				strconv.FormatUint(entry.PerfMetrics.CacheReferences, 10),
				strconv.FormatUint(entry.PerfMetrics.CacheMisses, 10),
				strconv.FormatUint(entry.PerfMetrics.BranchInstructions, 10),
				strconv.FormatUint(entry.PerfMetrics.BranchMisses, 10),
				strconv.FormatFloat(entry.PerfMetrics.IPC, 'f', 6, 64),
				strconv.FormatFloat(entry.PerfMetrics.CacheMissRate, 'f', 6, 64),
			}...)
		} else {
			row = append(row, []string{"", "", "", "", "", "", "", ""}...)
		}

		// Add Docker metrics
		if entry.DockerMetrics != nil {
			row = append(row, []string{
				strconv.FormatFloat(entry.DockerMetrics.CPUUsagePercent, 'f', 6, 64),
				strconv.FormatUint(entry.DockerMetrics.MemoryUsage, 10),
				strconv.FormatUint(entry.DockerMetrics.MemoryLimit, 10),
				strconv.FormatUint(entry.DockerMetrics.NetworkRxBytes, 10),
				strconv.FormatUint(entry.DockerMetrics.NetworkTxBytes, 10),
				strconv.FormatUint(entry.DockerMetrics.DiskReadBytes, 10),
				strconv.FormatUint(entry.DockerMetrics.DiskWriteBytes, 10),
			}...)
		} else {
			row = append(row, []string{"", "", "", "", "", "", ""}...)
		}

		// Add RDT metrics
		if entry.RDTMetrics != nil {
			row = append(row, []string{
				strconv.FormatUint(entry.RDTMetrics.LLCOccupancy, 10),
				strconv.FormatFloat(entry.RDTMetrics.LocalMemBW, 'f', 6, 64),
				strconv.FormatFloat(entry.RDTMetrics.RemoteMemBW, 'f', 6, 64),
				strconv.FormatFloat(entry.RDTMetrics.TotalMemBW, 'f', 6, 64),
			}...)
		} else {
			row = append(row, []string{"", "", "", ""}...)
		}

		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}
