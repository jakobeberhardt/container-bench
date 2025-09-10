package database

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/logging"
	
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/sirupsen/logrus"
)

// BenchmarkMetadata contains all metadata about a benchmark run
type BenchmarkMetadata struct {
	BenchmarkID            int    `json:"benchmark_id"`
	BenchmarkName          string `json:"benchmark_name"`
	Description            string `json:"description"`
	DurationSeconds        int64  `json:"duration_seconds"`
	BenchmarkStarted       string `json:"benchmark_started"`       // RFC3339 timestamp
	BenchmarkFinished      string `json:"benchmark_finished"`      // RFC3339 timestamp
	TotalContainers        int    `json:"total_containers"`
	DriverVersion          string `json:"driver_version"`
	UsedScheduler          string `json:"used_scheduler"`
	SchedulerVersion       string `json:"scheduler_version"`
	Hostname               string `json:"hostname"`
	ExecutionHost          string `json:"execution_host"`
	OSInfo                 string `json:"os_info"`
	KernelVersion          string `json:"kernel_version"`
	CPUVendor              string `json:"cpu_vendor"`
	CPUModel               string `json:"cpu_model"`
	TotalCPUCores          int    `json:"total_cpu_cores"`
	CPUThreads             int    `json:"cpu_threads"`
	MaxDurationSeconds     int    `json:"max_duration_seconds"`
	SamplingFrequencyMS    int    `json:"sampling_frequency_ms"`
	TotalSamplingSteps     int    `json:"total_sampling_steps"`
	TotalMeasurements      int    `json:"total_measurements"`
	TotalDataSizeBytes     int64  `json:"total_data_size_bytes"`
	ConfigFile             string `json:"config_file"`
}

// SystemInfo contains host system information
type SystemInfo struct {
	Hostname      string
	OSInfo        string
	KernelVersion string
	CPUVendor     string
	CPUModel      string
	CPUCores      int
	CPUThreads    int
}

// collectSystemInfo gathers host system information
func collectSystemInfo() (*SystemInfo, error) {
	info := &SystemInfo{}
	
	// Get hostname
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	info.Hostname = hostname
	
	// Get OS info
	info.OSInfo = runtime.GOOS + "/" + runtime.GOARCH
	
	// Get kernel version from /proc/version
	if data, err := os.ReadFile("/proc/version"); err == nil {
		parts := strings.Fields(string(data))
		if len(parts) >= 3 {
			info.KernelVersion = parts[2]
		}
	}
	if info.KernelVersion == "" {
		info.KernelVersion = "unknown"
	}
	
	// Get CPU info from /proc/cpuinfo
	if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "vendor_id") {
				parts := strings.Split(line, ":")
				if len(parts) >= 2 {
					info.CPUVendor = strings.TrimSpace(parts[1])
				}
			} else if strings.HasPrefix(line, "model name") {
				parts := strings.Split(line, ":")
				if len(parts) >= 2 {
					info.CPUModel = strings.TrimSpace(parts[1])
				}
			}
		}
	}
	
	// Set defaults if not found
	if info.CPUVendor == "" {
		info.CPUVendor = "unknown"
	}
	if info.CPUModel == "" {
		info.CPUModel = "unknown"
	}
	
	// Get CPU core/thread count
	info.CPUCores = runtime.NumCPU()
	info.CPUThreads = runtime.NumCPU() // This might be the same as cores in some cases
	
	return info, nil
}

type InfluxDBClient struct {
	client   influxdb2.Client
	writeAPI api.WriteAPIBlocking
	queryAPI api.QueryAPI
	bucket   string
	org      string
}

func NewInfluxDBClient(config config.DatabaseConfig) (*InfluxDBClient, error) {
	logger := logging.GetLogger()
	
	client := influxdb2.NewClient(config.Host, config.Password)
	
	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	health, err := client.Health(ctx)
	if err != nil {
		logger.WithField("host", config.Host).WithError(err).Error("Failed to connect to InfluxDB")
		return nil, err
	}
	
	if health.Status != "pass" {
		logger.WithFields(logrus.Fields{
			"host":    config.Host,
			"status":  health.Status,
			"message": health.Message,
		}).Error("InfluxDB health check failed")
		return nil, err
	}

	writeAPI := client.WriteAPIBlocking(config.Org, config.Name)
	queryAPI := client.QueryAPI(config.Org)

	logger.WithFields(logrus.Fields{
		"host":   config.Host,
		"bucket": config.Name,
		"org":    config.Org,
	}).Info("Connected to InfluxDB")

	return &InfluxDBClient{
		client:   client,
		writeAPI: writeAPI,
		queryAPI: queryAPI,
		bucket:   config.Name,
		org:      config.Org,
	}, nil
}

func (idb *InfluxDBClient) GetLastBenchmarkID() (int, error) {
	ctx := context.Background()
	
	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: -30d)
		|> filter(fn: (r) => r._measurement == "benchmark_metrics")
		|> distinct(column: "benchmark_id")
		|> map(fn: (r) => ({_value: int(v: r.benchmark_id)}))
		|> max()
		|> yield(name: "max_benchmark_id")
	`, idb.bucket)

	result, err := idb.queryAPI.Query(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to query last benchmark ID: %w", err)
	}
	defer result.Close()

	maxID := 0
	for result.Next() {
		if result.Record().Value() != nil {
			if id, ok := result.Record().Value().(int64); ok {
				maxID = int(id)
			}
		}
	}

	if result.Err() != nil {
		return 0, fmt.Errorf("error reading query results: %w", result.Err())
	}

	return maxID, nil
}

func (idb *InfluxDBClient) WriteDataFrames(benchmarkID int, benchmarkConfig *config.BenchmarkConfig, dataframes *dataframe.DataFrames, startTime, endTime time.Time) error {
	ctx := context.Background()

	// Create points for all container data
	var points []*write.Point

	containers := dataframes.GetAllContainers()
	for containerIndex, containerDF := range containers {
		containerConfig := idb.getContainerConfig(benchmarkConfig, containerIndex)
		if containerConfig == nil {
			continue
		}

		steps := containerDF.GetAllSteps()
		for stepNumber, step := range steps {
			if step == nil {
				continue
			}

			// Create base point with common tags
			point := influxdb2.NewPoint("benchmark_metrics",
				map[string]string{
					"benchmark_id":     fmt.Sprintf("%d", benchmarkID),
					"container_index":  fmt.Sprintf("%d", containerIndex),
					"container_image":  containerConfig.Image,
					"container_core":   fmt.Sprintf("%d", containerConfig.Core),
					"benchmark_started": startTime.Format(time.RFC3339),
					"benchmark_finished": endTime.Format(time.RFC3339),
				},
				idb.createFields(step, stepNumber),
				step.Timestamp)

			points = append(points, point)
		}
	}

	// Write all points
	if len(points) > 0 {
		if err := idb.writeAPI.WritePoint(ctx, points...); err != nil {
			return fmt.Errorf("failed to write data points: %w", err)
		}
	}

	return nil
}

func (idb *InfluxDBClient) WriteMetadata(metadata *BenchmarkMetadata) error {
	ctx := context.Background()

	// Create point for metadata
	point := influxdb2.NewPoint("benchmark_meta",
		map[string]string{
			"benchmark_id": fmt.Sprintf("%d", metadata.BenchmarkID),
		},
		map[string]interface{}{
			"benchmark_name":          metadata.BenchmarkName,
			"description":             metadata.Description,
			"duration_seconds":        metadata.DurationSeconds,
			"benchmark_started":       metadata.BenchmarkStarted,
			"benchmark_finished":      metadata.BenchmarkFinished,
			"total_containers":        metadata.TotalContainers,
			"driver_version":          metadata.DriverVersion,
			"used_scheduler":          metadata.UsedScheduler,
			"scheduler_version":       metadata.SchedulerVersion,
			"hostname":                metadata.Hostname,
			"execution_host":          metadata.ExecutionHost,
			"os_info":                 metadata.OSInfo,
			"kernel_version":          metadata.KernelVersion,
			"cpu_vendor":              metadata.CPUVendor,
			"cpu_model":               metadata.CPUModel,
			"total_cpu_cores":         metadata.TotalCPUCores,
			"cpu_threads":             metadata.CPUThreads,
			"max_duration_seconds":    metadata.MaxDurationSeconds,
			"sampling_frequency_ms":   metadata.SamplingFrequencyMS,
			"total_sampling_steps":    metadata.TotalSamplingSteps,
			"total_measurements":      metadata.TotalMeasurements,
			"total_data_size_bytes":   metadata.TotalDataSizeBytes,
			"config_file":             metadata.ConfigFile,
		},
		time.Now())

	// Write metadata point
	if err := idb.writeAPI.WritePoint(ctx, point); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

func CollectBenchmarkMetadata(benchmarkID int, config *config.BenchmarkConfig, configContent string, dataframes *dataframe.DataFrames, startTime, endTime time.Time, driverVersion string) (*BenchmarkMetadata, error) {
	// Collect system information
	sysInfo, err := collectSystemInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to collect system info: %w", err)
	}

	// Calculate metrics from dataframes
	totalSteps := 0
	totalMeasurements := 0
	
	containers := dataframes.GetAllContainers()
	for _, containerDF := range containers {
		steps := containerDF.GetAllSteps()
		totalSteps += len(steps)
		
		// Count measurements (each step with data counts as multiple measurements)
		for _, step := range steps {
			if step != nil {
				measurements := 0
				if step.Perf != nil {
					measurements += 10 // Approximate number of perf metrics
				}
				if step.Docker != nil {
					measurements += 18 // Approximate number of docker metrics
				}
				if step.RDT != nil {
					measurements += 3 // Approximate number of RDT metrics
				}
				totalMeasurements += measurements
			}
		}
	}

	// Calculate average sampling frequency
	avgFrequency := 0
	if len(config.Containers) > 0 {
		totalFreq := 0
		for _, container := range config.Containers {
			totalFreq += container.Data.Frequency
		}
		avgFrequency = totalFreq / len(config.Containers)
	}

	// Estimate data size (rough calculation)
	// Each measurement is approximately 16 bytes (8 bytes timestamp + 8 bytes value)
	estimatedDataSize := int64(totalMeasurements * 16)

	metadata := &BenchmarkMetadata{
		BenchmarkID:         benchmarkID,
		BenchmarkName:       config.Benchmark.Name,
		Description:         config.Benchmark.Description,
		DurationSeconds:     int64(endTime.Sub(startTime).Seconds()),
		BenchmarkStarted:    startTime.Format(time.RFC3339),
		BenchmarkFinished:   endTime.Format(time.RFC3339),
		TotalContainers:     len(config.Containers),
		DriverVersion:       driverVersion,
		UsedScheduler:       config.Benchmark.Scheduler.Implementation,
		SchedulerVersion:    "1.0.0", // Default scheduler version
		Hostname:            sysInfo.Hostname,
		ExecutionHost:       sysInfo.Hostname, // Same as hostname for now
		OSInfo:              sysInfo.OSInfo,
		KernelVersion:       sysInfo.KernelVersion,
		CPUVendor:           sysInfo.CPUVendor,
		CPUModel:            sysInfo.CPUModel,
		TotalCPUCores:       sysInfo.CPUCores,
		CPUThreads:          sysInfo.CPUThreads,
		MaxDurationSeconds:  config.Benchmark.MaxT,
		SamplingFrequencyMS: avgFrequency,
		TotalSamplingSteps:  totalSteps,
		TotalMeasurements:   totalMeasurements,
		TotalDataSizeBytes:  estimatedDataSize,
		ConfigFile:          configContent,
	}

	return metadata, nil
}

func (idb *InfluxDBClient) getContainerConfig(benchmarkConfig *config.BenchmarkConfig, containerIndex int) *config.ContainerConfig {
	for _, container := range benchmarkConfig.Containers {
		if container.Index == containerIndex {
			return &container
		}
	}
	return nil
}

func (idb *InfluxDBClient) createFields(step *dataframe.SamplingStep, stepNumber int) map[string]interface{} {
	fields := make(map[string]interface{})

	// Add step number
	fields["step_number"] = stepNumber

	// Add Perf metrics
	if step.Perf != nil {
		if step.Perf.CacheMisses != nil {
			fields["perf_cache_misses"] = *step.Perf.CacheMisses
		}
		if step.Perf.CacheReferences != nil {
			fields["perf_cache_references"] = *step.Perf.CacheReferences
		}
		if step.Perf.Instructions != nil {
			fields["perf_instructions"] = *step.Perf.Instructions
		}
		if step.Perf.Cycles != nil {
			fields["perf_cycles"] = *step.Perf.Cycles
		}
		if step.Perf.BranchInstructions != nil {
			fields["perf_branch_instructions"] = *step.Perf.BranchInstructions
		}
		if step.Perf.BranchMisses != nil {
			fields["perf_branch_misses"] = *step.Perf.BranchMisses
		}
		if step.Perf.BusCycles != nil {
			fields["perf_bus_cycles"] = *step.Perf.BusCycles
		}
		if step.Perf.CacheMissRate != nil {
			fields["perf_cache_miss_rate"] = *step.Perf.CacheMissRate
		}
		if step.Perf.BranchMissRate != nil {
			fields["perf_branch_miss_rate"] = *step.Perf.BranchMissRate
		}
		if step.Perf.InstructionsPerCycle != nil {
			fields["perf_instructions_per_cycle"] = *step.Perf.InstructionsPerCycle
		}
	}

	// Add Docker metrics
	if step.Docker != nil {
		if step.Docker.CPUUsageTotal != nil {
			fields["docker_cpu_usage_total"] = *step.Docker.CPUUsageTotal
		}
		if step.Docker.CPUUsageKernel != nil {
			fields["docker_cpu_usage_kernel"] = *step.Docker.CPUUsageKernel
		}
		if step.Docker.CPUUsageUser != nil {
			fields["docker_cpu_usage_user"] = *step.Docker.CPUUsageUser
		}
		if step.Docker.CPUUsagePercent != nil {
			fields["docker_cpu_usage_percent"] = *step.Docker.CPUUsagePercent
		}
		if step.Docker.CPUThrottling != nil {
			fields["docker_cpu_throttling"] = *step.Docker.CPUThrottling
		}
		if step.Docker.MemoryUsage != nil {
			fields["docker_memory_usage"] = *step.Docker.MemoryUsage
		}
		if step.Docker.MemoryLimit != nil {
			fields["docker_memory_limit"] = *step.Docker.MemoryLimit
		}
		if step.Docker.MemoryCache != nil {
			fields["docker_memory_cache"] = *step.Docker.MemoryCache
		}
		if step.Docker.MemoryRSS != nil {
			fields["docker_memory_rss"] = *step.Docker.MemoryRSS
		}
		if step.Docker.MemorySwap != nil {
			fields["docker_memory_swap"] = *step.Docker.MemorySwap
		}
		if step.Docker.MemoryUsagePercent != nil {
			fields["docker_memory_usage_percent"] = *step.Docker.MemoryUsagePercent
		}
		if step.Docker.NetworkRxBytes != nil {
			fields["docker_network_rx_bytes"] = *step.Docker.NetworkRxBytes
		}
		if step.Docker.NetworkTxBytes != nil {
			fields["docker_network_tx_bytes"] = *step.Docker.NetworkTxBytes
		}
		if step.Docker.NetworkRxPackets != nil {
			fields["docker_network_rx_packets"] = *step.Docker.NetworkRxPackets
		}
		if step.Docker.NetworkTxPackets != nil {
			fields["docker_network_tx_packets"] = *step.Docker.NetworkTxPackets
		}
		if step.Docker.DiskReadBytes != nil {
			fields["docker_disk_read_bytes"] = *step.Docker.DiskReadBytes
		}
		if step.Docker.DiskWriteBytes != nil {
			fields["docker_disk_write_bytes"] = *step.Docker.DiskWriteBytes
		}
		if step.Docker.DiskReadOps != nil {
			fields["docker_disk_read_ops"] = *step.Docker.DiskReadOps
		}
		if step.Docker.DiskWriteOps != nil {
			fields["docker_disk_write_ops"] = *step.Docker.DiskWriteOps
		}
	}

	// Add RDT metrics
	if step.RDT != nil {
		if step.RDT.L3CacheUsage != nil {
			fields["rdt_l3_cache_usage"] = *step.RDT.L3CacheUsage
		}
		if step.RDT.MemoryBandwidth != nil {
			fields["rdt_memory_bandwidth"] = *step.RDT.MemoryBandwidth
		}
		if step.RDT.CLOSGroup != nil {
			fields["rdt_clos_group"] = *step.RDT.CLOSGroup
		}
	}

	return fields
}

func (idb *InfluxDBClient) Close() {
	if idb.client != nil {
		idb.client.Close()
	}
}
