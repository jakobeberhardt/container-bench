package database

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/datahandeling"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/sirupsen/logrus"
)

// defines the interface for database operations
type DatabaseClient interface {
	GetLastBenchmarkID() (int, error)
	WriteBenchmarkMetrics(benchmarkID int, benchmarkConfig *config.BenchmarkConfig, metrics *datahandeling.BenchmarkMetrics, startTime, endTime time.Time) error
	WriteMetadata(metadata *BenchmarkMetadata) error
	Close()
}

// contains all metadata about a benchmark run
type BenchmarkMetadata struct {
	BenchmarkID            int    `json:"benchmark_id"`
	BenchmarkName          string `json:"benchmark_name"`
	Description            string `json:"description"`
	DurationSeconds        int64  `json:"duration_seconds"`
	BenchmarkStarted       string `json:"benchmark_started"`       
	BenchmarkFinished      string `json:"benchmark_finished"`      
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
	L3CacheSizeBytes       int64  `json:"l3_cache_size_bytes"`
	MaxMemoryBandwidthMBps int64  `json:"max_memory_bandwidth_mbps"`
	MaxDurationSeconds     int    `json:"max_duration_seconds"`
	SamplingFrequencyMS    int    `json:"sampling_frequency_ms"`
	TotalSamplingSteps     int    `json:"total_sampling_steps"`
	TotalMeasurements      int    `json:"total_measurements"`
	TotalDataSizeBytes     int64  `json:"total_data_size_bytes"`
	ConfigFile             string `json:"config_file"`
}

// contains host system information
type SystemInfo struct {
	Hostname               string
	OSInfo                 string
	KernelVersion          string
	CPUVendor              string
	CPUModel               string
	CPUCores               int
	CPUThreads             int
	L3CacheSizeBytes       int64
	MaxMemoryBandwidthMBps int64
}

// collectSystemInfo gathers host system information
// TODO: We should use the hostconfig here for consistency
func collectSystemInfo() (*SystemInfo, error) {
	info := &SystemInfo{}
	
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	info.Hostname = hostname
	
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
	info.CPUThreads = runtime.NumCPU()
	
	// Try to collect L3 cache size from /sys/devices/system/cpu/cpu0/cache/index3/size
	if data, err := os.ReadFile("/sys/devices/system/cpu/cpu0/cache/index3/size"); err == nil {
		sizeStr := strings.TrimSpace(string(data))
		if strings.HasSuffix(sizeStr, "K") {
			if size, err := strconv.ParseInt(strings.TrimSuffix(sizeStr, "K"), 10, 64); err == nil {
				info.L3CacheSizeBytes = size * 1024
			}
		} else if strings.HasSuffix(sizeStr, "M") {
			if size, err := strconv.ParseInt(strings.TrimSuffix(sizeStr, "M"), 10, 64); err == nil {
				info.L3CacheSizeBytes = size * 1024 * 1024
			}
		}
	}
	
	
	return info, nil
}

type InfluxDBClient struct {
	client     influxdb2.Client
	writeAPI   api.WriteAPIBlocking
	queryAPI   api.QueryAPI
	bucket     string
	org        string
	config     config.DatabaseConfig
	maxRetries int
	batchSize  int
}

func NewInfluxDBClient(config config.DatabaseConfig) (*InfluxDBClient, error) {
	logger := logging.GetLogger()
	
	client := influxdb2.NewClient(config.Host, config.Password)
	
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
		client:     client,
		writeAPI:   writeAPI,
		queryAPI:   queryAPI,
		bucket:     config.Name,
		org:        config.Org,
		config:     config,
		maxRetries: 5,     
		batchSize:  1000,  
	}, nil
}

func (idb *InfluxDBClient) GetLastBenchmarkID() (int, error) {
	logger := logging.GetLogger()
	var result int
	
	operation := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		
		query := fmt.Sprintf(`
			from(bucket: "%s")
			|> range(start: -300d)
			|> filter(fn: (r) => r._measurement == "benchmark_metrics")
			|> distinct(column: "benchmark_id")
			|> map(fn: (r) => ({_value: int(v: r.benchmark_id)}))
			|> max()
			|> yield(name: "max_benchmark_id")
		`, idb.bucket)

		queryResult, err := idb.queryAPI.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to query last benchmark ID: %w", err)
		}
		defer queryResult.Close()

		maxID := 0
		for queryResult.Next() {
			if queryResult.Record().Value() != nil {
				if id, ok := queryResult.Record().Value().(int64); ok {
					maxID = int(id)
				}
			}
		}

		if queryResult.Err() != nil {
			return fmt.Errorf("error reading query results: %w", queryResult.Err())
		}

		result = maxID
		return nil
	}
	
	err := idb.retryWithBackoff(operation, "get_last_benchmark_id")
	if err != nil {
		logger.WithError(err).Warn("Failed to get last benchmark ID, defaulting to 0")
		return 0, nil 
	}
	
	logger.WithField("last_benchmark_id", result).Debug("Retrieved last benchmark ID")
	return result, nil
}

// checks if the InfluxDB connection is still healthy
func (idb *InfluxDBClient) validateConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	health, err := idb.client.Health(ctx)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	
	if health.Status != "pass" {
		return fmt.Errorf("health check failed: status %s, message: %s", health.Status, health.Message)
	}
	
	return nil
}

// recreates the InfluxDB client and APIs
func (idb *InfluxDBClient) refreshConnection() error {
	logger := logging.GetLogger()
	logger.Info("Refreshing InfluxDB connection")
	
	if idb.client != nil {
		idb.client.Close()
	}
	
	idb.client = influxdb2.NewClient(idb.config.Host, idb.config.Password)
	idb.writeAPI = idb.client.WriteAPIBlocking(idb.config.Org, idb.config.Name)
	idb.queryAPI = idb.client.QueryAPI(idb.config.Org)
	
	return idb.validateConnection()
}

// executes a function with exponential backoff retry logic
func (idb *InfluxDBClient) retryWithBackoff(operation func() error, operationName string) error {
	logger := logging.GetLogger()
	var lastErr error
	
	for attempt := 0; attempt < idb.maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			logger.WithFields(logrus.Fields{
				"operation": operationName,
				"attempt":   attempt + 1,
				"delay":     delay,
			}).Warn("Retrying operation after delay")
			time.Sleep(delay)
		}
		
		if err := idb.validateConnection(); err != nil {
			logger.WithField("operation", operationName).WithError(err).Warn("Connection validation failed, refreshing")
			if refreshErr := idb.refreshConnection(); refreshErr != nil {
				lastErr = fmt.Errorf("failed to refresh connection: %w", refreshErr)
				continue
			}
		}
		
		if err := operation(); err != nil {
			lastErr = err
			logger.WithFields(logrus.Fields{
				"operation": operationName,
				"attempt":   attempt + 1,
				"error":     err,
			}).Warn("Operation failed")
			continue
		}
		
		// Success
		if attempt > 0 {
			logger.WithFields(logrus.Fields{
				"operation": operationName,
				"attempt":   attempt + 1,
			}).Info("Operation succeeded after retry")
		}
		return nil
	}
	
	return fmt.Errorf("operation %s failed after %d attempts: %w", operationName, idb.maxRetries, lastErr)
}

// writes a batch of points with timeout and retry logic
func (idb *InfluxDBClient) writePointsBatch(points []*write.Point, batchNum, totalBatches int) error {
	logger := logging.GetLogger()
	
	operation := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // 60s timeout for writes
		defer cancel()
		
		logger.WithFields(logrus.Fields{
			"batch_num":     batchNum,
			"total_batches": totalBatches,
			"points_count":  len(points),
		}).Debug("Writing batch to InfluxDB")
		
		return idb.writeAPI.WritePoint(ctx, points...)
	}
	
	operationName := fmt.Sprintf("write_batch_%d_%d", batchNum, totalBatches)
	return idb.retryWithBackoff(operation, operationName)
}

func (idb *InfluxDBClient) WriteBenchmarkMetrics(benchmarkID int, benchmarkConfig *config.BenchmarkConfig, metrics *datahandeling.BenchmarkMetrics, startTime, endTime time.Time) error {
	logger := logging.GetLogger()

	// Create points for all container data
	var points []*write.Point

	for _, containerMetrics := range metrics.ContainerMetrics {
		for _, step := range containerMetrics.Steps {
			// Create base point with common tags
			point := influxdb2.NewPoint("benchmark_metrics",
				map[string]string{
					"benchmark_id":     fmt.Sprintf("%d", benchmarkID),
					"container_index":  fmt.Sprintf("%d", containerMetrics.ContainerIndex),
					"container_name":   containerMetrics.ContainerName,
					"container_image":  containerMetrics.ContainerImage,
					"container_core":   fmt.Sprintf("%d", containerMetrics.ContainerCore),
					"benchmark_started": startTime.Format(time.RFC3339),
					"benchmark_finished": endTime.Format(time.RFC3339),
				},
				idb.createFieldsFromMetricStep(&step),
				step.Timestamp)

			points = append(points, point)
		}
	}

	logger.WithField("total_points", len(points)).Info("Writing benchmark metrics to InfluxDB")

	// Write points in batches if there are many
	if len(points) == 0 {
		logger.Info("No data points to write")
		return nil
	}

	// Split into batches
	totalBatches := (len(points) + idb.batchSize - 1) / idb.batchSize
	logger.WithFields(logrus.Fields{
		"total_points":  len(points),
		"batch_size":    idb.batchSize,
		"total_batches": totalBatches,
	}).Info("Writing data in batches")

	for i := 0; i < len(points); i += idb.batchSize {
		end := i + idb.batchSize
		if end > len(points) {
			end = len(points)
		}
		
		batch := points[i:end]
		batchNum := (i / idb.batchSize) + 1
		
		if err := idb.writePointsBatch(batch, batchNum, totalBatches); err != nil {
			return fmt.Errorf("failed to write batch %d/%d: %w", batchNum, totalBatches, err)
		}
		
		logger.WithFields(logrus.Fields{
			"batch_num":     batchNum,
			"total_batches": totalBatches,
		}).Debug("Batch written successfully")
	}

	logger.Info("All benchmark metrics written successfully")
	return nil
}

func (idb *InfluxDBClient) WriteMetadata(metadata *BenchmarkMetadata) error {
	logger := logging.GetLogger()

	// Create point for metadata
	point := influxdb2.NewPoint("benchmark_meta",
		map[string]string{
			"benchmark_id": fmt.Sprintf("%d", metadata.BenchmarkID),
		},
		map[string]interface{}{
			"benchmark_name":            metadata.BenchmarkName,
			"description":               metadata.Description,
			"duration_seconds":          metadata.DurationSeconds,
			"benchmark_started":         metadata.BenchmarkStarted,
			"benchmark_finished":        metadata.BenchmarkFinished,
			"total_containers":          metadata.TotalContainers,
			"driver_version":            metadata.DriverVersion,
			"used_scheduler":            metadata.UsedScheduler,
			"scheduler_version":         metadata.SchedulerVersion,
			"hostname":                  metadata.Hostname,
			"execution_host":            metadata.ExecutionHost,
			"os_info":                   metadata.OSInfo,
			"kernel_version":            metadata.KernelVersion,
			"cpu_vendor":                metadata.CPUVendor,
			"cpu_model":                 metadata.CPUModel,
			"total_cpu_cores":           metadata.TotalCPUCores,
			"cpu_threads":               metadata.CPUThreads,
			"l3_cache_size_bytes":       metadata.L3CacheSizeBytes,
			"max_memory_bandwidth_mbps": metadata.MaxMemoryBandwidthMBps,
			"max_duration_seconds":      metadata.MaxDurationSeconds,
			"sampling_frequency_ms":     metadata.SamplingFrequencyMS,
			"total_sampling_steps":      metadata.TotalSamplingSteps,
			"total_measurements":        metadata.TotalMeasurements,
			"total_data_size_bytes":     metadata.TotalDataSizeBytes,
			"config_file":               metadata.ConfigFile,
		},
		time.Now())

	logger.WithField("benchmark_id", metadata.BenchmarkID).Info("Writing benchmark metadata to InfluxDB")

	// Write metadata point with retry logic
	operation := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // 30s timeout for metadata
		defer cancel()
		
		return idb.writeAPI.WritePoint(ctx, point)
	}

	return idb.retryWithBackoff(operation, "write_metadata")
}

func CollectBenchmarkMetadata(benchmarkID int, config *config.BenchmarkConfig, configContent string, dataframes *dataframe.DataFrames, startTime, endTime time.Time, driverVersion string) (*BenchmarkMetadata, error) {
	// Get host configuration (unified system info)
	hostConfig, err := host.GetHostConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get host config: %w", err)
	}

	// Calculate metrics from dataframes
	totalSteps := 0
	totalMeasurements := 0
	
	containers := dataframes.GetAllContainers()
	for _, containerDF := range containers {
		steps := containerDF.GetAllSteps()
		totalSteps += len(steps)
		
		// Count measurements (each step with data counts as multiple measurements)
		// TODO: We should use sizeof()
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

	// Estimate data size
	estimatedDataSize := int64(totalMeasurements * 16)

	metadata := &BenchmarkMetadata{
		BenchmarkID:            benchmarkID,
		BenchmarkName:          config.Benchmark.Name,
		Description:            config.Benchmark.Description,
		DurationSeconds:        int64(endTime.Sub(startTime).Seconds()),
		BenchmarkStarted:       startTime.Format(time.RFC3339),
		BenchmarkFinished:      endTime.Format(time.RFC3339),
		TotalContainers:        len(config.Containers),
		DriverVersion:          driverVersion,
		UsedScheduler:          config.Benchmark.Scheduler.Implementation,
		SchedulerVersion:       "1.0.0", 
		Hostname:               hostConfig.Hostname,
		ExecutionHost:          hostConfig.Hostname, 
		OSInfo:                 hostConfig.OSInfo,
		KernelVersion:          hostConfig.KernelVersion,
		CPUVendor:              hostConfig.CPUVendor,
		CPUModel:               hostConfig.CPUModel,
		TotalCPUCores:          hostConfig.Topology.PhysicalCores,
		CPUThreads:             hostConfig.Topology.LogicalCores,
		L3CacheSizeBytes:       hostConfig.L3Cache.SizeBytes,
		MaxMemoryBandwidthMBps: 0, // NYI: No reliable source for memory bandwidth
		MaxDurationSeconds:     config.Benchmark.MaxT,
		SamplingFrequencyMS:    avgFrequency,
		TotalSamplingSteps:     totalSteps,
		TotalMeasurements:      totalMeasurements,
		TotalDataSizeBytes:     estimatedDataSize,
		ConfigFile:             configContent,
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
		if step.Docker.DiskReadBytes != nil {
			fields["docker_disk_read_bytes"] = *step.Docker.DiskReadBytes
		}
		if step.Docker.DiskWriteBytes != nil {
			fields["docker_disk_write_bytes"] = *step.Docker.DiskWriteBytes
		}
	}

	// Add RDT metrics
	if step.RDT != nil {
		if step.RDT.L3CacheOccupancy != nil {
			fields["rdt_l3_cache_occupancy"] = *step.RDT.L3CacheOccupancy
		}
		if step.RDT.MemoryBandwidthTotal != nil {
			fields["rdt_memory_bandwidth_total"] = *step.RDT.MemoryBandwidthTotal
		}
		if step.RDT.MemoryBandwidthLocal != nil {
			fields["rdt_memory_bandwidth_local"] = *step.RDT.MemoryBandwidthLocal
		}
		if step.RDT.RDTClassName != nil {
			fields["rdt_class_name"] = *step.RDT.RDTClassName
		}
		if step.RDT.MonGroupName != nil {
			fields["rdt_mon_group_name"] = *step.RDT.MonGroupName
		}
		if step.RDT.L3CacheAllocation != nil {
			fields["rdt_l3_cache_allocation"] = *step.RDT.L3CacheAllocation
		}
		if step.RDT.L3CacheAllocationPct != nil {
			fields["rdt_l3_cache_allocation_pct"] = *step.RDT.L3CacheAllocationPct
		}
		if step.RDT.MBAThrottle != nil {
			fields["rdt_mba_throttle"] = *step.RDT.MBAThrottle
		}
		if step.RDT.CacheLLCUtilizationPercent != nil {
			fields["rdt_cache_llc_utilization_percent"] = *step.RDT.CacheLLCUtilizationPercent
		}
		if step.RDT.BandwidthUtilizationPercent != nil {
			fields["rdt_bandwidth_utilization_percent"] = *step.RDT.BandwidthUtilizationPercent
		}
	}

	return fields
}

//  creates InfluxDB fields from a processed MetricStep
func (idb *InfluxDBClient) createFieldsFromMetricStep(step *datahandeling.MetricStep) map[string]interface{} {
	fields := make(map[string]interface{})

	// Add step number
	fields["step_number"] = step.StepNumber

	// Add relative time 
	fields["relative_time"] = step.RelativeTime

	// Add Perf metrics
	if step.PerfCacheMisses != nil {
		fields["perf_cache_misses"] = *step.PerfCacheMisses
	}
	if step.PerfCacheReferences != nil {
		fields["perf_cache_references"] = *step.PerfCacheReferences
	}
	if step.PerfInstructions != nil {
		fields["perf_instructions"] = *step.PerfInstructions
	}
	if step.PerfCycles != nil {
		fields["perf_cycles"] = *step.PerfCycles
	}
	if step.PerfBranchInstructions != nil {
		fields["perf_branch_instructions"] = *step.PerfBranchInstructions
	}
	if step.PerfBranchMisses != nil {
		fields["perf_branch_misses"] = *step.PerfBranchMisses
	}
	if step.PerfBusCycles != nil {
		fields["perf_bus_cycles"] = *step.PerfBusCycles
	}
	if step.PerfCacheMissRate != nil {
		fields["perf_cache_miss_rate"] = *step.PerfCacheMissRate
	}
	if step.PerfBranchMissRate != nil {
		fields["perf_branch_miss_rate"] = *step.PerfBranchMissRate
	}
	if step.PerfInstructionsPerCycle != nil {
		fields["perf_instructions_per_cycle"] = *step.PerfInstructionsPerCycle
	}

	// Add Docker metrics
	if step.DockerCPUUsageTotal != nil {
		fields["docker_cpu_usage_total"] = *step.DockerCPUUsageTotal
	}
	if step.DockerCPUUsageKernel != nil {
		fields["docker_cpu_usage_kernel"] = *step.DockerCPUUsageKernel
	}
	if step.DockerCPUUsageUser != nil {
		fields["docker_cpu_usage_user"] = *step.DockerCPUUsageUser
	}
	if step.DockerCPUUsagePercent != nil {
		fields["docker_cpu_usage_percent"] = *step.DockerCPUUsagePercent
	}
	if step.DockerCPUThrottling != nil {
		fields["docker_cpu_throttling"] = *step.DockerCPUThrottling
	}
	if step.DockerMemoryUsage != nil {
		fields["docker_memory_usage"] = *step.DockerMemoryUsage
	}
	if step.DockerMemoryLimit != nil {
		fields["docker_memory_limit"] = *step.DockerMemoryLimit
	}
	if step.DockerMemoryCache != nil {
		fields["docker_memory_cache"] = *step.DockerMemoryCache
	}
	if step.DockerMemoryRSS != nil {
		fields["docker_memory_rss"] = *step.DockerMemoryRSS
	}
	if step.DockerMemorySwap != nil {
		fields["docker_memory_swap"] = *step.DockerMemorySwap
	}
	if step.DockerMemoryUsagePercent != nil {
		fields["docker_memory_usage_percent"] = *step.DockerMemoryUsagePercent
	}
	if step.DockerNetworkRxBytes != nil {
		fields["docker_network_rx_bytes"] = *step.DockerNetworkRxBytes
	}
	if step.DockerNetworkTxBytes != nil {
		fields["docker_network_tx_bytes"] = *step.DockerNetworkTxBytes
	}
	if step.DockerDiskReadBytes != nil {
		fields["docker_disk_read_bytes"] = *step.DockerDiskReadBytes
	}
	if step.DockerDiskWriteBytes != nil {
		fields["docker_disk_write_bytes"] = *step.DockerDiskWriteBytes
	}

	// Add RDT metrics
	if step.RDTClassName != nil {
		fields["rdt_class_name"] = *step.RDTClassName
	}
	if step.RDTMonGroupName != nil {
		fields["rdt_mon_group_name"] = *step.RDTMonGroupName
	}
	if step.RDTL3CacheOccupancy != nil {
		fields["rdt_l3_cache_occupancy"] = *step.RDTL3CacheOccupancy
	}
	if step.RDTMemoryBandwidthTotal != nil {
		fields["rdt_memory_bandwidth_total"] = *step.RDTMemoryBandwidthTotal
	}
	if step.RDTMemoryBandwidthLocal != nil {
		fields["rdt_memory_bandwidth_local"] = *step.RDTMemoryBandwidthLocal
	}
	if step.RDTL3CacheAllocation != nil {
		fields["rdt_l3_cache_allocation"] = *step.RDTL3CacheAllocation
	}
	if step.RDTL3CacheAllocationPct != nil {
		fields["rdt_l3_cache_allocation_pct"] = *step.RDTL3CacheAllocationPct
	}
	if step.RDTMBAThrottle != nil {
		fields["rdt_mba_throttle"] = *step.RDTMBAThrottle
	}
	if step.RDTCacheLLCUtilizationPercent != nil {
		fields["rdt_cache_llc_utilization_percent"] = *step.RDTCacheLLCUtilizationPercent
	}
	if step.RDTBandwidthUtilizationPercent != nil {
		fields["rdt_bandwidth_utilization_percent"] = *step.RDTBandwidthUtilizationPercent
	}

	return fields
}

func (idb *InfluxDBClient) Close() {
	if idb.client != nil {
		idb.client.Close()
	}
}
