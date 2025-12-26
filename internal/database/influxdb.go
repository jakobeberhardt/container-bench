package database

import (
	"context"
	"fmt"
	"math"
	"time"

	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/datahandeling"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/probe"
	"container-bench/internal/probe/resources"

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
	WriteContainerTimingMetadata(containerMetadata []*ContainerTimingMetadata) error
	WriteProbeResults(probeResults []*probe.ProbeResult) error
	WriteAllocationProbeResults(allocationProbeResults []*resources.AllocationProbeResult) error
	Close()
}

// contains all metadata about a benchmark run
type BenchmarkMetadata struct {
	BenchmarkID            int     `json:"benchmark_id"`
	BenchmarkName          string  `json:"benchmark_name"`
	Description            string  `json:"description"`
	TraceChecksum          string  `json:"trace_checksum"`
	DurationSeconds        int64   `json:"duration_seconds"`
	BenchmarkStarted       string  `json:"benchmark_started"`
	BenchmarkFinished      string  `json:"benchmark_finished"`
	TotalContainers        int     `json:"total_containers"`
	DriverVersion          string  `json:"driver_version"`
	UsedScheduler          string  `json:"used_scheduler"`
	SchedulerVersion       string  `json:"scheduler_version"`
	Hostname               string  `json:"hostname"`
	ExecutionHost          string  `json:"execution_host"`
	OSInfo                 string  `json:"os_info"`
	KernelVersion          string  `json:"kernel_version"`
	CPUVendor              string  `json:"cpu_vendor"`
	CPUModel               string  `json:"cpu_model"`
	TotalCPUCores          int     `json:"total_cpu_cores"`
	CPUThreads             int     `json:"cpu_threads"`
	CPUSockets             int     `json:"cpu_sockets"`
	L1CacheSizeKB          float64 `json:"l1_cache_size_kb"`
	L2CacheSizeKB          float64 `json:"l2_cache_size_kb"`
	L3CacheSizeBytes       int64   `json:"l3_cache_size_bytes"`
	L3CacheSizeMB          float64 `json:"l3_cache_size_mb"`
	L3CacheWays            int     `json:"l3_cache_ways"`
	RDTSupported           bool    `json:"rdt_supported"`
	RDTMonitoringSupported bool    `json:"rdt_monitoring_supported"`
	RDTAllocationSupported bool    `json:"rdt_allocation_supported"`
	MaxMemoryBandwidthMBps int64   `json:"max_memory_bandwidth_mbps"`
	MaxDurationSeconds     int     `json:"max_duration_seconds"`
	SamplingFrequencyMS    int     `json:"sampling_frequency_ms"`
	TotalSamplingSteps     int     `json:"total_sampling_steps"`
	TotalMeasurements      int     `json:"total_measurements"`
	TotalDataSizeBytes     int64   `json:"total_data_size_bytes"`
	ConfigFile             string  `json:"config_file"`

	// Probe configuration
	ProberEnabled        bool   `json:"prober_enabled"`
	ProberImplementation string `json:"prober_implementation"`
	ProberAbortable      bool   `json:"prober_abortable"`
	ProberIsolated       bool   `json:"prober_isolated"`

	// Feature flags
	PerfEnabled        bool `json:"perf_enabled"`
	DockerStatsEnabled bool `json:"docker_stats_enabled"`
	RDTEnabled         bool `json:"rdt_enabled"`
}

// contains per-container timing metadata for a benchmark run
type ContainerTimingMetadata struct {
	BenchmarkID      int
	ContainerIndex   int
	ContainerName    string
	StartTSeconds    int
	WaitTSeconds     *int
	StopTSeconds     int
	ExpectedTSeconds *int
	ExitTSeconds     *int
	ActualTSeconds   int
	DeltaTSeconds    *int
	JobAborted       bool
}

// contains host system information
// DEPRECATED: Use host.GetHostConfig() instead
type SystemInfo struct {
	Hostname               string
	OSInfo                 string
	KernelVersion          string
	CPUVendor              string
	CPUModel               string
	CPUCores               int
	CPUThreads             int
	L3CacheSizeBytes       int64
	MaxMemoryBandwidthMbps int64
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

	queryMaxFromMeta := func(ctx context.Context, window string) (int, bool, error) {
		// Use the tag index to fetch benchmark_id tag values.
		// This avoids Flux schema-collision errors if historical data accidentally wrote benchmark_id
		// with mixed types (e.g., as a field in some runs and a tag in others).
		query := fmt.Sprintf(`
			import "influxdata/influxdb/schema"

			schema.tagValues(
				bucket: "%s",
				tag: "benchmark_id",
				predicate: (r) => r._measurement == "benchmark_meta",
				start: %s,
			)
			|> map(fn: (r) => ({ _value: int(v: r._value) }))
			|> max()
			|> yield(name: "max_benchmark_id")
		`, idb.bucket, window)

		queryResult, err := idb.queryAPI.Query(ctx, query)
		if err != nil {
			return 0, false, fmt.Errorf("failed to query last benchmark ID from benchmark_meta: %w", err)
		}
		defer queryResult.Close()

		if !queryResult.Next() {
			if queryResult.Err() != nil {
				return 0, false, fmt.Errorf("error reading query results: %w", queryResult.Err())
			}
			return 0, false, nil
		}

		switch v := queryResult.Record().Value().(type) {
		case int64:
			return int(v), true, nil
		case uint64:
			return int(v), true, nil
		default:
			if queryResult.Record().Value() == nil {
				return 0, false, nil
			}
			return 0, false, fmt.Errorf("unexpected max_benchmark_id value type %T", queryResult.Record().Value())
		}
	}

	operation := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Fast path: query max benchmark_id from benchmark_meta (sparse; one per benchmark).
		for _, window := range []string{"-7d", "-30d", "-365d", "-3000d"} {
			id, ok, err := queryMaxFromMeta(ctx, window)
			if err != nil {
				return err
			}
			if ok {
				result = id
				return nil
			}
		}

		// Fallback (expensive): derive max ID from benchmark_metrics.
		// Keep range bounded to avoid scanning unbounded history.
		fallbackQuery := fmt.Sprintf(`
			from(bucket: "%s")
			|> range(start: -300d)
			|> filter(fn: (r) => r._measurement == "benchmark_metrics")
			|> distinct(column: "benchmark_id")
			|> map(fn: (r) => ({_value: int(v: r.benchmark_id)}))
			|> max()
			|> yield(name: "max_benchmark_id")
		`, idb.bucket)
		queryResult, err := idb.queryAPI.Query(ctx, fallbackQuery)
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
		msg := ""
		if health.Message != nil {
			msg = *health.Message
		}
		return fmt.Errorf("health check failed: status %s, message: %s", health.Status, msg)
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
					"benchmark_id":       fmt.Sprintf("%d", benchmarkID),
					"container_index":    fmt.Sprintf("%d", containerMetrics.ContainerIndex),
					"container_name":     containerMetrics.ContainerName,
					"container_image":    containerMetrics.ContainerImage,
					"container_core":     containerMetrics.ContainerCore,
					"benchmark_started":  startTime.Format(time.RFC3339),
					"benchmark_finished": endTime.Format(time.RFC3339),
				},
				idb.createFieldsFromMetricStep(&step),
				step.Timestamp)

			points = append(points, point)
		}
	}

	// Add benchmark-level core allocation time series points
	for _, step := range metrics.CoreAllocationSteps {
		point := influxdb2.NewPoint("benchmark_core_allocation",
			map[string]string{
				"benchmark_id":       fmt.Sprintf("%d", benchmarkID),
				"benchmark_started":  startTime.Format(time.RFC3339),
				"benchmark_finished": endTime.Format(time.RFC3339),
			},
			map[string]interface{}{
				"step_number":              step.StepNumber,
				"relative_time":            step.RelativeTime,
				"coresAllocatedSocketZero": step.CoresAllocatedSocketZero,
				"coresAllocatedSocketOne":  step.CoresAllocatedSocketOne,
			},
			step.Timestamp)
		points = append(points, point)
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
			"trace_checksum":            metadata.TraceChecksum,
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
			"cpu_sockets":               metadata.CPUSockets,
			"l1_cache_size_kb":          metadata.L1CacheSizeKB,
			"l2_cache_size_kb":          metadata.L2CacheSizeKB,
			"l3_cache_size_bytes":       metadata.L3CacheSizeBytes,
			"l3_cache_size_mb":          metadata.L3CacheSizeMB,
			"l3_cache_ways":             metadata.L3CacheWays,
			"rdt_supported":             metadata.RDTSupported,
			"rdt_monitoring_supported":  metadata.RDTMonitoringSupported,
			"rdt_allocation_supported":  metadata.RDTAllocationSupported,
			"max_memory_bandwidth_mbps": metadata.MaxMemoryBandwidthMBps,
			"max_duration_seconds":      metadata.MaxDurationSeconds,
			"sampling_frequency_ms":     metadata.SamplingFrequencyMS,
			"total_sampling_steps":      metadata.TotalSamplingSteps,
			"total_measurements":        metadata.TotalMeasurements,
			"total_data_size_bytes":     metadata.TotalDataSizeBytes,
			"config_file":               metadata.ConfigFile,
			"prober_enabled":            metadata.ProberEnabled,
			"prober_implementation":     metadata.ProberImplementation,
			"prober_abortable":          metadata.ProberAbortable,
			"prober_isolated":           metadata.ProberIsolated,
			"perf_enabled":              metadata.PerfEnabled,
			"docker_stats_enabled":      metadata.DockerStatsEnabled,
			"rdt_enabled":               metadata.RDTEnabled,
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

func (idb *InfluxDBClient) WriteContainerTimingMetadata(containerMetadata []*ContainerTimingMetadata) error {
	logger := logging.GetLogger()

	if len(containerMetadata) == 0 {
		logger.Info("No container timing metadata to write")
		return nil
	}

	logger.WithField("container_count", len(containerMetadata)).Info("Writing container timing metadata to InfluxDB")

	now := time.Now()
	points := make([]*write.Point, 0, len(containerMetadata))

	for _, m := range containerMetadata {
		tags := map[string]string{
			"benchmark_id":    fmt.Sprintf("%d", m.BenchmarkID),
			"container_index": fmt.Sprintf("%d", m.ContainerIndex),
			"container_name":  m.ContainerName,
		}

		fields := map[string]interface{}{
			"start_t":     m.StartTSeconds,
			"wait_t":      0,
			"stop_t":      m.StopTSeconds,
			"actual_t":    m.ActualTSeconds,
			"job_aborted": m.JobAborted,
		}
		if m.WaitTSeconds != nil {
			fields["wait_t"] = *m.WaitTSeconds
		}
		if m.ExpectedTSeconds != nil {
			fields["expected_t"] = *m.ExpectedTSeconds
		}
		if m.ExitTSeconds != nil {
			fields["exit_t"] = *m.ExitTSeconds
		}
		if m.DeltaTSeconds != nil {
			fields["delta_t"] = *m.DeltaTSeconds
		}

		points = append(points, influxdb2.NewPoint("benchmark_container_meta", tags, fields, now))
	}

	// Split into batches (reuse shared batch+retry logic)
	totalBatches := (len(points) + idb.batchSize - 1) / idb.batchSize
	logger.WithFields(logrus.Fields{
		"total_points":  len(points),
		"batch_size":    idb.batchSize,
		"total_batches": totalBatches,
	}).Info("Writing container timing metadata in batches")

	for i := 0; i < len(points); i += idb.batchSize {
		end := i + idb.batchSize
		if end > len(points) {
			end = len(points)
		}

		batch := points[i:end]
		batchNum := (i / idb.batchSize) + 1
		if err := idb.writePointsBatch(batch, batchNum, totalBatches); err != nil {
			return fmt.Errorf("failed to write container timing metadata batch %d/%d: %w", batchNum, totalBatches, err)
		}
	}

	logger.WithField("container_count", len(containerMetadata)).Info("Container timing metadata written successfully")
	return nil
}

// WriteProbeResults writes probe results to the benchmark_probes table
func (idb *InfluxDBClient) WriteProbeResults(probeResults []*probe.ProbeResult) error {
	logger := logging.GetLogger()

	if len(probeResults) == 0 {
		logger.Info("No probe results to write")
		return nil
	}

	logger.WithField("probe_count", len(probeResults)).Info("Writing probe results to InfluxDB")

	var points []*write.Point

	for _, result := range probeResults {
		tags := map[string]string{
			"benchmark_id":            fmt.Sprintf("%d", result.BenchmarkID),
			"container_id":            result.ContainerID,
			"container_name":          result.ContainerName,
			"container_index":         fmt.Sprintf("%d", result.ContainerIndex),
			"container_cores":         result.ContainerCores,
			"container_image":         result.ContainerImage,
			"probing_container_id":    result.ProbingContainerID,
			"probing_container_name":  result.ProbingContainerName,
			"probing_container_cores": result.ProbingContainerCores,
			"used_probe_kernel":       result.UsedProbeKernel,
		}

		fields := map[string]interface{}{
			"container_command":    result.ContainerCommand,
			"probe_time_ns":        result.ProbeTime.Nanoseconds(),
			"isolated":             result.Isolated,
			"aborted":              result.Aborted,
			"started":              result.Started.Format(time.RFC3339),
			"finished":             result.Finished.Format(time.RFC3339),
			"first_dataframe_step": result.FirstDataframeStep,
			"last_dataframe_step":  result.LastDataframeStep,
		}

		// Add optional fields
		if result.ContainerSocket > 0 {
			fields["container_socket"] = result.ContainerSocket
		}
		if result.ProbingContainerSocket > 0 {
			fields["probing_container_socket"] = result.ProbingContainerSocket
		}
		if result.AbortedAt != nil {
			fields["aborted_at"] = result.AbortedAt.Format(time.RFC3339)
		}

		// Add sensitivity metrics with prefixes based on metric type (e.g., ipc_llc, scp_llc)
		if result.Sensitivities != nil {
			for metricType, metrics := range result.Sensitivities {
				prefix := metricType + "_"

				if metrics.LLC != nil {
					fields[prefix+"llc"] = *metrics.LLC
				}
				if metrics.MemRead != nil {
					fields[prefix+"mem_read"] = *metrics.MemRead
				}
				if metrics.MemWrite != nil {
					fields[prefix+"mem_write"] = *metrics.MemWrite
				}
				if metrics.SysCall != nil {
					fields[prefix+"syscall"] = *metrics.SysCall
				}
				if metrics.Prefetch != nil {
					fields[prefix+"prefetch"] = *metrics.Prefetch
				}
			}
		}

		point := influxdb2.NewPoint("benchmark_probes", tags, fields, result.Started)
		points = append(points, point)
	}

	// Write all probe result points
	operation := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		for _, point := range points {
			if err := idb.writeAPI.WritePoint(ctx, point); err != nil {
				return err
			}
		}
		return nil
	}

	if err := idb.retryWithBackoff(operation, "write_probe_results"); err != nil {
		return fmt.Errorf("failed to write probe results: %w", err)
	}

	logger.WithField("probe_count", len(probeResults)).Info("Probe results written successfully")
	return nil
}

// WriteAllocationProbeResults writes allocation probe results to the benchmark_allocation table
func (idb *InfluxDBClient) WriteAllocationProbeResults(allocationProbeResults []*resources.AllocationProbeResult) error {
	logger := logging.GetLogger()

	if len(allocationProbeResults) == 0 {
		logger.Info("No allocation probe results to write")
		return nil
	}

	logger.WithField("allocation_probe_count", len(allocationProbeResults)).Info("Writing allocation probe results to InfluxDB")

	var points []*write.Point

	for probeIndex, probeResult := range allocationProbeResults {
		// Write each allocation result as a separate point
		for _, allocResult := range probeResult.Allocations {
			tags := map[string]string{
				"benchmark_id":           fmt.Sprintf("%d", probeResult.BenchmarkID),
				"allocation_probe_index": fmt.Sprintf("%d", probeIndex),
				"container_id":           probeResult.ContainerID,
				"container_name":         probeResult.ContainerName,
				"container_index":        fmt.Sprintf("%d", probeResult.ContainerIndex),
				"container_cores":        probeResult.ContainerCores,
				"container_image":        probeResult.ContainerImage,
				"socket_id":              fmt.Sprintf("%d", allocResult.SocketID),
				"isolated_others":        fmt.Sprintf("%v", allocResult.IsolatedOthers),
			}

			fields := map[string]interface{}{
				// Container info
				"container_command": probeResult.ContainerCommand,

				// Probe metadata
				"probe_aborted":    probeResult.Aborted,
				"probe_started":    probeResult.Started.Format(time.RFC3339),
				"probe_finished":   probeResult.Finished.Format(time.RFC3339),
				"total_probe_time": probeResult.TotalProbeTime.Nanoseconds(),

				// Allocation parameters
				"l3_ways":       allocResult.L3Ways,
				"mem_bandwidth": allocResult.MemBandwidth,

				// Allocation timing
				"allocation_started":  allocResult.Started.Format(time.RFC3339),
				"allocation_duration": allocResult.Duration.Nanoseconds(),

				// Performance metrics
				"avg_ipc":                    allocResult.AvgIPC,
				"avg_theoretical_ipc":        allocResult.AvgTheoreticalIPC,
				"ipc_efficiency":             allocResult.IPCEfficiency,
				"avg_cache_miss_rate":        allocResult.AvgCacheMissRate,
				"avg_stalled_cycles":         allocResult.AvgStalledCycles,
				"avg_stalls_l3_miss_percent": allocResult.AvgStallsL3MissPercent,
				"avg_l3_occupancy":           allocResult.AvgL3Occupancy,
				"avg_mem_bandwidth_used":     allocResult.AvgMemBandwidthUsed,
			} // Add dataframe references if available
			if len(allocResult.DataFrameSteps) > 0 {
				fields["first_dataframe_step"] = allocResult.DataFrameSteps[0]
				fields["last_dataframe_step"] = allocResult.DataFrameSteps[len(allocResult.DataFrameSteps)-1]
				fields["num_dataframe_steps"] = len(allocResult.DataFrameSteps)
			}

			// Add optional fields
			if probeResult.ContainerSocket > 0 {
				fields["container_socket"] = probeResult.ContainerSocket
			}
			if probeResult.Aborted {
				fields["abort_reason"] = probeResult.AbortReason
				if probeResult.AbortedAt != nil {
					fields["aborted_at"] = probeResult.AbortedAt.Format(time.RFC3339)
				}
			}

			// Add range configuration as metadata
			fields["range_min_l3_ways"] = probeResult.Range.MinL3Ways
			fields["range_max_l3_ways"] = probeResult.Range.MaxL3Ways
			fields["range_min_mem_bandwidth"] = probeResult.Range.MinMemBandwidth
			fields["range_max_mem_bandwidth"] = probeResult.Range.MaxMemBandwidth
			fields["range_step_l3_ways"] = probeResult.Range.StepL3Ways
			fields["range_step_mem_bandwidth"] = probeResult.Range.StepMemBandwidth
			fields["range_order"] = probeResult.Range.Order
			fields["range_duration_per_alloc"] = probeResult.Range.DurationPerAlloc
			fields["range_max_total_duration"] = probeResult.Range.MaxTotalDuration
			fields["range_isolate_others"] = probeResult.Range.IsolateOthers
			fields["range_force_reallocation"] = probeResult.Range.ForceReallocation

			point := influxdb2.NewPoint("benchmark_allocation", tags, fields, allocResult.Started)
			points = append(points, point)
		}
	}

	// Write all allocation probe result points
	operation := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		for _, point := range points {
			if err := idb.writeAPI.WritePoint(ctx, point); err != nil {
				return err
			}
		}
		return nil
	}

	if err := idb.retryWithBackoff(operation, "write_allocation_probe_results"); err != nil {
		return fmt.Errorf("failed to write allocation probe results: %w", err)
	}

	totalAllocations := 0
	for _, probeResult := range allocationProbeResults {
		totalAllocations += len(probeResult.Allocations)
	}

	logger.WithFields(logrus.Fields{
		"allocation_probe_count": len(allocationProbeResults),
		"total_allocations":      totalAllocations,
	}).Info("Allocation probe results written successfully")
	return nil
}

func CollectContainerTimingMetadata(benchmarkID int, cfg *config.BenchmarkConfig, benchmarkStart time.Time, startTimes map[int]time.Time, exitTimes map[int]time.Time, aborted map[int]bool) []*ContainerTimingMetadata {
	containers := cfg.GetContainersSorted()
	results := make([]*ContainerTimingMetadata, 0, len(containers))

	for _, c := range containers {
		scheduledStartT := c.GetStartSeconds()
		admittedStartT := -1
		if startTimes != nil {
			if t, ok := startTimes[c.Index]; ok {
				startSec := int(t.Sub(benchmarkStart).Seconds())
				if startSec < 0 {
					startSec = 0
				}
				admittedStartT = startSec
			}
		}

		var waitPtr *int
		if admittedStartT >= 0 {
			waitSec := admittedStartT - scheduledStartT
			if waitSec < 0 {
				waitSec = 0
			}
			waitPtr = &waitSec
		}

		// Persist start_t as the configured schedule start; use admission for actual runtime.
		startT := scheduledStartT
		stopT := c.GetStopSeconds(cfg.Benchmark.MaxT)
		jobAborted := false
		if aborted != nil {
			jobAborted = aborted[c.Index]
		}

		expectedSec, hasExpected := c.GetExpectedSeconds()
		explicitExpected := hasExpected
		if !hasExpected {
			expectedSec = cfg.Benchmark.MaxT
			hasExpected = true
		}
		expectedPtr := &expectedSec

		var exitPtr *int
		if exitTimes != nil {
			if t, ok := exitTimes[c.Index]; ok {
				exitSec := int(t.Sub(benchmarkStart).Seconds())
				if exitSec < 0 {
					exitSec = 0
				}
				exitPtr = &exitSec
			}
		}

		endSec := stopT
		if exitPtr != nil {
			endSec = *exitPtr
		}
		runtimeStart := startT
		if admittedStartT >= 0 {
			runtimeStart = admittedStartT
		}
		actualSec := endSec - runtimeStart
		if actualSec < 0 {
			actualSec = 0
		}

		delta := actualSec - expectedSec
		if jobAborted && explicitExpected {
			delta = 100
		}
		deltaPtr := &delta

		results = append(results, &ContainerTimingMetadata{
			BenchmarkID:      benchmarkID,
			ContainerIndex:   c.Index,
			ContainerName:    c.GetContainerName(benchmarkID),
			StartTSeconds:    startT,
			WaitTSeconds:     waitPtr,
			StopTSeconds:     stopT,
			ExpectedTSeconds: expectedPtr,
			ExitTSeconds:     exitPtr,
			ActualTSeconds:   actualSec,
			DeltaTSeconds:    deltaPtr,
			JobAborted:       jobAborted,
		})
	}

	return results
}

func CollectBenchmarkMetadata(benchmarkID int, benchmarkConfig *config.BenchmarkConfig, configContent string, dataframes *dataframe.DataFrames, startTime, endTime time.Time, driverVersion string) (*BenchmarkMetadata, error) {
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
	if len(benchmarkConfig.Containers) > 0 {
		totalFreq := 0
		for _, container := range benchmarkConfig.Containers {
			totalFreq += container.Data.Frequency
		}
		avgFrequency = totalFreq / len(benchmarkConfig.Containers)
	}

	// Determine feature flags from containers
	perfEnabled := false
	dockerStatsEnabled := false
	rdtEnabled := false

	for _, container := range benchmarkConfig.Containers {
		if container.Data.GetPerfConfig() != nil {
			perfEnabled = true
		}
		if container.Data.GetDockerConfig() != nil {
			dockerStatsEnabled = true
		}
		if container.Data.GetRDTConfig() != nil {
			rdtEnabled = true
		}
	}

	// Extract probe configuration if available
	proberEnabled := false
	proberImplementation := ""
	proberAbortable := false
	proberIsolated := false

	if benchmarkConfig.Benchmark.Scheduler.Prober != nil {
		proberEnabled = true
		proberImplementation = benchmarkConfig.Benchmark.Scheduler.Prober.Implementation
		proberAbortable = benchmarkConfig.Benchmark.Scheduler.Prober.Abortable
		proberIsolated = benchmarkConfig.Benchmark.Scheduler.Prober.Isolated
	}

	// Estimate data size
	estimatedDataSize := int64(totalMeasurements * 16)

	traceChecksum := ""
	if benchmarkConfig != nil {
		if cs, err := config.TraceChecksum(benchmarkConfig); err == nil {
			traceChecksum = cs
		} else {
			logging.GetLogger().WithError(err).Warn("Failed to compute trace checksum")
		}
	}

	metadata := &BenchmarkMetadata{
		BenchmarkID:            benchmarkID,
		BenchmarkName:          benchmarkConfig.Benchmark.Name,
		Description:            benchmarkConfig.Benchmark.Description,
		TraceChecksum:          traceChecksum,
		DurationSeconds:        int64(endTime.Sub(startTime).Seconds()),
		BenchmarkStarted:       startTime.Format(time.RFC3339),
		BenchmarkFinished:      endTime.Format(time.RFC3339),
		TotalContainers:        len(benchmarkConfig.Containers),
		DriverVersion:          driverVersion,
		UsedScheduler:          benchmarkConfig.Benchmark.Scheduler.Implementation,
		SchedulerVersion:       "1.0.0",
		Hostname:               hostConfig.Hostname,
		ExecutionHost:          hostConfig.Hostname,
		OSInfo:                 hostConfig.OSInfo,
		KernelVersion:          hostConfig.KernelVersion,
		CPUVendor:              hostConfig.CPUVendor,
		CPUModel:               hostConfig.CPUModel,
		TotalCPUCores:          hostConfig.Topology.PhysicalCores,
		CPUThreads:             hostConfig.Topology.LogicalCores,
		CPUSockets:             hostConfig.Topology.Sockets,
		L1CacheSizeKB:          hostConfig.L1Cache.SizeKB,
		L2CacheSizeKB:          hostConfig.L2Cache.SizeKB,
		L3CacheSizeBytes:       hostConfig.L3Cache.SizeBytes,
		L3CacheSizeMB:          hostConfig.L3Cache.SizeMB,
		L3CacheWays:            hostConfig.L3Cache.WaysPerCache,
		RDTSupported:           hostConfig.RDT.Supported,
		RDTMonitoringSupported: hostConfig.RDT.MonitoringSupported,
		RDTAllocationSupported: hostConfig.RDT.AllocationSupported,
		MaxMemoryBandwidthMBps: 0, // NYI: No reliable source for memory bandwidth
		MaxDurationSeconds:     benchmarkConfig.Benchmark.MaxT,
		SamplingFrequencyMS:    avgFrequency,
		TotalSamplingSteps:     totalSteps,
		TotalMeasurements:      totalMeasurements,
		TotalDataSizeBytes:     estimatedDataSize,
		ConfigFile:             configContent,
		ProberEnabled:          proberEnabled,
		ProberImplementation:   proberImplementation,
		ProberAbortable:        proberAbortable,
		ProberIsolated:         proberIsolated,
		PerfEnabled:            perfEnabled,
		DockerStatsEnabled:     dockerStatsEnabled,
		RDTEnabled:             rdtEnabled,
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
		if step.Docker.AssignedCoresCSV != nil {
			fields["docker_assigned_cores"] = *step.Docker.AssignedCoresCSV
		}
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
		if step.RDT.RDTClassName != nil {
			fields["rdt_class_name"] = *step.RDT.RDTClassName
		}
		if step.RDT.MonGroupName != nil {
			fields["rdt_mon_group_name"] = *step.RDT.MonGroupName
		}
		if step.RDT.MBAThrottle != nil {
			fields["rdt_mba_throttle"] = *step.RDT.MBAThrottle
		}

		// Add per-socket monitoring metrics
		if step.RDT.L3OccupancyPerSocket != nil {
			if occ, ok := step.RDT.L3OccupancyPerSocket[0]; ok {
				fields["rdt_l3_occupancy_socket0"] = occ
			}
			if occ, ok := step.RDT.L3OccupancyPerSocket[1]; ok {
				fields["rdt_l3_occupancy_socket1"] = occ
			}
		}
		if step.RDT.MemoryBandwidthTotalPerSocket != nil {
			if bw, ok := step.RDT.MemoryBandwidthTotalPerSocket[0]; ok {
				fields["rdt_memory_bandwidth_total_socket0"] = bw
			}
			if bw, ok := step.RDT.MemoryBandwidthTotalPerSocket[1]; ok {
				fields["rdt_memory_bandwidth_total_socket1"] = bw
			}
		}
		if step.RDT.MemoryBandwidthLocalPerSocket != nil {
			if bw, ok := step.RDT.MemoryBandwidthLocalPerSocket[0]; ok {
				fields["rdt_memory_bandwidth_local_socket0"] = bw
			}
			if bw, ok := step.RDT.MemoryBandwidthLocalPerSocket[1]; ok {
				fields["rdt_memory_bandwidth_local_socket1"] = bw
			}
		}
		if step.RDT.L3UtilizationPctPerSocket != nil {
			if pct, ok := step.RDT.L3UtilizationPctPerSocket[0]; ok {
				fields["rdt_l3_utilization_pct_socket0"] = pct
			}
			if pct, ok := step.RDT.L3UtilizationPctPerSocket[1]; ok {
				fields["rdt_l3_utilization_pct_socket1"] = pct
			}
		}
		if step.RDT.MemBandwidthMbpsPerSocket != nil {
			if mbps, ok := step.RDT.MemBandwidthMbpsPerSocket[0]; ok {
				fields["rdt_mem_bandwidth_mbps_socket0"] = mbps
			}
			if mbps, ok := step.RDT.MemBandwidthMbpsPerSocket[1]; ok {
				fields["rdt_mem_bandwidth_mbps_socket1"] = mbps
			}
		}

		// Add per-socket L3 allocation details
		if step.RDT.L3BitmaskPerSocket != nil {
			if bitmask, ok := step.RDT.L3BitmaskPerSocket[0]; ok {
				fields["rdt_l3_bitmask_socket0"] = bitmask
			}
			if bitmask, ok := step.RDT.L3BitmaskPerSocket[1]; ok {
				fields["rdt_l3_bitmask_socket1"] = bitmask
			}
		}
		if step.RDT.L3WaysPerSocket != nil {
			if ways, ok := step.RDT.L3WaysPerSocket[0]; ok {
				fields["rdt_l3_ways_socket0"] = ways
			}
			if ways, ok := step.RDT.L3WaysPerSocket[1]; ok {
				fields["rdt_l3_ways_socket1"] = ways
			}
		}
		if step.RDT.L3AllocationPctPerSocket != nil {
			if pct, ok := step.RDT.L3AllocationPctPerSocket[0]; ok {
				fields["rdt_l3_allocation_pct_socket0"] = pct
			}
			if pct, ok := step.RDT.L3AllocationPctPerSocket[1]; ok {
				fields["rdt_l3_allocation_pct_socket1"] = pct
			}
		}

		// Add per-socket MBA allocation details
		if step.RDT.MBAPercentPerSocket != nil {
			if mba, ok := step.RDT.MBAPercentPerSocket[0]; ok {
				fields["rdt_mba_percent_socket0"] = mba
			}
			if mba, ok := step.RDT.MBAPercentPerSocket[1]; ok {
				fields["rdt_mba_percent_socket1"] = mba
			}
		}

		// Add full allocation strings
		if step.RDT.L3AllocationString != nil {
			fields["rdt_l3_allocation_string"] = *step.RDT.L3AllocationString
		}
		if step.RDT.MBAAllocationString != nil {
			fields["rdt_mba_allocation_string"] = *step.RDT.MBAAllocationString
		}
	}

	return fields
}

// creates InfluxDB fields from a processed MetricStep
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

	// Add Docker affinity metadata
	if step.DockerAssignedCoresCSV != nil {
		fields["docker_assigned_cores"] = *step.DockerAssignedCoresCSV
	}

	// Add stall counters
	if step.PerfStallsTotal != nil {
		fields["perf_stalls_total"] = *step.PerfStallsTotal
	}
	if step.PerfStallsL3Miss != nil {
		fields["perf_stalls_l3_miss"] = *step.PerfStallsL3Miss
	}
	if step.PerfStallsL2Miss != nil {
		fields["perf_stalls_l2_miss"] = *step.PerfStallsL2Miss
	}
	if step.PerfStallsL1dMiss != nil {
		fields["perf_stalls_l1d_miss"] = *step.PerfStallsL1dMiss
	}
	if step.PerfStallsMemAny != nil {
		fields["perf_stalls_mem_any"] = *step.PerfStallsMemAny
	}
	if step.PerfResourceStallsSB != nil {
		fields["perf_resource_stalls_sb"] = *step.PerfResourceStallsSB
	}
	if step.PerfResourceStallsScoreboard != nil {
		fields["perf_resource_stalls_scoreboard"] = *step.PerfResourceStallsScoreboard
	}

	if step.PerfCacheMissRate != nil {
		fields["perf_cache_miss_rate"] = *step.PerfCacheMissRate
	}
	if step.PerfBranchMissRate != nil {
		fields["perf_branch_miss_rate"] = *step.PerfBranchMissRate
	}
	if step.PerfStalledCyclesPercent != nil {
		fields["perf_stalled_cycles_percent"] = *step.PerfStalledCyclesPercent
	}
	if step.PerfStallsL3MissPercent != nil {
		fields["perf_stalls_l3_miss_percent"] = *step.PerfStallsL3MissPercent
	}
	if step.PerfInstructionsPerCycle != nil {
		fields["perf_instructions_per_cycle"] = *step.PerfInstructionsPerCycle
	}
	if step.PerfTheoreticalIPC != nil {
		fields["perf_theoretical_ipc"] = *step.PerfTheoreticalIPC
	}
	if step.PerfIPCEfficancy != nil {
		fields["perf_ipc_efficancy"] = *step.PerfIPCEfficancy
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
	if step.RDTMBAThrottle != nil {
		fields["rdt_mba_throttle"] = *step.RDTMBAThrottle
	}

	// Add per-socket monitoring metrics
	if step.RDTL3OccupancyPerSocket != nil {
		if occ, ok := step.RDTL3OccupancyPerSocket[0]; ok {
			fields["rdt_l3_occupancy_socket0"] = occ
		}
		if occ, ok := step.RDTL3OccupancyPerSocket[1]; ok {
			fields["rdt_l3_occupancy_socket1"] = occ
		}
	}
	if step.RDTMemoryBandwidthTotalPerSocket != nil {
		if bw, ok := step.RDTMemoryBandwidthTotalPerSocket[0]; ok {
			fields["rdt_memory_bandwidth_total_socket0"] = bw
		}
		if bw, ok := step.RDTMemoryBandwidthTotalPerSocket[1]; ok {
			fields["rdt_memory_bandwidth_total_socket1"] = bw
		}
	}
	if step.RDTMemoryBandwidthLocalPerSocket != nil {
		if bw, ok := step.RDTMemoryBandwidthLocalPerSocket[0]; ok {
			fields["rdt_memory_bandwidth_local_socket0"] = bw
		}
		if bw, ok := step.RDTMemoryBandwidthLocalPerSocket[1]; ok {
			fields["rdt_memory_bandwidth_local_socket1"] = bw
		}
	}
	if step.RDTL3UtilizationPctPerSocket != nil {
		if pct, ok := step.RDTL3UtilizationPctPerSocket[0]; ok {
			fields["rdt_l3_utilization_pct_socket0"] = pct
		}
		if pct, ok := step.RDTL3UtilizationPctPerSocket[1]; ok {
			fields["rdt_l3_utilization_pct_socket1"] = pct
		}
	}
	if step.RDTMemBandwidthMbpsPerSocket != nil {
		if mbps, ok := step.RDTMemBandwidthMbpsPerSocket[0]; ok {
			fields["rdt_mem_bandwidth_mbps_socket0"] = mbps
		}
		if mbps, ok := step.RDTMemBandwidthMbpsPerSocket[1]; ok {
			fields["rdt_mem_bandwidth_mbps_socket1"] = mbps
		}
	}

	// Add per-socket L3 allocation details
	if step.RDTL3BitmaskPerSocket != nil {
		if bitmask, ok := step.RDTL3BitmaskPerSocket[0]; ok {
			fields["rdt_l3_bitmask_socket0"] = bitmask
		}
		if bitmask, ok := step.RDTL3BitmaskPerSocket[1]; ok {
			fields["rdt_l3_bitmask_socket1"] = bitmask
		}
	}
	if step.RDTL3WaysPerSocket != nil {
		if ways, ok := step.RDTL3WaysPerSocket[0]; ok {
			fields["rdt_l3_ways_socket0"] = ways
		}
		if ways, ok := step.RDTL3WaysPerSocket[1]; ok {
			fields["rdt_l3_ways_socket1"] = ways
		}
	}
	if step.RDTL3AllocationPctPerSocket != nil {
		if pct, ok := step.RDTL3AllocationPctPerSocket[0]; ok {
			fields["rdt_l3_allocation_pct_socket0"] = pct
		}
		if pct, ok := step.RDTL3AllocationPctPerSocket[1]; ok {
			fields["rdt_l3_allocation_pct_socket1"] = pct
		}
	}

	// Add per-socket MBA allocation details
	if step.RDTMBAPercentPerSocket != nil {
		if mba, ok := step.RDTMBAPercentPerSocket[0]; ok {
			fields["rdt_mba_percent_socket0"] = mba
		}
		if mba, ok := step.RDTMBAPercentPerSocket[1]; ok {
			fields["rdt_mba_percent_socket1"] = mba
		}
	}

	// Add full allocation strings
	if step.RDTL3AllocationString != nil {
		fields["rdt_l3_allocation_string"] = *step.RDTL3AllocationString
	}
	if step.RDTMBAAllocationString != nil {
		fields["rdt_mba_allocation_string"] = *step.RDTMBAAllocationString
	}

	return fields
}

func (idb *InfluxDBClient) Close() {
	if idb.client != nil {
		idb.client.Close()
	}
}
