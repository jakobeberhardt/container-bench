package database

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/sirupsen/logrus"
)

type PlotDBClient struct {
	client   influxdb2.Client
	queryAPI api.QueryAPI
	bucket   string
	org      string
	logger   *logrus.Logger
}

type MetricsDataPoint struct {
	Time           time.Time
	RelativeTime   int64
	StepNumber     int64
	ContainerIndex int
	ContainerName  string
	ContainerCore  string
	ContainerImage string
	Fields         map[string]interface{}
}

type MetaData struct {
	BenchmarkID            int
	BenchmarkName          string
	Description            string
	BenchmarkStarted       string
	BenchmarkFinished      string
	DurationSeconds        int64
	MaxDurationSeconds     int64
	SamplingFrequencyMs    int64
	TotalContainers        int64
	TotalSamplingSteps     int64
	TotalMeasurements      int64
	TotalDataSizeBytes     int64
	UsedScheduler          string
	SchedulerVersion       string
	DriverVersion          string
	Hostname               string
	ExecutionHost          string
	CPUVendor              string
	CPUModel               string
	TotalCPUCores          int64
	CPUSockets             int64
	CPUThreads             int64
	L1CacheSizeKB          float64
	L2CacheSizeKB          float64
	L3CacheSizeMB          float64
	L3CacheSizeBytes       int64
	L3CacheWays            int64
	MaxMemoryBandwidthMbps int64
	KernelVersion          string
	OSInfo                 string
	ConfigFile             string
	PerfEnabled            bool
	DockerStatsEnabled     bool
	RDTEnabled             bool
	RDTSupported           bool
	RDTAllocationSupported bool
	RDTMonitoringSupported bool
	ProberEnabled          bool
	ProberImplementation   string
	ProberIsolated         bool
	ProberAbortable        bool
}

type ProbeData struct {
	BenchmarkID           int
	ContainerIndex        int
	ContainerID           string
	ContainerName         string
	ContainerImage        string
	ContainerCores        string
	ContainerCommand      string
	ProbingContainerID    string
	ProbingContainerName  string
	ProbingContainerCores string
	UsedProbeKernel       string
	LLC                   float64
	MemRead               float64
	MemWrite              float64
	Prefetch              float64
	Syscall               float64
	ProbeTimeNs           int64
	Started               string
	Finished              string
	Isolated              bool
	Aborted               bool
	FirstDataframeStep    int64
	LastDataframeStep     int64
}

func NewPlotDBClient(logger *logrus.Logger) (*PlotDBClient, error) {
	host := os.Getenv("INFLUXDB_HOST")
	token := os.Getenv("INFLUXDB_TOKEN")
	org := os.Getenv("INFLUXDB_ORG")
	bucket := os.Getenv("INFLUXDB_BUCKET")

	if host == "" || token == "" || org == "" || bucket == "" {
		return nil, fmt.Errorf("missing required environment variables for InfluxDB connection")
	}

	client := influxdb2.NewClient(host, token)
	queryAPI := client.QueryAPI(org)

	return &PlotDBClient{
		client:   client,
		queryAPI: queryAPI,
		bucket:   bucket,
		org:      org,
		logger:   logger,
	}, nil
}

func (c *PlotDBClient) Close() {
	c.client.Close()
}

func (c *PlotDBClient) QueryMetrics(ctx context.Context, benchmarkID int, yField string, interval float64) ([]MetricsDataPoint, error) {
	c.logger.WithFields(logrus.Fields{
		"benchmark_id": benchmarkID,
		"y_field":      yField,
		"interval":     interval,
	}).Debug("Querying benchmark metrics")

	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: 0)
		|> filter(fn: (r) => r["_measurement"] == "benchmark_metrics")
		|> filter(fn: (r) => r["benchmark_id"] == "%d")
		|> filter(fn: (r) => r["_field"] == "relative_time" or r["_field"] == "step_number" or r["_field"] == "%s")
		|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
		|> sort(columns: ["container_index", "_time"])
	`, c.bucket, benchmarkID, yField)

	result, err := c.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	var dataPoints []MetricsDataPoint
	for result.Next() {
		record := result.Record()

		dp := MetricsDataPoint{
			Time:           record.Time(),
			ContainerIndex: -1,
			Fields:         make(map[string]interface{}),
		}

		// container_index is a tag (string), need to parse it
		if idxStr, ok := record.ValueByKey("container_index").(string); ok {
			var idx int
			fmt.Sscanf(idxStr, "%d", &idx)
			dp.ContainerIndex = idx
		} else if idx, ok := record.ValueByKey("container_index").(int64); ok {
			dp.ContainerIndex = int(idx)
		}

		if name, ok := record.ValueByKey("container_name").(string); ok {
			dp.ContainerName = name
		}
		if core, ok := record.ValueByKey("container_core").(string); ok {
			dp.ContainerCore = core
		}
		if image, ok := record.ValueByKey("container_image").(string); ok {
			dp.ContainerImage = image
		}
		if relTime, ok := record.ValueByKey("relative_time").(int64); ok {
			dp.RelativeTime = relTime
		}
		if step, ok := record.ValueByKey("step_number").(int64); ok {
			dp.StepNumber = step
		}

		if val := record.ValueByKey(yField); val != nil {
			dp.Fields[yField] = val
		}

		dataPoints = append(dataPoints, dp)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query parsing failed: %w", result.Err())
	}

	c.logger.WithField("data_points", len(dataPoints)).Debug("Query completed")
	return dataPoints, nil
}

func (c *PlotDBClient) QueryMetaData(ctx context.Context, benchmarkID int) (*MetaData, error) {
	c.logger.WithField("benchmark_id", benchmarkID).Debug("Querying benchmark metadata")

	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: 0)
		|> filter(fn: (r) => r["_measurement"] == "benchmark_meta")
		|> filter(fn: (r) => r["benchmark_id"] == "%d")
		|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	`, c.bucket, benchmarkID)

	result, err := c.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	var meta *MetaData
	if result.Next() {
		record := result.Record()
		meta = &MetaData{
			BenchmarkID: benchmarkID,
		}

		if v, ok := record.ValueByKey("benchmark_name").(string); ok {
			meta.BenchmarkName = v
		}
		if v, ok := record.ValueByKey("description").(string); ok {
			meta.Description = v
		}
		if v, ok := record.ValueByKey("benchmark_started").(string); ok {
			meta.BenchmarkStarted = v
		}
		if v, ok := record.ValueByKey("benchmark_finished").(string); ok {
			meta.BenchmarkFinished = v
		}
		if v, ok := record.ValueByKey("duration_seconds").(int64); ok {
			meta.DurationSeconds = v
		}
		if v, ok := record.ValueByKey("max_duration_seconds").(int64); ok {
			meta.MaxDurationSeconds = v
		}
		if v, ok := record.ValueByKey("sampling_frequency_ms").(int64); ok {
			meta.SamplingFrequencyMs = v
		}
		if v, ok := record.ValueByKey("total_containers").(int64); ok {
			meta.TotalContainers = v
		}
		if v, ok := record.ValueByKey("total_sampling_steps").(int64); ok {
			meta.TotalSamplingSteps = v
		}
		if v, ok := record.ValueByKey("total_measurements").(int64); ok {
			meta.TotalMeasurements = v
		}
		if v, ok := record.ValueByKey("total_data_size_bytes").(int64); ok {
			meta.TotalDataSizeBytes = v
		}
		if v, ok := record.ValueByKey("used_scheduler").(string); ok {
			meta.UsedScheduler = v
		}
		if v, ok := record.ValueByKey("scheduler_version").(string); ok {
			meta.SchedulerVersion = v
		}
		if v, ok := record.ValueByKey("driver_version").(string); ok {
			meta.DriverVersion = v
		}
		if v, ok := record.ValueByKey("hostname").(string); ok {
			meta.Hostname = v
		}
		if v, ok := record.ValueByKey("execution_host").(string); ok {
			meta.ExecutionHost = v
		}
		if v, ok := record.ValueByKey("cpu_vendor").(string); ok {
			meta.CPUVendor = v
		}
		if v, ok := record.ValueByKey("cpu_model").(string); ok {
			meta.CPUModel = v
		}
		if v, ok := record.ValueByKey("total_cpu_cores").(int64); ok {
			meta.TotalCPUCores = v
		}
		if v, ok := record.ValueByKey("cpu_sockets").(int64); ok {
			meta.CPUSockets = v
		}
		if v, ok := record.ValueByKey("cpu_threads").(int64); ok {
			meta.CPUThreads = v
		}
		if v, ok := record.ValueByKey("l1_cache_size_kb").(float64); ok {
			meta.L1CacheSizeKB = v
		}
		if v, ok := record.ValueByKey("l2_cache_size_kb").(float64); ok {
			meta.L2CacheSizeKB = v
		}
		if v, ok := record.ValueByKey("l3_cache_size_mb").(float64); ok {
			meta.L3CacheSizeMB = v
		}
		if v, ok := record.ValueByKey("l3_cache_size_bytes").(int64); ok {
			meta.L3CacheSizeBytes = v
		}
		if v, ok := record.ValueByKey("l3_cache_ways").(int64); ok {
			meta.L3CacheWays = v
		}
		if v, ok := record.ValueByKey("max_memory_bandwidth_mbps").(int64); ok {
			meta.MaxMemoryBandwidthMbps = v
		}
		if v, ok := record.ValueByKey("kernel_version").(string); ok {
			meta.KernelVersion = v
		}
		if v, ok := record.ValueByKey("os_info").(string); ok {
			meta.OSInfo = v
		}
		if v, ok := record.ValueByKey("config_file").(string); ok {
			meta.ConfigFile = v
		}
		if v, ok := record.ValueByKey("perf_enabled").(bool); ok {
			meta.PerfEnabled = v
		}
		if v, ok := record.ValueByKey("docker_stats_enabled").(bool); ok {
			meta.DockerStatsEnabled = v
		}
		if v, ok := record.ValueByKey("rdt_enabled").(bool); ok {
			meta.RDTEnabled = v
		}
		if v, ok := record.ValueByKey("rdt_supported").(bool); ok {
			meta.RDTSupported = v
		}
		if v, ok := record.ValueByKey("rdt_allocation_supported").(bool); ok {
			meta.RDTAllocationSupported = v
		}
		if v, ok := record.ValueByKey("rdt_monitoring_supported").(bool); ok {
			meta.RDTMonitoringSupported = v
		}
		if v, ok := record.ValueByKey("prober_enabled").(bool); ok {
			meta.ProberEnabled = v
		}
		if v, ok := record.ValueByKey("prober_implementation").(string); ok {
			meta.ProberImplementation = v
		}
		if v, ok := record.ValueByKey("prober_isolated").(bool); ok {
			meta.ProberIsolated = v
		}
		if v, ok := record.ValueByKey("prober_abortable").(bool); ok {
			meta.ProberAbortable = v
		}
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query parsing failed: %w", result.Err())
	}

	if meta == nil {
		return nil, fmt.Errorf("no metadata found for benchmark_id %d", benchmarkID)
	}

	c.logger.Debug("Metadata query completed")
	return meta, nil
}

func (c *PlotDBClient) QueryProbes(ctx context.Context, probeIndices []int, metricType string) ([]ProbeData, error) {
	c.logger.WithFields(logrus.Fields{
		"probe_indices": probeIndices,
		"metric_type":   metricType,
	}).Debug("Querying benchmark probes")

	if len(probeIndices) == 0 {
		return nil, fmt.Errorf("no probe indices provided")
	}

	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: 0)
		|> filter(fn: (r) => r["_measurement"] == "benchmark_probes")
		|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
		|> sort(columns: ["_time"])
	`, c.bucket)

	result, err := c.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	var probes []ProbeData
	for result.Next() {
		record := result.Record()

		probe := ProbeData{}

		if v, ok := record.ValueByKey("benchmark_id").(string); ok {
			fmt.Sscanf(v, "%d", &probe.BenchmarkID)
		}
		if v, ok := record.ValueByKey("container_index").(string); ok {
			fmt.Sscanf(v, "%d", &probe.ContainerIndex)
		}
		if v, ok := record.ValueByKey("container_id").(string); ok {
			probe.ContainerID = v
		}
		if v, ok := record.ValueByKey("container_name").(string); ok {
			probe.ContainerName = v
		}
		if v, ok := record.ValueByKey("container_image").(string); ok {
			probe.ContainerImage = v
		}
		if v, ok := record.ValueByKey("container_cores").(string); ok {
			probe.ContainerCores = v
		}
		if v, ok := record.ValueByKey("container_command").(string); ok {
			probe.ContainerCommand = v
		}
		if v, ok := record.ValueByKey("probing_container_id").(string); ok {
			probe.ProbingContainerID = v
		}
		if v, ok := record.ValueByKey("probing_container_name").(string); ok {
			probe.ProbingContainerName = v
		}
		if v, ok := record.ValueByKey("probing_container_cores").(string); ok {
			probe.ProbingContainerCores = v
		}
		if v, ok := record.ValueByKey("used_probe_kernel").(string); ok {
			probe.UsedProbeKernel = v
		}

		// Query sensitivity metrics with the specified metric type prefix
		prefix := metricType + "_"
		if v, ok := record.ValueByKey(prefix + "llc").(float64); ok {
			probe.LLC = v
		}
		if v, ok := record.ValueByKey(prefix + "mem_read").(float64); ok {
			probe.MemRead = v
		}
		if v, ok := record.ValueByKey(prefix + "mem_write").(float64); ok {
			probe.MemWrite = v
		}
		if v, ok := record.ValueByKey(prefix + "prefetch").(float64); ok {
			probe.Prefetch = v
		}
		if v, ok := record.ValueByKey(prefix + "syscall").(float64); ok {
			probe.Syscall = v
		}

		if v, ok := record.ValueByKey("probe_time_ns").(int64); ok {
			probe.ProbeTimeNs = v
		}
		if v, ok := record.ValueByKey("started").(string); ok {
			probe.Started = v
		}
		if v, ok := record.ValueByKey("finished").(string); ok {
			probe.Finished = v
		}
		if v, ok := record.ValueByKey("isolated").(bool); ok {
			probe.Isolated = v
		}
		if v, ok := record.ValueByKey("aborted").(bool); ok {
			probe.Aborted = v
		}
		if v, ok := record.ValueByKey("first_dataframe_step").(int64); ok {
			probe.FirstDataframeStep = v
		}
		if v, ok := record.ValueByKey("last_dataframe_step").(int64); ok {
			probe.LastDataframeStep = v
		}

		probes = append(probes, probe)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query parsing failed: %w", result.Err())
	}

	if len(probes) == 0 {
		return nil, fmt.Errorf("no probe data found in database")
	}

	sort.Slice(probes, func(i, j int) bool {
		return probes[i].Started < probes[j].Started
	})

	// Filter by requested indices
	var selectedProbes []ProbeData
	for _, idx := range probeIndices {
		if idx < 1 || idx > len(probes) {
			c.logger.WithFields(map[string]interface{}{
				"requested_index": idx,
				"total_probes":    len(probes),
			}).Warn("Probe index out of range, skipping")
			continue
		}
		// Convert from 1-based to 0-based indexing
		selectedProbes = append(selectedProbes, probes[idx-1])
	}

	if len(selectedProbes) == 0 {
		return nil, fmt.Errorf("no probe data found for specified indices")
	}

	c.logger.WithFields(map[string]interface{}{
		"total_probes":    len(probes),
		"selected_probes": len(selectedProbes),
	}).Debug("Probe query completed")
	return selectedProbes, nil
}

type AllocationData struct {
	BenchmarkID            int
	AllocationProbeIndex   int
	ContainerIndex         int
	ContainerID            string
	ContainerName          string
	ContainerImage         string
	ContainerCores         string
	ContainerCommand       string
	SocketID               int
	IsolatedOthers         bool
	L3Ways                 int
	MemBandwidth           float64
	AllocationStarted      string
	AllocationDuration     int64
	AvgIPC                 float64
	AvgTheoreticalIPC      float64
	IPCEfficiency          float64
	AvgCacheMissRate       float64
	AvgStalledCycles       float64
	AvgStallsL3MissPercent float64
	AvgL3Occupancy         uint64
	AvgMemBandwidthUsed    uint64
	ProbeAborted           bool
	ProbeStarted           string
	ProbeFinished          string
	TotalProbeTime         int64
	FirstDataframeStep     int64
	LastDataframeStep      int64
	NumDataframeSteps      int64
	RangeMinL3Ways         int
	RangeMaxL3Ways         int
	RangeMinMemBandwidth   float64
	RangeMaxMemBandwidth   float64
	RangeStepL3Ways        int
	RangeStepMemBandwidth  float64
	RangeOrder             string
	RangeDurationPerAlloc  int
	RangeMaxTotalDuration  int
	RangeIsolateOthers     bool
	RangeForceReallocation bool
}

func (c *PlotDBClient) QueryAllocationData(ctx context.Context, benchmarkID int, allocationProbeIndex int) ([]AllocationData, error) {
	c.logger.WithFields(map[string]interface{}{
		"benchmark_id":           benchmarkID,
		"allocation_probe_index": allocationProbeIndex,
	}).Debug("Querying allocation probe data")

	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: 0)
		|> filter(fn: (r) => r["_measurement"] == "benchmark_allocation")
		|> filter(fn: (r) => r["benchmark_id"] == "%d")
		|> filter(fn: (r) => r["allocation_probe_index"] == "%d")
		|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	`, c.bucket, benchmarkID, allocationProbeIndex)

	result, err := c.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	var allocations []AllocationData

	for result.Next() {
		record := result.Record()
		alloc := AllocationData{}

		// Extract tags
		if v, ok := record.ValueByKey("benchmark_id").(string); ok {
			fmt.Sscanf(v, "%d", &alloc.BenchmarkID)
		}
		if v, ok := record.ValueByKey("allocation_probe_index").(string); ok {
			fmt.Sscanf(v, "%d", &alloc.AllocationProbeIndex)
		}
		if v, ok := record.ValueByKey("container_index").(string); ok {
			fmt.Sscanf(v, "%d", &alloc.ContainerIndex)
		}
		if v, ok := record.ValueByKey("container_id").(string); ok {
			alloc.ContainerID = v
		}
		if v, ok := record.ValueByKey("container_name").(string); ok {
			alloc.ContainerName = v
		}
		if v, ok := record.ValueByKey("container_image").(string); ok {
			alloc.ContainerImage = v
		}
		if v, ok := record.ValueByKey("container_cores").(string); ok {
			alloc.ContainerCores = v
		}
		if v, ok := record.ValueByKey("socket_id").(string); ok {
			fmt.Sscanf(v, "%d", &alloc.SocketID)
		}
		if v, ok := record.ValueByKey("isolated_others").(string); ok {
			alloc.IsolatedOthers = (v == "true")
		}

		// Extract fields
		if v, ok := record.ValueByKey("container_command").(string); ok {
			alloc.ContainerCommand = v
		}
		if v, ok := record.ValueByKey("l3_ways").(int64); ok {
			alloc.L3Ways = int(v)
		}
		if v, ok := record.ValueByKey("mem_bandwidth").(float64); ok {
			alloc.MemBandwidth = v
		}
		if v, ok := record.ValueByKey("allocation_started").(string); ok {
			alloc.AllocationStarted = v
		}
		if v, ok := record.ValueByKey("allocation_duration").(int64); ok {
			alloc.AllocationDuration = v
		}
		if v, ok := record.ValueByKey("avg_ipc").(float64); ok {
			alloc.AvgIPC = v
		}
		if v, ok := record.ValueByKey("avg_theoretical_ipc").(float64); ok {
			alloc.AvgTheoreticalIPC = v
		}
		if v, ok := record.ValueByKey("ipc_efficiency").(float64); ok {
			alloc.IPCEfficiency = v
		}
		if v, ok := record.ValueByKey("avg_cache_miss_rate").(float64); ok {
			alloc.AvgCacheMissRate = v
		}
		if v, ok := record.ValueByKey("avg_stalled_cycles").(float64); ok {
			alloc.AvgStalledCycles = v
		}
		if v, ok := record.ValueByKey("avg_stalls_l3_miss_percent").(float64); ok {
			alloc.AvgStallsL3MissPercent = v
		}
		if v, ok := record.ValueByKey("avg_l3_occupancy").(uint64); ok {
			alloc.AvgL3Occupancy = v
		}
		if v, ok := record.ValueByKey("avg_mem_bandwidth_used").(uint64); ok {
			alloc.AvgMemBandwidthUsed = v
		}
		if v, ok := record.ValueByKey("probe_aborted").(bool); ok {
			alloc.ProbeAborted = v
		}
		if v, ok := record.ValueByKey("probe_started").(string); ok {
			alloc.ProbeStarted = v
		}
		if v, ok := record.ValueByKey("probe_finished").(string); ok {
			alloc.ProbeFinished = v
		}
		if v, ok := record.ValueByKey("total_probe_time").(int64); ok {
			alloc.TotalProbeTime = v
		}
		if v, ok := record.ValueByKey("first_dataframe_step").(int64); ok {
			alloc.FirstDataframeStep = v
		}
		if v, ok := record.ValueByKey("last_dataframe_step").(int64); ok {
			alloc.LastDataframeStep = v
		}
		if v, ok := record.ValueByKey("num_dataframe_steps").(int64); ok {
			alloc.NumDataframeSteps = v
		}
		if v, ok := record.ValueByKey("range_min_l3_ways").(int64); ok {
			alloc.RangeMinL3Ways = int(v)
		}
		if v, ok := record.ValueByKey("range_max_l3_ways").(int64); ok {
			alloc.RangeMaxL3Ways = int(v)
		}
		if v, ok := record.ValueByKey("range_min_mem_bandwidth").(float64); ok {
			alloc.RangeMinMemBandwidth = v
		}
		if v, ok := record.ValueByKey("range_max_mem_bandwidth").(float64); ok {
			alloc.RangeMaxMemBandwidth = v
		}
		if v, ok := record.ValueByKey("range_step_l3_ways").(int64); ok {
			alloc.RangeStepL3Ways = int(v)
		}
		if v, ok := record.ValueByKey("range_step_mem_bandwidth").(float64); ok {
			alloc.RangeStepMemBandwidth = v
		}
		if v, ok := record.ValueByKey("range_order").(string); ok {
			alloc.RangeOrder = v
		}
		if v, ok := record.ValueByKey("range_duration_per_alloc").(int64); ok {
			alloc.RangeDurationPerAlloc = int(v)
		}
		if v, ok := record.ValueByKey("range_max_total_duration").(int64); ok {
			alloc.RangeMaxTotalDuration = int(v)
		}
		if v, ok := record.ValueByKey("range_isolate_others").(bool); ok {
			alloc.RangeIsolateOthers = v
		}
		if v, ok := record.ValueByKey("range_force_reallocation").(bool); ok {
			alloc.RangeForceReallocation = v
		}

		allocations = append(allocations, alloc)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query parsing failed: %w", result.Err())
	}

	if len(allocations) == 0 {
		return nil, fmt.Errorf("no allocation data found for benchmark %d, probe %d", benchmarkID, allocationProbeIndex)
	}

	c.logger.WithFields(map[string]interface{}{
		"benchmark_id":           benchmarkID,
		"allocation_probe_index": allocationProbeIndex,
		"allocations_count":      len(allocations),
	}).Debug("Allocation query completed")

	return allocations, nil
}
