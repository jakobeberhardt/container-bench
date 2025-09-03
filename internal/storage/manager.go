package storage

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/jakobeberhardt/container-bench/internal/config"
	log "github.com/sirupsen/logrus"
)

// Manager handles data storage operations
type Manager struct {
	client           influxdb2.Client
	writeAPI         api.WriteAPI
	config           config.DBConfig
	org              string
	totalBytesWritten int64  
}

// Measurement represents a data point to be stored
type Measurement struct {
	Name      string
	Tags      map[string]string
	Fields    map[string]interface{}
	Timestamp time.Time
}

// NewManager creates a new storage manager
func NewManager(config config.DBConfig) (*Manager, error) {
	// Use the token from password field for InfluxDB 2.x authentication
	token := config.Password
	org := config.Org
	if org == "" {
		org = "rdt4nn" // Default organization for RDT4NN
	}
	
	if config.User != "" && config.Password != "" {
		log.WithField("user", config.User).Info("InfluxDB authentication configured")
	}
	
	// Debug logging to check actual values
	log.WithFields(log.Fields{
		"host":   config.Host,
		"bucket": config.Name,
		"org":    org,
		"token":  "***", 
	}).Debug("Creating InfluxDB client with values")
	
	client := influxdb2.NewClient(config.Host, token)
	
	// Test connection with a ping (with timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	health, err := client.Health(ctx)
	if err != nil {
		log.WithError(err).Warn("Failed to connect to InfluxDB - continuing without storage")
		return &Manager{
			client:   client,
			writeAPI: client.WriteAPI(org, config.Name),
			config:   config,
			org:      org,
		}, nil
	}
	
	if health.Status != "pass" {
		log.WithField("status", health.Status).Warn("InfluxDB health check warning")
	}

	writeAPI := client.WriteAPI(org, config.Name)

	errorsCh := writeAPI.Errors()
	go func() {
		for err := range errorsCh {
			log.WithError(err).Error("InfluxDB write error")
		}
	}()

	sm := &Manager{
		client:   client,
		writeAPI: writeAPI,
		config:   config,
		org:      org,
	}

	log.WithFields(log.Fields{
		"host":     config.Host,
		"database": config.Name,
	}).Info("Storage manager initialized")

	return sm, nil
}

// calculateLineProtocolSize estimates the size of a measurement in InfluxDB line protocol format
func (sm *Manager) calculateLineProtocolSize(m Measurement) int64 {
	// InfluxDB line protocol format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
	size := int64(len(m.Name))
	
	// Add tags size
	for key, value := range m.Tags {
		size += int64(len(key) + len(value) + 2) // +2 for '=' and ','
	}
	
	// Add fields size
	fieldCount := 0
	for key, value := range m.Fields {
		if fieldCount > 0 {
			size += 1 // comma separator
		}
		size += int64(len(key) + 1) // +1 for '='
		
		switch v := value.(type) {
		case string:
			size += int64(len(v) + 2) // +2 for quotes
		case int, int8, int16, int32, int64:
			size += int64(len(fmt.Sprintf("%d", v)))
		case uint, uint8, uint16, uint32, uint64:
			size += int64(len(fmt.Sprintf("%d", v)))
		case float32, float64:
			size += int64(len(fmt.Sprintf("%.6f", v)))
		case bool:
			if v {
				size += 4 // "true"
			} else {
				size += 5 // "false"
			}
		default:
			size += int64(len(fmt.Sprintf("%v", v)))
		}
		fieldCount++
	}
	
	size += 20
	
	size += 3
	
	return size
}

func (sm *Manager) WriteMeasurements(ctx context.Context, measurements []Measurement) error {
	if len(measurements) == 0 {
		return nil
	}

	var totalSize int64

	// Convert measurements to InfluxDB points
	for _, measurement := range measurements {
		point := influxdb2.NewPoint(
			measurement.Name,
			measurement.Tags,
			measurement.Fields,
			measurement.Timestamp,
		)
		
		lineProtocolSize := sm.calculateLineProtocolSize(measurement)
		totalSize += lineProtocolSize
		
		sm.writeAPI.WritePoint(point)
	}

	atomic.AddInt64(&sm.totalBytesWritten, totalSize)

	log.WithFields(log.Fields{
		"count":      len(measurements),
		"bytes_size": totalSize,
	}).Debug("Measurements written to storage")
	return nil
}

func (sm *Manager) WriteMeasurement(ctx context.Context, measurement Measurement) error {
	return sm.WriteMeasurements(ctx, []Measurement{measurement})
}

func (sm *Manager) Query(ctx context.Context, query string) ([]map[string]interface{}, error) {
	queryAPI := sm.client.QueryAPI(sm.org)
	
	result, err := queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	var results []map[string]interface{}
	for result.Next() {
		record := result.Record()
		recordMap := make(map[string]interface{})
		
		for key, value := range record.Values() {
			recordMap[key] = value
		}
		
		results = append(results, recordMap)
	}
	
	if result.Err() != nil {
		return nil, fmt.Errorf("query result error: %w", result.Err())
	}

	return results, nil
}

// CreateBucket creates a bucket in InfluxDB (if needed)
func (sm *Manager) CreateBucket(ctx context.Context, bucketName string, retention time.Duration) error {
	
	log.WithField("bucket", bucketName).Info("Bucket creation skipped - ensure bucket exists in InfluxDB")
	return nil
}

func (sm *Manager) Flush() error {
	sm.writeAPI.Flush()
	return nil
}

func (sm *Manager) Close() error {
	log.Info("Closing storage manager")
	
	sm.writeAPI.Flush()
	
	sm.client.Close()
	
	return nil
}

func (sm *Manager) GetStats(ctx context.Context) (map[string]interface{}, error) {
	stats := make(map[string]interface{})
	
	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: -1h)
		|> group()
		|> count()
	`, sm.config.Name)
	
	results, err := sm.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage stats: %w", err)
	}
	
	if len(results) > 0 {
		stats["total_points_last_hour"] = results[0]["_value"]
	}
	
	stats["database"] = sm.config.Name
	stats["host"] = sm.config.Host
	
	return stats, nil
}

func (sm *Manager) WriteMetadata(ctx context.Context, benchmarkID string, metadata map[string]interface{}) error {
	tags := map[string]string{
		"benchmark_id": benchmarkID,
		"type":         "metadata",
	}
	
	measurement := Measurement{
		Name:      "benchmark_metadata",
		Tags:      tags,
		Fields:    metadata,
		Timestamp: time.Now(),
	}
	
	return sm.WriteMeasurement(ctx, measurement)
}

// WriteBenchmarkMetadata writes comprehensive benchmark metadata to storage
func (sm *Manager) WriteBenchmarkMetadata(ctx context.Context, metadata *BenchmarkMetadata) error {
	tags := map[string]string{
		"benchmark_id":   strconv.FormatInt(metadata.BenchmarkID, 10),
		"benchmark_name": metadata.BenchmarkName,
		"execution_host": metadata.ExecutionHost,
		"used_scheduler": metadata.UsedScheduler,
		"type":          "benchmark_metadata",
	}

	fields := map[string]interface{}{
		"benchmark_id":           metadata.BenchmarkID,
		"benchmark_started":      metadata.BenchmarkStarted.Unix(),
		"cpu_executed_on":        metadata.CPUExecutedOn,
		"total_cpu_cores":        metadata.TotalCPUCores,
		"os_info":               metadata.OSInfo,
		"kernel_version":         metadata.KernelVersion,
		
		// System information
		"driver_version":         metadata.DriverVersion,
		"build_date":            metadata.BuildDate,
		"cpu_model":             metadata.CPUModel,
		"cpu_vendor":            metadata.CPUVendor,
		"cpu_threads":           metadata.CPUThreads,
		"architecture":          metadata.Architecture,
		"hostname":              metadata.Hostname,
		"description":           metadata.Description,
		"scheduler_version":     metadata.SchedulerVersion,
		
		// Configuration
		"config_file":           metadata.ConfigFile,
		"config_file_path":      metadata.ConfigFilePath,
		"sampling_frequency_ms": metadata.SamplingFrequency,
		"max_duration_seconds":  metadata.MaxDuration,
		
		// Data collection settings
		"rdt_enabled":           metadata.RDTEnabled,
		"perf_enabled":          metadata.PerfEnabled,
		"docker_stats_enabled":  metadata.DockerStatsEnabled,
		
		// Container information
		"total_containers":      metadata.TotalContainers,
		
		// Results summary
		"total_sampling_steps":  metadata.TotalSamplingSteps,
		"total_measurements":    metadata.TotalMeasurements,
		"total_data_size_bytes": metadata.TotalDataSize,
		
		// Database information
		"database_host":         metadata.DatabaseHost,
		"database_name":         metadata.DatabaseName,
		"database_user":         metadata.DatabaseUser,
	}

	// Add benchmark finished if available
	if !metadata.BenchmarkFinished.IsZero() {
		fields["benchmark_finished"] = metadata.BenchmarkFinished.Unix()
		fields["duration_seconds"] = metadata.BenchmarkFinished.Sub(metadata.BenchmarkStarted).Seconds()
	}

	measurement := Measurement{
		Name:      "benchmark_meta",
		Tags:      tags,
		Fields:    fields,
		Timestamp: metadata.BenchmarkStarted,
	}

	log.WithFields(log.Fields{
		"benchmark_id":   metadata.BenchmarkID,
		"benchmark_name": metadata.BenchmarkName,
		"execution_host": metadata.ExecutionHost,
	}).Info("Writing benchmark metadata to storage")

	return sm.WriteMeasurement(ctx, measurement)
}

// GetTotalBytesWritten returns the total number of bytes written during this benchmark session
func (sm *Manager) GetTotalBytesWritten() int64 {
	return atomic.LoadInt64(&sm.totalBytesWritten)
}

// ResetBytesCounter resets the byte counter (call at the start of a new benchmark)
func (sm *Manager) ResetBytesCounter() {
	atomic.StoreInt64(&sm.totalBytesWritten, 0)
}

// FormatDataSize formats bytes into a human-readable format (KB, MB, GB)
func (sm *Manager) FormatDataSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}
