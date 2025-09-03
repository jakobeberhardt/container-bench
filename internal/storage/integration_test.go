// +build integration

package storage

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jakobeberhardt/container-bench/internal/config"
)

// TestInfluxDBIntegration runs when INFLUXDB_TOKEN environment variable is set
// This is used in CI with the ephemeral InfluxDB service
func TestInfluxDBIntegration(t *testing.T) {
	token := os.Getenv("INFLUXDB_TOKEN")
	if token == "" {
		t.Skip("Skipping integration test: INFLUXDB_TOKEN not set")
	}

	host := os.Getenv("INFLUXDB_HOST")
	if host == "" {
		host = "localhost:8086"
	}

	org := os.Getenv("INFLUXDB_ORG")
	if org == "" {
		org = "rdt4nn"
	}

	bucket := os.Getenv("INFLUXDB_BUCKET")
	if bucket == "" {
		bucket = "testbucket"
	}

	dbConfig := config.DBConfig{
		Host:         host,
		Organization: org,
		Bucket:       bucket,
		Token:        token,
	}

	// Test manager creation
	manager, err := NewManager(dbConfig)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	ctx := context.Background()

	// Test basic write operation
	measurement := Measurement{
		Name: "integration_test",
		Tags: map[string]string{
			"test_type": "basic",
		},
		Fields: map[string]interface{}{
			"value":     42.0,
			"status":    "success",
			"timestamp": time.Now().Unix(),
		},
		Timestamp: time.Now(),
	}

	err = manager.WriteMeasurement(ctx, measurement)
	if err != nil {
		t.Fatalf("Failed to write measurement: %v", err)
	}

	// Test benchmark ID functionality
	benchmarkID, err := manager.GetNextBenchmarkID(ctx)
	if err != nil {
		t.Fatalf("Failed to get benchmark ID: %v", err)
	}

	if benchmarkID <= 0 {
		t.Fatalf("Expected positive benchmark ID, got: %d", benchmarkID)
	}

	t.Logf("Successfully created benchmark ID: %d", benchmarkID)

	// Test comprehensive benchmark metrics write
	testMetrics := &BenchmarkMetrics{
		BenchmarkID:     benchmarkID,
		TestName:        "integration_test",
		StartTime:       time.Now().Add(-time.Minute),
		EndTime:         time.Now(),
		ContainerID:     "test_container_123",
		Success:         true,
		
		// Docker metrics
		DockerCPUPercent:    75.5,
		DockerMemoryUsage:   1024*1024*512, // 512MB
		DockerMemoryLimit:   1024*1024*1024, // 1GB
		DockerMemoryPercent: 50.0,
		DockerNetworkRxBytes: 1024*100,
		DockerNetworkTxBytes: 1024*200,
		DockerBlockReadBytes:  1024*50,
		DockerBlockWriteBytes: 1024*75,

		// Perf metrics
		PerfCPUCycles:        1000000,
		PerfInstructions:     2000000,
		PerfCacheReferences: 50000,
		PerfCacheMisses:     5000,
		PerfBranchInstructions: 100000,
		PerfBranchMisses:    10000,
		PerfPageFaults:      1000,

		// RDT metrics
		RdtL3Occupancy:      256*1024,  // 256KB
		RdtMemoryBandwidth: 1024*1024*10, // 10MB/s
		RdtIPC:             1.5,
		RdtFrequency:       2400,
	}

	err = manager.WriteBenchmarkMetrics(ctx, testMetrics)
	if err != nil {
		t.Fatalf("Failed to write benchmark metrics: %v", err)
	}

	t.Logf("Successfully wrote comprehensive benchmark metrics")

	// Verify we can get another benchmark ID
	nextID, err := manager.GetNextBenchmarkID(ctx)
	if err != nil {
		t.Fatalf("Failed to get next benchmark ID: %v", err)
	}

	if nextID != benchmarkID+1 {
		t.Fatalf("Expected next benchmark ID to be %d, got %d", benchmarkID+1, nextID)
	}

	t.Logf("Benchmark ID properly incremented from %d to %d", benchmarkID, nextID)
}
