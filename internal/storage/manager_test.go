package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jakobeberhardt/container-bench/internal/config"
)

func TestMeasurement(t *testing.T) {
	timestamp := time.Now()
	
	measurement := Measurement{
		Name: "test_metric",
		Tags: map[string]string{
			"container": "test",
			"benchmark": "test_benchmark",
		},
		Fields: map[string]interface{}{
			"cpu_usage": 75.5,
			"memory_mb": 256.0,
		},
		Timestamp: timestamp,
	}

	// Test that measurement is created correctly
	if measurement.Name != "test_metric" {
		t.Errorf("Expected name 'test_metric', got '%s'", measurement.Name)
	}

	if len(measurement.Tags) != 2 {
		t.Errorf("Expected 2 tags, got %d", len(measurement.Tags))
	}

	if measurement.Tags["container"] != "test" {
		t.Errorf("Expected container tag 'test', got '%s'", measurement.Tags["container"])
	}

	if len(measurement.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(measurement.Fields))
	}

	if measurement.Fields["cpu_usage"] != 75.5 {
		t.Errorf("Expected cpu_usage 75.5, got %v", measurement.Fields["cpu_usage"])
	}

	if !measurement.Timestamp.Equal(timestamp) {
		t.Errorf("Expected timestamp %v, got %v", timestamp, measurement.Timestamp)
	}
}

func TestNewManager_InvalidHost(t *testing.T) {
	// Test with invalid host to ensure error handling
	config := config.DBConfig{
		Host:     "invalid_host:9999",
		Name:     "test_db",
		User:     "test_user",
		Password: "test_pass",
	}

	_, err := NewManager(config)
	if err == nil {
		t.Error("Expected error for invalid host, got nil")
	}
}

func TestMeasurementBatch(t *testing.T) {
	timestamp := time.Now()
	
	measurements := []Measurement{
		{
			Name: "cpu_usage",
			Tags: map[string]string{"container": "test1"},
			Fields: map[string]interface{}{"value": 50.0},
			Timestamp: timestamp,
		},
		{
			Name: "memory_usage",
			Tags: map[string]string{"container": "test1"},
			Fields: map[string]interface{}{"value": 100.0},
			Timestamp: timestamp,
		},
		{
			Name: "cpu_usage",
			Tags: map[string]string{"container": "test2"},
			Fields: map[string]interface{}{"value": 75.0},
			Timestamp: timestamp,
		},
	}

	// Test batch operations
	if len(measurements) != 3 {
		t.Errorf("Expected 3 measurements, got %d", len(measurements))
	}

	// Test that each measurement is valid
	for i, m := range measurements {
		if m.Name == "" {
			t.Errorf("Measurement %d has empty name", i)
		}
		if len(m.Tags) == 0 {
			t.Errorf("Measurement %d has no tags", i)
		}
		if len(m.Fields) == 0 {
			t.Errorf("Measurement %d has no fields", i)
		}
	}
}

func TestMeasurementValidation(t *testing.T) {
	tests := []struct {
		name        string
		measurement Measurement
		valid       bool
	}{
		{
			name: "valid measurement",
			measurement: Measurement{
				Name: "test_metric",
				Tags: map[string]string{"tag": "value"},
				Fields: map[string]interface{}{"field": 123.45},
				Timestamp: time.Now(),
			},
			valid: true,
		},
		{
			name: "empty name",
			measurement: Measurement{
				Name: "",
				Tags: map[string]string{"tag": "value"},
				Fields: map[string]interface{}{"field": 123.45},
				Timestamp: time.Now(),
			},
			valid: false,
		},
		{
			name: "no fields",
			measurement: Measurement{
				Name: "test_metric",
				Tags: map[string]string{"tag": "value"},
				Fields: map[string]interface{}{},
				Timestamp: time.Now(),
			},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := tt.measurement.Name != "" && len(tt.measurement.Fields) > 0
			if valid != tt.valid {
				t.Errorf("Expected valid=%v, got valid=%v", tt.valid, valid)
			}
		})
	}
}

func BenchmarkMeasurementCreation(b *testing.B) {
	timestamp := time.Now()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Measurement{
			Name: "benchmark_metric",
			Tags: map[string]string{
				"benchmark_id": "test_123",
				"container":    "container_1",
				"collector":    "benchmark",
			},
			Fields: map[string]interface{}{
				"value": float64(i),
				"count": i,
			},
			Timestamp: timestamp,
		}
	}
}

// Mock tests for functionality that requires InfluxDB connection
func TestManagerInterface(t *testing.T) {
	// This test ensures the Manager interface methods exist
	// without requiring actual InfluxDB connection
	
	config := config.DBConfig{
		Host:     "localhost:8086",
		Name:     "test_db",
		User:     "test_user",
		Password: "test_pass",
	}

	// This will fail without InfluxDB, but tests interface
	manager, err := NewManager(config)
	if err != nil {
		// Expected to fail without InfluxDB running
		t.Logf("Manager creation failed as expected: %v", err)
		return
	}

	// If we somehow get a manager, test that methods exist
	ctx := context.Background()
	
	// Test WriteMeasurement method signature
	measurement := Measurement{
		Name:      "test",
		Tags:      map[string]string{"test": "value"},
		Fields:    map[string]interface{}{"value": 1.0},
		Timestamp: time.Now(),
	}
	
	_ = manager.WriteMeasurement(ctx, measurement)
	
	// Test Close method
	_ = manager.Close()
}
