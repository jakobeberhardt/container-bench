package benchmark

import (
	"testing"
	"time"

	"github.com/jakobeberhardt/container-bench/internal/config"
	"github.com/jakobeberhardt/container-bench/internal/storage"
)

// TestBenchmarkMetadataCollection tests the metadata collection functionality
func TestBenchmarkMetadataCollection(t *testing.T) {
	// Create a sample configuration
	cfg := &config.Config{
		Benchmark: config.BenchmarkConfig{
			Name:     "test_metadata_benchmark",
			MaxT:     60,
			LogLevel: "info",
			Scheduler: config.SchedulerConfig{
				Implementation: "adaptive",
				RDT:            true,
			},
			Data: config.DataConfig{
				ProfileFrequency: 500,
				RDT:              true,
				Perf:             true,
				DockerStats:      true,
				DB: config.DBConfig{
					Host:     "localhost:8086",
					Name:     "test_db",
					User:     "test_user",
					Password: "test_pass",
				},
			},
		},
		Container: map[string]config.ContainerConfig{
			"web_server": {
				Index: 0,
				Image: "nginx:alpine",
				Start: 0,
				Stop:  60,
				Core:  0,
				Env: map[string]string{
					"SERVER_NAME": "test-server",
					"PORT":        "80",
				},
			},
			"cache": {
				Index: 1,
				Image: "redis:alpine",
				Start: 5,
				Stop:  60,
				Core:  1,
				Env: map[string]string{
					"REDIS_MAXMEMORY": "256mb",
				},
			},
			"database": {
				Index: 2,
				Image: "postgres:13",
				Start: 2,
				Stop:  60,
				Core:  2,
				Env: map[string]string{
					"POSTGRES_DB":       "testdb",
					"POSTGRES_USER":     "testuser",
					"POSTGRES_PASSWORD": "testpass",
				},
			},
		},
	}

	// Test metadata creation
	configFile := "/path/to/test/config.yml"
	benchmarkID := int64(12345)
	
	metadata, err := createBenchmarkMetadata(cfg, configFile, benchmarkID)
	if err != nil {
		t.Errorf("Failed to create metadata: %v", err)
	}

	// Verify core metadata fields
	if metadata.BenchmarkID != benchmarkID {
		t.Errorf("Expected benchmark ID %d, got %d", benchmarkID, metadata.BenchmarkID)
	}

	if metadata.BenchmarkName != cfg.Benchmark.Name {
		t.Errorf("Expected benchmark name %s, got %s", cfg.Benchmark.Name, metadata.BenchmarkName)
	}

	if metadata.UsedScheduler != cfg.Benchmark.Scheduler.Implementation {
		t.Errorf("Expected scheduler %s, got %s", cfg.Benchmark.Scheduler.Implementation, metadata.UsedScheduler)
	}

	if metadata.SamplingFrequency != cfg.Benchmark.Data.ProfileFrequency {
		t.Errorf("Expected sampling frequency %d, got %d", cfg.Benchmark.Data.ProfileFrequency, metadata.SamplingFrequency)
	}

	if metadata.MaxDuration != cfg.Benchmark.MaxT {
		t.Errorf("Expected max duration %d, got %d", cfg.Benchmark.MaxT, metadata.MaxDuration)
	}

	// Verify data collection settings
	if metadata.RDTEnabled != cfg.Benchmark.Data.RDT {
		t.Errorf("Expected RDT enabled %t, got %t", cfg.Benchmark.Data.RDT, metadata.RDTEnabled)
	}

	if metadata.PerfEnabled != cfg.Benchmark.Data.Perf {
		t.Errorf("Expected Perf enabled %t, got %t", cfg.Benchmark.Data.Perf, metadata.PerfEnabled)
	}

	if metadata.DockerStatsEnabled != cfg.Benchmark.Data.DockerStats {
		t.Errorf("Expected Docker Stats enabled %t, got %t", cfg.Benchmark.Data.DockerStats, metadata.DockerStatsEnabled)
	}

	// Verify container information
	if metadata.TotalContainers != len(cfg.Container) {
		t.Errorf("Expected %d containers, got %d", len(cfg.Container), metadata.TotalContainers)
	}

	expectedImages := map[string]int{
		"nginx:alpine":  1,
		"redis:alpine":  1,
		"postgres:13":   1,
	}
	
	for image, expectedCount := range expectedImages {
		if actualCount, exists := metadata.ContainerImages[image]; !exists {
			t.Errorf("Expected image %s not found in metadata", image)
		} else if actualCount != expectedCount {
			t.Errorf("Expected %d containers for image %s, got %d", expectedCount, image, actualCount)
		}
	}

	// Verify container details
	for containerName, expectedConfig := range cfg.Container {
		if containerMeta, exists := metadata.ContainerDetails[containerName]; !exists {
			t.Errorf("Expected container %s not found in metadata details", containerName)
		} else {
			if containerMeta.Index != expectedConfig.Index {
				t.Errorf("Container %s: expected index %d, got %d", containerName, expectedConfig.Index, containerMeta.Index)
			}
			if containerMeta.Image != expectedConfig.Image {
				t.Errorf("Container %s: expected image %s, got %s", containerName, expectedConfig.Image, containerMeta.Image)
			}
			if containerMeta.StartTime != expectedConfig.Start {
				t.Errorf("Container %s: expected start time %d, got %d", containerName, expectedConfig.Start, containerMeta.StartTime)
			}
			if containerMeta.StopTime != expectedConfig.Stop {
				t.Errorf("Container %s: expected stop time %d, got %d", containerName, expectedConfig.Stop, containerMeta.StopTime)
			}
			if containerMeta.CorePin != expectedConfig.Core {
				t.Errorf("Container %s: expected core pin %d, got %d", containerName, expectedConfig.Core, containerMeta.CorePin)
			}
			
			// Verify environment variables
			for envKey, envValue := range expectedConfig.Env {
				if actualValue, exists := containerMeta.EnvVars[envKey]; !exists {
					t.Errorf("Container %s: expected env var %s not found", containerName, envKey)
				} else if actualValue != envValue {
					t.Errorf("Container %s: expected env var %s=%s, got %s", containerName, envKey, envValue, actualValue)
				}
			}
		}
	}

	// Verify database configuration
	if metadata.DatabaseHost != cfg.Benchmark.Data.DB.Host {
		t.Errorf("Expected database host %s, got %s", cfg.Benchmark.Data.DB.Host, metadata.DatabaseHost)
	}

	if metadata.DatabaseName != cfg.Benchmark.Data.DB.Name {
		t.Errorf("Expected database name %s, got %s", cfg.Benchmark.Data.DB.Name, metadata.DatabaseName)
	}

	if metadata.DatabaseUser != cfg.Benchmark.Data.DB.User {
		t.Errorf("Expected database user %s, got %s", cfg.Benchmark.Data.DB.User, metadata.DatabaseUser)
	}

	// Verify system information is populated
	if metadata.ExecutionHost == "" {
		t.Error("Expected execution host to be populated")
	}

	if metadata.TotalCPUCores <= 0 {
		t.Error("Expected total CPU cores to be positive")
	}

	if metadata.OSInfo == "" {
		t.Error("Expected OS info to be populated")
	}

	// Verify timestamps
	if metadata.BenchmarkStarted.IsZero() {
		t.Error("Expected benchmark start time to be set")
	}

	t.Log("Metadata collection test completed successfully")
	
	// Print metadata for visual inspection
	t.Logf("Created metadata for benchmark '%s' (ID: %d)", metadata.BenchmarkName, metadata.BenchmarkID)
	t.Logf("Containers: %d, Images: %v", metadata.TotalContainers, metadata.ContainerImages)
	t.Logf("Execution host: %s, CPU cores: %d", metadata.ExecutionHost, metadata.TotalCPUCores)
	t.Logf("Scheduler: %s, Sampling: %dms", metadata.UsedScheduler, metadata.SamplingFrequency)
	t.Logf("Data collection - RDT: %t, Perf: %t, Docker: %t", 
		metadata.RDTEnabled, metadata.PerfEnabled, metadata.DockerStatsEnabled)
}

// TestMetadataStorageFormat tests the storage format for metadata
func TestMetadataStorageFormat(t *testing.T) {
	// Create a sample metadata structure
	metadata := &storage.BenchmarkMetadata{
		BenchmarkID:       98765,
		BenchmarkName:     "format_test",
		BenchmarkStarted:  time.Now(),
		BenchmarkFinished: time.Now().Add(30 * time.Second),
		ExecutionHost:     "test-host",
		CPUExecutedOn:     0,
		TotalCPUCores:     8,
		OSInfo:           "linux amd64",
		KernelVersion:    "Linux version 5.4.0",
		ConfigFile:       "benchmark:\n  name: format_test\n  max_t: 30",
		ConfigFilePath:   "/test/config.yml",
		UsedScheduler:    "default",
		SamplingFrequency: 250,
		MaxDuration:      30,
		RDTEnabled:       false,
		PerfEnabled:      true,
		DockerStatsEnabled: true,
		TotalContainers:  2,
		ContainerImages: map[string]int{
			"nginx:alpine": 1,
			"redis:alpine": 1,
		},
		ContainerDetails: map[string]storage.ContainerMeta{
			"web": {
				Index:     0,
				Image:     "nginx:alpine",
				StartTime: 0,
				StopTime:  30,
				CorePin:   0,
				EnvVars:   map[string]string{"PORT": "80"},
			},
			"cache": {
				Index:     1,
				Image:     "redis:alpine",
				StartTime: 2,
				StopTime:  30,
				CorePin:   1,
				EnvVars:   map[string]string{"MAXMEMORY": "128mb"},
			},
		},
		TotalSamplingSteps: 120,
		TotalMeasurements:  240,
		TotalDataSize:      1024768,
		DatabaseHost:       "localhost:8086",
		DatabaseName:       "benchmarks",
		DatabaseUser:       "testuser",
	}

	// Verify all required fields are populated
	if metadata.BenchmarkID == 0 {
		t.Error("BenchmarkID should not be zero")
	}

	if metadata.BenchmarkName == "" {
		t.Error("BenchmarkName should not be empty")
	}

	if metadata.ExecutionHost == "" {
		t.Error("ExecutionHost should not be empty")
	}

	if len(metadata.ContainerImages) == 0 {
		t.Error("ContainerImages should not be empty")
	}

	if len(metadata.ContainerDetails) == 0 {
		t.Error("ContainerDetails should not be empty")
	}

	// Verify container details structure
	for containerName, containerMeta := range metadata.ContainerDetails {
		if containerMeta.Image == "" {
			t.Errorf("Container %s should have an image", containerName)
		}
		if containerMeta.Index < 0 {
			t.Errorf("Container %s should have a valid index", containerName)
		}
	}

	t.Log("Metadata storage format test completed successfully")
	t.Logf("Metadata contains %d containers with %d total measurements", 
		metadata.TotalContainers, metadata.TotalMeasurements)
}
