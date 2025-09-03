package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	testConfig := `
benchmark:
  name: test_benchmark
  max_t: 300
  log_level: info
  scheduler:
    implementation: default
    rdt: false
  data:
    profilefrequency: 100
    db:
      host: localhost:8086
      name: test_db
      user: test_user
      password: test_pass
    rdt: false
    perf: true
    dockerstats: true
  docker:
    auth:
      registry: ""
      username: ""
      password: ""

container0:
  index: 0
  image: nginx:alpine
  port: "80:8080"
  start: 0
  stop: -1
  core: 1
`

	// Write test config to temporary file
	tmpFile, err := os.CreateTemp("", "test_config_*.yml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(testConfig); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}
	tmpFile.Close()

	// Load the config
	config, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Validate loaded config
	if config.Benchmark.Name != "test_benchmark" {
		t.Errorf("Expected benchmark name 'test_benchmark', got '%s'", config.Benchmark.Name)
	}

	if config.Benchmark.MaxT != 300 {
		t.Errorf("Expected max_t 300, got %d", config.Benchmark.MaxT)
	}

	if config.Benchmark.LogLevel != "info" {
		t.Errorf("Expected log_level 'info', got '%s'", config.Benchmark.LogLevel)
	}

	if len(config.Container) != 1 {
		t.Errorf("Expected 1 container, got %d", len(config.Container))
	}

	if container, exists := config.Container["container0"]; exists {
		if container.Index != 0 {
			t.Errorf("Expected container index 0, got %d", container.Index)
		}
		if container.Image != "nginx:alpine" {
			t.Errorf("Expected image 'nginx:alpine', got '%s'", container.Image)
		}
		if container.Start != 0 {
			t.Errorf("Expected start time 0, got %d", container.Start)
		}
		if container.Stop != -1 {
			t.Errorf("Expected stop time -1, got %d", container.Stop)
		}
	} else {
		t.Error("container0 not found in loaded config")
	}
}

func TestLoadConfigWithInvalidFile(t *testing.T) {
	_, err := LoadConfig("non_existent_file.yml")
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
	}{
		{
			name: "valid config",
			config: Config{
				Benchmark: BenchmarkConfig{
					Name:     "test",
					LogLevel: "info",
					Data: DataConfig{
						ProfileFrequency: 100,
						DB: DBConfig{
							Host: "localhost:8086",
							Name: "test_db",
						},
					},
				},
				Container: map[string]ContainerConfig{
					"test_container": {
						Index: 0,
						Image: "nginx:alpine",
						Start: 0,
						Stop:  -1,
					},
				},
			},
			expectError: false,
		},
		{
			name: "missing benchmark name",
			config: Config{
				Benchmark: BenchmarkConfig{
					LogLevel: "info",
				},
				Container: map[string]ContainerConfig{
					"test_container": {
						Index: 0,
						Image: "nginx:alpine",
						Start: 0,
						Stop:  -1,
					},
				},
			},
			expectError: true,
		},
		{
			name: "invalid log level",
			config: Config{
				Benchmark: BenchmarkConfig{
					Name:     "test",
					LogLevel: "invalid",
				},
			},
			expectError: true,
		},
		{
			name: "negative profile frequency",
			config: Config{
				Benchmark: BenchmarkConfig{
					Name:     "test",
					LogLevel: "info",
					Data: DataConfig{
						ProfileFrequency: -1,
					},
				},
			},
			expectError: true,
		},
		{
			name: "no containers",
			config: Config{
				Benchmark: BenchmarkConfig{
					Name:     "test",
					LogLevel: "info",
					Data: DataConfig{
						ProfileFrequency: 100,
						DB: DBConfig{
							Host: "localhost:8086",
							Name: "test_db",
						},
					},
				},
				Container: map[string]ContainerConfig{},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConfig(&tt.config)
			if tt.expectError && err == nil {
				t.Error("Expected validation error, got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no validation error, got: %v", err)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	testConfig := `
benchmark:
  name: test_benchmark
  max_t: 300
  log_level: info
  data:
    profilefrequency: 100
    db:
      host: localhost:8086
      name: test_db
      user: test_user
      password: test_pass
    dockerstats: true
  docker:
    auth:
      registry: ""
      username: ""
      password: ""

container0:
  index: 0
  image: nginx:alpine
  start: 0
  stop: -1
`

	tmpFile, err := os.CreateTemp("", "test_config_defaults_*.yml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(testConfig); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}
	tmpFile.Close()

	config, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Check that defaults are applied
	if config.Benchmark.Scheduler.Implementation != "default" {
		t.Errorf("Expected default scheduler implementation 'default', got '%s'", config.Benchmark.Scheduler.Implementation)
	}
}

func BenchmarkLoadConfig(b *testing.B) {
	testConfig := `
benchmark:
  name: benchmark_test
  max_t: 300
  log_level: info
  scheduler:
    implementation: default
    rdt: false
  data:
    profilefrequency: 100
    db:
      host: localhost:8086
      name: test_db
      user: test_user
      password: test_pass
    dockerstats: true
  docker:
    auth:
      registry: ""
      username: ""
      password: ""

container0:
  index: 0
  image: nginx:alpine
  start: 0
  stop: -1
`

	tmpFile, err := os.CreateTemp("", "benchmark_config_*.yml")
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(testConfig); err != nil {
		b.Fatalf("Failed to write test config: %v", err)
	}
	tmpFile.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := LoadConfig(tmpFile.Name())
		if err != nil {
			b.Fatalf("LoadConfig failed: %v", err)
		}
	}
}
