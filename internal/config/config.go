package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

// Config represents the complete benchmark configuration
type Config struct {
	Benchmark BenchmarkConfig            `yaml:"benchmark"`
	Container map[string]ContainerConfig `yaml:",inline"`
}

type BenchmarkConfig struct {
	Name        string          `yaml:"name"`
	Description string          `yaml:"description,omitempty"`
	MaxT        int             `yaml:"max_t"`        
	LogLevel    string          `yaml:"log_level"`
	Scheduler   SchedulerConfig `yaml:"scheduler"`
	Data        DataConfig      `yaml:"data"`
	Docker      DockerConfig    `yaml:"docker"`
}

type SchedulerConfig struct {
	Implementation string `yaml:"implementation"`
	RDT            bool   `yaml:"rdt"`
}

type DataConfig struct {
	ProfileFrequency int        `yaml:"profilefrequency"`
	DB               DBConfig   `yaml:"db"`
	RDT              bool       `yaml:"rdt"`
	Perf             bool       `yaml:"perf"`
	DockerStats      bool       `yaml:"dockerstats"`
}

type DBConfig struct {
	Host     string `yaml:"host"`
	Name     string `yaml:"name"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Org      string `yaml:"org,omitempty"` // InfluxDB 2.x organization
}

type DockerConfig struct {
	Auth AuthConfig `yaml:"auth"`
}

type AuthConfig struct {
	Registry string `yaml:"registry"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type ContainerConfig struct {
	Index   int               `yaml:"index"`
	Image   string            `yaml:"image"`
	Port    string            `yaml:"port,omitempty"`
	Start   int               `yaml:"start"` // start time in seconds
	Stop    int               `yaml:"stop"`  // stop time in seconds, -1 for manual stop
	Core    int               `yaml:"core,omitempty"` // CPU core to pin to
	Env     map[string]string `yaml:"env,omitempty"` // Environment variables
	Command string            `yaml:"command,omitempty"` // Optional command to execute in container
}

// LoadConfig loads and validates the benchmark configuration from a YAML file
func LoadConfig(filename string) (*Config, error) {
	configDir := filepath.Dir(filename)
	
	// Try multiple locations for .env file
	envPaths := []string{
		filepath.Join(configDir, "..", ".env"), // examples/../.env (from examples to project root)
		filepath.Join("..", ".env"),            // ../env (from driver to project root)  
		".env",                                 // current directory
	}
	
	var envLoaded bool
	for _, envPath := range envPaths {
		if _, err := os.Stat(envPath); err == nil {
			if err := godotenv.Load(envPath); err != nil {
				return nil, fmt.Errorf("failed to load .env file: %w", err)
			}
			envLoaded = true
			break
		}
	}
	
	if !envLoaded {
		fmt.Printf("Warning: No .env file found (this is optional)\n")
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Substitute environment variables in the YAML content
	expandedData := expandEnvVars(string(data))

	var config Config
	if err := yaml.Unmarshal([]byte(expandedData), &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &config, nil
}

// expandEnvVars expands environment variables in the format ${VAR_NAME} or $VAR_NAME
func expandEnvVars(content string) string {
	// Replace ${VAR_NAME} format
	re1 := regexp.MustCompile(`\$\{([^}]+)\}`)
	content = re1.ReplaceAllStringFunc(content, func(match string) string {
		varName := strings.TrimPrefix(strings.TrimSuffix(match, "}"), "${")
		if value := os.Getenv(varName); value != "" {
			return value
		}
		return match // Keep original if env var not found
	})

	// Replace $VAR_NAME format (word boundary)
	re2 := regexp.MustCompile(`\$([A-Z_][A-Z0-9_]*)\b`)
	content = re2.ReplaceAllStringFunc(content, func(match string) string {
		varName := strings.TrimPrefix(match, "$")
		if value := os.Getenv(varName); value != "" {
			return value
		}
		return match // Keep original if env var not found
	})

	return content
}

// validateConfig performs basic validation on the configuration
func validateConfig(config *Config) error {
	// Validate benchmark name
	if config.Benchmark.Name == "" {
		return fmt.Errorf("benchmark name is required")
	}

	// Validate log level
	validLogLevels := map[string]bool{
		"trace": true, "debug": true, "info": true,
		"warn": true, "error": true, "fatal": true, "panic": true,
	}
	if !validLogLevels[config.Benchmark.LogLevel] {
		return fmt.Errorf("invalid log level: %s", config.Benchmark.LogLevel)
	}

	// Validate scheduler implementation
	if config.Benchmark.Scheduler.Implementation == "" {
		config.Benchmark.Scheduler.Implementation = "default"
	}

	// Validate data collection settings
	if config.Benchmark.Data.ProfileFrequency <= 0 {
		return fmt.Errorf("profile frequency must be positive")
	}

	// Validate database settings
	if config.Benchmark.Data.DB.Host == "" {
		return fmt.Errorf("database host is required")
	}
	if config.Benchmark.Data.DB.Name == "" {
		return fmt.Errorf("database name is required")
	}

	// Validate containers
	if len(config.Container) == 0 {
		return fmt.Errorf("at least one container must be defined")
	}

	for name, container := range config.Container {
		if container.Image == "" {
			return fmt.Errorf("container %s: image is required", name)
		}
		if container.Start < 0 {
			return fmt.Errorf("container %s: start time cannot be negative", name)
		}
		if container.Stop != -1 && container.Stop < container.Start {
			return fmt.Errorf("container %s: stop time must be after start time or -1", name)
		}
	}

	return nil
}
