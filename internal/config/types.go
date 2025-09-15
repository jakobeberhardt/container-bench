package config

import (
	"fmt"
	"os"
	"time"
)

type BenchmarkConfig struct {
	Benchmark  BenchmarkInfo             `yaml:"benchmark"`
	Containers map[string]ContainerConfig `yaml:",inline"`
}

type BenchmarkInfo struct {
	Name        string            `yaml:"name"`
	Description string            `yaml:"description"`
	MaxT        int               `yaml:"max_t"`
	LogLevel    string            `yaml:"log_level"`
	Scheduler   SchedulerConfig   `yaml:"scheduler"`
	Data        DataConfig        `yaml:"data"`
	Docker      *DockerConfig     `yaml:"docker,omitempty"`
}

type DockerConfig struct {
	Registry *RegistryConfig `yaml:"registry,omitempty"`
}

type RegistryConfig struct {
	Host     string `yaml:"host"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type SchedulerConfig struct {
	Implementation string `yaml:"implementation"`
	RDT           bool   `yaml:"rdt"`
	LogLevel      string `yaml:"log_level,omitempty"`
}

type DataConfig struct {
	DB DatabaseConfig `yaml:"db"`
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Name     string `yaml:"name"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Org      string `yaml:"org"`
}

type ContainerConfig struct {
	Index    int             `yaml:"index"`
	Name     string          `yaml:"name,omitempty"`
	KeyName  string          `yaml:"-"` // Store the YAML key name, not serialized
	Image    string          `yaml:"image"`
	Port     string          `yaml:"port,omitempty"`
	Core     int             `yaml:"core"`
	Command  string          `yaml:"command,omitempty"`
	Data     CollectorConfig `yaml:"data"`
}

type CollectorConfig struct {
	Frequency int  `yaml:"frequency"`
	Perf      bool `yaml:"perf"`
	Docker    bool `yaml:"docker"`
	RDT       bool `yaml:"rdt"`
}

func (c *BenchmarkConfig) GetMaxDuration() time.Duration {
	return time.Duration(c.Benchmark.MaxT) * time.Second
}

func (c *BenchmarkConfig) GetRegistryConfig() *RegistryConfig {
	// First check if registry is configured in the YAML file
	if c.Benchmark.Docker != nil && c.Benchmark.Docker.Registry != nil {
		return c.Benchmark.Docker.Registry
	}
	
	// Otherwise, try to get from environment variables (legacy support)
	host := os.Getenv("DOCKER_REGISTRY_HOST")
	username := os.Getenv("DOCKER_REGISTRY_USERNAME")
	password := os.Getenv("DOCKER_REGISTRY_PASSWORD")
	
	if host != "" && username != "" && password != "" {
		return &RegistryConfig{
			Host:     host,
			Username: username,
			Password: password,
		}
	}
	
	return nil
}

func (c *BenchmarkConfig) GetContainersSorted() []ContainerConfig {
	var containers []ContainerConfig
	for _, container := range c.Containers {
		containers = append(containers, container)
	}
	
	// Sort by index
	for i := 0; i < len(containers)-1; i++ {
		for j := i + 1; j < len(containers); j++ {
			if containers[i].Index > containers[j].Index {
				containers[i], containers[j] = containers[j], containers[i]
			}
		}
	}
	
	return containers
}

// returns the effective container name for a given container config
func (c *ContainerConfig) GetContainerName(benchmarkID int) string {
	if c.Name != "" {
		// Use explicit name if specified in yml
		return c.Name
	}
	if c.KeyName != "" {
		// Use the YAML key name directly 
		return c.KeyName
	}
	// Fallback to generated name if neither is available
	return fmt.Sprintf("bench-%d-container-%d", benchmarkID, c.Index)
}
