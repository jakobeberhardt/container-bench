package config

import (
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
}

type SchedulerConfig struct {
	Implementation string `yaml:"implementation"`
	RDT           bool   `yaml:"rdt"`
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
