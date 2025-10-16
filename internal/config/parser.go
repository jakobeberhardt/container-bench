package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"container-bench/internal/logging"

	"gopkg.in/yaml.v3"
)

func LoadConfig(filepath string) (*BenchmarkConfig, error) {
	config, _, err := LoadConfigWithContent(filepath)
	return config, err
}

func LoadConfigWithContent(filepath string) (*BenchmarkConfig, string, error) {
	logger := logging.GetLogger()

	data, err := os.ReadFile(filepath)
	if err != nil {
		logger.WithField("filepath", filepath).WithError(err).Error("Failed to read config file")
		return nil, "", err
	}

	originalContent := string(data)

	// Expand environment variables
	expanded := expandEnvVars(originalContent)

	var config BenchmarkConfig
	if err := yaml.Unmarshal([]byte(expanded), &config); err != nil {
		logger.WithField("filepath", filepath).WithError(err).Error("Failed to parse config file")
		return nil, "", err
	}

	// Set KeyName field for each container based on the YAML key
	// and parse CPU cores
	for keyName, container := range config.Containers {
		container.KeyName = keyName

		// Parse CPU cores from the Core string
		if container.Core != "" {
			cpus, err := parseCPUSpec(container.Core)
			if err != nil {
				logger.WithField("container", keyName).WithField("core_spec", container.Core).WithError(err).Error("Failed to parse CPU specification")
				return nil, "", fmt.Errorf("container %s: invalid CPU specification '%s': %w", keyName, container.Core, err)
			}
			container.CPUCores = cpus
		}

		config.Containers[keyName] = container
	}

	if err := validateConfig(&config); err != nil {
		return nil, "", fmt.Errorf("invalid config: %w", err)
	}

	return &config, originalContent, nil
}

func expandEnvVars(content string) string {
	re := regexp.MustCompile(`\$\{([^}]+)\}`)
	return re.ReplaceAllStringFunc(content, func(match string) string {
		envVar := strings.Trim(match, "${}")
		if value := os.Getenv(envVar); value != "" {
			return value
		}
		return match
	})
}

// CPU specification strings like "0", "0,2,4", or "0-3"
func parseCPUSpec(spec string) ([]int, error) {
	var cpus []int
	seen := make(map[int]bool)

	parts := strings.Split(spec, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) != 2 {
				return nil, fmt.Errorf("invalid CPU range: %s", part)
			}

			start, err := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
			if err != nil {
				return nil, fmt.Errorf("invalid CPU range start: %s", rangeParts[0])
			}

			end, err := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
			if err != nil {
				return nil, fmt.Errorf("invalid CPU range end: %s", rangeParts[1])
			}

			if start > end {
				return nil, fmt.Errorf("invalid CPU range: start > end (%d > %d)", start, end)
			}

			for i := start; i <= end; i++ {
				if !seen[i] {
					cpus = append(cpus, i)
					seen[i] = true
				}
			}
		} else {
			cpu, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid CPU number: %s", part)
			}

			if !seen[cpu] {
				cpus = append(cpus, cpu)
				seen[cpu] = true
			}
		}
	}

	if len(cpus) == 0 {
		return nil, fmt.Errorf("no CPUs specified")
	}

	return cpus, nil
}

func validateConfig(config *BenchmarkConfig) error {
	if config.Benchmark.Name == "" {
		return fmt.Errorf("benchmark name is required")
	}

	if config.Benchmark.MaxT <= 0 {
		return fmt.Errorf("max_t must be greater than 0")
	}

	if len(config.Containers) == 0 {
		return fmt.Errorf("at least one container must be defined")
	}

	// Validate database config
	db := config.Benchmark.Data.DB
	if db.Host == "" || db.Name == "" || db.User == "" || db.Password == "" || db.Org == "" {
		return fmt.Errorf("incomplete database configuration")
	}

	// Validate containers
	indices := make(map[int]bool)
	for name, container := range config.Containers {
		if container.Image == "" {
			return fmt.Errorf("container %s: image is required", name)
		}

		if container.Data.Frequency <= 0 {
			return fmt.Errorf("container %s: frequency must be greater than 0", name)
		}

		if !container.Data.Perf && !container.Data.Docker && !container.Data.RDT {
			return fmt.Errorf("container %s: at least one data collection method must be enabled", name)
		}

		if indices[container.Index] {
			return fmt.Errorf("container %s: index %d is already used", name, container.Index)
		}
		indices[container.Index] = true
	}

	return nil
}
