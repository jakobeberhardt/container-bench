package config

import (
	"fmt"
	"os"
	"regexp"
	"sort"
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

	// Derive container key order from YAML, so indices are assigned deterministically
	orderedKeys := getContainerKeysInYAMLOrder(expanded)
	if len(orderedKeys) == 0 {
		orderedKeys = make([]string, 0, len(config.Containers))
		for k := range config.Containers {
			orderedKeys = append(orderedKeys, k)
		}
		sort.Strings(orderedKeys)
	}

	// Auto-assign indices 0..N-1 (ignore any explicit index mapping)
	for i, keyName := range orderedKeys {
		container, ok := config.Containers[keyName]
		if !ok {
			continue
		}
		container.Index = i
		config.Containers[keyName] = container
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

func getContainerKeysInYAMLOrder(expandedYAML string) []string {
	var root yaml.Node
	if err := yaml.Unmarshal([]byte(expandedYAML), &root); err != nil {
		return nil
	}
	if len(root.Content) == 0 {
		return nil
	}
	m := root.Content[0]
	if m == nil || m.Kind != yaml.MappingNode {
		return nil
	}

	keys := make([]string, 0)
	for i := 0; i+1 < len(m.Content); i += 2 {
		k := m.Content[i]
		if k == nil {
			continue
		}
		key := k.Value
		if key == "" || key == "benchmark" {
			continue
		}
		keys = append(keys, key)
	}
	return keys
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

		startT := container.GetStartSeconds()
		stopT := container.GetStopSeconds(config.Benchmark.MaxT)

		if startT < 0 {
			return fmt.Errorf("container %s: start_t must be >= 0", name)
		}
		if stopT <= 0 {
			return fmt.Errorf("container %s: stop_t must be > 0", name)
		}
		if stopT > config.Benchmark.MaxT {
			return fmt.Errorf("container %s: stop_t (%d) must be <= max_t (%d)", name, stopT, config.Benchmark.MaxT)
		}
		if startT >= stopT {
			return fmt.Errorf("container %s: start_t (%d) must be < stop_t (%d)", name, startT, stopT)
		}
		if expectedT, ok := container.GetExpectedSeconds(); ok {
			if expectedT < 0 {
				return fmt.Errorf("container %s: expected_t must be >= 0", name)
			}
			if expectedT > (stopT - startT) {
				return fmt.Errorf("container %s: expected_t (%d) must be <= (stop_t-start_t) (%d)", name, expectedT, stopT-startT)
			}
		}

		if container.Data.Frequency <= 0 {
			return fmt.Errorf("container %s: frequency must be greater than 0", name)
		}

		// Check if at least one collector is enabled
		perfEnabled := container.Data.GetPerfConfig() != nil
		dockerEnabled := container.Data.GetDockerConfig() != nil
		rdtEnabled := container.Data.GetRDTConfig() != nil

		if !perfEnabled && !dockerEnabled && !rdtEnabled {
			return fmt.Errorf("container %s: at least one data collection method must be enabled", name)
		}

		if indices[container.Index] {
			return fmt.Errorf("container %s: internal index %d is already used", name, container.Index)
		}
		indices[container.Index] = true
	}

	return nil
}
