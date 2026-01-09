package config

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
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

	// Support importing workload templates from external YAML files/directories.
	// This must happen before unmarshalling into BenchmarkConfig because `workloads.input` is not a WorkloadConfig.
	if rewritten, err := expandWorkloadsInput(expanded, filepath); err != nil {
		return nil, "", err
	} else {
		expanded = rewritten
	}

	var config BenchmarkConfig
	if err := yaml.Unmarshal([]byte(expanded), &config); err != nil {
		logger.WithField("filepath", filepath).WithError(err).Error("Failed to parse config file")
		return nil, "", err
	}

	// Generated trace mode: expand workloads into concrete containers.
	orderedKeys := make([]string, 0)
	if config.Arrival != nil && len(config.Workloads) > 0 {
		generated, order, err := expandGeneratedTrace(&config)
		if err != nil {
			return nil, "", fmt.Errorf("failed to expand generated trace: %w", err)
		}
		config.Containers = generated
		orderedKeys = order
	} else {
		// Static mode: derive container key order from YAML, so indices are assigned deterministically.
		orderedKeys = getContainerKeysInYAMLOrder(expanded)
		if len(orderedKeys) == 0 {
			orderedKeys = make([]string, 0, len(config.Containers))
			for k := range config.Containers {
				orderedKeys = append(orderedKeys, k)
			}
			sort.Strings(orderedKeys)
		}
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

		// Default num_cores to 1 when not set (only used when Core is empty)
		if container.NumCores <= 0 {
			container.NumCores = 1
		}

		// Parse CPU cores from the Core string
		if container.Core != "" {
			cpus, err := ParseCPUSpec(container.Core)
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

func expandWorkloadsInput(expandedYAML string, configPath string) (string, error) {
	baseDir := filepath.Dir(configPath)

	var root yaml.Node
	if err := yaml.Unmarshal([]byte(expandedYAML), &root); err != nil {
		return expandedYAML, nil
	}
	if len(root.Content) == 0 {
		return expandedYAML, nil
	}
	m := root.Content[0]
	if m == nil || m.Kind != yaml.MappingNode {
		return expandedYAML, nil
	}

	var workloadsNode *yaml.Node
	for i := 0; i+1 < len(m.Content); i += 2 {
		k := m.Content[i]
		v := m.Content[i+1]
		if k != nil && k.Value == "workloads" {
			workloadsNode = v
			break
		}
	}
	if workloadsNode == nil {
		return expandedYAML, nil
	}
	if workloadsNode.Kind != yaml.MappingNode {
		return expandedYAML, nil
	}

	inputPaths, cleaned, err := extractAndRemoveWorkloadsInput(workloadsNode)
	if err != nil {
		return "", err
	}
	if !cleaned || len(inputPaths) == 0 {
		return expandedYAML, nil
	}

	imported, err := loadWorkloadsFromInputs(inputPaths, baseDir)
	if err != nil {
		return "", err
	}
	mergeWorkloadsIntoNode(workloadsNode, imported)

	out, err := yaml.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func extractAndRemoveWorkloadsInput(workloadsNode *yaml.Node) ([]string, bool, error) {
	if workloadsNode == nil || workloadsNode.Kind != yaml.MappingNode {
		return nil, false, nil
	}

	paths := make([]string, 0)
	removed := false
	newContent := make([]*yaml.Node, 0, len(workloadsNode.Content))
	for i := 0; i+1 < len(workloadsNode.Content); i += 2 {
		k := workloadsNode.Content[i]
		v := workloadsNode.Content[i+1]
		if k != nil && k.Value == "input" {
			removed = true
			switch v.Kind {
			case yaml.ScalarNode:
				if strings.TrimSpace(v.Value) != "" {
					paths = append(paths, v.Value)
				}
			case yaml.SequenceNode:
				for _, item := range v.Content {
					if item == nil || item.Kind != yaml.ScalarNode {
						return nil, false, fmt.Errorf("workloads.input must be a string or list of strings")
					}
					if strings.TrimSpace(item.Value) != "" {
						paths = append(paths, item.Value)
					}
				}
			default:
				return nil, false, fmt.Errorf("workloads.input must be a string or list of strings")
			}
			continue
		}
		newContent = append(newContent, k, v)
	}
	if removed {
		workloadsNode.Content = newContent
	}
	return paths, removed, nil
}

func loadWorkloadsFromInputs(inputs []string, baseDir string) (map[string]WorkloadConfig, error) {
	files, err := resolveWorkloadInputFiles(inputs, baseDir)
	if err != nil {
		return nil, err
	}

	merged := make(map[string]WorkloadConfig)
	for _, filePath := range files {
		b, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read workloads file %q: %w", filePath, err)
		}
		content := expandEnvVars(string(b))

		// Support both a raw map (key -> WorkloadConfig) and a wrapped {workloads: ...} file.
		var wrapped struct {
			Workloads map[string]WorkloadConfig `yaml:"workloads"`
		}
		_ = yaml.Unmarshal([]byte(content), &wrapped)
		var m map[string]WorkloadConfig
		if len(wrapped.Workloads) > 0 {
			m = wrapped.Workloads
		} else {
			if err := yaml.Unmarshal([]byte(content), &m); err != nil {
				return nil, fmt.Errorf("failed to parse workloads file %q: %w", filePath, err)
			}
		}

		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if _, exists := merged[k]; exists {
				return nil, fmt.Errorf("duplicate workload key %q while importing %q", k, filePath)
			}
			merged[k] = m[k]
		}
	}
	return merged, nil
}

func resolveWorkloadInputFiles(inputs []string, baseDir string) ([]string, error) {
	all := make([]string, 0)
	seen := make(map[string]struct{})

	for _, in := range inputs {
		p := strings.TrimSpace(in)
		if p == "" {
			continue
		}
		if !filepath.IsAbs(p) {
			p = filepath.Join(baseDir, p)
		}
		p = filepath.Clean(p)

		st, err := os.Stat(p)
		if err != nil {
			return nil, fmt.Errorf("workloads.input path %q not found: %w", p, err)
		}

		if st.IsDir() {
			var dirFiles []string
			err := filepath.WalkDir(p, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if d == nil || d.IsDir() {
					return nil
				}
				ext := strings.ToLower(filepath.Ext(d.Name()))
				if ext != ".yml" && ext != ".yaml" {
					return nil
				}
				dirFiles = append(dirFiles, path)
				return nil
			})
			if err != nil {
				return nil, fmt.Errorf("failed to list workloads in dir %q: %w", p, err)
			}
			sort.Strings(dirFiles)
			for _, f := range dirFiles {
				if _, ok := seen[f]; ok {
					continue
				}
				seen[f] = struct{}{}
				all = append(all, f)
			}
			continue
		}

		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		all = append(all, p)
	}

	sort.Strings(all)
	return all, nil
}

func mergeWorkloadsIntoNode(workloadsNode *yaml.Node, imported map[string]WorkloadConfig) {
	if workloadsNode == nil || workloadsNode.Kind != yaml.MappingNode || len(imported) == 0 {
		return
	}

	existing := make(map[string]struct{})
	for i := 0; i+1 < len(workloadsNode.Content); i += 2 {
		k := workloadsNode.Content[i]
		if k != nil {
			existing[k.Value] = struct{}{}
		}
	}

	keys := make([]string, 0, len(imported))
	for k := range imported {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if _, ok := existing[k]; ok {
			// Inline workloads override imported ones.
			continue
		}
		wn := yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: k}

		b, err := yaml.Marshal(imported[k])
		if err != nil {
			continue
		}
		var doc yaml.Node
		if err := yaml.Unmarshal(b, &doc); err != nil {
			continue
		}
		val := doc.Content[0]
		workloadsNode.Content = append(workloadsNode.Content, &wn, val)
		existing[k] = struct{}{}
	}
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
		if key == "" {
			continue
		}
		// Reserved top-level keys (non-container definitions)
		switch key {
		case "benchmark", "arrival", "data", "workloads":
			continue
		}
		keys = append(keys, key)
	}
	return keys
}

func expandGeneratedTrace(cfg *BenchmarkConfig) (map[string]ContainerConfig, []string, error) {
	if cfg == nil || cfg.Arrival == nil {
		return nil, nil, fmt.Errorf("arrival config missing")
	}
	if len(cfg.Workloads) == 0 {
		return nil, nil, fmt.Errorf("workloads pool is empty")
	}
	if cfg.Data == nil {
		return nil, nil, fmt.Errorf("top-level data defaults are required in generated-trace mode")
	}
	if cfg.Data.Frequency <= 0 {
		return nil, nil, fmt.Errorf("top-level data.frequency must be > 0")
	}
	if cfg.Arrival.Mean <= 0 {
		return nil, nil, fmt.Errorf("arrival.mean must be > 0")
	}
	if cfg.Arrival.Sigma < 0 {
		return nil, nil, fmt.Errorf("arrival.sigma must be >= 0")
	}

	length := cfg.Arrival.Length
	if length == nil {
		length = &NormalDistConfig{Mean: 30, Sigma: 10, Min: 1}
	}
	if length.Mean <= 0 {
		return nil, nil, fmt.Errorf("arrival.length.mean must be > 0")
	}
	if length.Sigma < 0 {
		return nil, nil, fmt.Errorf("arrival.length.sigma must be >= 0")
	}

	// Prefer correctly spelled sensitivities if present.
	sens := cfg.Arrival.Sensitivities
	if len(sens.Weights) == 0 && cfg.Arrival.Sensetivities.Weights != nil {
		sens = cfg.Arrival.Sensetivities
	}

	rng := rand.New(rand.NewSource(cfg.Arrival.Seed))
	containers := make(map[string]ContainerConfig)
	order := make([]string, 0)

	// Build template list for selection.
	templateKeys := make([]string, 0, len(cfg.Workloads))
	for k := range cfg.Workloads {
		templateKeys = append(templateKeys, k)
	}
	sort.Strings(templateKeys)

	chooseFromWeights := func(ws WeightedSet) (string, bool) {
		if ws.Random || len(ws.Weights) == 0 {
			return "", false
		}
		// deterministic iteration order
		keys := make([]string, 0, len(ws.Weights))
		for k := range ws.Weights {
			if k == "" {
				continue
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		total := 0.0
		for _, k := range keys {
			w := ws.Weights[k]
			if w > 0 {
				total += w
			}
		}
		if total <= 0 {
			return "", false
		}
		r := rng.Float64() * total
		acc := 0.0
		for _, k := range keys {
			w := ws.Weights[k]
			if w <= 0 {
				continue
			}
			acc += w
			if r <= acc {
				return k, true
			}
		}
		return keys[len(keys)-1], true
	}

	sampleNormalClamped := func(mean, sigma, min, max float64) float64 {
		v := mean
		if sigma > 0 {
			v = mean + sigma*rng.NormFloat64()
		}
		if min != 0 && v < min {
			v = min
		}
		if max != 0 && v > max {
			v = max
		}
		if v < 0 {
			v = 0
		}
		return v
	}

	// First job at t=0, then sample inter-arrival deltas.
	// We work in integer seconds to match ContainerConfig start_t granularity.
	// Important: advance by integer-rounded deltas (not rounding the cumulative time),
	// otherwise rounding artifacts can create duplicate start_t values and distort the distribution.
	startSec := 0
	jobID := 1
	for {
		if startSec >= cfg.Benchmark.MaxT {
			break
		}

		kind := ""
		if v, ok := chooseFromWeights(cfg.Arrival.Split); ok {
			kind = v
		}
		sensitivity := ""
		if v, ok := chooseFromWeights(sens); ok {
			sensitivity = v
		}

		// Pick a workload template matching (kind, sensitivity) when possible.
		candidates := make([]string, 0)
		for _, k := range templateKeys {
			w := cfg.Workloads[k]
			if w.Image == "" || w.Command == "" {
				continue
			}
			if kind != "" && w.Kind != "" && w.Kind != kind {
				continue
			}
			if sensitivity != "" && w.Sensitivity != "" && w.Sensitivity != sensitivity {
				continue
			}
			candidates = append(candidates, k)
		}
		if len(candidates) == 0 {
			// Relax constraints.
			for _, k := range templateKeys {
				w := cfg.Workloads[k]
				if w.Image == "" || w.Command == "" {
					continue
				}
				candidates = append(candidates, k)
			}
		}
		if len(candidates) == 0 {
			return nil, nil, fmt.Errorf("no valid workloads (missing image/command)")
		}
		picked := candidates[rng.Intn(len(candidates))]
		w := cfg.Workloads[picked]

		cores := w.NumCores
		if cores <= 0 {
			cores = 1
		}
		baseLen := sampleNormalClamped(length.Mean, length.Sigma, length.Min, length.Max)
		durSec := int(math.Round(baseLen))
		if durSec < 1 {
			durSec = 1
		}

		stopSec := startSec + durSec
		if !cfg.Benchmark.Drain {
			if stopSec > cfg.Benchmark.MaxT {
				stopSec = cfg.Benchmark.MaxT
			}
		}
		if stopSec <= startSec {
			break
		}
		expectedSec := stopSec - startSec

		key := fmt.Sprintf("%06d-job-%s", jobID, picked)
		jobID++
		st := startSec
		sp := stopSec
		ex := expectedSec
		c := ContainerConfig{
			Name:      key,
			Image:     w.Image,
			Command:   w.Command,
			NumCores:  cores,
			Critical:  w.Critical,
			IPC:       w.IPC,
			IPCEfficiency: w.IPCEfficiency,
			StartT:    &st,
			StopT:     &sp,
			ExpectedT: &ex,
			Data:      *cfg.Data,
		}
		containers[key] = c
		order = append(order, key)

		// Next arrival (inter-arrival time in seconds).
		delta := sampleNormalClamped(cfg.Arrival.Mean, cfg.Arrival.Sigma, 0, 0)
		deltaSec := int(math.Round(delta))
		if deltaSec < 1 {
			deltaSec = 1
		}
		startSec += deltaSec
		// Avoid pathological huge job counts.
		if jobID > 100000 {
			return nil, nil, fmt.Errorf("generated too many jobs; check arrival parameters")
		}
	}

	// Ensure container order is stable by start time then key.
	sort.SliceStable(order, func(i, j int) bool {
		a := containers[order[i]]
		b := containers[order[j]]
		as := a.GetStartSeconds()
		bs := b.GetStartSeconds()
		if as != bs {
			return as < bs
		}
		return order[i] < order[j]
	})

	return containers, order, nil
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
func ParseCPUSpec(spec string) ([]int, error) {
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

func FormatCPUSpec(cpus []int) string {
	if len(cpus) == 0 {
		return ""
	}
	vals := append([]int(nil), cpus...)
	sort.Ints(vals)
	parts := make([]string, 0, len(vals))
	for _, cpu := range vals {
		parts = append(parts, strconv.Itoa(cpu))
	}
	return strings.Join(parts, ",")
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
		if !config.Benchmark.Drain {
			if stopT > config.Benchmark.MaxT {
				return fmt.Errorf("container %s: stop_t (%d) must be <= max_t (%d)", name, stopT, config.Benchmark.MaxT)
			}
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

		// Validate CPU request: either explicit Core or requested NumCores
		if container.Core == "" {
			if container.NumCores <= 0 {
				return fmt.Errorf("container %s: num_cores must be >= 1", name)
			}
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
