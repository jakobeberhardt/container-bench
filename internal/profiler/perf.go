package profiler

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jakobeberhardt/container-bench/internal/storage"
	log "github.com/sirupsen/logrus"
)

// PerfData represents buffered perf measurements for a container
type PerfBuffer struct {
	containerName string
	data         map[string]uint64
	timestamp    time.Time
	mutex        sync.RWMutex
}

// PerfCollector collects hardware performance counters using perf with cgroup filtering and interval approach
type PerfCollector struct {
	metadataProvider *MetadataProvider
	
	// Interval-based collection per container
	containerCommands map[string]*exec.Cmd    // container name -> perf command
	containerReaders  map[string]*bufio.Scanner // container name -> output scanner
	buffers           map[string]*PerfBuffer  // container name -> latest perf data
	bufferMutex       sync.RWMutex
	stopChan          chan struct{}
	wg                sync.WaitGroup
	running           bool
}

// NewPerfCollector creates a new perf collector
func NewPerfCollector(metadataProvider *MetadataProvider) *PerfCollector {
	return &PerfCollector{
		metadataProvider:  metadataProvider,
		containerCommands: make(map[string]*exec.Cmd),
		containerReaders:  make(map[string]*bufio.Scanner),
		buffers:           make(map[string]*PerfBuffer),
		stopChan:          make(chan struct{}),
	}
}

// SetContainerIDs sets the container IDs for monitoring
func (p *PerfCollector) SetContainerIDs(containerIDs map[string]string) {
	// Initialize buffers for each container
	p.bufferMutex.Lock()
	defer p.bufferMutex.Unlock()
	
	for containerName := range containerIDs {
		p.buffers[containerName] = &PerfBuffer{
			containerName: containerName,
			data:         make(map[string]uint64),
		}
	}
}

// SetSamplingRate sets the sampling rate from configuration (deprecated - now handled by metadata provider)
func (p *PerfCollector) SetSamplingRate(rateMs int) {
	// This method is kept for backward compatibility but no longer used
	// The sampling rate is now managed by the metadata provider
}

// Initialize initializes the perf collector and starts interval-based collection per container
func (p *PerfCollector) Initialize(ctx context.Context) error {
	// Check if perf is available
	if _, err := exec.LookPath("perf"); err != nil {
		return fmt.Errorf("perf command not found: %w", err)
	}

	// Get configuration from metadata provider
	config := p.metadataProvider.GetConfig()
	
	log.WithFields(log.Fields{
		"config_containers": len(config.ContainerMetadata),
		"config_keys": func() []string {
			keys := make([]string, 0, len(config.ContainerMetadata))
			for k := range config.ContainerMetadata {
				keys = append(keys, k)
			}
			return keys
		}(),
	}).Debug("Perf collector initializing with container metadata")
	
	// Initialize buffers for each container
	p.bufferMutex.Lock()
	for containerName := range config.ContainerMetadata {
		p.buffers[containerName] = &PerfBuffer{
			containerName: containerName,
			data:         make(map[string]uint64),
		}
		log.WithField("container", containerName).Debug("Initialized perf buffer for container")
	}
	p.bufferMutex.Unlock()

	// Start interval-based collection for each container
	p.running = true
	for containerName := range config.ContainerMetadata {
		log.WithField("container", containerName).Debug("Starting perf collection goroutine for container")
		p.wg.Add(1)
		go p.startContainerCollection(ctx, containerName)
	}

	log.WithField("sampling_rate_ms", config.SamplingRate).Info("Perf collector initialized with interval-based collection per container")
	return nil
}

// startContainerCollection starts continuous perf collection for a single container using interval approach
func (p *PerfCollector) startContainerCollection(ctx context.Context, containerName string) {
	defer p.wg.Done()
	
	// Wait for container to be running before starting perf collection
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	var metadata *ContainerMetadata
	
	// Force an immediate metadata refresh to catch newly started containers
	if err := p.metadataProvider.RefreshContainerMetadata(); err != nil {
		log.WithFields(log.Fields{
			"container": containerName,
			"error": err,
		}).Debug("Failed to force metadata refresh")
	}
	
	// Wait for container to have valid metadata (running with PID and cgroup)
waitLoop:
	for {
		select {
		case <-ctx.Done():
			log.WithField("container", containerName).Debug("Context cancelled while waiting for container to start")
			return
		case <-ticker.C:
			// Force a metadata refresh on each check to get latest container status
			if err := p.metadataProvider.RefreshContainerMetadata(); err != nil {
				log.WithFields(log.Fields{
					"container": containerName,
					"error": err,
				}).Debug("Failed to refresh metadata while waiting for container")
			}
			
			var err error
			metadata, err = p.metadataProvider.GetContainerMetadata(containerName)
			if err != nil {
				log.WithFields(log.Fields{
					"container": containerName,
					"error": err,
				}).Debug("Failed to get container metadata")
				continue
			}
			
			log.WithFields(log.Fields{
				"container": containerName,
				"pid": metadata.PID,
				"cgroup": metadata.Cgroup,
				"has_pid": metadata.PID > 0,
				"has_cgroup": metadata.Cgroup != "",
			}).Debug("Container metadata debug info")
			
			if metadata.PID > 0 && metadata.Cgroup != "" {
				log.WithFields(log.Fields{
					"container": containerName,
					"pid": metadata.PID,
					"cgroup": metadata.Cgroup,
				}).Debug("Container is now running, starting perf collection")
				break waitLoop
			}
			
			log.WithField("container", containerName).Debug("Waiting for container to start (PID=0 or empty cgroup)")
		}
	}
	
	if metadata == nil {
		log.WithField("container", containerName).Error("Failed to get valid container metadata")
		return
	}

	// Get configuration
	config := p.metadataProvider.GetConfig()
	samplingRate := config.SamplingRate

	// Define comprehensive hardware performance events to collect
	events := []string{
		// Hardware events
		"cycles",           // cpu-cycles
		"instructions",     // instructions
		"cache-misses",     // cache-misses
		"cache-references", // cache-references
		"branches",         // branch-instructions
		"branch-misses",    // branch-misses
		"bus-cycles",       // bus-cycles
		"ref-cycles",       // ref-cycles
		"stalled-cycles-frontend", // stalled-cycles-frontend
		
		// Software events
		"task-clock",       // task-clock
		"context-switches", // context-switches
		"cpu-migrations",   // cpu-migrations
		"page-faults",      // page-faults
		"major-faults",     // major-faults
		"minor-faults",     // minor-faults
		"alignment-faults", // alignment-faults
		
		// Hardware cache events
		"L1-dcache-loads",        // L1 data cache loads
		"L1-dcache-load-misses",  // L1 data cache load misses
		"L1-dcache-stores",       // L1 data cache stores
		"L1-dcache-store-misses", // L1 data cache store misses
		"L1-icache-load-misses",  // L1 instruction cache load misses
		"LLC-loads",              // Last Level Cache loads
		"LLC-load-misses",        // Last Level Cache load misses
		"LLC-stores",             // Last Level Cache stores
		"LLC-store-misses",       // Last Level Cache store misses
		"LLC-prefetches",         // Last Level Cache prefetches
		"LLC-prefetch-misses",    // Last Level Cache prefetch misses
		"L1-dcache-prefetch-misses", // L1 data cache prefetch misses
	}

	log.WithFields(log.Fields{
		"container": containerName,
		"cgroup": metadata.Cgroup,
		"interval_ms": samplingRate,
		"events": events,
	}).Debug("Starting interval-based perf collection for container")

	// Build perf command with interval collection and cgroup filtering
	// Using a long duration with interval sampling
	perfCmd := exec.CommandContext(ctx, "sudo", "perf", "stat", 
		"-e", strings.Join(events, ","),
		"-a", 
		"-G", metadata.Cgroup,
		"-I", fmt.Sprintf("%d", samplingRate), // Interval in milliseconds
		"--", "sleep", "3600") // Run for 1 hour (will be cancelled by context)

	log.WithField("command", perfCmd.String()).Debug("Executing perf command")

	// Get stderr pipe - perf stat outputs to stderr
	stderr, err := perfCmd.StderrPipe()
	if err != nil {
		log.WithError(err).WithField("container", containerName).Error("Failed to create stderr pipe for perf command")
		return
	}

	// Start the command
	if err := perfCmd.Start(); err != nil {
		log.WithError(err).WithField("container", containerName).Error("Failed to start perf command")
		return
	}

	log.WithFields(log.Fields{
		"container": containerName,
		"cgroup": metadata.Cgroup,
		"interval_ms": samplingRate,
		"pid": perfCmd.Process.Pid,
	}).Debug("Successfully started interval-based perf collection for container")

	// Store the command and create scanner for stderr (where perf stat outputs)
	p.bufferMutex.Lock()
	p.containerCommands[containerName] = perfCmd
	p.containerReaders[containerName] = bufio.NewScanner(stderr)
	p.bufferMutex.Unlock()

	// Read and process perf output in real-time
	scanner := bufio.NewScanner(stderr)
	lineCount := 0
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			log.WithField("container", containerName).Debug("Perf collection stopped due to context cancellation")
			return
		case <-p.stopChan:
			log.WithField("container", containerName).Debug("Perf collection stopped")
			return
		default:
			line := scanner.Text()
			lineCount++
			log.WithFields(log.Fields{
				"container": containerName,
				"line_count": lineCount,
				"line": line,
			}).Debug("Received perf output line")
			p.parseIntervalLine(line, containerName)
		}
	}

	if err := scanner.Err(); err != nil {
		log.WithError(err).WithField("container", containerName).Error("Error reading perf output")
	}

	// Wait for command to finish
	if err := perfCmd.Wait(); err != nil && !strings.Contains(err.Error(), "signal: killed") {
		log.WithError(err).WithField("container", containerName).Debug("Perf command finished with error (expected if cancelled)")
	}

	log.WithField("container", containerName).Debug("Perf collection goroutine finished")
}

// parseIntervalLine parses a single line of perf stat interval output
func (p *PerfCollector) parseIntervalLine(line string, containerName string) {
	line = strings.TrimSpace(line)
	log.WithFields(log.Fields{
		"container": containerName,
		"raw_line": line,
	}).Debug("Parsing perf interval line")
	
	if line == "" || strings.HasPrefix(line, "#") {
		log.WithField("container", containerName).Debug("Skipping comment or empty line")
		return
	}

	// Parse interval output format:
	// timestamp count unit event cgroup_path
	parts := strings.Fields(line)
	log.WithFields(log.Fields{
		"container": containerName,
		"parts_count": len(parts),
		"parts": parts,
	}).Debug("Split perf line into parts")
	
	if len(parts) < 4 {
		log.WithField("container", containerName).Debug("Line has insufficient parts")
		return
	}

	// Extract count (remove commas)
	countStr := strings.ReplaceAll(parts[1], ",", "")
	if countStr == "<not" || countStr == "" {
		log.WithFields(log.Fields{
			"container": containerName,
			"count_str": countStr,
		}).Debug("Skipping line with invalid count")
		return
	}

	// Handle both integer and float values
	var count uint64
	if strings.Contains(countStr, ".") {
		// For floating point values like task-clock, convert to milliseconds or appropriate unit
		floatVal, err := strconv.ParseFloat(countStr, 64)
		if err != nil {
			log.WithFields(log.Fields{
				"container": containerName,
				"count_str": countStr,
				"error": err,
			}).Debug("Failed to parse float count")
			return
		}
		count = uint64(floatVal * 1000) // Convert to microseconds for task-clock
	} else {
		var err error
		count, err = strconv.ParseUint(countStr, 10, 64)
		if err != nil {
			log.WithFields(log.Fields{
				"container": containerName,
				"count_str": countStr,
				"error": err,
			}).Debug("Failed to parse count")
			return
		}
	}

	// Find event name - it's typically at index 2 or 3 depending on format
	var eventName string
	for i := 2; i < len(parts) && i < 5; i++ {
		candidate := parts[i]
		// Check for all supported event names
		if isValidEventName(candidate) {
			eventName = candidate
			break
		}
	}
	
	if eventName == "" {
		log.WithFields(log.Fields{
			"container": containerName,
			"parts": parts,
		}).Debug("Could not find valid event name, skipping")
		return
	}
	
	// Map event name to our standard format
	standardName := mapEventName(eventName)
	if standardName == "" {
		log.WithFields(log.Fields{
			"container": containerName,
			"event_name": eventName,
		}).Debug("Unknown event name, skipping")
		return // Skip unknown events
	}

	// Update buffer
	p.bufferMutex.Lock()
	if buffer, exists := p.buffers[containerName]; exists {
		buffer.mutex.Lock()
		buffer.data[standardName] = count
		buffer.timestamp = time.Now()
		buffer.mutex.Unlock()
		
		log.WithFields(log.Fields{
			"container": containerName,
			"metric": standardName,
			"value": count,
		}).Debug("Updated perf metric")
	} else {
		log.WithField("container", containerName).Warn("Buffer does not exist for container")
	}
	p.bufferMutex.Unlock()
}

// isValidEventName checks if the given string is a valid perf event name
func isValidEventName(candidate string) bool {
	validEvents := map[string]bool{
		// Hardware events
		"cycles":                   true,
		"instructions":             true,
		"cache-misses":             true,
		"cache-references":         true,
		"branches":                 true,
		"branch-misses":            true,
		"bus-cycles":               true,
		"ref-cycles":               true,
		"stalled-cycles-frontend":  true,
		"idle-cycles-frontend":     true, // Alternative name
		
		// Software events
		"task-clock":               true,
		"context-switches":         true,
		"cs":                       true, // Alternative name
		"cpu-migrations":           true,
		"migrations":               true, // Alternative name
		"page-faults":              true,
		"faults":                   true, // Alternative name
		"major-faults":             true,
		"minor-faults":             true,
		"alignment-faults":         true,
		"cpu-clock":                true,
		
		// Hardware cache events
		"L1-dcache-loads":          true,
		"L1-dcache-load-misses":    true,
		"L1-dcache-stores":         true,
		"L1-dcache-store-misses":   true,
		"L1-icache-load-misses":    true,
		"L1-dcache-prefetch-misses": true,
		"LLC-loads":                true,
		"LLC-load-misses":          true,
		"LLC-stores":               true,
		"LLC-store-misses":         true,
		"LLC-prefetches":           true,
		"LLC-prefetch-misses":      true,
	}
	return validEvents[candidate]
}

// mapEventName maps perf event names to our standard database field names
func mapEventName(eventName string) string {
	eventMap := map[string]string{
		// Hardware events
		"cycles":                   "cpu_cycles",
		"instructions":             "instructions",
		"cache-misses":             "cache_misses",
		"cache-references":         "cache_references",
		"branches":                 "branch_instructions",
		"branch-misses":            "branch_misses",
		"bus-cycles":               "bus_cycles",
		"ref-cycles":               "ref_cycles",
		"stalled-cycles-frontend":  "stalled_cycles_frontend",
		"idle-cycles-frontend":     "stalled_cycles_frontend", // Alternative name
		
		// Software events
		"task-clock":               "task_clock",
		"context-switches":         "context_switches",
		"cs":                       "context_switches", // Alternative name
		"cpu-migrations":           "cpu_migrations",
		"migrations":               "cpu_migrations", // Alternative name
		"page-faults":              "page_faults",
		"faults":                   "page_faults", // Alternative name
		"major-faults":             "major_faults",
		"minor-faults":             "minor_faults",
		"alignment-faults":         "alignment_faults",
		"cpu-clock":                "cpu_clock",
		
		// Hardware cache events
		"L1-dcache-loads":          "l1_dcache_loads",
		"L1-dcache-load-misses":    "l1_dcache_load_misses",
		"L1-dcache-stores":         "l1_dcache_stores",
		"L1-dcache-store-misses":   "l1_dcache_store_misses",
		"L1-icache-load-misses":    "l1_icache_load_misses",
		"L1-dcache-prefetch-misses": "l1_dcache_prefetch_misses",
		"LLC-loads":                "llc_loads",
		"LLC-load-misses":          "llc_load_misses",
		"LLC-stores":               "llc_stores",
		"LLC-store-misses":         "llc_store_misses",
		"LLC-prefetches":           "llc_prefetches",
		"LLC-prefetch-misses":      "llc_prefetch_misses",
	}
	return eventMap[eventName]
}

// getContainerCgroup gets the cgroup path for a container
func (p *PerfCollector) getContainerCgroup(pid int) (string, error) {
	cgroupFile := fmt.Sprintf("/proc/%d/cgroup", pid)
	file, err := os.Open(cgroupFile)
	if err != nil {
		return "", fmt.Errorf("failed to open cgroup file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Look for the unified cgroup (cgroup v2) or the cpu,cpuacct cgroup
		if strings.HasPrefix(line, "0:") {
			parts := strings.Split(line, ":")
			if len(parts) >= 3 {
				return parts[2], nil
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("failed to read cgroup file: %w", err)
	}

	return "", fmt.Errorf("cgroup not found for PID %d", pid)
}

// Collect returns the latest buffered performance data from interval collection
func (p *PerfCollector) Collect(ctx context.Context, timestamp time.Time) ([]storage.Measurement, error) {
	var measurements []storage.Measurement

	p.bufferMutex.RLock()
	defer p.bufferMutex.RUnlock()

	// Get container names from metadata provider
	containerNames := p.metadataProvider.GetAllContainerNames()

	// Get latest buffered data for each container
	for _, containerName := range containerNames {
		metadata, err := p.metadataProvider.GetContainerMetadata(containerName)
		if err != nil {
			continue
		}

		buffer, exists := p.buffers[containerName]
		if !exists {
			continue
		}

		buffer.mutex.RLock()
		perfData := make(map[string]uint64)
		bufferTimestamp := buffer.timestamp
		// Copy data to avoid holding lock too long
		for k, v := range buffer.data {
			perfData[k] = v
		}
		buffer.mutex.RUnlock()

		// Skip if no data available or data is too old (more than 5 seconds)
		if len(perfData) == 0 || time.Since(bufferTimestamp) > 5*time.Second {
			log.WithFields(log.Fields{
				"container": containerName,
				"data_age": time.Since(bufferTimestamp),
				"has_data": len(perfData) > 0,
			}).Debug("Skipping stale or missing perf data")
			continue
		}

		// Convert buffered data to measurements
		containerMeasurements := p.convertToMeasurements(perfData, timestamp, metadata)
		measurements = append(measurements, containerMeasurements...)

		log.WithFields(log.Fields{
			"container": containerName,
			"metrics":   len(perfData),
			"data_age":  time.Since(bufferTimestamp),
		}).Debug("Using buffered interval perf data")
	}

	return measurements, nil
}

// convertToMeasurements converts perf data map to measurement format
func (p *PerfCollector) convertToMeasurements(perfData map[string]uint64, timestamp time.Time, metadata *ContainerMetadata) []storage.Measurement {
	var measurements []storage.Measurement

	// Get configuration for benchmark ID
	config := p.metadataProvider.GetConfig()

	// Create measurements for each counter
	for metricName, value := range perfData {
		tags := map[string]string{
			"benchmark_id":  config.BenchmarkID,
			"collector":     "perf",
			"container":     metadata.Name,
			"container_id":  metadata.ShortID,
			"metric":        metricName,
		}

		measurements = append(measurements, storage.Measurement{
			Name:      "perf_counter",
			Tags:      tags,
			Fields:    map[string]interface{}{"value": value},
			Timestamp: timestamp,
		})
	}

	// Create additional measurements for calculated metrics (if we have the base metrics)
	if cycles, hasCycles := perfData["cpu_cycles"]; hasCycles {
		if instructions, hasInstr := perfData["instructions"]; hasInstr && cycles > 0 {
			ipc := float64(instructions) / float64(cycles)
			tags := map[string]string{
				"benchmark_id":  config.BenchmarkID,
				"collector":     "perf",
				"container":     metadata.Name,
				"container_id":  metadata.ShortID,
				"metric":        "ipc",
			}
			measurements = append(measurements, storage.Measurement{
				Name:      "perf_counter",
				Tags:      tags,
				Fields:    map[string]interface{}{"value": ipc},
				Timestamp: timestamp,
			})
		}
	}

	if cacheRefs, hasRefs := perfData["cache_references"]; hasRefs {
		if cacheMisses, hasMisses := perfData["cache_misses"]; hasMisses && cacheRefs > 0 {
			missRate := float64(cacheMisses) / float64(cacheRefs)
			tags := map[string]string{
				"benchmark_id":  config.BenchmarkID,
				"collector":     "perf",
				"container":     metadata.Name,
				"container_id":  metadata.ShortID,
				"metric":        "cache_miss_rate",
			}
			measurements = append(measurements, storage.Measurement{
				Name:      "perf_counter",
				Tags:      tags,
				Fields:    map[string]interface{}{"value": missRate},
				Timestamp: timestamp,
			})
		}
	}

	return measurements
}

// parsePerfOutputToMap parses perf stat output and returns a map of metrics
func (p *PerfCollector) parsePerfOutputToMap(output string) map[string]uint64 {
	perfData := make(map[string]uint64)
	lines := strings.Split(output, "\n")
	
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, " Performance counter stats") ||
		   strings.HasPrefix(line, "Performance counter stats") || strings.Contains(line, "seconds time elapsed") ||
		   strings.Contains(line, "Some events weren't counted") || strings.Contains(line, "echo") ||
		   strings.Contains(line, "perf stat") {
			continue
		}

		// Parse standard perf stat output format
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		countStr := strings.TrimSpace(parts[0])
		if strings.HasPrefix(countStr, "<not") || countStr == "" {
			continue
		}

		// Remove comma separators and parse count
		countStr = strings.ReplaceAll(countStr, ",", "")
		count, err := strconv.ParseUint(countStr, 10, 64)
		if err != nil {
			continue
		}

		// Extract event name (remove :u suffix if present)
		eventName := strings.TrimSpace(parts[1])
		eventName = strings.Split(eventName, ":")[0] // Remove :u, :k suffixes
		
		// Map event names to our standard names
		switch eventName {
		case "cycles", "cpu-cycles":
			perfData["cpu_cycles"] = count
		case "instructions":
			perfData["instructions"] = count
		case "cache-references":
			perfData["cache_references"] = count
		case "cache-misses":
			perfData["cache_misses"] = count
		case "branches", "branch-instructions":
			perfData["branch_instructions"] = count
		case "branch-misses":
			perfData["branch_misses"] = count
		case "bus-cycles":
			perfData["bus_cycles"] = count
		case "ref-cycles":
			perfData["ref_cycles"] = count
		case "stalled-cycles-frontend", "idle-cycles-frontend":
			perfData["stalled_cycles_frontend"] = count
		case "L1-dcache-loads":
			perfData["l1_dcache_loads"] = count
		case "L1-dcache-load-misses":
			perfData["l1_dcache_load_misses"] = count
		case "L1-dcache-stores":
			perfData["l1_dcache_stores"] = count
		case "L1-dcache-store-misses":
			perfData["l1_dcache_store_misses"] = count
		case "L1-icache-load-misses":
			perfData["l1_icache_load_misses"] = count
		case "LLC-loads":
			perfData["llc_loads"] = count
		case "LLC-load-misses":
			perfData["llc_load_misses"] = count
		case "LLC-stores":
			perfData["llc_stores"] = count
		case "LLC-store-misses":
			perfData["llc_store_misses"] = count
		}
	}
	
	return perfData
}

// Close closes the perf collector and stops all interval collection processes
func (p *PerfCollector) Close() error {
	if p.running {
		p.running = false
		close(p.stopChan)
		
		// Terminate all perf processes
		p.bufferMutex.Lock()
		for containerName, cmd := range p.containerCommands {
			if cmd != nil && cmd.Process != nil {
				log.WithField("container", containerName).Debug("Terminating perf process")
				cmd.Process.Kill()
			}
		}
		p.bufferMutex.Unlock()
		
		p.wg.Wait()
		log.Debug("Perf collector interval collection stopped")
	}
	return nil
}

// Name returns the collector name
func (p *PerfCollector) Name() string {
	return "perf"
}
