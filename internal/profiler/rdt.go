package profiler

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/jakobeberhardt/container-bench/internal/storage"
	log "github.com/sirupsen/logrus"
)

// RDTCollector collects Intel RDT (Resource Director Technology) metrics
type RDTCollector struct {
	benchmarkID string
}

// NewRDTCollector creates a new RDT collector
func NewRDTCollector(benchmarkID string) *RDTCollector {
	return &RDTCollector{
		benchmarkID: benchmarkID,
	}
}

// Initialize initializes the RDT collector
func (r *RDTCollector) Initialize(ctx context.Context) error {
	// Check if RDT tools are available
	if err := r.checkRDTAvailability(); err != nil {
		return fmt.Errorf("RDT not available: %w", err)
	}

	log.Info("RDT collector initialized")
	return nil
}

// checkRDTAvailability checks if Intel RDT is available on the system
func (r *RDTCollector) checkRDTAvailability() error {
	// Check if pqos tool is available (part of Intel RDT toolkit)
	if _, err := exec.LookPath("pqos"); err == nil {
		return nil
	}

	// Check if rdtset is available
	if _, err := exec.LookPath("rdtset"); err == nil {
		return nil
	}

	// Check if RDT is supported via /proc/cpuinfo
	cmd := exec.Command("grep", "-q", "rdt", "/proc/cpuinfo")
	if err := cmd.Run(); err == nil {
		log.Warn("RDT support detected but tools not found, using fallback methods")
		return nil
	}

	return fmt.Errorf("Intel RDT not supported or tools not installed")
}

// Collect collects RDT metrics
func (r *RDTCollector) Collect(ctx context.Context, timestamp time.Time) ([]storage.Measurement, error) {
	var measurements []storage.Measurement

	// Try to collect using pqos if available
	if pqosMeasurements, err := r.collectWithPqos(ctx, timestamp); err == nil {
		measurements = append(measurements, pqosMeasurements...)
	} else {
		log.WithError(err).Debug("Failed to collect RDT metrics with pqos")
	}

	// Collect cache occupancy from /sys/fs/resctrl if available
	if cacheMeasurements, err := r.collectCacheOccupancy(ctx, timestamp); err == nil {
		measurements = append(measurements, cacheMeasurements...)
	} else {
		log.WithError(err).Debug("Failed to collect cache occupancy from resctrl")
	}

	// Collect memory bandwidth information
	if bwMeasurements, err := r.collectMemoryBandwidth(ctx, timestamp); err == nil {
		measurements = append(measurements, bwMeasurements...)
	} else {
		log.WithError(err).Debug("Failed to collect memory bandwidth")
	}

	return measurements, nil
}

// collectWithPqos collects RDT metrics using the pqos tool
func (r *RDTCollector) collectWithPqos(ctx context.Context, timestamp time.Time) ([]storage.Measurement, error) {
	// Run pqos to get LLC occupancy and memory bandwidth
	cmd := exec.CommandContext(ctx, "pqos", "-m", "all:0", "-t", "1")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to run pqos: %w", err)
	}

	return r.parsePqosOutput(string(output), timestamp), nil
}

// parsePqosOutput parses pqos output and converts to measurements
func (r *RDTCollector) parsePqosOutput(output string, timestamp time.Time) []storage.Measurement {
	var measurements []storage.Measurement
	
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "TIME") {
			continue
		}

		// Parse pqos output format
		if strings.Contains(line, "LLC") || strings.Contains(line, "MBL") || strings.Contains(line, "MBR") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				coreID := parts[1]
				
				// Extract LLC occupancy
				if len(parts) >= 4 && parts[2] != "-" {
					if llc, err := strconv.ParseFloat(parts[2], 64); err == nil {
						tags := map[string]string{
							"benchmark_id": r.benchmarkID,
							"collector":    "rdt",
							"metric":       "llc_occupancy",
							"core":         coreID,
						}
						measurements = append(measurements, storage.Measurement{
							Name:      "rdt_llc_occupancy_kb",
							Tags:      tags,
							Fields:    map[string]interface{}{"value": llc},
							Timestamp: timestamp,
						})
					}
				}

				// Extract Memory Bandwidth Local (MBL)
				if len(parts) >= 5 && parts[3] != "-" {
					if mbl, err := strconv.ParseFloat(parts[3], 64); err == nil {
						tags := map[string]string{
							"benchmark_id": r.benchmarkID,
							"collector":    "rdt",
							"metric":       "memory_bandwidth_local",
							"core":         coreID,
						}
						measurements = append(measurements, storage.Measurement{
							Name:      "rdt_memory_bandwidth_mb_s",
							Tags:      tags,
							Fields:    map[string]interface{}{"value": mbl},
							Timestamp: timestamp,
						})
					}
				}

				// Extract Memory Bandwidth Remote (MBR)
				if len(parts) >= 6 && parts[4] != "-" {
					if mbr, err := strconv.ParseFloat(parts[4], 64); err == nil {
						tags := map[string]string{
							"benchmark_id": r.benchmarkID,
							"collector":    "rdt",
							"metric":       "memory_bandwidth_remote",
							"core":         coreID,
						}
						measurements = append(measurements, storage.Measurement{
							Name:      "rdt_memory_bandwidth_mb_s",
							Tags:      tags,
							Fields:    map[string]interface{}{"value": mbr},
							Timestamp: timestamp,
						})
					}
				}
			}
		}
	}

	return measurements
}

// collectCacheOccupancy collects cache occupancy from /sys/fs/resctrl
func (r *RDTCollector) collectCacheOccupancy(ctx context.Context, timestamp time.Time) ([]storage.Measurement, error) {
	// Check if resctrl filesystem is mounted
	cmd := exec.CommandContext(ctx, "find", "/sys/fs/resctrl", "-name", "mon_data", "-type", "d")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to access resctrl: %w", err)
	}

	var measurements []storage.Measurement
	
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}

		// Read LLC occupancy
		llcFile := line + "/mon_L3_00/llc_occupancy"
		cmd := exec.CommandContext(ctx, "cat", llcFile)
		if output, err := cmd.Output(); err == nil {
			if llc, err := strconv.ParseFloat(strings.TrimSpace(string(output)), 64); err == nil {
				// Convert bytes to KB
				llc = llc / 1024

				tags := map[string]string{
					"benchmark_id": r.benchmarkID,
					"collector":    "rdt",
					"metric":       "llc_occupancy",
					"source":       "resctrl",
				}
				measurements = append(measurements, storage.Measurement{
					Name:      "rdt_llc_occupancy_kb",
					Tags:      tags,
					Fields:    map[string]interface{}{"value": llc},
					Timestamp: timestamp,
				})
			}
		}

		// Read memory bandwidth
		mbFile := line + "/mon_L3_00/mbm_total_bytes"
		cmd = exec.CommandContext(ctx, "cat", mbFile)
		if output, err := cmd.Output(); err == nil {
			if mb, err := strconv.ParseFloat(strings.TrimSpace(string(output)), 64); err == nil {
				tags := map[string]string{
					"benchmark_id": r.benchmarkID,
					"collector":    "rdt",
					"metric":       "memory_bandwidth_total",
					"source":       "resctrl",
				}
				measurements = append(measurements, storage.Measurement{
					Name:      "rdt_memory_bandwidth_bytes",
					Tags:      tags,
					Fields:    map[string]interface{}{"value": mb},
					Timestamp: timestamp,
				})
			}
		}
	}

	return measurements, nil
}

// collectMemoryBandwidth collects memory bandwidth information
func (r *RDTCollector) collectMemoryBandwidth(ctx context.Context, timestamp time.Time) ([]storage.Measurement, error) {
	// Try to get memory bandwidth from /proc/meminfo and other sources
	cmd := exec.CommandContext(ctx, "cat", "/proc/meminfo")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to read /proc/meminfo: %w", err)
	}

	var measurements []storage.Measurement
	lines := strings.Split(string(output), "\n")
	
	for _, line := range lines {
		if strings.HasPrefix(line, "MemAvailable:") || strings.HasPrefix(line, "MemFree:") || strings.HasPrefix(line, "Buffers:") || strings.HasPrefix(line, "Cached:") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				metricName := strings.TrimSuffix(parts[0], ":")
				if value, err := strconv.ParseFloat(parts[1], 64); err == nil {
					// Convert KB to bytes
					value = value * 1024

					tags := map[string]string{
						"benchmark_id": r.benchmarkID,
						"collector":    "rdt",
						"metric":       strings.ToLower(metricName),
						"source":       "meminfo",
					}
					measurements = append(measurements, storage.Measurement{
						Name:      "system_memory_bytes",
						Tags:      tags,
						Fields:    map[string]interface{}{"value": value},
						Timestamp: timestamp,
					})
				}
			}
		}
	}

	return measurements, nil
}

// Close closes the RDT collector
func (r *RDTCollector) Close() error {
	// Nothing specific to close for RDT
	return nil
}

// Name returns the collector name
func (r *RDTCollector) Name() string {
	return "rdt"
}
