package resources

import (
	"container-bench/internal/accounting"
	"container-bench/internal/dataframe"
	"container-bench/internal/logging"
	"fmt"
	"sort"
	"time"

	"github.com/sirupsen/logrus"
)

// AllocationProbeResult represents the result of testing a specific allocation
type AllocationProbeResult struct {
	BenchmarkID int `json:"benchmark_id"`

	// Target container info
	ContainerID      string `json:"container_id"`
	ContainerName    string `json:"container_name"`
	ContainerIndex   int    `json:"container_index"`
	ContainerCores   string `json:"container_cores"`
	ContainerSocket  int    `json:"container_socket,omitempty"`
	ContainerImage   string `json:"container_image"`
	ContainerCommand string `json:"container_command,omitempty"`

	// Probing execution metadata
	TotalProbeTime time.Duration `json:"total_probe_time"`
	Aborted        bool          `json:"aborted"`
	AbortReason    string        `json:"abort_reason,omitempty"`

	Started   time.Time  `json:"started"`
	Finished  time.Time  `json:"finished"`
	AbortedAt *time.Time `json:"aborted_at,omitempty"`

	// Tested allocations and results
	Allocations []AllocationResult `json:"allocations"`

	// Probe configuration
	Range AllocationRange `json:"range"`
}

// AllocationResult represents the result of a single allocation test
type AllocationResult struct {
	// Allocation parameters
	L3Ways         int     `json:"l3_ways"`
	MemBandwidth   float64 `json:"mem_bandwidth_percent"`
	SocketID       int     `json:"socket_id"`
	IsolatedOthers bool    `json:"isolated_others"`

	// Timing
	Started  time.Time     `json:"started"`
	Duration time.Duration `json:"duration"`

	// Performance metrics (computed from dataframes)
	AvgIPC                 float64 `json:"avg_ipc,omitempty"`
	AvgTheoreticalIPC      float64 `json:"avg_theoretical_ipc,omitempty"`
	IPCEfficiency          float64 `json:"ipc_efficiency,omitempty"` // IPC / Theoretical IPC
	AvgCacheMissRate       float64 `json:"avg_cache_miss_rate,omitempty"`
	AvgStalledCycles       float64 `json:"avg_stalled_cycles,omitempty"`
	AvgStallsL3MissPercent float64 `json:"avg_stalls_l3_miss_percent,omitempty"`
	AvgL3Occupancy         uint64  `json:"avg_l3_occupancy,omitempty"`
	AvgMemBandwidthUsed    uint64  `json:"avg_mem_bandwidth_used,omitempty"`

	// Raw data frames for this allocation period
	DataFrameSteps []int `json:"dataframe_steps"` // Step numbers captured during this allocation
}

// AllocationRange defines the range of allocations to test
type AllocationRange struct {
	MinL3Ways         int     `json:"min_l3_ways"`
	MaxL3Ways         int     `json:"max_l3_ways"`
	MinMemBandwidth   float64 `json:"min_memory_bandwidth"` // Percentage 0-100
	MaxMemBandwidth   float64 `json:"max_memory_bandwidth"` // Percentage 0-100
	StepL3Ways        int     `json:"step_l3_ways"`
	StepMemBandwidth  float64 `json:"step_memory_bandwidth"` // Percentage points
	Order             string  `json:"order"`                 // "asc" (start smallest) or "desc" (start largest)
	DurationPerAlloc  int     `json:"duration_per_alloc"`    // Milliseconds to test each allocation
	MaxTotalDuration  int     `json:"max_total_duration"`    // Maximum total probe time in seconds
	SocketID          int     `json:"socket_id"`             // Which socket to test on
	IsolateOthers     bool    `json:"isolate_others"`        // Move other containers to different allocation
	ForceReallocation bool    `json:"force_reallocation"`    // Remove existing allocations if needed
}

// AllocationSpec defines a specific allocation to test (shared helper type).
// It is intentionally minimal so schedulers can reuse the same search ordering logic
// without pulling in the full probe execution loop.
type AllocationSpec struct {
	L3Ways       int
	MemBandwidth float64
}

// BreakCondition defines when to stop probing early
type BreakCondition func(result *AllocationResult, allResults []AllocationResult) bool

// ContainerInfo holds information about a container for probing
type ContainerInfo struct {
	Index   int
	PID     int
	Name    string
	ID      string
	Cores   string
	Socket  int
	Image   string
	Command string
}

// ProbeAllocation tests different resource allocations for a container and measures performance
func ProbeAllocation(
	targetContainer ContainerInfo,
	otherContainers []ContainerInfo,
	dataframes *dataframe.DataFrames,
	rdtAccountant *accounting.RDTAccountant,
	allocRange AllocationRange,
	breakCondition BreakCondition,
	benchmarkID int,
) (*AllocationProbeResult, error) {

	logger := logging.GetSchedulerLogger()

	result := &AllocationProbeResult{
		BenchmarkID:      benchmarkID,
		ContainerID:      targetContainer.ID,
		ContainerName:    targetContainer.Name,
		ContainerIndex:   targetContainer.Index,
		ContainerCores:   targetContainer.Cores,
		ContainerSocket:  targetContainer.Socket,
		ContainerImage:   targetContainer.Image,
		ContainerCommand: targetContainer.Command,
		Started:          time.Now(),
		Range:            allocRange,
		Allocations:      make([]AllocationResult, 0),
	}

	logger.WithFields(logrus.Fields{
		"container":       targetContainer.Name,
		"container_index": targetContainer.Index,
		"min_l3_ways":     allocRange.MinL3Ways,
		"max_l3_ways":     allocRange.MaxL3Ways,
		"min_mem_bw":      allocRange.MinMemBandwidth,
		"max_mem_bw":      allocRange.MaxMemBandwidth,
		"order":           allocRange.Order,
	}).Info("Starting allocation probe")

	// Generate allocation sequence based on order
	allocations := generateAllocationSequence(allocRange)

	logger.WithField("total_allocations", len(allocations)).Info("Generated allocation sequence")

	// Track original classes for cleanup
	// Record all containers' current classes - we'll decide what to restore based on force flag
	originalClasses := make(map[int]string)
	for _, container := range append([]ContainerInfo{targetContainer}, otherContainers...) {
		if class, err := rdtAccountant.GetContainerClass(container.PID); err == nil {
			originalClasses[container.PID] = class
			logger.WithFields(logrus.Fields{
				"pid":   container.PID,
				"class": class,
			}).Debug("Recorded original container class")
		}
	}

	// Create probe classes once at the start (we'll update them for each allocation)
	probeClassName := fmt.Sprintf("probe-target-%d", targetContainer.Index)
	isolationClassName := fmt.Sprintf("probe-isolated-%d", targetContainer.Index)
	probeClassCreated := false
	isolationClassCreated := false

	// IMPORTANT: Before creating any classes, we need to ensure system/default is shrunk
	// to make room for our allocations. We do this by creating a temporary minimal allocation
	// for system/default so the resctrl filesystem allows our overlapping allocations.
	// This is necessary because by default, system/default owns all cache ways.
	logger.Debug("Preparing system/default for probe allocations")

	probeStartTime := time.Now()

	// Test each allocation
	for i, alloc := range allocations {
		// Check total duration limit
		if allocRange.MaxTotalDuration > 0 {
			elapsed := time.Since(probeStartTime).Seconds()
			if elapsed >= float64(allocRange.MaxTotalDuration) {
				logger.WithField("elapsed_seconds", int(elapsed)).Info("Maximum total probe duration reached")
				result.Aborted = true
				result.AbortReason = "max_duration_reached"
				now := time.Now()
				result.AbortedAt = &now
				break
			}
		}

		logger.WithFields(logrus.Fields{
			"allocation": i + 1,
			"total":      len(allocations),
			"l3_ways":    alloc.L3Ways,
			"mem_bw":     alloc.MemBandwidth,
		}).Info("Testing allocation")

		// Apply the allocation (pass class names and creation status)
		allocResult, classesCreated, err := applyAndMeasureAllocation(
			targetContainer,
			otherContainers,
			alloc,
			allocRange,
			dataframes,
			rdtAccountant,
			logger,
			probeClassName,
			isolationClassName,
			probeClassCreated,
			isolationClassCreated,
		)

		if err != nil {
			if allocRange.ForceReallocation {
				logger.WithError(err).Warn("Failed to apply allocation, continuing to next")
				continue
			} else {
				logger.WithError(err).Debug("Skipping impossible allocation (force=false)")
				continue
			}
		}

		// Update creation flags
		if !probeClassCreated && classesCreated.ProbeCreated {
			probeClassCreated = true
		}
		if !isolationClassCreated && classesCreated.IsolationCreated {
			isolationClassCreated = true
		}

		result.Allocations = append(result.Allocations, *allocResult)

		// Check break condition
		if breakCondition != nil && breakCondition(allocResult, result.Allocations) {
			logger.WithFields(logrus.Fields{
				"allocation":     i + 1,
				"ipc_efficiency": allocResult.IPCEfficiency,
			}).Info("Break condition met, stopping probe")
			result.Aborted = true
			result.AbortReason = "break_condition_met"
			now := time.Now()
			result.AbortedAt = &now
			break
		}
	}

	result.Finished = time.Now()
	result.TotalProbeTime = result.Finished.Sub(result.Started)

	// Cleanup probe classes if they were created
	// DeleteClass automatically moves containers back to the default group
	if probeClassCreated {
		logger.WithField("class", probeClassName).Debug("Cleaning up probe class")
		if err := rdtAccountant.DeleteClass(probeClassName); err != nil {
			logger.WithError(err).WithField("class", probeClassName).Warn("Failed to cleanup probe class")
		}
	}
	if isolationClassCreated {
		logger.WithField("class", isolationClassName).Debug("Cleaning up isolation class")
		if err := rdtAccountant.DeleteClass(isolationClassName); err != nil {
			logger.WithError(err).WithField("class", isolationClassName).Warn("Failed to cleanup isolation class")
		}
	}

	// Restore containers that were in existing RDT classes (not system/default)
	// Containers originally in system/default are already restored by DeleteClass
	logger.Debug("Restoring containers to their original RDT classes")
	for pid, className := range originalClasses {
		// Skip if container was in probe/isolation classes (already moved to default by DeleteClass)
		if className == probeClassName || className == isolationClassName {
			continue
		}

		// Skip if container was in system/default (already in default after DeleteClass)
		if className == "system/default" || className == "" {
			continue
		}

		// Restore to original RDT class
		if err := rdtAccountant.MoveContainer(pid, className); err != nil {
			logger.WithError(err).WithFields(logrus.Fields{
				"pid":   pid,
				"class": className,
			}).Warn("Failed to restore container to original RDT class")
		} else {
			logger.WithFields(logrus.Fields{
				"pid":   pid,
				"class": className,
			}).Debug("Restored container to original RDT class")
		}
	}

	logger.WithFields(logrus.Fields{
		"tested_allocations": len(result.Allocations),
		"total_time":         result.TotalProbeTime.Seconds(),
		"aborted":            result.Aborted,
	}).Info("Allocation probe completed")

	return result, nil
}

// generateAllocationSequence creates the sequence of allocations to test
// Sequence: Fix L3 ways, vary memory bandwidth from min to max, then change L3 ways and repeat
// Example with asc order: (2 ways, 20% mem), (2 ways, 30% mem), ..., (2 ways, 100% mem),
//
//	(4 ways, 20% mem), (4 ways, 30% mem), ..., (4 ways, 100% mem), ...
func generateAllocationSequence(allocRange AllocationRange) []AllocationSpec {
	sequence := make([]AllocationSpec, 0)

	// Determine iteration order
	var l3Start, l3End, l3Step int
	var memStart, memEnd, memStep float64

	if allocRange.Order == "desc" {
		// Start with maximum, go to minimum
		l3Start = allocRange.MaxL3Ways
		l3End = allocRange.MinL3Ways
		l3Step = -allocRange.StepL3Ways

		memStart = allocRange.MaxMemBandwidth
		memEnd = allocRange.MinMemBandwidth
		memStep = -allocRange.StepMemBandwidth
	} else {
		// Default: ascending (start with minimum, go to maximum)
		l3Start = allocRange.MinL3Ways
		l3End = allocRange.MaxL3Ways
		l3Step = allocRange.StepL3Ways

		memStart = allocRange.MinMemBandwidth
		memEnd = allocRange.MaxMemBandwidth
		memStep = allocRange.StepMemBandwidth
	}

	// Generate sequence: outer loop L3 ways, inner loop memory bandwidth
	// This tests all bandwidth allocations for each L3 configuration
	for l3 := l3Start; ; l3 += l3Step {
		// Check l3 boundary
		if l3Step > 0 && l3 > l3End {
			break
		}
		if l3Step < 0 && l3 < l3End {
			break
		}

		for mem := memStart; ; mem += memStep {
			// Check mem boundary
			if memStep > 0 && mem > memEnd {
				break
			}
			if memStep < 0 && mem < memEnd {
				break
			}

			sequence = append(sequence, AllocationSpec{
				L3Ways:       l3,
				MemBandwidth: mem,
			})

			if mem == memEnd {
				break
			}
		}

		if l3 == l3End {
			break
		}
	}

	return sequence
}

// GenerateAllocationSequence returns the allocation search sequence for the given range.
// This mirrors the internal probing order used by `ProbeAllocation`.
func GenerateAllocationSequence(allocRange AllocationRange) []AllocationSpec {
	return generateAllocationSequence(allocRange)
}

// classCreationStatus tracks which classes were created
type classCreationStatus struct {
	ProbeCreated     bool
	IsolationCreated bool
}

// applyAndMeasureAllocation applies an allocation and measures its impact
// Instead of creating/deleting classes, it creates them on first call and updates them subsequently
func applyAndMeasureAllocation(
	targetContainer ContainerInfo,
	otherContainers []ContainerInfo,
	alloc AllocationSpec,
	allocRange AllocationRange,
	dataframes *dataframe.DataFrames,
	rdtAccountant *accounting.RDTAccountant,
	logger *logrus.Logger,
	probeClassName string,
	isolationClassName string,
	probeClassExists bool,
	isolationClassExists bool,
) (*AllocationResult, *classCreationStatus, error) {

	status := &classCreationStatus{
		ProbeCreated:     probeClassExists,
		IsolationCreated: isolationClassExists,
	}

	allocResult := &AllocationResult{
		L3Ways:         alloc.L3Ways,
		MemBandwidth:   alloc.MemBandwidth,
		SocketID:       allocRange.SocketID,
		IsolatedOthers: allocRange.IsolateOthers,
		Started:        time.Now(),
		DataFrameSteps: make([]int, 0),
	}

	// Calculate ways range string (e.g., "0-3" for 4 ways)
	waysRange := fmt.Sprintf("0-%d", alloc.L3Ways-1)

	// Step 1: Create or update probe class for target container
	if !probeClassExists {
		// First allocation
		err := rdtAccountant.CreateClass(probeClassName,
			&accounting.AllocationRequest{
				L3Ways:       waysRange,
				MemBandwidth: alloc.MemBandwidth,
			},
			&accounting.AllocationRequest{
				L3Ways:       waysRange,
				MemBandwidth: alloc.MemBandwidth,
			},
		)

		if err != nil {
			if allocRange.ForceReallocation {
				logger.WithField("class", probeClassName).Debug("Class exists, removing and recreating")
				_ = rdtAccountant.DeleteClass(probeClassName)
				err = rdtAccountant.CreateClass(probeClassName,
					&accounting.AllocationRequest{
						L3Ways:       waysRange,
						MemBandwidth: alloc.MemBandwidth,
					},
					&accounting.AllocationRequest{
						L3Ways:       waysRange,
						MemBandwidth: alloc.MemBandwidth,
					},
				)
				if err != nil {
					return nil, status, fmt.Errorf("failed to create probe class: %w", err)
				}
			} else {
				return nil, status, fmt.Errorf("failed to create probe class: %w", err)
			}
		}
		status.ProbeCreated = true

		// Assign target container to probe class immediately after creation
		err = rdtAccountant.MoveContainer(targetContainer.PID, probeClassName)
		if err != nil {
			return nil, status, fmt.Errorf("failed to move container to probe class: %w", err)
		}
	} else {
		// Subsequent allocations
		err := rdtAccountant.UpdateClass(probeClassName,
			&accounting.AllocationRequest{
				L3Ways:       waysRange,
				MemBandwidth: alloc.MemBandwidth,
			},
			&accounting.AllocationRequest{
				L3Ways:       waysRange,
				MemBandwidth: alloc.MemBandwidth,
			},
		)
		if err != nil {
			return nil, status, fmt.Errorf("failed to update probe class: %w", err)
		}
	}

	// Step 2: If isolation requested, create or update isolation class for other containers
	if allocRange.IsolateOthers && len(otherContainers) > 0 {
		// Calculate remaining ways for isolation
		totalWays := rdtAccountant.GetTotalWays(allocRange.SocketID)
		remainingWays := totalWays - alloc.L3Ways

		if remainingWays > 0 {
			isolationWaysRange := fmt.Sprintf("%d-%d", alloc.L3Ways, totalWays-1)
			remainingMemBW := 100.0 - alloc.MemBandwidth
			if remainingMemBW < 1.0 {
				remainingMemBW = 1.0
			}

			if !isolationClassExists {
				// First allocation
				err := rdtAccountant.CreateClass(isolationClassName,
					&accounting.AllocationRequest{
						L3Ways:       isolationWaysRange,
						MemBandwidth: remainingMemBW,
					},
					&accounting.AllocationRequest{
						L3Ways:       isolationWaysRange,
						MemBandwidth: remainingMemBW,
					},
				)

				if err != nil {
					logger.WithError(err).Warn("Failed to create isolation class, continuing without isolation")
				} else {
					status.IsolationCreated = true

					// Move other containers to isolation class based on force flag
					for _, other := range otherContainers {
						if other.Socket == allocRange.SocketID {
							// Check if we should move this container
							currentClass, err := rdtAccountant.GetContainerClass(other.PID)
							if err != nil {
								logger.WithError(err).WithField("pid", other.PID).Debug("Failed to get container class")
								continue
							}

							// Only move if in system/default OR if force is enabled
							if currentClass == "system/default" || currentClass == "" || allocRange.ForceReallocation {
								if err := rdtAccountant.MoveContainer(other.PID, isolationClassName); err != nil {
									logger.WithError(err).WithFields(logrus.Fields{
										"pid":           other.PID,
										"current_class": currentClass,
									}).Warn("Failed to move container to isolation class")
								} else {
									logger.WithFields(logrus.Fields{
										"pid":        other.PID,
										"from_class": currentClass,
										"to_class":   isolationClassName,
									}).Debug("Moved container to isolation class")
								}
							} else {
								logger.WithFields(logrus.Fields{
									"pid":   other.PID,
									"class": currentClass,
								}).Debug("Skipping container in non-default class (force=false)")
							}
						}
					}
				}
			} else {
				// Subsequent allocations
				err := rdtAccountant.UpdateClass(isolationClassName,
					&accounting.AllocationRequest{
						L3Ways:       isolationWaysRange,
						MemBandwidth: remainingMemBW,
					},
					&accounting.AllocationRequest{
						L3Ways:       isolationWaysRange,
						MemBandwidth: remainingMemBW,
					},
				)
				if err != nil {
					logger.WithError(err).Warn("Failed to update isolation class")
				}
			}
		}
	}

	// Step 4: Wait for the allocation duration and collect metrics
	startStepNumber := getLatestStepNumber(dataframes, targetContainer.Index)

	time.Sleep(time.Duration(allocRange.DurationPerAlloc) * time.Millisecond)

	endStepNumber := getLatestStepNumber(dataframes, targetContainer.Index)

	allocResult.Duration = time.Since(allocResult.Started)

	// Step 5: Compute performance metrics from dataframes
	computeAllocationMetrics(allocResult, dataframes, targetContainer.Index, startStepNumber, endStepNumber)

	// Note: We do NOT cleanup probe classes here
	// The scheduler or ProbeAllocation caller will clean them up at the end

	return allocResult, status, nil
}

// getLatestStepNumber returns the latest step number for a container
func getLatestStepNumber(dataframes *dataframe.DataFrames, containerIndex int) int {
	containerDF := dataframes.GetContainer(containerIndex)
	if containerDF == nil {
		return 0
	}

	steps := containerDF.GetAllSteps()
	maxStep := 0
	for stepNum := range steps {
		if stepNum > maxStep {
			maxStep = stepNum
		}
	}
	return maxStep
}

// removeOutliers removes the top 3 and bottom 3 values from a slice if it has >= 20 elements
func removeOutliers(values []float64) []float64 {
	if len(values) < 20 {
		return values
	}

	// Sort the values
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	// Remove bottom 3 and top 3
	return sorted[3 : len(sorted)-3]
}

// computeAllocationMetrics calculates performance metrics from collected dataframes
func computeAllocationMetrics(
	result *AllocationResult,
	dataframes *dataframe.DataFrames,
	containerIndex int,
	startStep, endStep int,
) {
	containerDF := dataframes.GetContainer(containerIndex)
	if containerDF == nil {
		return
	}

	var ipcValues, theoreticalIPCValues, cacheMissRateValues, stalledCyclesValues, stallsL3MissPercentValues []float64
	var l3OccupancyValues, memBandwidthValues []uint64

	steps := containerDF.GetAllSteps()
	for stepNum := startStep + 1; stepNum <= endStep; stepNum++ {
		step, exists := steps[stepNum]
		if !exists || step == nil {
			continue
		}

		result.DataFrameSteps = append(result.DataFrameSteps, stepNum)

		// Collect performance metrics
		if step.Perf != nil {
			if step.Perf.InstructionsPerCycle != nil {
				ipcValues = append(ipcValues, *step.Perf.InstructionsPerCycle)
			}
			if step.Perf.TheoreticalIPC != nil {
				theoreticalIPCValues = append(theoreticalIPCValues, *step.Perf.TheoreticalIPC)
			}
			if step.Perf.CacheMissRate != nil {
				cacheMissRateValues = append(cacheMissRateValues, *step.Perf.CacheMissRate)
			}
			if step.Perf.StalledCyclesPercent != nil {
				stalledCyclesValues = append(stalledCyclesValues, *step.Perf.StalledCyclesPercent)
			}
			if step.Perf.StallsL3MissPercent != nil {
				stallsL3MissPercentValues = append(stallsL3MissPercentValues, *step.Perf.StallsL3MissPercent)
			}
		}

		if step.RDT != nil {
			if step.RDT.L3OccupancyPerSocket != nil {
				if occ, ok := step.RDT.L3OccupancyPerSocket[result.SocketID]; ok {
					l3OccupancyValues = append(l3OccupancyValues, occ)
				}
			}
			if step.RDT.MemoryBandwidthTotalPerSocket != nil {
				if bw, ok := step.RDT.MemoryBandwidthTotalPerSocket[result.SocketID]; ok {
					memBandwidthValues = append(memBandwidthValues, bw)
				}
			}
		}
	}

	// Remove outliers from float metrics
	ipcValues = removeOutliers(ipcValues)
	theoreticalIPCValues = removeOutliers(theoreticalIPCValues)
	cacheMissRateValues = removeOutliers(cacheMissRateValues)
	stalledCyclesValues = removeOutliers(stalledCyclesValues)
	stallsL3MissPercentValues = removeOutliers(stallsL3MissPercentValues)

	// Calculate averages from filtered values
	if len(ipcValues) > 0 {
		var sum float64
		for _, v := range ipcValues {
			sum += v
		}
		result.AvgIPC = sum / float64(len(ipcValues))
	}

	if len(theoreticalIPCValues) > 0 {
		var sum float64
		for _, v := range theoreticalIPCValues {
			sum += v
		}
		result.AvgTheoreticalIPC = sum / float64(len(theoreticalIPCValues))
	}

	if len(cacheMissRateValues) > 0 {
		var sum float64
		for _, v := range cacheMissRateValues {
			sum += v
		}
		result.AvgCacheMissRate = sum / float64(len(cacheMissRateValues))
	}

	if len(stalledCyclesValues) > 0 {
		var sum float64
		for _, v := range stalledCyclesValues {
			sum += v
		}
		result.AvgStalledCycles = sum / float64(len(stalledCyclesValues))
	}

	if len(stallsL3MissPercentValues) > 0 {
		var sum float64
		for _, v := range stallsL3MissPercentValues {
			sum += v
		}
		result.AvgStallsL3MissPercent = sum / float64(len(stallsL3MissPercentValues))
	}

	if len(l3OccupancyValues) > 0 {
		var sum uint64
		for _, v := range l3OccupancyValues {
			sum += v
		}
		result.AvgL3Occupancy = sum / uint64(len(l3OccupancyValues))
	}

	if len(memBandwidthValues) > 0 {
		var sum uint64
		for _, v := range memBandwidthValues {
			sum += v
		}
		result.AvgMemBandwidthUsed = sum / uint64(len(memBandwidthValues))
	}

	// Calculate IPC efficiency
	if result.AvgTheoreticalIPC > 0 {
		result.IPCEfficiency = result.AvgIPC / result.AvgTheoreticalIPC
	}
}
