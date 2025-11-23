package kernels

import (
	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/logging"
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/sirupsen/logrus"
)

type DefaultProbeKernel struct {
	name    string
	version string
	logger  *logrus.Logger
}

func NewDefaultProbeKernel() *DefaultProbeKernel {
	return &DefaultProbeKernel{
		name:    "default",
		version: "1.0.0",
		logger:  logging.GetLogger(),
	}
}

func (dpk *DefaultProbeKernel) GetName() string {
	return dpk.name
}

func (dpk *DefaultProbeKernel) GetVersion() string {
	return dpk.version
}

// ExecuteProbe runs a sequence of stress tests by restarting the probing container
func (dpk *DefaultProbeKernel) ExecuteProbe(
	ctx context.Context,
	dockerClient *client.Client,
	probingContainerID string,
	totalTime time.Duration,
	cores string,
	dataframes *dataframe.DataFrames,
	targetContainerIndex int,
	targetContainerConfig *config.ContainerConfig,
) (map[string]*ProbeSensitivities, error) {

	results := make(map[string]*ProbeSensitivities)
	ipcResult := &ProbeSensitivities{}
	scpResult := &ProbeSensitivities{}

	// Divide time into 6 segments (baseline + 5 tests)
	segmentTime := totalTime / 6

	// Get container dataframe for the victim container
	containerDF := dataframes.GetContainer(targetContainerIndex)
	if containerDF == nil {
		return nil, fmt.Errorf("container dataframe not found for index %d", targetContainerIndex)
	}

	// starting dataframe step
	startStep := dpk.getCurrentMaxStep(containerDF)
	ipcResult.FirstDataframeStep = startStep
	scpResult.FirstDataframeStep = startStep

	dpk.logger.WithFields(logrus.Fields{
		"target_container_index": targetContainerIndex,
		"probing_container_id":   probingContainerID[:12],
		"total_time":             totalTime,
		"segment_time":           segmentTime,
	}).Debug("Starting probe kernel execution sequence")

	// 1. Baseline
	baselineIPC, baselineSCP, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "", segmentTime)
	if err != nil {
		return nil, fmt.Errorf("baseline measurement failed: %w", err)
	}

	dpk.logger.WithFields(logrus.Fields{
		"baseline_ipc": baselineIPC,
		"baseline_scp": baselineSCP,
	}).Info("Baseline metrics established")

	// 2. LLC
	llcIPC, llcSCP, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --cache 0 --cache-level 3", segmentTime)
	if err == nil && baselineIPC > 0 {
		ipcSensitivity := (baselineIPC - llcIPC) / baselineIPC
		ipcVal := clampSensitivity(ipcSensitivity)
		ipcResult.LLC = &ipcVal
		dpk.logger.WithFields(logrus.Fields{
			"baseline_ipc": baselineIPC,
			"stressed_ipc": llcIPC,
			"sensitivity":  ipcVal,
		}).Info("LLC IPC sensitivity calculated")
	}
	if err == nil && baselineSCP > 0 {
		// SCP sensitivity: normalized to 0-1 where 0 is no impact and 1 is full degradation
		// Formula: (stressed_scp - baseline_scp) / (100 - baseline_scp)
		// This represents how much of the remaining "headroom" was consumed
		scpSensitivity := (llcSCP - baselineSCP) / (100.0 - baselineSCP)
		scpVal := clampSensitivity(scpSensitivity)
		scpResult.LLC = &scpVal
		dpk.logger.WithFields(logrus.Fields{
			"baseline_scp": baselineSCP,
			"stressed_scp": llcSCP,
			"sensitivity":  scpVal,
		}).Info("LLC SCP sensitivity calculated")
	}

	// 3. Memory Read
	memReadIPC, memReadSCP, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --stream 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		ipcSensitivity := (baselineIPC - memReadIPC) / baselineIPC
		ipcVal := clampSensitivity(ipcSensitivity)
		ipcResult.MemRead = &ipcVal
		dpk.logger.WithFields(logrus.Fields{
			"baseline_ipc": baselineIPC,
			"stressed_ipc": memReadIPC,
			"sensitivity":  ipcVal,
		}).Info("Memory read IPC sensitivity calculated")
	}
	if err == nil && baselineSCP > 0 {
		scpSensitivity := (memReadSCP - baselineSCP) / (100.0 - baselineSCP)
		scpVal := clampSensitivity(scpSensitivity)
		scpResult.MemRead = &scpVal
		dpk.logger.WithFields(logrus.Fields{
			"baseline_scp": baselineSCP,
			"stressed_scp": memReadSCP,
			"sensitivity":  scpVal,
		}).Info("Memory read SCP sensitivity calculated")
	}

	// 4. Memory Write
	memWriteIPC, memWriteSCP, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --vm 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		ipcSensitivity := (baselineIPC - memWriteIPC) / baselineIPC
		ipcVal := clampSensitivity(ipcSensitivity)
		ipcResult.MemWrite = &ipcVal
		dpk.logger.WithFields(logrus.Fields{
			"baseline_ipc": baselineIPC,
			"stressed_ipc": memWriteIPC,
			"sensitivity":  ipcVal,
		}).Info("Memory write IPC sensitivity calculated")
	}
	if err == nil && baselineSCP > 0 {
		scpSensitivity := (memWriteSCP - baselineSCP) / (100.0 - baselineSCP)
		scpVal := clampSensitivity(scpSensitivity)
		scpResult.MemWrite = &scpVal
		dpk.logger.WithFields(logrus.Fields{
			"baseline_scp": baselineSCP,
			"stressed_scp": memWriteSCP,
			"sensitivity":  scpVal,
		}).Info("Memory write SCP sensitivity calculated")
	}

	// 5. SysCall (contention on the kernel)
	syscallIPC, syscallSCP, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --x86syscall 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		ipcSensitivity := (baselineIPC - syscallIPC) / baselineIPC
		ipcVal := clampSensitivity(ipcSensitivity)
		ipcResult.SysCall = &ipcVal
		dpk.logger.WithFields(logrus.Fields{
			"baseline_ipc": baselineIPC,
			"stressed_ipc": syscallIPC,
			"sensitivity":  ipcVal,
		}).Info("SysCall IPC sensitivity calculated")
	}
	if err == nil && baselineSCP > 0 {
		scpSensitivity := (syscallSCP - baselineSCP) / (100.0 - baselineSCP)
		scpVal := clampSensitivity(scpSensitivity)
		scpResult.SysCall = &scpVal
		dpk.logger.WithFields(logrus.Fields{
			"baseline_scp": baselineSCP,
			"stressed_scp": syscallSCP,
			"sensitivity":  scpVal,
		}).Info("SysCall SCP sensitivity calculated")
	}

	// 6. Prefetch
	prefetchIPC, prefetchSCP, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --prefetch 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		ipcSensitivity := (baselineIPC - prefetchIPC) / baselineIPC
		ipcVal := clampSensitivity(ipcSensitivity)
		ipcResult.Prefetch = &ipcVal
		dpk.logger.WithFields(logrus.Fields{
			"baseline_ipc": baselineIPC,
			"stressed_ipc": prefetchIPC,
			"sensitivity":  ipcVal,
		}).Info("Prefetch IPC sensitivity calculated")
	}
	if err == nil && baselineSCP > 0 {
		scpSensitivity := (prefetchSCP - baselineSCP) / (100.0 - baselineSCP)
		scpVal := clampSensitivity(scpSensitivity)
		scpResult.Prefetch = &scpVal
		dpk.logger.WithFields(logrus.Fields{
			"baseline_scp": baselineSCP,
			"stressed_scp": prefetchSCP,
			"sensitivity":  scpVal,
		}).Info("Prefetch SCP sensitivity calculated")
	}

	// Record ending
	endStep := dpk.getCurrentMaxStep(containerDF)
	ipcResult.LastDataframeStep = endStep
	scpResult.LastDataframeStep = endStep

	results["ipc"] = ipcResult
	results["scp"] = scpResult

	return results, nil
}

// execute micro benchmark and return both IPC and SCP metrics
func (dpk *DefaultProbeKernel) measureWithWorkload(
	ctx context.Context,
	dockerClient *client.Client,
	probingContainerID string,
	targetContainerDF *dataframe.ContainerDataFrame,
	cores string,
	command string,
	duration time.Duration,
) (float64, float64, error) {

	// Get current max step before test
	stepBefore := dpk.getCurrentMaxStep(targetContainerDF)

	if command == "" {
		dpk.logger.WithField("duration", duration).Debug("Running baseline measurement (no interference)")
		time.Sleep(duration)
	} else {
		// Execute stress command in probing container
		fullCommand := fmt.Sprintf("%s --timeout %ds", command, int(duration.Seconds()))

		dpk.logger.WithFields(logrus.Fields{
			"command":              fullCommand,
			"probing_container_id": probingContainerID[:12],
			"duration":             duration,
		}).Debug("Executing stress command in probing container")

		execConfig := types.ExecConfig{
			Cmd:          []string{"sh", "-c", fullCommand},
			AttachStdout: false,
			AttachStderr: false,
		}

		execResp, err := dockerClient.ContainerExecCreate(ctx, probingContainerID, execConfig)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to create exec for command '%s': %w", command, err)
		}

		if err := dockerClient.ContainerExecStart(ctx, execResp.ID, types.ExecStartCheck{}); err != nil {
			return 0, 0, fmt.Errorf("failed to start exec for command '%s': %w", command, err)
		}

		dpk.logger.WithField("exec_id", execResp.ID[:12]).Debug("Stress command started, waiting for completion")

		// Wait for the stress test to complete
		time.Sleep(duration)
	}

	// Get current max step after test
	stepAfter := dpk.getCurrentMaxStep(targetContainerDF)

	// Calculate average metrics of the victim container during this period
	avgIPC := dpk.getAvgIPC(targetContainerDF, stepBefore, stepAfter)
	avgSCP := dpk.getAvgSCP(targetContainerDF, stepBefore, stepAfter)

	dpk.logger.WithFields(logrus.Fields{
		"step_before": stepBefore,
		"step_after":  stepAfter,
		"avg_ipc":     avgIPC,
		"avg_scp":     avgSCP,
		"command":     command,
	}).Debug("Workload measurement completed")

	return avgIPC, avgSCP, nil
}

// the current maximum step number in the dataframe
func (dpk *DefaultProbeKernel) getCurrentMaxStep(containerDF *dataframe.ContainerDataFrame) int {
	steps := containerDF.GetAllSteps()
	maxStep := 0
	for step := range steps {
		if step > maxStep {
			maxStep = step
		}
	}
	return maxStep
}

func (dpk *DefaultProbeKernel) getAvgIPC(containerDF *dataframe.ContainerDataFrame, fromStep, toStep int) float64 {
	steps := containerDF.GetAllSteps()

	var values []float64

	dpk.logger.WithFields(logrus.Fields{
		"from_step":   fromStep,
		"to_step":     toStep,
		"total_steps": len(steps),
	}).Debug("Calculating average IPC")

	for stepNum, step := range steps {
		if stepNum > fromStep && stepNum <= toStep {
			if step != nil && step.Perf != nil && step.Perf.InstructionsPerCycle != nil {
				values = append(values, *step.Perf.InstructionsPerCycle)
				dpk.logger.WithFields(logrus.Fields{
					"step": stepNum,
					"ipc":  *step.Perf.InstructionsPerCycle,
				}).Trace("Found IPC data point")
			} else {
				dpk.logger.WithField("step", stepNum).Trace("Step has no IPC data")
			}
		}
	}

	avgIPC := dpk.calculateAverage(values, "IPC")

	dpk.logger.WithFields(logrus.Fields{
		"count":   len(values),
		"avg_ipc": avgIPC,
	}).Debug("Average IPC calculated")

	return avgIPC
}

func (dpk *DefaultProbeKernel) getAvgSCP(containerDF *dataframe.ContainerDataFrame, fromStep, toStep int) float64 {
	steps := containerDF.GetAllSteps()

	var values []float64

	dpk.logger.WithFields(logrus.Fields{
		"from_step":   fromStep,
		"to_step":     toStep,
		"total_steps": len(steps),
	}).Debug("Calculating average SCP")

	for stepNum, step := range steps {
		if stepNum > fromStep && stepNum <= toStep {
			if step != nil && step.Perf != nil && step.Perf.StalledCyclesPercent != nil {
				values = append(values, *step.Perf.StalledCyclesPercent)
				dpk.logger.WithFields(logrus.Fields{
					"step": stepNum,
					"scp":  *step.Perf.StalledCyclesPercent,
				}).Trace("Found SCP data point")
			} else {
				dpk.logger.WithField("step", stepNum).Trace("Step has no SCP data")
			}
		}
	}

	avgSCP := dpk.calculateAverage(values, "SCP")

	dpk.logger.WithFields(logrus.Fields{
		"count":   len(values),
		"avg_scp": avgSCP,
	}).Debug("Average SCP calculated")

	return avgSCP
}

func (dpk *DefaultProbeKernel) calculateAverage(values []float64, metricName string) float64 {
	if len(values) == 0 {
		return 0
	}

	// If we have > 30 samples, remove top and bottom 3 outliers
	if len(values) > 30 {
		// Sort values
		sorted := make([]float64, len(values))
		copy(sorted, values)
		sort.Float64s(sorted)

		// Remove bottom 3 and top 3
		trimmed := sorted[3 : len(sorted)-3]

		dpk.logger.WithFields(logrus.Fields{
			"metric":         metricName,
			"original_count": len(values),
			"trimmed_count":  len(trimmed),
			"removed_bottom": sorted[0:3],
			"removed_top":    sorted[len(sorted)-3:],
		}).Debug("Removed outliers from average calculation")

		values = trimmed
	}

	total := float64(0)
	for _, v := range values {
		total += v
	}

	return total / float64(len(values))
}

func clampSensitivity(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1.0 {
		return 1.0
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value
}
