package kernels

import (
	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/logging"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/sirupsen/logrus"
)

// DefaultProbeKernel implements a comprehensive sensitivity analysis
// by restarting the probing container with different stress-ng workloads
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
// with different stress-ng commands and measuring the target container's performance
func (dpk *DefaultProbeKernel) ExecuteProbe(
	ctx context.Context,
	dockerClient *client.Client,
	probingContainerID string,
	totalTime time.Duration,
	cores string,
	dataframes *dataframe.DataFrames,
	targetContainerIndex int,
	targetContainerConfig *config.ContainerConfig,
) (*ProbeSensitivities, error) {

	result := &ProbeSensitivities{}

	// Divide time into 11 segments (baseline + 10 tests)
	segmentTime := totalTime / 11

	// Get container dataframe for the TARGET container (the one being probed)
	containerDF := dataframes.GetContainer(targetContainerIndex)
	if containerDF == nil {
		return nil, fmt.Errorf("container dataframe not found for index %d", targetContainerIndex)
	}

	// Record starting dataframe step
	startStep := dpk.getCurrentMaxStep(containerDF)
	result.FirstDataframeStep = startStep

	dpk.logger.WithFields(logrus.Fields{
		"target_container_index": targetContainerIndex,
		"probing_container_id":   probingContainerID[:12],
		"total_time":             totalTime,
		"segment_time":           segmentTime,
	}).Debug("Starting probe kernel execution sequence")

	// 1. Baseline: No interference, just measure target performance
	baselineIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "", segmentTime)
	if err != nil {
		return nil, fmt.Errorf("baseline measurement failed: %w", err)
	}

	dpk.logger.WithField("baseline_ipc", baselineIPC).Info("Baseline IPC established")

	// 2. CPU Integer: stress-ng --cpu
	cpuIntIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --cpu 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - cpuIntIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.CPUInteger = &val
		dpk.logger.WithFields(logrus.Fields{
			"baseline_ipc": baselineIPC,
			"stressed_ipc": cpuIntIPC,
			"sensitivity":  val,
		}).Info("CPU Integer sensitivity calculated")
	}

	// 3. CPU Float: stress-ng --matrixprod
	cpuFloatIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --matrixprod 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - cpuFloatIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.CPUFloat = &val
	}

	// 4. LLC: stress-ng --cache
	llcIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --matrix-3d 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - llcIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.LLC = &val
		dpk.logger.WithFields(logrus.Fields{
			"baseline_ipc": baselineIPC,
			"stressed_ipc": llcIPC,
			"sensitivity":  val,
		}).Info("LLC sensitivity calculated")
	}

	// 5. Memory Read: stress-ng --stream
	memReadIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --stream 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - memReadIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.MemRead = &val
	}

	// 6. Memory Write: stress-ng --vm
	memWriteIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --vm 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - memWriteIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.MemWrite = &val
	}

	// 7. Store Buffer: stress-ng --memcpy
	sbIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --memcpy 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - sbIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.StoreBuffer = &val
	}

	// 8. Scoreboard: stress-ng --branch
	scoreboardIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --branch 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - scoreboardIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.Scoreboard = &val
	}

	// 9. Network Read: stress-ng --sock
	netReadIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --sock 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - netReadIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.NetworkRead = &val
	}

	// 10. Network Write: stress-ng --sock (same)
	netWriteIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --sock 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - netWriteIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.NetworkWrite = &val
	}

	// 11. SysCall: stress-ng --x86syscall
	syscallIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --x86syscall 1", segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - syscallIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.SysCall = &val
	}

	// Record ending dataframe step
	endStep := dpk.getCurrentMaxStep(containerDF)
	result.LastDataframeStep = endStep

	return result, nil
}

// measureWithWorkload runs a workload in the probing container and measures target performance
func (dpk *DefaultProbeKernel) measureWithWorkload(
	ctx context.Context,
	dockerClient *client.Client,
	probingContainerID string,
	targetContainerDF *dataframe.ContainerDataFrame,
	cores string,
	command string,
	duration time.Duration,
) (float64, error) {

	// Get current max step before test
	stepBefore := dpk.getCurrentMaxStep(targetContainerDF)

	if command == "" {
		// Baseline: no interference, just sleep
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
			return 0, fmt.Errorf("failed to create exec for command '%s': %w", command, err)
		}

		if err := dockerClient.ContainerExecStart(ctx, execResp.ID, types.ExecStartCheck{}); err != nil {
			return 0, fmt.Errorf("failed to start exec for command '%s': %w", command, err)
		}

		dpk.logger.WithField("exec_id", execResp.ID[:12]).Debug("Stress command started, waiting for completion")

		// Wait for the stress test to complete
		time.Sleep(duration)
	}

	// Get current max step after test
	stepAfter := dpk.getCurrentMaxStep(targetContainerDF)

	// Calculate average IPC of the TARGET container during this period
	avgIPC := dpk.getAvgIPC(targetContainerDF, stepBefore, stepAfter)

	dpk.logger.WithFields(logrus.Fields{
		"step_before": stepBefore,
		"step_after":  stepAfter,
		"avg_ipc":     avgIPC,
		"command":     command,
	}).Debug("Workload measurement completed")

	return avgIPC, nil
}

// getCurrentMaxStep returns the current maximum step number in the dataframe
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

// getAvgIPC calculates average IPC from dataframes between two step numbers
func (dpk *DefaultProbeKernel) getAvgIPC(containerDF *dataframe.ContainerDataFrame, fromStep, toStep int) float64 {
	steps := containerDF.GetAllSteps()

	var totalIPC float64
	var count int

	dpk.logger.WithFields(logrus.Fields{
		"from_step":   fromStep,
		"to_step":     toStep,
		"total_steps": len(steps),
	}).Debug("Calculating average IPC")

	for stepNum, step := range steps {
		if stepNum > fromStep && stepNum <= toStep {
			if step != nil && step.Perf != nil && step.Perf.InstructionsPerCycle != nil {
				totalIPC += *step.Perf.InstructionsPerCycle
				count++
				dpk.logger.WithFields(logrus.Fields{
					"step": stepNum,
					"ipc":  *step.Perf.InstructionsPerCycle,
				}).Trace("Found IPC data point")
			} else {
				dpk.logger.WithField("step", stepNum).Trace("Step has no IPC data")
			}
		}
	}

	avgIPC := float64(0)
	if count > 0 {
		avgIPC = totalIPC / float64(count)
	}

	dpk.logger.WithFields(logrus.Fields{
		"count":   count,
		"avg_ipc": avgIPC,
	}).Debug("Average IPC calculated")

	return avgIPC
}

// clampSensitivity ensures sensitivity values are in [0.0, 1.0] range
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
