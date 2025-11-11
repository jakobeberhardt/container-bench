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
) (*ProbeSensitivities, error) {

	result := &ProbeSensitivities{}

	// Divide time into 6 segments (baseline + 5 tests)
	segmentTime := totalTime / 6

	// Get container dataframe for the victim container
	containerDF := dataframes.GetContainer(targetContainerIndex)
	if containerDF == nil {
		return nil, fmt.Errorf("container dataframe not found for index %d", targetContainerIndex)
	}

	// starting dataframe step
	startStep := dpk.getCurrentMaxStep(containerDF)
	result.FirstDataframeStep = startStep

	dpk.logger.WithFields(logrus.Fields{
		"target_container_index": targetContainerIndex,
		"probing_container_id":   probingContainerID[:12],
		"total_time":             totalTime,
		"segment_time":           segmentTime,
	}).Debug("Starting probe kernel execution sequence")

	// 1. Baseline
	baselineIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "", segmentTime)
	if err != nil {
		return nil, fmt.Errorf("baseline measurement failed: %w", err)
	}

	dpk.logger.WithField("baseline_ipc", baselineIPC).Info("Baseline IPC established")

	// 2. LLC
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

	// 3. Memory Read
	memReadIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --stream 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - memReadIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.MemRead = &val
		dpk.logger.WithFields(logrus.Fields{
			"baseline_ipc": baselineIPC,
			"stressed_ipc": memReadIPC,
			"sensitivity":  val,
		}).Info("Memory read sensitivity calculated")
	}

	// 4. Memory Write
	memWriteIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --vm 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - memWriteIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.MemWrite = &val
		dpk.logger.WithFields(logrus.Fields{
			"baseline_ipc": baselineIPC,
			"stressed_ipc": memWriteIPC,
			"sensitivity":  val,
		}).Info("Memory write sensitivity calculated")
	}

	// 5. SysCall (contention on the kernel)
	syscallIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --x86syscall 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - syscallIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.SysCall = &val
		dpk.logger.WithFields(logrus.Fields{
			"baseline_ipc": baselineIPC,
			"stressed_ipc": syscallIPC,
			"sensitivity":  val,
		}).Info("SysCall sensitivity calculated")
	}

	// 6. Prefetch
	prefetchIPC, err := dpk.measureWithWorkload(ctx, dockerClient, probingContainerID, containerDF,
		cores, "stress-ng --prefetch 0", segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - prefetchIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.Prefetch = &val
		dpk.logger.WithFields(logrus.Fields{
			"baseline_ipc": baselineIPC,
			"stressed_ipc": prefetchIPC,
			"sensitivity":  val,
		}).Info("Prefetch sensitivity calculated")
	}

	// Record ending
	endStep := dpk.getCurrentMaxStep(containerDF)
	result.LastDataframeStep = endStep

	return result, nil
}

// execute micro benchmark
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

	// Calculate average IPC of the victim container during this period
	avgIPC := dpk.getAvgIPC(targetContainerDF, stepBefore, stepAfter)

	dpk.logger.WithFields(logrus.Fields{
		"step_before": stepBefore,
		"step_after":  stepAfter,
		"avg_ipc":     avgIPC,
		"command":     command,
	}).Debug("Workload measurement completed")

	return avgIPC, nil
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
