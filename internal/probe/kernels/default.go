package kernels

import (
	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

// DefaultProbeKernel implements a comprehensive sensitivity analysis
// using stress-ng commands executed sequentially
type DefaultProbeKernel struct {
	name    string
	version string
}

func NewDefaultProbeKernel() *DefaultProbeKernel {
	return &DefaultProbeKernel{
		name:    "default",
		version: "1.0.0",
	}
}

func (dpk *DefaultProbeKernel) GetName() string {
	return dpk.name
}

func (dpk *DefaultProbeKernel) GetVersion() string {
	return dpk.version
}

// ExecuteProbe runs a sequence of stress tests and measures sensitivity
func (dpk *DefaultProbeKernel) ExecuteProbe(
	ctx context.Context,
	dockerClient *client.Client,
	containerID string,
	totalTime time.Duration,
	cores string,
	dataframes *dataframe.DataFrames,
	targetContainerIndex int,
	targetContainerConfig *config.ContainerConfig,
) (*ProbeSensitivities, error) {

	result := &ProbeSensitivities{}

	// Divide time into 11 segments (baseline + 10 tests)
	segmentTime := totalTime / 11

	// Get container dataframe
	containerDF := dataframes.GetContainer(targetContainerIndex)
	if containerDF == nil {
		return nil, fmt.Errorf("container dataframe not found for index %d", targetContainerIndex)
	}

	// Record starting dataframe step
	startStep := dpk.getCurrentMaxStep(containerDF)
	result.FirstDataframeStep = startStep

	// 1. Baseline: Sleep to establish baseline performance
	baselineIPC, err := dpk.runTest(ctx, dockerClient, containerID, containerDF,
		fmt.Sprintf("sleep %d", int(segmentTime.Seconds())), segmentTime)
	if err != nil {
		return nil, fmt.Errorf("baseline test failed: %w", err)
	}

	// 2. CPU Integer: stress-ng --cpu
	cpuIntIPC, err := dpk.runTest(ctx, dockerClient, containerID, containerDF,
		fmt.Sprintf("stress-ng --cpu 0 --timeout %ds", int(segmentTime.Seconds())), segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - cpuIntIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.CPUInteger = &val
	}

	// 3. CPU Float: stress-ng --matrixprod
	cpuFloatIPC, err := dpk.runTest(ctx, dockerClient, containerID, containerDF,
		fmt.Sprintf("stress-ng --matrixprod 0 --timeout %ds", int(segmentTime.Seconds())), segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - cpuFloatIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.CPUFloat = &val
	}

	// 4. LLC: stress-ng --cache
	llcIPC, err := dpk.runTest(ctx, dockerClient, containerID, containerDF,
		fmt.Sprintf("stress-ng --cache 0 --timeout %ds", int(segmentTime.Seconds())), segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - llcIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.LLC = &val
	}

	// 5. Memory Read: stress-ng --stream (simplified with hdd)
	memReadIPC, err := dpk.runTest(ctx, dockerClient, containerID, containerDF,
		fmt.Sprintf("stress-ng --hdd 0 --hdd-opts rd --timeout %ds", int(segmentTime.Seconds())), segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - memReadIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.MemRead = &val
	}

	// 6. Memory Write: stress-ng --hdd write
	memWriteIPC, err := dpk.runTest(ctx, dockerClient, containerID, containerDF,
		fmt.Sprintf("stress-ng --hdd 0 --hdd-opts wr --timeout %ds", int(segmentTime.Seconds())), segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - memWriteIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.MemWrite = &val
	}

	// 7. Store Buffer: stress-ng --vm (simplified)
	sbIPC, err := dpk.runTest(ctx, dockerClient, containerID, containerDF,
		fmt.Sprintf("stress-ng --vm 0 --timeout %ds", int(segmentTime.Seconds())), segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - sbIPC) / baselineIPC
		val := clampSensitivity(sensitivity * 0.5) // scaled down
		result.StoreBuffer = &val
	}

	// 8. Scoreboard: stress-ng --branch
	scoreboardIPC, err := dpk.runTest(ctx, dockerClient, containerID, containerDF,
		fmt.Sprintf("stress-ng --branch 0 --timeout %ds", int(segmentTime.Seconds())), segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - scoreboardIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.Scoreboard = &val
	}

	// 9. Network Read: stress-ng --sock (simplified)
	netReadIPC, err := dpk.runTest(ctx, dockerClient, containerID, containerDF,
		fmt.Sprintf("stress-ng --sock 0 --timeout %ds", int(segmentTime.Seconds())), segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - netReadIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.NetworkRead = &val
	}

	// 10. Network Write: stress-ng --sock (same as read, simplified)
	netWriteIPC, err := dpk.runTest(ctx, dockerClient, containerID, containerDF,
		fmt.Sprintf("stress-ng --sock 0 --timeout %ds", int(segmentTime.Seconds())), segmentTime)
	if err == nil && baselineIPC > 0 {
		sensitivity := (baselineIPC - netWriteIPC) / baselineIPC
		val := clampSensitivity(sensitivity)
		result.NetworkWrite = &val
	}

	// 11. SysCall: stress-ng --x86syscall
	syscallIPC, err := dpk.runTest(ctx, dockerClient, containerID, containerDF,
		fmt.Sprintf("stress-ng --x86syscall 1 --timeout %ds", int(segmentTime.Seconds())), segmentTime)
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

// runTest executes a command in the container and returns average IPC during execution
func (dpk *DefaultProbeKernel) runTest(
	ctx context.Context,
	dockerClient *client.Client,
	containerID string,
	containerDF *dataframe.ContainerDataFrame,
	command string,
	duration time.Duration,
) (float64, error) {

	// Get current max step before test
	stepBefore := dpk.getCurrentMaxStep(containerDF)

	// Execute command in container
	execConfig := types.ExecConfig{
		Cmd:          []string{"sh", "-c", command},
		AttachStdout: false,
		AttachStderr: false,
		Detach:       false,
	}

	execResp, err := dockerClient.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return 0, fmt.Errorf("failed to create exec: %w", err)
	}

	if err := dockerClient.ContainerExecStart(ctx, execResp.ID, types.ExecStartCheck{}); err != nil {
		return 0, fmt.Errorf("failed to start exec: %w", err)
	}

	// Wait for duration to allow metrics to be collected
	time.Sleep(duration)

	// Get current max step after test
	stepAfter := dpk.getCurrentMaxStep(containerDF)

	// Calculate average IPC during this period
	avgIPC := dpk.getAvgIPC(containerDF, stepBefore, stepAfter)

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

	for stepNum, step := range steps {
		if stepNum > fromStep && stepNum <= toStep {
			if step != nil && step.Perf != nil && step.Perf.InstructionsPerCycle != nil {
				totalIPC += *step.Perf.InstructionsPerCycle
				count++
			}
		}
	}

	if count == 0 {
		return 0
	}
	return totalIPC / float64(count)
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
