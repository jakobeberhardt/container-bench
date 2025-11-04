package probe

import (
"context"
"fmt"
"sync"
"time"

"container-bench/internal/config"
"container-bench/internal/dataframe"
"container-bench/internal/host"
"container-bench/internal/logging"
"container-bench/internal/probe/kernels"

"github.com/docker/docker/api/types"
"github.com/docker/docker/api/types/container"
"github.com/docker/docker/client"
"github.com/sirupsen/logrus"
)

// Probe orchestrates container sensitivity probing
type Probe struct {
dockerClient  *client.Client
probeKernel   kernels.ProbeKernel
hostConfig    *host.HostConfig
benchmarkID   int
benchmarkName string
probeImage    string

mu            sync.Mutex
probeResults  []*ProbeResult
logger        *logrus.Logger
}

// NewProbe creates a new Probe singleton
func NewProbe(
dockerClient *client.Client,
probeKernel kernels.ProbeKernel,
hostConfig *host.HostConfig,
benchmarkID int,
benchmarkName string,
probeImage string,
) *Probe {
return &Probe{
dockerClient:  dockerClient,
probeKernel:   probeKernel,
hostConfig:    hostConfig,
benchmarkID:   benchmarkID,
benchmarkName: benchmarkName,
probeImage:    probeImage,
probeResults:  make([]*ProbeResult, 0),
logger:        logging.GetLogger(),
}
}

// ProbeRequest contains parameters for a probing operation
type ProbeRequest struct {
ContainerConfig *config.ContainerConfig
ContainerID     string
Dataframes      *dataframe.DataFrames
ProbeDuration   time.Duration
ProbeCores      string
ProbeSocket     int
Isolated        bool
Abortable       bool
}

// Probe executes a sensitivity probe on a target container
// Returns a channel that will receive the ProbeResult asynchronously
func (p *Probe) Probe(ctx context.Context, req ProbeRequest) <-chan *ProbeResult {
resultChan := make(chan *ProbeResult, 1)

go func() {
result := p.executeProbe(ctx, req)

// Store result
p.mu.Lock()
p.probeResults = append(p.probeResults, result)
p.mu.Unlock()

resultChan <- result
close(resultChan)
}()

return resultChan
}

// executeProbe performs the actual probing operation
func (p *Probe) executeProbe(ctx context.Context, req ProbeRequest) *ProbeResult {
result := &ProbeResult{
BenchmarkID:     p.benchmarkID,
UsedProbeKernel: p.probeKernel.GetName() + " v" + p.probeKernel.GetVersion(),
ContainerID:     req.ContainerID,
ContainerName:   req.ContainerConfig.GetContainerName(p.benchmarkID),
ContainerIndex:  req.ContainerConfig.Index,
ContainerCores:  req.ContainerConfig.Core,
ContainerImage:  req.ContainerConfig.Image,
ContainerCommand: req.ContainerConfig.Command,
ProbeTime:       req.ProbeDuration,
Isolated:        req.Isolated,
Aborted:         false,
Started:         time.Now(),
}

p.logger.WithFields(logrus.Fields{
"container_index": req.ContainerConfig.Index,
"probe_duration":  req.ProbeDuration,
"probe_cores":     req.ProbeCores,
}).Info("Starting probe")

// Capture baseline dataframe steps (before probing)
containerDF := req.Dataframes.GetContainer(req.ContainerConfig.Index)
if containerDF == nil {
p.logger.WithField("container_index", req.ContainerConfig.Index).Error("Container dataframe not found")
result.Finished = time.Now()
result.Aborted = true
now := time.Now()
result.AbortedAt = &now
return result
}

// Get current step number as baseline start
baselineSteps := containerDF.GetAllSteps()
maxBaselineStep := 0
for step := range baselineSteps {
if step > maxBaselineStep {
maxBaselineStep = step
}
}

result.FirstDataframeStep = maxBaselineStep

// Start probing container
probingContainerID, err := p.startProbingContainer(ctx, req.ProbeCores, req.ProbeSocket)
if err != nil {
p.logger.WithError(err).Error("Failed to start probing container")
result.Finished = time.Now()
result.Aborted = true
now := time.Now()
result.AbortedAt = &now
return result
}

result.ProbingContainerID = probingContainerID[:12]
result.ProbingContainerName = fmt.Sprintf("probe-%d-container-%d", p.benchmarkID, req.ContainerConfig.Index)
result.ProbingContainerCores = req.ProbeCores
result.ProbingContainerSocket = req.ProbeSocket

// Wait for probe duration
time.Sleep(req.ProbeDuration)

// Stop and remove probing container
p.stopProbingContainer(ctx, probingContainerID)

// Capture probing dataframe steps (during probing)
probingSteps := containerDF.GetAllSteps()
maxProbingStep := 0
for step := range probingSteps {
if step > maxProbingStep {
maxProbingStep = step
}
}

result.LastDataframeStep = maxProbingStep

// Extract baseline and probing step ranges
baselineRange := make(map[int]*dataframe.SamplingStep)
probingRange := make(map[int]*dataframe.SamplingStep)

for step, data := range baselineSteps {
if step <= maxBaselineStep {
baselineRange[step] = data
}
}

for step, data := range probingSteps {
if step > maxBaselineStep && step <= maxProbingStep {
probingRange[step] = data
}
}

// Analyze sensitivity using probe kernel
cpuInt, cpuFloat, llc, memRead, memWrite, storeBuffer, scoreboard, netRead, netWrite, sysCall := 
p.probeKernel.AnalyzeSensitivity(baselineRange, probingRange, req.ContainerConfig)

result.CPUInteger = cpuInt
result.CPUFloat = cpuFloat
result.LLC = llc
result.MemRead = memRead
result.MemWrite = memWrite
result.StoreBuffer = storeBuffer
result.Scoreboard = scoreboard
result.NetworkRead = netRead
result.NetworkWrite = netWrite
result.SysCall = sysCall

result.Finished = time.Now()

p.logger.WithFields(logrus.Fields{
"container_index": req.ContainerConfig.Index,
"llc_sensitivity": llc,
"cpu_sensitivity": cpuInt,
}).Info("Probe completed")

return result
}

// startProbingContainer starts a stress-ng container for interference
func (p *Probe) startProbingContainer(ctx context.Context, cores string, socket int) (string, error) {
p.logger.WithFields(logrus.Fields{
"image": p.probeImage,
"cores": cores,
}).Debug("Starting probing container")

	// Create container with stress-ng LLC stressor
	containerName := fmt.Sprintf("probe-%d-%d", p.benchmarkID, time.Now().Unix())
	
	config := &container.Config{
		Image: p.probeImage,
		Cmd:   []string{"stress-ng", "--l3cache", "0", "--timeout", "3600s"},
	}
	
	hostConfig := &container.HostConfig{}
	hostConfig.CpusetCpus = cores
	hostConfig.AutoRemove = false
	
	resp, err := p.dockerClient.ContainerCreate(ctx, config, hostConfig, nil, nil, containerName)
if err != nil {
return "", fmt.Errorf("failed to create probing container: %w", err)
}

if err := p.dockerClient.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
p.dockerClient.ContainerRemove(ctx, resp.ID, types.ContainerRemoveOptions{Force: true})
return "", fmt.Errorf("failed to start probing container: %w", err)
}

p.logger.WithField("container_id", resp.ID[:12]).Debug("Probing container started")

return resp.ID, nil
}

// stopProbingContainer stops and removes a probing container
func (p *Probe) stopProbingContainer(ctx context.Context, containerID string) {
p.logger.WithField("container_id", containerID[:12]).Debug("Stopping probing container")

removeOpts := types.ContainerRemoveOptions{
Force:         true,
RemoveVolumes: true,
}

if err := p.dockerClient.ContainerRemove(ctx, containerID, removeOpts); err != nil {
p.logger.WithField("container_id", containerID[:12]).WithError(err).Warn("Failed to remove probing container")
}
}

// GetResults returns all probe results collected
func (p *Probe) GetResults() []*ProbeResult {
p.mu.Lock()
defer p.mu.Unlock()

results := make([]*ProbeResult, len(p.probeResults))
copy(results, p.probeResults)
return results
}

// GetProbeImage returns the configured probe image
func (p *Probe) GetProbeImage() string {
return p.probeImage
}
