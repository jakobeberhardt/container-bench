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

	mu           sync.Mutex
	probeResults []*ProbeResult
	logger       *logrus.Logger
}

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

// Performs the actual probing operation
func (p *Probe) executeProbe(ctx context.Context, req ProbeRequest) *ProbeResult {
	result := &ProbeResult{
		BenchmarkID:      p.benchmarkID,
		UsedProbeKernel:  p.probeKernel.GetName() + " v" + p.probeKernel.GetVersion(),
		ContainerID:      req.ContainerID,
		ContainerName:    req.ContainerConfig.GetContainerName(p.benchmarkID),
		ContainerIndex:   req.ContainerConfig.Index,
		ContainerCores:   req.ContainerConfig.Core,
		ContainerImage:   req.ContainerConfig.Image,
		ContainerCommand: req.ContainerConfig.Command,
		ProbeTime:        req.ProbeDuration,
		Isolated:         req.Isolated,
		Aborted:          false,
		Started:          time.Now(),
	}

	p.logger.WithFields(logrus.Fields{
		"container_index": req.ContainerConfig.Index,
		"probe_duration":  req.ProbeDuration,
		"probe_cores":     req.ProbeCores,
	}).Info("Starting probe")

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

	p.logger.WithFields(logrus.Fields{
		"probing_container_id": probingContainerID[:12],
		"cores":                req.ProbeCores,
	}).Debug("Probing container started, handing to kernel")

	result.ProbingContainerID = probingContainerID[:12]
	result.ProbingContainerName = fmt.Sprintf("probe-%d-container-%d", p.benchmarkID, req.ContainerConfig.Index)
	result.ProbingContainerCores = req.ProbeCores
	result.ProbingContainerSocket = req.ProbeSocket

	// Delegate to ProbeKernel to execute stress tests and analyze sensitivity
	p.logger.WithField("container_index", req.ContainerConfig.Index).Debug("Kernel executing probe sequence")
	sensitivityMap, err := p.probeKernel.ExecuteProbe(
		ctx,
		p.dockerClient,
		probingContainerID,
		req.ProbeDuration,
		req.ProbeCores,
		req.Dataframes,
		req.ContainerConfig.Index,
		req.ContainerConfig,
	)

	// Stop and remove probing container
	p.logger.WithField("probing_container_id", probingContainerID[:12]).Debug("Cleaning up probing container")
	p.stopProbingContainer(ctx, probingContainerID)

	if err != nil {
		p.logger.WithError(err).Error("Probe kernel execution failed")
		result.Finished = time.Now()
		result.Aborted = true
		now := time.Now()
		result.AbortedAt = &now
		return result
	}

	// Convert kernel sensitivities map to result format
	result.Sensitivities = make(map[string]*SensitivityMetrics)

	// Track dataframe range from first metric (they should all be the same)
	for metricType, sens := range sensitivityMap {
		result.FirstDataframeStep = sens.FirstDataframeStep
		result.LastDataframeStep = sens.LastDataframeStep

		result.Sensitivities[metricType] = &SensitivityMetrics{
			LLC:      sens.LLC,
			MemRead:  sens.MemRead,
			MemWrite: sens.MemWrite,
			SysCall:  sens.SysCall,
			Prefetch: sens.Prefetch,
		}
	}

	result.Finished = time.Now()

	// Log all available metrics
	for metricType, metrics := range result.Sensitivities {
		llcVal := "nil"
		if metrics.LLC != nil {
			llcVal = fmt.Sprintf("%.4f", *metrics.LLC)
		}
		memReadVal := "nil"
		if metrics.MemRead != nil {
			memReadVal = fmt.Sprintf("%.4f", *metrics.MemRead)
		}

		p.logger.WithFields(logrus.Fields{
			"container_index": req.ContainerConfig.Index,
			"metric_type":     metricType,
			"llc_sensitivity": llcVal,
			"mem_sensitivity": memReadVal,
		}).Info("Probe metric completed")
	}

	return result
}

// startProbingContainer that sleeps, ready for the kernel launch
func (p *Probe) startProbingContainer(ctx context.Context, cores string, socket int) (string, error) {
	p.logger.WithFields(logrus.Fields{
		"image": p.probeImage,
		"cores": cores,
	}).Debug("Creating probing container")

	// containerName := fmt.Sprintf("probe-%d-%d", p.benchmarkID, time.Now().Unix())
	containerName := "probe"

	config := &container.Config{
		Image: p.probeImage,
		Cmd:   []string{"sleep", "3600"},
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

	p.logger.WithFields(logrus.Fields{
		"container_id":   resp.ID[:12],
		"container_name": containerName,
	}).Debug("Probing container created and started")

	return resp.ID, nil
}

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

// Returns all probe results collected
func (p *Probe) GetResults() []*ProbeResult {
	p.mu.Lock()
	defer p.mu.Unlock()

	results := make([]*ProbeResult, len(p.probeResults))
	copy(results, p.probeResults)
	return results
}

// Returns the configured probe image, e.g. a image with stress-ng
func (p *Probe) GetProbeImage() string {
	return p.probeImage
}
