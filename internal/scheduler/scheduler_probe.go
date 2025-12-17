package scheduler

import (
	"container-bench/internal/accounting"
	"container-bench/internal/config"
	"container-bench/internal/cpuallocator"
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/probe"
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

type ProbeScheduler struct {
	name            string
	version         string
	schedulerLogger *logrus.Logger
	hostConfig      *host.HostConfig
	containers      []ContainerInfo
	rdtAccountant   *accounting.RDTAccountant
	prober          *probe.Probe
	config          *config.SchedulerConfig

	// Probing state
	probingStarted      bool
	nextProbeIndex      int
	activeProbe         <-chan *probe.ProbeResult
	lastProbeFinishTime time.Time
	warmupCompleteTime  time.Time

	cpuAllocator cpuallocator.Allocator
}

func NewProbeScheduler() *ProbeScheduler {
	return &ProbeScheduler{
		name:            "probe",
		version:         "1.0.0",
		schedulerLogger: logging.GetSchedulerLogger(),
		probingStarted:  false,
		nextProbeIndex:  0,
	}
}

func (ps *ProbeScheduler) Initialize(accountant *accounting.RDTAccountant, containers []ContainerInfo, schedulerConfig *config.SchedulerConfig) error {
	ps.rdtAccountant = accountant
	ps.containers = containers
	ps.config = schedulerConfig

	// Set warmup complete time
	warmupSeconds := 0
	if schedulerConfig != nil && schedulerConfig.Prober != nil && schedulerConfig.Prober.WarmupT > 0 {
		warmupSeconds = schedulerConfig.Prober.WarmupT
	}
	ps.warmupCompleteTime = time.Now().Add(time.Duration(warmupSeconds) * time.Second)

	ps.schedulerLogger.WithFields(logrus.Fields{
		"containers":     len(containers),
		"warmup_seconds": warmupSeconds,
	}).Info("Probe scheduler initialized")
	return nil
}

func (ps *ProbeScheduler) ProcessDataFrames(dataframes *dataframe.DataFrames) error {
	if ps.prober == nil {
		return nil
	}

	// Check warmup period
	if time.Now().Before(ps.warmupCompleteTime) {
		return nil
	}

	// Check if we have an active probe running
	if ps.activeProbe != nil {
		// Non-blocking check if probe completed
		select {
		case result := <-ps.activeProbe:
			// Log all available metrics from the probe result
			for metricType, metrics := range result.Sensitivities {
				llcVal := "nil"
				if metrics.LLC != nil {
					llcVal = fmt.Sprintf("%.4f", *metrics.LLC)
				}
				memReadVal := "nil"
				if metrics.MemRead != nil {
					memReadVal = fmt.Sprintf("%.4f", *metrics.MemRead)
				}
				memWriteVal := "nil"
				if metrics.MemWrite != nil {
					memWriteVal = fmt.Sprintf("%.4f", *metrics.MemWrite)
				}
				syscallVal := "nil"
				if metrics.SysCall != nil {
					syscallVal = fmt.Sprintf("%.4f", *metrics.SysCall)
				}
				prefetchVal := "nil"
				if metrics.Prefetch != nil {
					prefetchVal = fmt.Sprintf("%.4f", *metrics.Prefetch)
				}

				ps.schedulerLogger.WithFields(logrus.Fields{
					"container_index": result.ContainerIndex,
					"container_name":  result.ContainerName,
					"metric_type":     metricType,
					"llc_sensitivity": llcVal,
					"mem_read":        memReadVal,
					"mem_write":       memWriteVal,
					"syscall":         syscallVal,
					"prefetch":        prefetchVal,
				}).Info("Probe completed")
			}

			ps.activeProbe = nil
			ps.lastProbeFinishTime = time.Now()
		default:
			// Probe still running
			return nil
		}
	}

	// Check cooldown period
	cooldownSeconds := 0
	if ps.config != nil && ps.config.Prober != nil && ps.config.Prober.CooldownT > 0 {
		cooldownSeconds = ps.config.Prober.CooldownT
	}
	if !ps.lastProbeFinishTime.IsZero() {
		cooldownEnd := ps.lastProbeFinishTime.Add(time.Duration(cooldownSeconds) * time.Second)
		if time.Now().Before(cooldownEnd) {
			return nil
		}
	}

	// Start next probe if we have containers left to probe
	if ps.nextProbeIndex < len(ps.containers) {
		containerInfo := ps.containers[ps.nextProbeIndex]

		// Wait until the container is actually running
		if containerInfo.PID == 0 {
			return nil
		}

		// Only probe containers with index 0
		if containerInfo.Index != 0 {
			ps.schedulerLogger.WithFields(logrus.Fields{
				"container_index": containerInfo.Index,
				"container_name":  containerInfo.Config.GetContainerName(0),
			}).Info("Skipping probe for container (only probing index 0)")
			ps.nextProbeIndex++
			return nil
		}

		// Get probe configuration
		abortable := false
		isolated := true
		probeDuration := 30 * time.Second
		probeCores := "1"
		if ps.config != nil && ps.config.Prober != nil {
			abortable = ps.config.Prober.Abortable
			isolated = ps.config.Prober.Isolated
			if ps.config.Prober.DefaultT > 0 {
				probeDuration = time.Duration(ps.config.Prober.DefaultT) * time.Second
			}
			if ps.config.Prober.DefaultProbeCores != "" {
				probeCores = ps.config.Prober.DefaultProbeCores
			}
		}

		ps.schedulerLogger.WithFields(logrus.Fields{
			"container_index": containerInfo.Index,
			"container_name":  containerInfo.Config.GetContainerName(0),
			"probe_duration":  probeDuration,
			"probe_cores":     probeCores,
			"abortable":       abortable,
			"isolated":        isolated,
		}).Info("Starting probe")

		// Create probe request
		req := probe.ProbeRequest{
			ContainerConfig: containerInfo.Config,
			ContainerID:     containerInfo.ContainerID,
			Dataframes:      dataframes,
			ProbeDuration:   probeDuration,
			ProbeCores:      probeCores,
			ProbeSocket:     0,
			Isolated:        isolated,
			Abortable:       abortable,
		}

		// Launch probe asynchronously
		ps.activeProbe = ps.prober.Probe(context.Background(), req)
		ps.nextProbeIndex++
	}

	return nil
}

func (ps *ProbeScheduler) Shutdown() error {
	// Wait for active probe to complete
	if ps.activeProbe != nil {
		ps.schedulerLogger.Info("Waiting for active probe to complete before shutdown")
		result := <-ps.activeProbe

		// Log all available metrics from the final probe result
		for metricType, metrics := range result.Sensitivities {
			llcVal := "nil"
			if metrics.LLC != nil {
				llcVal = fmt.Sprintf("%.4f", *metrics.LLC)
			}
			memReadVal := "nil"
			if metrics.MemRead != nil {
				memReadVal = fmt.Sprintf("%.4f", *metrics.MemRead)
			}
			memWriteVal := "nil"
			if metrics.MemWrite != nil {
				memWriteVal = fmt.Sprintf("%.4f", *metrics.MemWrite)
			}

			ps.schedulerLogger.WithFields(logrus.Fields{
				"container_index": result.ContainerIndex,
				"container_name":  result.ContainerName,
				"metric_type":     metricType,
				"llc_sensitivity": llcVal,
				"mem_sensitivity": memReadVal,
				"mem_write":       memWriteVal,
			}).Info("Final probe completed")
		}
	}
	return nil
}

func (ps *ProbeScheduler) GetVersion() string {
	return ps.version
}

func (ps *ProbeScheduler) SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	ps.schedulerLogger.SetLevel(logLevel)
	return nil
}

func (ps *ProbeScheduler) SetHostConfig(hostConfig *host.HostConfig) {
	ps.hostConfig = hostConfig
}

func (ps *ProbeScheduler) SetCPUAllocator(allocator cpuallocator.Allocator) {
	ps.cpuAllocator = allocator
}

func (ps *ProbeScheduler) AssignCPUCores(containerIndex int) ([]int, error) {
	if ps.cpuAllocator == nil {
		return nil, nil
	}
	var cfg *config.ContainerConfig
	for i := range ps.containers {
		if ps.containers[i].Index == containerIndex {
			cfg = ps.containers[i].Config
			break
		}
	}
	if cfg == nil {
		return nil, nil
	}

	return ps.cpuAllocator.EnsureAssigned(containerIndex, cfg)
}

func (ps *ProbeScheduler) SetProbe(prober *probe.Probe) {
	ps.prober = prober
	ps.schedulerLogger.Info("Probe injected into scheduler")
}

func (ps *ProbeScheduler) SetBenchmarkID(benchmarkID int) {
	// Probe scheduler doesn't use benchmark ID
}

func (ps *ProbeScheduler) OnContainerStart(info ContainerInfo) error {
	for i := range ps.containers {
		if ps.containers[i].Index == info.Index {
			ps.containers[i].PID = info.PID
			ps.containers[i].ContainerID = info.ContainerID
			return nil
		}
	}
	ps.containers = append(ps.containers, info)
	return nil
}

func (ps *ProbeScheduler) OnContainerStop(containerIndex int) error {
	if ps.cpuAllocator != nil {
		ps.cpuAllocator.Release(containerIndex)
	}
	for i := range ps.containers {
		if ps.containers[i].Index == containerIndex {
			ps.containers[i].PID = 0
			return nil
		}
	}
	return nil
}
