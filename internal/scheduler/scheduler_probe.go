package scheduler

import (
	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/probe"
	"context"
	"fmt"
	"time"

	"github.com/intel/goresctrl/pkg/rdt"
	"github.com/sirupsen/logrus"
)

type ProbeScheduler struct {
	name            string
	version         string
	schedulerLogger *logrus.Logger
	hostConfig      *host.HostConfig
	containers      []ContainerInfo
	rdtAllocator    RDTAllocator
	prober          *probe.Probe
	config          *config.SchedulerConfig

	// RDT probe isolation
	isolationManager *ProbeIsolationManager

	// Probing state
	probingStarted      bool
	nextProbeIndex      int
	activeProbe         <-chan *probe.ProbeResult
	lastProbeFinishTime time.Time
	warmupCompleteTime  time.Time
	currentTargetPID    int // PID of container being probed
}

func NewProbeScheduler() *ProbeScheduler {
	logger := logging.GetSchedulerLogger()
	return &ProbeScheduler{
		name:             "probe",
		version:          "1.0.0",
		schedulerLogger:  logger,
		probingStarted:   false,
		nextProbeIndex:   0,
		isolationManager: NewProbeIsolationManager(logger),
	}
}

func (ps *ProbeScheduler) Initialize(allocator RDTAllocator, containers []ContainerInfo, schedulerConfig *config.SchedulerConfig) error {
	ps.rdtAllocator = allocator
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

			// Teardown RDT isolation after probe completes
			if ps.isolationManager.IsActive() {
				ps.schedulerLogger.Info("Tearing down RDT probe isolation")
				if err := ps.isolationManager.TeardownProbeIsolation(); err != nil {
					ps.schedulerLogger.WithError(err).Error("Failed to teardown RDT isolation")
				}
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
		probeWays := ""
		probeMemBW := 0
		if ps.config != nil && ps.config.Prober != nil {
			abortable = ps.config.Prober.Abortable
			isolated = ps.config.Prober.Isolated
			if ps.config.Prober.DefaultT > 0 {
				probeDuration = time.Duration(ps.config.Prober.DefaultT) * time.Second
			}
			if ps.config.Prober.DefaultProbeCores != "" {
				probeCores = ps.config.Prober.DefaultProbeCores
			}
			probeWays = ps.config.Prober.Ways
			probeMemBW = ps.config.Prober.MemBW
		}

		ps.schedulerLogger.WithFields(logrus.Fields{
			"container_index": containerInfo.Index,
			"container_name":  containerInfo.Config.GetContainerName(0),
			"probe_duration":  probeDuration,
			"probe_cores":     probeCores,
			"abortable":       abortable,
			"isolated":        isolated,
			"probe_ways":      probeWays,
			"probe_mem_bw":    probeMemBW,
		}).Info("Starting probe")

		// Store target container PID for RDT isolation
		ps.currentTargetPID = containerInfo.PID

		// Setup RDT isolation if configured
		if isolated && probeWays != "" && probeMemBW > 0 {
			// Collect PIDs and container IDs of all other containers (not the target being probed)
			var otherPIDs []int
			var otherContainerIDs []string
			for _, c := range ps.containers {
				if c.Index != containerInfo.Index && c.PID > 0 {
					otherPIDs = append(otherPIDs, c.PID)
					otherContainerIDs = append(otherContainerIDs, c.ContainerID)
				}
			}

			// Probe container ID will be added via callback, so we initially just have target container
			probeContainerIDs := []string{containerInfo.ContainerID}

			ps.schedulerLogger.WithFields(logrus.Fields{
				"target_pid":       containerInfo.PID,
				"target_container": containerInfo.ContainerID[:12],
				"other_pids":       len(otherPIDs),
				"other_containers": len(otherContainerIDs),
				"probe_ways":       probeWays,
				"probe_mem_bw":     probeMemBW,
			}).Info("Setting up RDT isolation for probe")

			// Initially setup with target PID only, probe container PID will be added via callback
			if err := ps.isolationManager.SetupProbeIsolation(probeWays, probeMemBW, []int{containerInfo.PID}, otherPIDs, probeContainerIDs, otherContainerIDs); err != nil {
				ps.schedulerLogger.WithError(err).Error("Failed to setup RDT isolation, continuing without isolation")
			}
		}

		// Create probe request with PID callback
		req := probe.ProbeRequest{
			ContainerConfig: containerInfo.Config,
			ContainerID:     containerInfo.ContainerID,
			Dataframes:      dataframes,
			ProbeDuration:   probeDuration,
			ProbeCores:      probeCores,
			ProbeSocket:     0,
			Isolated:        isolated,
			Abortable:       abortable,
			ProbePIDCallback: func(probePID int, probeContainerID string) {
				// When probe container starts, add its PID to the probe RDT class
				// and add its cgroup path for PID syncing
				if ps.isolationManager.IsActive() {
					// Add cgroup path for ongoing PID sync
					cgroupPath := fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope", probeContainerID)
					ps.isolationManager.AddProbeCgroupPath(cgroupPath)

					// Add initial PID to probe class
					probeClass, ok := rdt.GetClass("probe")
					if ok {
						if err := probeClass.AddPids(fmt.Sprintf("%d", probePID)); err != nil {
							ps.schedulerLogger.WithError(err).WithField("probe_pid", probePID).Error("Failed to add probe PID to RDT class")
						} else {
							ps.schedulerLogger.WithFields(logrus.Fields{
								"probe_pid":          probePID,
								"probe_container_id": probeContainerID[:12],
							}).Info("Added probe container to probe RDT class")
						}
					}
				}
			},
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

	// Ensure RDT isolation is torn down on shutdown
	if ps.isolationManager.IsActive() {
		ps.schedulerLogger.Info("Cleaning up RDT isolation on shutdown")
		if err := ps.isolationManager.TeardownProbeIsolation(); err != nil {
			ps.schedulerLogger.WithError(err).Error("Failed to teardown RDT isolation during shutdown")
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

func (ps *ProbeScheduler) SetProbe(prober *probe.Probe) {
	ps.prober = prober
	ps.schedulerLogger.Info("Probe injected into scheduler")
}
