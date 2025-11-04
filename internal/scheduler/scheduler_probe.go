package scheduler

import (
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/probe"
	"context"
	"time"

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
	
	// Probing state
	probingStarted  bool
	nextProbeIndex  int
	activeProbe     <-chan *probe.ProbeResult
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

func (ps *ProbeScheduler) Initialize(allocator RDTAllocator, containers []ContainerInfo) error {
	ps.rdtAllocator = allocator
	ps.containers = containers

	ps.schedulerLogger.WithField("containers", len(containers)).Info("Probe scheduler initialized")
	return nil
}

func (ps *ProbeScheduler) ProcessDataFrames(dataframes *dataframe.DataFrames) error {
	// Check if we have a prober
	if ps.prober == nil {
		return nil
	}
	
	// Check if we have an active probe running
	if ps.activeProbe != nil {
		// Non-blocking check if probe completed
		select {
		case result := <-ps.activeProbe:
			ps.schedulerLogger.WithFields(logrus.Fields{
				"container_index": result.ContainerIndex,
				"container_name":  result.ContainerName,
				"llc_sensitivity": result.LLC,
				"cpu_sensitivity": result.CPUInteger,
			}).Info("Probe completed and received")
			ps.activeProbe = nil
		default:
			// Probe still running, nothing to do
			return nil
		}
	}
	
	// Start next probe if we have containers left to probe
	if ps.nextProbeIndex < len(ps.containers) {
		containerInfo := ps.containers[ps.nextProbeIndex]
		
		ps.schedulerLogger.WithFields(logrus.Fields{
			"container_index": containerInfo.Index,
			"container_name":  containerInfo.Config.GetContainerName(0), // Benchmark ID not available here
			"probe_duration":  "60s",
			"probe_core":      "5",
		}).Info("Starting probe")
		
		// Create probe request
		req := probe.ProbeRequest{
			ContainerConfig: containerInfo.Config,
			ContainerID:     containerInfo.ContainerID,
			Dataframes:      dataframes,
			ProbeDuration:   60 * time.Second,
			ProbeCores:      "5",
			ProbeSocket:     0,
			Isolated:        true,
			Abortable:       false,
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
		ps.schedulerLogger.WithFields(logrus.Fields{
			"container_index": result.ContainerIndex,
			"container_name":  result.ContainerName,
		}).Info("Final probe completed and received")
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
