package scheduler

import (
	"container-bench/internal/accounting"
	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/probe"

	"time"

	"github.com/sirupsen/logrus"
)

type AllocationScheduler struct {
	name               string
	version            string
	schedulerLogger    *logrus.Logger
	hostConfig         *host.HostConfig
	containers         []ContainerInfo
	rdtAccountant      *accounting.RDTAccountant
	prober             *probe.Probe
	config             *config.SchedulerConfig
	accounting         Accounts
	warmupCompleteTime time.Time
	classCreated       bool
}

type Accounts struct {
	accounts []Account
}

type Account struct {
	L3Allocation              float64
	MemoryBandwidthAllocation float64
}

func NewAllocationScheduler() *AllocationScheduler {
	return &AllocationScheduler{
		name:            "allocation",
		version:         "1.0.0",
		schedulerLogger: logging.GetSchedulerLogger(),
	}
}

func (as *AllocationScheduler) Initialize(accountant *accounting.RDTAccountant, containers []ContainerInfo, schedulerConfig *config.SchedulerConfig) error {
	// Store the accountant directly
	as.rdtAccountant = accountant
	as.containers = containers
	as.config = schedulerConfig
	as.accounting = Accounts{}
	as.accounting.accounts = make([]Account, len(containers))
	as.classCreated = false

	for i, _ := range containers {
		as.accounting.accounts[i] = Account{
			L3Allocation:              0.0,
			MemoryBandwidthAllocation: 0.0,
		}
	}

	warmupSeconds := 0
	if as.config != nil && as.config.Allocator != nil && as.config.Allocator.WarmupT > 0 {
		warmupSeconds = as.config.Allocator.WarmupT
		as.schedulerLogger.WithField("seconds", as.config.Allocator.WarmupT).Info("Warming up benchmark")
	}

	as.warmupCompleteTime = time.Now().Add(time.Duration(warmupSeconds) * time.Second)

	as.schedulerLogger.WithFields(logrus.Fields{
		"containers":     len(containers),
		"warmup_seconds": warmupSeconds,
	}).Info("Allocation scheduler initialized")
	return nil
}

func (as *AllocationScheduler) ProcessDataFrames(dataframes *dataframe.DataFrames) error {
	if time.Now().Before(as.warmupCompleteTime) {
		return nil
	}

	// Create victim class with 30% cache allocation after warmup (once)
	if !as.classCreated {
		className := "victim-test"
		err := as.rdtAccountant.CreateClass(className,
			&accounting.AllocationRequest{
				L3Percent:    30.0, // 30% of L3 cache
				MemBandwidth: 30.0, // 30% of memory bandwidth
			},
			&accounting.AllocationRequest{
				L3Percent:    30.0,
				MemBandwidth: 30.0,
			},
		)

		if err != nil {
			as.schedulerLogger.WithError(err).Error("Failed to create RDT class")
		} else {
			as.classCreated = true
			as.schedulerLogger.WithField("class", className).Info("RDT class created with 30% cache allocation")

			// Assign all containers to the class
			for i, container := range as.containers {
				if container.PID != 0 {
					err := as.rdtAccountant.MoveContainer(container.PID, className)
					if err != nil {
						as.schedulerLogger.WithError(err).WithFields(logrus.Fields{
							"container": i,
							"pid":       container.PID,
						}).Error("Failed to assign container to RDT class")
					} else {
						as.schedulerLogger.WithFields(logrus.Fields{
							"container": i,
							"pid":       container.PID,
							"class":     className,
						}).Info("Container assigned to RDT class")
					}
				}
			}
		}
	}

	containers := dataframes.GetAllContainers()

	for containerIndex, containerDF := range containers {
		latest := containerDF.GetLatestStep()
		if latest == nil {
			continue
		}

		// Log cache miss rate
		if latest.Perf != nil && latest.Perf.CacheMissRate != nil {
			as.schedulerLogger.WithFields(logrus.Fields{
				"container":       containerIndex,
				"cache_miss_rate": *latest.Perf.CacheMissRate,
			}).Debug("Cache miss rate")
		}
	}

	return nil
}

func (as *AllocationScheduler) Shutdown() error {
	err := as.rdtAccountant.Cleanup()
	if err != nil {
		as.schedulerLogger.WithError(err).Error("Could not clean up RDT classes")
	}
	return nil
}

func (as *AllocationScheduler) GetVersion() string {
	return as.version
}

func (as *AllocationScheduler) SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	as.schedulerLogger.SetLevel(logLevel)
	return nil
}

func (as *AllocationScheduler) SetHostConfig(hostConfig *host.HostConfig) {
	as.hostConfig = hostConfig
}

func (as *AllocationScheduler) SetProbe(prober *probe.Probe) {
	as.prober = prober
	as.schedulerLogger.Debug("Probe injected into scheduler")
}
