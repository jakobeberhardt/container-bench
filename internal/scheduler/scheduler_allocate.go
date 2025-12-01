package scheduler

import (
	"container-bench/internal/accounting"
	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/probe"
	"fmt"
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

	// Sweep configuration
	currentL3Ways     int
	currentMemBW      float64
	allocationStarted time.Time
	sweepStarted      bool
	maxL3Ways         int
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
	as.sweepStarted = false

	// Initialize sweep parameters
	as.currentL3Ways = 1  // Start with 1 way
	as.currentMemBW = 1.0 // Start with 1% memory bandwidth

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

	// Get allocator config
	if as.config == nil || as.config.Allocator == nil {
		return nil
	}

	allocCfg := as.config.Allocator
	duration := allocCfg.Duration
	stepSizeL3 := allocCfg.StepSizeL3
	stepSizeMB := allocCfg.StepSizeMB

	// Default values if not configured
	if duration <= 0 {
		duration = 10 // 10 seconds default
	}
	if stepSizeL3 <= 0 {
		stepSizeL3 = 1 // 1 way step default
	}
	if stepSizeMB <= 0 {
		stepSizeMB = 5 // 5% step default
	}

	// Get max ways from host config
	if as.hostConfig != nil && as.maxL3Ways == 0 {
		as.maxL3Ways = as.hostConfig.L3Cache.WaysPerCache
	}
	if as.maxL3Ways == 0 {
		as.maxL3Ways = 12 // Fallback default
	}

	className := "sweep-allocation"

	// Start sweep or advance to next allocation
	if !as.sweepStarted {
		// Create initial allocation with 1 way and 1% bandwidth
		as.sweepStarted = true
		as.allocationStarted = time.Now()
		as.classCreated = false

		err := as.createAllocation(className, as.currentL3Ways, as.currentMemBW)
		if err != nil {
			as.schedulerLogger.WithError(err).Error("Failed to create initial allocation")
			return err
		}

		as.schedulerLogger.WithFields(logrus.Fields{
			"l3_ways":      as.currentL3Ways,
			"mem_bw":       as.currentMemBW,
			"duration_sec": duration,
		}).Info("Started allocation sweep")

		return nil
	}

	// Check if current allocation duration is complete
	elapsed := time.Since(as.allocationStarted).Seconds()
	if elapsed < float64(duration) {
		// Still running current allocation
		return nil
	}

	// Duration complete - advance to next allocation
	as.schedulerLogger.WithFields(logrus.Fields{
		"completed_l3_ways": as.currentL3Ways,
		"completed_mem_bw":  as.currentMemBW,
		"duration_sec":      int(elapsed),
	}).Info("Allocation period complete, advancing")

	// Advance L3 ways
	as.currentL3Ways += stepSizeL3

	// If we've exceeded max ways, reset ways and increment memory bandwidth
	if as.currentL3Ways > as.maxL3Ways {
		as.currentL3Ways = 1
		as.currentMemBW += float64(stepSizeMB)

		// Check if we've exceeded 100% bandwidth
		if as.currentMemBW > 100.0 {
			as.schedulerLogger.Info("Allocation sweep complete")
			return nil
		}

		as.schedulerLogger.WithFields(logrus.Fields{
			"new_mem_bw":  as.currentMemBW,
			"reset_l3_to": as.currentL3Ways,
		}).Info("Completed L3 sweep, incrementing memory bandwidth")
	}

	// Update allocation
	err := as.updateAllocation(className, as.currentL3Ways, as.currentMemBW)
	if err != nil {
		as.schedulerLogger.WithError(err).Error("Failed to update allocation")
		return err
	}

	as.allocationStarted = time.Now()

	as.schedulerLogger.WithFields(logrus.Fields{
		"l3_ways": as.currentL3Ways,
		"mem_bw":  as.currentMemBW,
	}).Info("Advanced to next allocation")

	return nil
}

// createAllocation creates a new RDT class and assigns all containers to it
func (as *AllocationScheduler) createAllocation(className string, l3Ways int, memBW float64) error {
	// Create class with ways allocation
	waysRange := fmt.Sprintf("0-%d", l3Ways-1)

	err := as.rdtAccountant.CreateClass(className,
		&accounting.AllocationRequest{
			L3Ways:       waysRange,
			MemBandwidth: memBW,
		},
		&accounting.AllocationRequest{
			L3Ways:       waysRange,
			MemBandwidth: memBW,
		},
	)

	if err != nil {
		return err
	}

	as.classCreated = true
	as.schedulerLogger.WithFields(logrus.Fields{
		"class":   className,
		"l3_ways": l3Ways,
		"mem_bw":  memBW,
	}).Info("Created RDT allocation class")

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
				}).Debug("Container assigned to RDT class")
			}
		}
	}

	return nil
}

// updateAllocation updates an existing RDT class allocation
func (as *AllocationScheduler) updateAllocation(className string, l3Ways int, memBW float64) error {
	waysRange := fmt.Sprintf("0-%d", l3Ways-1)

	err := as.rdtAccountant.UpdateClass(className,
		&accounting.AllocationRequest{
			L3Ways:       waysRange,
			MemBandwidth: memBW,
		},
		&accounting.AllocationRequest{
			L3Percent:    0.1,
			MemBandwidth: 0.1,
		},
	)

	if err != nil {
		return err
	}

	as.schedulerLogger.WithFields(logrus.Fields{
		"class":   className,
		"l3_ways": l3Ways,
		"mem_bw":  memBW,
	}).Info("Updated RDT allocation class")

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
