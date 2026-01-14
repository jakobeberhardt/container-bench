package scheduler

import (
	"fmt"
	"sort"
	"sync"

	"container-bench/internal/accounting"
	"container-bench/internal/config"
	"container-bench/internal/cpuallocator"
	"container-bench/internal/dataframe"
	"container-bench/internal/host"
	"container-bench/internal/logging"
	"container-bench/internal/probe"

	"github.com/sirupsen/logrus"
)

// LeastLoadedScheduler admits containers onto the socket with the lowest number of allocated
// physical cores. If no socket can satisfy the next container's request, it is queued until
// cores are freed (OnContainerStop) or Shutdown is called.
type LeastLoadedScheduler struct {
	name            string
	version         string
	schedulerLogger *logrus.Logger
	hostConfig      *host.HostConfig
	containers      []ContainerInfo
	rdtAccountant   *accounting.RDTAccountant
	prober          *probe.Probe
	config          *config.SchedulerConfig
	cpuAllocator    cpuallocator.Allocator

	mu           sync.Mutex
	queuedSet    map[int]bool
	shuttingDown bool
}

func NewLeastLoadedScheduler() *LeastLoadedScheduler {
	ls := &LeastLoadedScheduler{
		name:            "least_loaded",
		version:         "1.1.0",
		schedulerLogger: logging.GetSchedulerLogger(),
		queuedSet:       make(map[int]bool),
	}
	return ls
}

func (ls *LeastLoadedScheduler) Initialize(accountant *accounting.RDTAccountant, containers []ContainerInfo, schedulerConfig *config.SchedulerConfig) error {
	ls.rdtAccountant = accountant
	ls.containers = containers
	ls.config = schedulerConfig
	ls.schedulerLogger.WithField("containers", len(containers)).Info("Least-loaded scheduler initialized")
	return nil
}

func (ls *LeastLoadedScheduler) ProcessDataFrames(_ *dataframe.DataFrames) error {
	// Admission-only scheduler; no periodic work.
	return nil
}

func (ls *LeastLoadedScheduler) Shutdown() error {
	ls.mu.Lock()
	ls.shuttingDown = true
	ls.mu.Unlock()
	return nil
}

func (ls *LeastLoadedScheduler) GetVersion() string { return ls.version }

func (ls *LeastLoadedScheduler) SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	ls.schedulerLogger.SetLevel(logLevel)
	return nil
}

func (ls *LeastLoadedScheduler) SetHostConfig(hostConfig *host.HostConfig) {
	ls.hostConfig = hostConfig
}

func (ls *LeastLoadedScheduler) SetCPUAllocator(allocator cpuallocator.Allocator) {
	ls.cpuAllocator = allocator
}

func (ls *LeastLoadedScheduler) AssignCPUCores(containerIndex int) ([]int, error) {
	if ls.cpuAllocator == nil {
		return nil, nil
	}
	if ls.hostConfig == nil {
		return nil, fmt.Errorf("host config is not set")
	}

	// Find config
	var cfg *config.ContainerConfig
	for i := range ls.containers {
		if ls.containers[i].Index == containerIndex {
			cfg = ls.containers[i].Config
			break
		}
	}
	if cfg == nil {
		return nil, nil
	}

	// Explicit request or pre-set: let allocator handle it.
	if cfg.Core != "" || len(cfg.CPUCores) > 0 {
		return ls.cpuAllocator.EnsureAssigned(containerIndex, cfg)
	}

	requested := cfg.GetRequestedNumCores()
	ls.mu.Lock()
	if ls.shuttingDown {
		ls.mu.Unlock()
		return nil, fmt.Errorf("scheduler is shutting down")
	}
	ls.mu.Unlock()

	cpus, err := ls.pickCPUsLeastLoaded(requested)
	if err != nil {
		// Non-blocking admission: surface the capacity error and let the benchmark loop
		// defer admission (strict FIFO) while continuing to tick stop times.
		ls.mu.Lock()
		if !ls.queuedSet[containerIndex] {
			ls.queuedSet[containerIndex] = true
			ls.schedulerLogger.WithFields(containerLogFields(ls.containers, containerIndex, cfg)).WithFields(logrus.Fields{
				"requested_num_cores": requested,
				"reason":             err.Error(),
			}).Debug("Admission deferred (insufficient CPU capacity)")
		}
		ls.mu.Unlock()
		return nil, err
	}

	// Reserve and publish assignment.
	if err := ls.cpuAllocator.Reserve(containerIndex, cpus); err != nil {
		// A race with another admission; return the allocator error so admission can be deferred.
		return nil, err
	}
	cfg.CPUCores = append([]int(nil), cpus...)
	cfg.Core = config.FormatCPUSpec(cpus)
	// Cache cfg pointer for release logging.
	_, _ = ls.cpuAllocator.EnsureAssigned(containerIndex, cfg)

	ls.mu.Lock()
	if ls.queuedSet[containerIndex] {
		delete(ls.queuedSet, containerIndex)
	}
	ls.mu.Unlock()

	ls.schedulerLogger.WithFields(containerLogFields(ls.containers, containerIndex, cfg)).WithFields(logrus.Fields{
		"cpus":                cpus,
		"cpuset":              cfg.Core,
		"requested_num_cores": requested,
		"source":              "least_loaded",
	}).Info("Assigned CPU cores")

	return cpus, nil
}

func (ls *LeastLoadedScheduler) SetProbe(prober *probe.Probe) {
	ls.prober = prober
	ls.schedulerLogger.Debug("Probe injected into least-loaded scheduler")
}

func (ls *LeastLoadedScheduler) SetBenchmarkID(_ int) {}

func (ls *LeastLoadedScheduler) OnContainerStart(info ContainerInfo) error {
	for i := range ls.containers {
		if ls.containers[i].Index == info.Index {
			ls.containers[i].PID = info.PID
			ls.containers[i].ContainerID = info.ContainerID
			return nil
		}
	}
	ls.containers = append(ls.containers, info)
	return nil
}

func (ls *LeastLoadedScheduler) OnContainerStop(containerIndex int) error {
	if ls.cpuAllocator != nil {
		ls.cpuAllocator.Release(containerIndex)
	}
	ls.mu.Lock()
	if ls.queuedSet[containerIndex] {
		delete(ls.queuedSet, containerIndex)
	}
	ls.mu.Unlock()
	for i := range ls.containers {
		if ls.containers[i].Index == containerIndex {
			ls.containers[i].PID = 0
			return nil
		}
	}
	return nil
}

func (ls *LeastLoadedScheduler) pickCPUsLeastLoaded(num int) ([]int, error) {
	bySocket, err := ls.hostConfig.PhysicalCPUsBySocket()
	if err != nil {
		return nil, err
	}
	snapshot := ls.cpuAllocator.Snapshot()

	// Build used set and per-socket usage.
	used := make(map[int]bool)
	usedPerSocket := make(map[int]int)
	for _, cpus := range snapshot {
		sock, err := ls.hostConfig.SocketOfPhysicalCPUs(cpus)
		if err == nil {
			usedPerSocket[sock] += len(cpus)
		}
		for _, cpu := range cpus {
			used[cpu] = true
		}
	}

	type sockLoad struct {
		socket int
		load   int
	}
	loads := make([]sockLoad, 0, len(bySocket))
	for socket := range bySocket {
		loads = append(loads, sockLoad{socket: socket, load: usedPerSocket[socket]})
	}
	sort.Slice(loads, func(i, j int) bool {
		if loads[i].load != loads[j].load {
			return loads[i].load < loads[j].load
		}
		return loads[i].socket < loads[j].socket
	})

	for _, cand := range loads {
		cpus := bySocket[cand.socket]
		picked := make([]int, 0, num)
		for _, cpu := range cpus {
			if used[cpu] {
				continue
			}
			picked = append(picked, cpu)
			if len(picked) == num {
				return picked, nil
			}
		}
	}

	return nil, fmt.Errorf("insufficient physical cores: requested %d", num)
}
