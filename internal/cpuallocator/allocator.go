package cpuallocator

import (
	"fmt"
	"sort"
	"sync"

	"container-bench/internal/config"
	"container-bench/internal/host"

	"github.com/sirupsen/logrus"
)

// Allocator assigns logical CPU IDs to containers, tracking reservations.
// Implementations should avoid assigning hyperthread siblings.
type Allocator interface {
	// EnsureAssigned ensures containerIndex has a CPU assignment based on cfg.
	// Rules:
	// - If cfg.CPUCores is already set, it is treated as the existing assignment.
	// - Else if cfg.Core is non-empty, it is parsed and reserved (explicit request).
	// - Else cfg.GetRequestedNumCores() physical cores are allocated.
	// The allocator updates cfg.CPUCores and cfg.Core (canonical formatted cpuset).
	EnsureAssigned(containerIndex int, cfg *config.ContainerConfig) ([]int, error)

	// Reserve marks the given CPUs as assigned to containerIndex.
	// It must fail if any CPU is already reserved by a different container.
	Reserve(containerIndex int, cpuIDs []int) error

	// Allocate reserves num CPUs for containerIndex and returns the assigned logical CPU IDs.
	Allocate(containerIndex int, num int) ([]int, error)

	// Release frees any CPUs reserved for containerIndex.
	Release(containerIndex int)

	// Get returns the CPUs reserved for containerIndex
	Get(containerIndex int) ([]int, bool)

	// Snapshot returns a copy of all current assignments
	Snapshot() map[int][]int
}

type PhysicalCoreAllocator struct {
	host       *host.HostConfig
	order      []int
	logger     logrus.FieldLogger
	mu         sync.Mutex
	assigned   map[int][]int // containerIndex -> cpuIDs
	reservedBy map[int]int   // cpuID -> containerIndex
	cfgByIndex map[int]*config.ContainerConfig
}

func NewPhysicalCoreAllocator(hostConfig *host.HostConfig, logger logrus.FieldLogger) (*PhysicalCoreAllocator, error) {
	if hostConfig == nil {
		return nil, fmt.Errorf("host config is nil")
	}
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	order, err := hostConfig.PhysicalCPUsInOrder()
	if err != nil {
		return nil, err
	}
	return &PhysicalCoreAllocator{
		host:       hostConfig,
		order:      order,
		logger:     logger,
		assigned:   make(map[int][]int),
		reservedBy: make(map[int]int),
		cfgByIndex: make(map[int]*config.ContainerConfig),
	}, nil
}

func (a *PhysicalCoreAllocator) EnsureAssigned(containerIndex int, cfg *config.ContainerConfig) ([]int, error) {
	if cfg == nil {
		return nil, nil
	}

	// Cache cfg pointer for later release logging
	a.mu.Lock()
	a.cfgByIndex[containerIndex] = cfg
	// If already assigned, return without re-logging
	if len(cfg.CPUCores) > 0 {
		assigned := append([]int(nil), cfg.CPUCores...)
		a.mu.Unlock()
		return assigned, nil
	}
	a.mu.Unlock()

	// Explicit cores: treat as reservation request
	if cfg.Core != "" {
		cpus, err := config.ParseCPUSpec(cfg.Core)
		if err != nil {
			return nil, err
		}
		if err := a.Reserve(containerIndex, cpus); err != nil {
			return nil, err
		}
		cfg.CPUCores = cpus
		cfg.Core = config.FormatCPUSpec(cpus)
		a.logger.WithFields(a.containerFields(containerIndex, cfg)).WithFields(logrus.Fields{
			"cpus":   cpus,
			"cpuset": cfg.Core,
			"source": "explicit",
		}).Info("Assigned CPU cores")
		return cpus, nil
	}

	assigned, err := a.Allocate(containerIndex, cfg.GetRequestedNumCores())
	if err != nil {
		return nil, err
	}
	cfg.CPUCores = assigned
	cfg.Core = config.FormatCPUSpec(assigned)
	a.logger.WithFields(a.containerFields(containerIndex, cfg)).WithFields(logrus.Fields{
		"cpus":                assigned,
		"cpuset":              cfg.Core,
		"requested_num_cores": cfg.GetRequestedNumCores(),
		"source":              "allocator",
	}).Info("Assigned CPU cores")
	return assigned, nil
}

func (a *PhysicalCoreAllocator) Reserve(containerIndex int, cpuIDs []int) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(cpuIDs) == 0 {
		return fmt.Errorf("no CPUs specified")
	}
	if err := a.host.ValidatePhysicalCPUs(cpuIDs); err != nil {
		return err
	}

	for _, cpu := range cpuIDs {
		if owner, ok := a.reservedBy[cpu]; ok && owner != containerIndex {
			return fmt.Errorf("cpu %d already reserved by container %d", cpu, owner)
		}
	}

	a.releaseLocked(containerIndex)

	uniq := uniqueSorted(cpuIDs)
	for _, cpu := range uniq {
		a.reservedBy[cpu] = containerIndex
	}
	a.assigned[containerIndex] = uniq
	return nil
}

func (a *PhysicalCoreAllocator) Allocate(containerIndex int, num int) ([]int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if num <= 0 {
		return nil, fmt.Errorf("num must be >= 1")
	}

	a.releaseLocked(containerIndex)

	picked := make([]int, 0, num)
	for _, cpu := range a.order {
		if _, used := a.reservedBy[cpu]; used {
			continue
		}
		picked = append(picked, cpu)
		if len(picked) == num {
			break
		}
	}
	if len(picked) != num {
		return nil, fmt.Errorf("insufficient physical cores: requested %d", num)
	}

	for _, cpu := range picked {
		a.reservedBy[cpu] = containerIndex
	}
	a.assigned[containerIndex] = append([]int(nil), picked...)
	return append([]int(nil), picked...), nil
}

func (a *PhysicalCoreAllocator) Release(containerIndex int) {
	var cpus []int
	var cfg *config.ContainerConfig

	a.mu.Lock()
	if prev, ok := a.assigned[containerIndex]; ok {
		cpus = append([]int(nil), prev...)
	}
	cfg = a.cfgByIndex[containerIndex]
	a.releaseLocked(containerIndex)
	delete(a.cfgByIndex, containerIndex)
	a.mu.Unlock()

	if len(cpus) > 0 {
		a.logger.WithFields(a.containerFields(containerIndex, cfg)).WithFields(logrus.Fields{
			"cpus":   cpus,
			"cpuset": config.FormatCPUSpec(cpus),
		}).Info("Released CPU cores")
	}
}

func (a *PhysicalCoreAllocator) releaseLocked(containerIndex int) {
	prev, ok := a.assigned[containerIndex]
	if !ok {
		return
	}
	for _, cpu := range prev {
		owner, ok := a.reservedBy[cpu]
		if ok && owner == containerIndex {
			delete(a.reservedBy, cpu)
		}
	}
	delete(a.assigned, containerIndex)
}

func (a *PhysicalCoreAllocator) Get(containerIndex int) ([]int, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	cpus, ok := a.assigned[containerIndex]
	if !ok {
		return nil, false
	}
	return append([]int(nil), cpus...), true
}

func (a *PhysicalCoreAllocator) Snapshot() map[int][]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[int][]int, len(a.assigned))
	for k, v := range a.assigned {
		out[k] = append([]int(nil), v...)
	}
	return out
}

func (a *PhysicalCoreAllocator) containerFields(containerIndex int, cfg *config.ContainerConfig) logrus.Fields {
	fields := logrus.Fields{"container_index": containerIndex}
	if cfg == nil {
		return fields
	}
	if cfg.Name != "" {
		fields["container_name"] = cfg.Name
	}
	if cfg.KeyName != "" {
		fields["container_key"] = cfg.KeyName
	}
	if cfg.Image != "" {
		fields["image"] = cfg.Image
	}
	return fields
}

func uniqueSorted(vals []int) []int {
	cp := append([]int(nil), vals...)
	sort.Ints(cp)
	out := make([]int, 0, len(cp))
	var last *int
	for _, v := range cp {
		if last != nil && *last == v {
			continue
		}
		vv := v
		last = &vv
		out = append(out, v)
	}
	return out
}
