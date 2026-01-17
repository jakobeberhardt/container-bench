package cpuallocator

import (
	"context"
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
	// If cfg.CPUCores is already set, it is treated as the existing assignment.
	// Else if cfg.Core is non empty, it is parsed and reserved as an explicit request.
	// Else cfg.GetRequestedNumCores() physical cores are allocated.
	// The allocator updates cfg.CPUCores and cfg.Core (canonical formatted cpuset).
	EnsureAssigned(containerIndex int, cfg *config.ContainerConfig) ([]int, error)

	// Reserve marks the given CPUs as assigned to containerIndex.
	// It must fail if any CPU is already reserved by a different container.
	Reserve(containerIndex int, cpuIDs []int) error

	// Allocate reserves num CPUs for containerIndex and returns the assigned logical CPU IDs.
	Allocate(containerIndex int, num int) ([]int, error)

	// Release frees any CPUs reserved for containerIndex.
	Release(containerIndex int)

	// Returns the CPUs reserved for containerIndex.
	Get(containerIndex int) ([]int, bool)

	// Returns a copy of all current assignments.
	Snapshot() map[int][]int

	// Move reassigns the container's current CPU allocation to physical cores on targetSocket.
	// It live-updates the running container's CpusetCpus via the injected cpuset applier.
	Move(containerIndex int, containerID string, targetSocket int) ([]int, error)

	// Swap exchanges CPU allocations across sockets between two containers.
	// If both containers are on the same socket, it moves the first container to a different socket.
	// The swap fails if the destination socket(s) cannot accommodate the required number of cores.
	Swap(aIndex int, aContainerID string, bIndex int, bContainerID string) error
}

// Applies a cpuset to a running container.
type CpusetApplier interface {
	UpdateCpuset(ctx context.Context, containerID string, cpuset string) error
}

type PhysicalCoreAllocator struct {
	host       *host.HostConfig
	order      []int
	logger     logrus.FieldLogger
	cpuset     CpusetApplier
	mu         sync.Mutex
	assigned   map[int][]int // containerIndex -> cpuIDs
	reservedBy map[int]int   // cpuID -> containerIndex
	cfgByIndex map[int]*config.ContainerConfig
}

func NewPhysicalCoreAllocator(hostConfig *host.HostConfig, cpusetApplier CpusetApplier, logger logrus.FieldLogger) (*PhysicalCoreAllocator, error) {
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
		cpuset:     cpusetApplier,
		assigned:   make(map[int][]int),
		reservedBy: make(map[int]int),
		cfgByIndex: make(map[int]*config.ContainerConfig),
	}, nil
}

func (a *PhysicalCoreAllocator) Move(containerIndex int, containerID string, targetSocket int) ([]int, error) {
	return a.movePlanned(containerIndex, containerID, targetSocket)
}

func (a *PhysicalCoreAllocator) Swap(aIndex int, aContainerID string, bIndex int, bContainerID string) error {
	// Determine current sockets (must be single-socket allocations).
	a.mu.Lock()
	aCPUs, aOK := a.assigned[aIndex]
	bCPUs, bOK := a.assigned[bIndex]
	aCPUs = append([]int(nil), aCPUs...)
	bCPUs = append([]int(nil), bCPUs...)
	socketCount := a.host.Topology.Sockets
	a.mu.Unlock()

	if !aOK || len(aCPUs) == 0 {
		return fmt.Errorf("container %d has no CPU assignment", aIndex)
	}
	if !bOK || len(bCPUs) == 0 {
		return fmt.Errorf("container %d has no CPU assignment", bIndex)
	}

	aSocket, err := a.host.SocketOfPhysicalCPUs(aCPUs)
	if err != nil {
		return fmt.Errorf("failed to determine socket for container %d: %w", aIndex, err)
	}
	bSocket, err := a.host.SocketOfPhysicalCPUs(bCPUs)
	if err != nil {
		return fmt.Errorf("failed to determine socket for container %d: %w", bIndex, err)
	}

	// Same socket: move the first container to a different socket.
	if aSocket == bSocket {
		if socketCount <= 1 {
			return fmt.Errorf("cannot swap/move across sockets: host has %d socket(s)", socketCount)
		}
		other := -1
		for s := 0; s < socketCount; s++ {
			if s != aSocket {
				other = s
				break
			}
		}
		if other == -1 {
			return fmt.Errorf("no alternative socket available")
		}
		_, err := a.Move(aIndex, aContainerID, other)
		return err
	}

	// Different sockets: compute both target sets first, then apply updates.
	neededA := len(aCPUs)
	neededB := len(bCPUs)

	a.mu.Lock()
	// Plan new CPU sets assuming the other container's CPUs on that socket become available.
	newA, err := a.pickCPUsOnSocketLocked(bSocket, neededA, map[int]bool{bIndex: true})
	if err != nil {
		a.mu.Unlock()
		return fmt.Errorf("insufficient capacity on socket %d for container %d: %w", bSocket, aIndex, err)
	}
	newB, err := a.pickCPUsOnSocketLocked(aSocket, neededB, map[int]bool{aIndex: true})
	if err != nil {
		a.mu.Unlock()
		return fmt.Errorf("insufficient capacity on socket %d for container %d: %w", aSocket, bIndex, err)
	}

	oldA := append([]int(nil), a.assigned[aIndex]...)
	oldB := append([]int(nil), a.assigned[bIndex]...)
	oldCpusetA := config.FormatCPUSpec(oldA)
	oldCpusetB := config.FormatCPUSpec(oldB)
	newCpusetA := config.FormatCPUSpec(newA)
	newCpusetB := config.FormatCPUSpec(newB)
	cfgA := a.cfgByIndex[aIndex]
	cfgB := a.cfgByIndex[bIndex]

	// Optimistically update allocator state (rollback on docker failure).
	a.releaseLocked(aIndex)
	a.releaseLocked(bIndex)
	if err := a.assignLocked(aIndex, newA); err != nil {
		// restore
		_ = a.assignLocked(aIndex, oldA)
		_ = a.assignLocked(bIndex, oldB)
		a.mu.Unlock()
		return err
	}
	if err := a.assignLocked(bIndex, newB); err != nil {
		// restore
		a.releaseLocked(aIndex)
		a.releaseLocked(bIndex)
		_ = a.assignLocked(aIndex, oldA)
		_ = a.assignLocked(bIndex, oldB)
		a.mu.Unlock()
		return err
	}
	if cfgA != nil {
		cfgA.CPUCores = append([]int(nil), newA...)
		cfgA.Core = newCpusetA
	}
	if cfgB != nil {
		cfgB.CPUCores = append([]int(nil), newB...)
		cfgB.Core = newCpusetB
	}
	a.mu.Unlock()

	ctx := context.Background()
	if a.cpuset == nil {
		return fmt.Errorf("cpuset applier is not configured")
	}
	if err := a.cpuset.UpdateCpuset(ctx, aContainerID, newCpusetA); err != nil {
		// rollback state
		a.mu.Lock()
		a.releaseLocked(aIndex)
		a.releaseLocked(bIndex)
		_ = a.assignLocked(aIndex, oldA)
		_ = a.assignLocked(bIndex, oldB)
		if cfgA != nil {
			cfgA.CPUCores = append([]int(nil), oldA...)
			cfgA.Core = oldCpusetA
		}
		if cfgB != nil {
			cfgB.CPUCores = append([]int(nil), oldB...)
			cfgB.Core = oldCpusetB
		}
		a.mu.Unlock()
		return fmt.Errorf("failed to update container %d cpuset: %w", aIndex, err)
	}
	if err := a.cpuset.UpdateCpuset(ctx, bContainerID, newCpusetB); err != nil {
		// best-effort rollback both containers and allocator state
		_ = a.cpuset.UpdateCpuset(ctx, aContainerID, oldCpusetA)
		a.mu.Lock()
		a.releaseLocked(aIndex)
		a.releaseLocked(bIndex)
		_ = a.assignLocked(aIndex, oldA)
		_ = a.assignLocked(bIndex, oldB)
		if cfgA != nil {
			cfgA.CPUCores = append([]int(nil), oldA...)
			cfgA.Core = oldCpusetA
		}
		if cfgB != nil {
			cfgB.CPUCores = append([]int(nil), oldB...)
			cfgB.Core = oldCpusetB
		}
		a.mu.Unlock()
		return fmt.Errorf("failed to update container %d cpuset: %w", bIndex, err)
	}

	a.logger.WithFields(logrus.Fields{
		"a_index":      aIndex,
		"b_index":      bIndex,
		"a_socket":     aSocket,
		"b_socket":     bSocket,
		"a_old_cpuset": oldCpusetA,
		"b_old_cpuset": oldCpusetB,
		"a_new_cpuset": newCpusetA,
		"b_new_cpuset": newCpusetB,
	}).Info("Swapped CPU allocations across sockets")

	return nil
}

func (a *PhysicalCoreAllocator) movePlanned(containerIndex int, containerID string, targetSocket int) ([]int, error) {
	if a == nil {
		return nil, fmt.Errorf("allocator is nil")
	}
	if a.cpuset == nil {
		return nil, fmt.Errorf("cpuset applier is not configured")
	}
	if containerID == "" {
		return nil, fmt.Errorf("containerID is empty")
	}

	a.mu.Lock()
	cur, ok := a.assigned[containerIndex]
	if !ok || len(cur) == 0 {
		a.mu.Unlock()
		return nil, fmt.Errorf("container %d has no CPU assignment", containerIndex)
	}
	cur = append([]int(nil), cur...)
	curSocket, err := a.host.SocketOfPhysicalCPUs(cur)
	if err != nil {
		a.mu.Unlock()
		return nil, fmt.Errorf("failed to determine current socket for container %d: %w", containerIndex, err)
	}
	if targetSocket == curSocket {
		out := append([]int(nil), cur...)
		a.mu.Unlock()
		return out, nil
	}

	needed := len(cur)
	newCPUs, err := a.pickCPUsOnSocketLocked(targetSocket, needed, map[int]bool{containerIndex: true})
	if err != nil {
		a.mu.Unlock()
		return nil, err
	}
	oldCPUs := append([]int(nil), cur...)
	oldCpuset := config.FormatCPUSpec(oldCPUs)
	newCpuset := config.FormatCPUSpec(newCPUs)
	cfg := a.cfgByIndex[containerIndex]

	// Update allocator state optimistically (rollback on docker update failure).
	a.releaseLocked(containerIndex)
	if err := a.assignLocked(containerIndex, newCPUs); err != nil {
		_ = a.assignLocked(containerIndex, oldCPUs) // best effort
		a.mu.Unlock()
		return nil, err
	}
	if cfg != nil {
		cfg.CPUCores = append([]int(nil), newCPUs...)
		cfg.Core = newCpuset
	}
	a.mu.Unlock()

	ctx := context.Background()
	if err := a.cpuset.UpdateCpuset(ctx, containerID, newCpuset); err != nil {
		// rollback allocator state
		a.mu.Lock()
		a.releaseLocked(containerIndex)
		_ = a.assignLocked(containerIndex, oldCPUs)
		if cfg != nil {
			cfg.CPUCores = append([]int(nil), oldCPUs...)
			cfg.Core = oldCpuset
		}
		a.mu.Unlock()
		return nil, fmt.Errorf("failed to update container cpuset: %w", err)
	}

	a.logger.WithFields(a.containerFields(containerIndex, cfg)).WithFields(logrus.Fields{
		"from_socket": curSocket,
		"to_socket":   targetSocket,
		"old_cpuset":  oldCpuset,
		"new_cpuset":  newCpuset,
	}).Info("Moved CPU allocation across sockets")

	return append([]int(nil), newCPUs...), nil
}

func (a *PhysicalCoreAllocator) pickCPUsOnSocketLocked(targetSocket int, num int, freeOwners map[int]bool) ([]int, error) {
	if num <= 0 {
		return nil, fmt.Errorf("num must be >= 1")
	}
	bySocket, err := a.host.PhysicalCPUsBySocket()
	if err != nil {
		return nil, err
	}
	cpus, ok := bySocket[targetSocket]
	if !ok {
		return nil, fmt.Errorf("socket %d not present in topology", targetSocket)
	}

	picked := make([]int, 0, num)
	for _, cpu := range cpus {
		owner, used := a.reservedBy[cpu]
		if used && !freeOwners[owner] {
			continue
		}
		picked = append(picked, cpu)
		if len(picked) == num {
			break
		}
	}
	if len(picked) != num {
		return nil, fmt.Errorf("insufficient physical cores on socket %d: requested %d", targetSocket, num)
	}
	return append([]int(nil), picked...), nil
}

func (a *PhysicalCoreAllocator) assignLocked(containerIndex int, cpuIDs []int) error {
	uniq := uniqueSorted(cpuIDs)
	for _, cpu := range uniq {
		if owner, ok := a.reservedBy[cpu]; ok && owner != containerIndex {
			return fmt.Errorf("cpu %d already reserved by container %d", cpu, owner)
		}
		a.reservedBy[cpu] = containerIndex
	}
	a.assigned[containerIndex] = uniq
	return nil
}

func (a *PhysicalCoreAllocator) EnsureAssigned(containerIndex int, cfg *config.ContainerConfig) ([]int, error) {
	if cfg == nil {
		return nil, nil
	}

	// Cache cfg pointer for later release logging
	a.mu.Lock()
	a.cfgByIndex[containerIndex] = cfg
	// If allocator already tracks an assignment, return it (avoid re-logging).
	if assigned, ok := a.assigned[containerIndex]; ok && len(assigned) > 0 {
		out := append([]int(nil), assigned...)
		// Keep cfg in sync for downstream users.
		if len(cfg.CPUCores) == 0 {
			cfg.CPUCores = append([]int(nil), assigned...)
			cfg.Core = config.FormatCPUSpec(cfg.CPUCores)
		}
		a.mu.Unlock()
		return out, nil
	}
	a.mu.Unlock()

	// Explicit cores: treat as reservation request.
	// Note: cfg.CPUCores may be pre-populated by callers (tests/schedulers). We must
	// still validate/reserve them; otherwise socket mapping and future allocations break.
	if cfg.Core != "" || len(cfg.CPUCores) > 0 {
		var cpus []int
		if cfg.Core != "" {
			parsed, err := config.ParseCPUSpec(cfg.Core)
			if err != nil {
				return nil, err
			}
			cpus = parsed
		} else {
			cpus = append([]int(nil), cfg.CPUCores...)
		}
		if err := a.Reserve(containerIndex, cpus); err != nil {
			return nil, err
		}
		cpus = uniqueSorted(cpus)
		cfg.CPUCores = append([]int(nil), cpus...)
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
