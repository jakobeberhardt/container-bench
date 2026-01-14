package rdtmanager

import (
	"container-bench/internal/allocation"
	"container-bench/internal/logging"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// RDTManager provides socket-aware RDT resource management with automatic
// consolidation to avoid bitmask fragmentation.
//
// Design principles:
// - Priority allocations are packed at the HIGH end of the cache-way space
// - The shared pool is always a contiguous LOW block
// - On release, allocations are consolidated to maintain contiguity
// - All operations are thread-safe
type RDTManager struct {
	config  Config
	backend RDTBackend
	logger  *logrus.Logger

	mu sync.RWMutex

	// Per-socket resource tracking
	sockets [2]SocketResources

	// Socket assignments are symbolic - just track where containers are placed.
	// No capacity is consumed until probing completes and allocation is committed.
	assignments map[int]*SocketAssignment

	// Active allocations by container index (actual RDT classes with committed resources)
	allocations map[int]*AllocationHandle

	// Dirty flag indicates the RDT configuration needs to be applied
	dirty bool
}

// NewRDTManager creates a new RDT resource manager.
func NewRDTManager(cfg Config, backend RDTBackend) (*RDTManager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	m := &RDTManager{
		config:      cfg,
		backend:     backend,
		logger:      logging.GetSchedulerLogger(),
		assignments: make(map[int]*SocketAssignment),
		allocations: make(map[int]*AllocationHandle),
	}

	// Initialize per-socket state
	for i := 0; i < cfg.Sockets; i++ {
		fullMask := lowMask(cfg.TotalWays)
		m.sockets[i] = SocketResources{
			TotalWays:     cfg.TotalWays,
			ReservedWays:  cfg.ReservedWays,
			ReservedMem:   cfg.ReservedMem,
			AllocatedWays: 0,
			AllocatedMem:  0,
			AllocatedMask: 0,
			SharedMask:    fullMask,
			SharedMem:     100.0 - cfg.ReservedMem,
		}
	}

	return m, nil
}

// Initialize initializes the RDT backend and applies initial configuration.
func (m *RDTManager) Initialize() error {
	if err := m.backend.Initialize(); err != nil {
		return err
	}
	return m.applyConfiguration()
}

// GetSocketResources returns a copy of the resource state for a socket.
func (m *RDTManager) GetSocketResources(socket int) (SocketResources, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if socket < 0 || socket >= m.config.Sockets {
		return SocketResources{}, fmt.Errorf("invalid socket: %d", socket)
	}
	return m.sockets[socket], nil
}

// ScoreSockets evaluates all sockets for a given resource request.
// Returns scores sorted by preference (best first).
func (m *RDTManager) ScoreSockets(minWays int, minMem float64) []SocketScore {
	m.mu.RLock()
	defer m.mu.RUnlock()

	scores := make([]SocketScore, 0, m.config.Sockets)
	for i := 0; i < m.config.Sockets; i++ {
		sr := m.sockets[i]
		availWays := sr.AvailableWays()
		availMem := sr.AvailableMem()
		canSatisfy := availWays >= minWays && availMem >= minMem

		// Score: more headroom is better
		// Normalize ways and mem to comparable scales
		waysScore := float64(availWays) / float64(m.config.TotalWays)
		memScore := availMem / 100.0
		score := waysScore + memScore

		scores = append(scores, SocketScore{
			Socket:     i,
			AvailWays:  availWays,
			AvailMem:   availMem,
			Score:      score,
			CanSatisfy: canSatisfy,
		})
	}

	// Sort by: can satisfy first, then by score descending
	sort.Slice(scores, func(i, j int) bool {
		if scores[i].CanSatisfy != scores[j].CanSatisfy {
			return scores[i].CanSatisfy
		}
		return scores[i].Score > scores[j].Score
	})

	return scores
}

// BestSocketFor returns the best socket for a given resource request.
// Returns the socket with the most headroom that can satisfy the request.
// If no socket can satisfy, returns the one with most headroom anyway.
func (m *RDTManager) BestSocketFor(minWays int, minMem float64) int {
	scores := m.ScoreSockets(minWays, minMem)
	if len(scores) == 0 {
		return 0
	}
	return scores[0].Socket
}

// ReserveBestEffort reserves resources for a container without creating an RDT class.
// AssignSocket assigns a container to a socket symbolically (no capacity consumed).
// The container stays in system default. Capacity is only consumed when probing
// completes and allocation is committed.
// Returns the socket assignment and whether the requested resources could theoretically fit.
func (m *RDTManager) AssignSocket(containerIndex int, containerKey string, wantWays int, wantMem float64) (*SocketAssignment, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if container already has an assignment or allocation
	if existing := m.assignments[containerIndex]; existing != nil {
		return nil, false, fmt.Errorf("container %d already has a socket assignment", containerIndex)
	}
	if existing := m.allocations[containerIndex]; existing != nil {
		return nil, false, fmt.Errorf("container %d already has an allocation", containerIndex)
	}

	// Find best socket based on current actual allocations (not pending)
	scores := m.scoreSocketsLocked(wantWays, wantMem)
	if len(scores) == 0 {
		return nil, false, fmt.Errorf("no sockets available")
	}
	socket := scores[0].Socket
	canSatisfy := scores[0].CanSatisfy

	// Create symbolic assignment (no capacity consumed)
	assignment := &SocketAssignment{
		ContainerIndex: containerIndex,
		ContainerKey:   containerKey,
		Socket:         socket,
		AssignedAt:     time.Now(),
	}

	// Track pending container count for load balancing hints
	m.sockets[socket].PendingContainers++
	m.assignments[containerIndex] = assignment

	m.logger.WithFields(logrus.Fields{
		"container":    containerIndex,
		"socket":       socket,
		"can_satisfy":  canSatisfy,
		"pending_on_socket": m.sockets[socket].PendingContainers,
	}).Debug("Socket assigned (symbolic, container stays in system default)")

	return assignment, canSatisfy, nil
}

// GetAssignment returns the socket assignment for a container, or nil if none exists.
func (m *RDTManager) GetAssignment(containerIndex int) *SocketAssignment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.assignments[containerIndex]
}

// ReleaseAssignment releases a container's socket assignment.
func (m *RDTManager) ReleaseAssignment(containerIndex int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if a := m.assignments[containerIndex]; a != nil {
		m.sockets[a.Socket].PendingContainers--
		if m.sockets[a.Socket].PendingContainers < 0 {
			m.sockets[a.Socket].PendingContainers = 0
		}
		delete(m.assignments, containerIndex)
	}
}

// StartProbeAllocation creates an initial probing allocation using max available resources.
// This is called when probing starts. The container gets a dedicated RDT class.
// Returns the allocation handle with the max resources available on the assigned socket.
func (m *RDTManager) StartProbeAllocation(containerIndex int) (*AllocationHandle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get the socket assignment
	assignment := m.assignments[containerIndex]
	if assignment == nil {
		return nil, fmt.Errorf("no socket assignment for container %d", containerIndex)
	}

	// Check if already has allocation
	if existing := m.allocations[containerIndex]; existing != nil {
		return existing, nil // Already allocated, return existing
	}

	socket := assignment.Socket
	sr := &m.sockets[socket]

	// Use all available resources for probing
	availWays := sr.AvailableWays()
	availMem := sr.AvailableMem()

	if availWays < 1 {
		return nil, fmt.Errorf("no cache ways available on socket %d for probing", socket)
	}

	// Create probing allocation at max available
	newMask := m.computeNewMaskLocked(socket, availWays)

	handle := &AllocationHandle{
		ContainerIndex: containerIndex,
		ContainerKey:   assignment.ContainerKey,
		ClassName:      m.containerClassName(containerIndex, assignment.ContainerKey),
		Socket:         socket,
		L3Ways:         availWays,
		L3Mask:         newMask,
		MemBandwidth:   availMem,
		GrantedAt:      time.Now(),
		FloorWays:      0, // No floor during probing
		FloorMem:       0,
	}

	// Update allocation tracking
	sr.AllocatedWays += availWays
	sr.AllocatedMem += availMem
	sr.AllocatedMask |= newMask
	m.recomputeSharedPoolLocked(socket)
	m.allocations[containerIndex] = handle
	m.dirty = true

	// Clear pending count since now allocated
	sr.PendingContainers--
	if sr.PendingContainers < 0 {
		sr.PendingContainers = 0
	}
	delete(m.assignments, containerIndex)

	m.logger.WithFields(logrus.Fields{
		"container": containerIndex,
		"socket":    socket,
		"ways":      availWays,
		"mem":       availMem,
		"mask":      fmt.Sprintf("0x%x", newMask),
		"class":     handle.ClassName,
	}).Info("Started probing allocation (max available resources)")

	return handle, nil
}

// CommitProbeResult finalizes a probe by setting the actual needed resources.
// This releases excess resources back to the shared pool.
// If probing determined the container needs less than initially allocated,
// the allocation is shrunk and consolidated.
func (m *RDTManager) CommitProbeResult(containerIndex int, neededWays int, neededMem float64) (*AllocationHandle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing := m.allocations[containerIndex]
	if existing == nil {
		return nil, fmt.Errorf("no allocation for container %d to commit", containerIndex)
	}

	// If probe found we need less, shrink the allocation
	if neededWays < existing.L3Ways || neededMem < existing.MemBandwidth {
		// Release old allocation
		m.releaseInternalLocked(existing)
		delete(m.allocations, containerIndex)

		// Allocate the committed amount
		socket := existing.Socket
		newMask := m.computeNewMaskLocked(socket, neededWays)

		handle := &AllocationHandle{
			ContainerIndex: containerIndex,
			ContainerKey:   existing.ContainerKey,
			ClassName:      existing.ClassName,
			Socket:         socket,
			L3Ways:         neededWays,
			L3Mask:         newMask,
			MemBandwidth:   neededMem,
			GrantedAt:      time.Now(),
			FloorWays:      neededWays, // Committed
			FloorMem:       neededMem,
		}

		sr := &m.sockets[socket]
		sr.AllocatedWays += neededWays
		sr.AllocatedMem += neededMem
		sr.AllocatedMask |= newMask
		m.recomputeSharedPoolLocked(socket)
		m.allocations[containerIndex] = handle
		m.dirty = true

		// Consolidate to reclaim released space
		if err := m.consolidateLocked(); err != nil {
			m.logger.WithError(err).Warn("Failed to consolidate after probe commit")
		}

		m.logger.WithFields(logrus.Fields{
			"container":     containerIndex,
			"socket":        socket,
			"probed_ways":   existing.L3Ways,
			"committed_ways": neededWays,
			"probed_mem":    existing.MemBandwidth,
			"committed_mem": neededMem,
		}).Info("Probe committed - released excess resources")

		return handle, nil
	}

	// Probe found we need what we have (or more, which we can't give)
	// Just set the floor
	existing.FloorWays = existing.L3Ways
	existing.FloorMem = existing.MemBandwidth

	m.logger.WithFields(logrus.Fields{
		"container":  containerIndex,
		"floor_ways": existing.FloorWays,
		"floor_mem":  existing.FloorMem,
	}).Info("Probe committed - using full allocation")

	return existing, nil
}

// Allocate allocates RDT resources for a container.
// If the container already has an allocation, this updates it.
// Returns the allocation handle.
func (m *RDTManager) Allocate(req AllocationRequest) (*AllocationHandle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if req.Socket < 0 || req.Socket >= m.config.Sockets {
		return nil, fmt.Errorf("invalid socket: %d", req.Socket)
	}
	if req.L3Ways <= 0 {
		return nil, fmt.Errorf("L3Ways must be > 0")
	}
	if req.MemBandwidth < 0 {
		return nil, fmt.Errorf("MemBandwidth must be >= 0")
	}

	sr := &m.sockets[req.Socket]

	// Check if this container already has an allocation
	existing := m.allocations[req.ContainerIndex]
	if existing != nil {
		// Release existing allocation first (within the same lock)
		m.releaseInternalLocked(existing)
	}

	// Check availability
	availWays := sr.AvailableWays()
	availMem := sr.AvailableMem()

	if req.L3Ways > availWays {
		return nil, fmt.Errorf("insufficient cache ways on socket %d: need %d, have %d",
			req.Socket, req.L3Ways, availWays)
	}
	if req.MemBandwidth > availMem {
		return nil, fmt.Errorf("insufficient memory bandwidth on socket %d: need %.2f, have %.2f",
			req.Socket, req.MemBandwidth, availMem)
	}

	// Compute the new mask: take ways from the high end of the remaining shared space
	newMask := m.computeNewMaskLocked(req.Socket, req.L3Ways)

	// Create allocation handle
	handle := &AllocationHandle{
		ContainerIndex: req.ContainerIndex,
		ContainerKey:   req.ContainerKey,
		ClassName:      m.containerClassName(req.ContainerIndex, req.ContainerKey),
		Socket:         req.Socket,
		L3Ways:         req.L3Ways,
		L3Mask:         newMask,
		MemBandwidth:   req.MemBandwidth,
		GrantedAt:      time.Now(),
	}

	// Set floor if this is a committed (non-probing) allocation
	if !req.IsProbing {
		handle.FloorWays = req.L3Ways
		handle.FloorMem = req.MemBandwidth
	}

	// Update resource tracking
	sr.AllocatedWays += req.L3Ways
	sr.AllocatedMem += req.MemBandwidth
	sr.AllocatedMask |= newMask

	// Recompute shared pool
	m.recomputeSharedPoolLocked(req.Socket)

	// Store allocation
	m.allocations[req.ContainerIndex] = handle
	m.dirty = true

	m.logger.WithFields(logrus.Fields{
		"container":     req.ContainerIndex,
		"socket":        req.Socket,
		"ways":          req.L3Ways,
		"mem":           req.MemBandwidth,
		"mask":          fmt.Sprintf("0x%x", newMask),
		"is_probing":    req.IsProbing,
		"class":         handle.ClassName,
	}).Debug("RDT allocation granted")

	return handle, nil
}

// AllocateBestEffort attempts to allocate the requested resources, but if not
// enough resources are available, it allocates the maximum available.
// It first finds the best socket for the request, then allocates what's possible.
// Returns the allocation handle and a boolean indicating if full request was satisfied.
func (m *RDTManager) AllocateBestEffort(req AllocationRequest) (*AllocationHandle, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find best socket if not specified or if current socket doesn't have enough
	originalSocket := req.Socket
	score := m.scoreSocketsLocked(req.L3Ways, req.MemBandwidth)

	// Use the best socket (most headroom)
	if len(score) > 0 {
		req.Socket = score[0].Socket
	}

	sr := &m.sockets[req.Socket]

	// Check if container already has an allocation
	existing := m.allocations[req.ContainerIndex]
	if existing != nil {
		m.releaseInternalLocked(existing)
	}

	availWays := sr.AvailableWays()
	availMem := sr.AvailableMem()

	// Determine what we can actually allocate (best-effort)
	actualWays := req.L3Ways
	actualMem := req.MemBandwidth
	fullySatisfied := true

	if actualWays > availWays {
		actualWays = availWays
		fullySatisfied = false
	}
	if actualMem > availMem {
		actualMem = availMem
		fullySatisfied = false
	}

	// Need at least 1 way to make an allocation
	if actualWays < 1 {
		return nil, false, fmt.Errorf("no cache ways available on any socket (requested: %d, original_socket: %d, best_socket: %d)",
			req.L3Ways, originalSocket, req.Socket)
	}

	// Compute the new mask
	newMask := m.computeNewMaskLocked(req.Socket, actualWays)

	handle := &AllocationHandle{
		ContainerIndex: req.ContainerIndex,
		ContainerKey:   req.ContainerKey,
		ClassName:      m.containerClassName(req.ContainerIndex, req.ContainerKey),
		Socket:         req.Socket,
		L3Ways:         actualWays,
		L3Mask:         newMask,
		MemBandwidth:   actualMem,
		GrantedAt:      time.Now(),
	}

	if !req.IsProbing {
		handle.FloorWays = actualWays
		handle.FloorMem = actualMem
	}

	// Update resource tracking
	sr.AllocatedWays += actualWays
	sr.AllocatedMem += actualMem
	sr.AllocatedMask |= newMask

	m.recomputeSharedPoolLocked(req.Socket)
	m.allocations[req.ContainerIndex] = handle
	m.dirty = true

	m.logger.WithFields(logrus.Fields{
		"container":        req.ContainerIndex,
		"socket":           req.Socket,
		"requested_ways":   req.L3Ways,
		"granted_ways":     actualWays,
		"requested_mem":    req.MemBandwidth,
		"granted_mem":      actualMem,
		"mask":             fmt.Sprintf("0x%x", newMask),
		"fully_satisfied":  fullySatisfied,
		"best_effort":      !fullySatisfied,
	}).Info("RDT best-effort allocation")

	return handle, fullySatisfied, nil
}

// scoreSocketsLocked evaluates sockets without acquiring the lock.
func (m *RDTManager) scoreSocketsLocked(minWays int, minMem float64) []SocketScore {
	scores := make([]SocketScore, 0, m.config.Sockets)
	for i := 0; i < m.config.Sockets; i++ {
		sr := m.sockets[i]
		availWays := sr.AvailableWays()
		availMem := sr.AvailableMem()
		canSatisfy := availWays >= minWays && availMem >= minMem

		waysScore := float64(availWays) / float64(m.config.TotalWays)
		memScore := availMem / 100.0
		score := waysScore + memScore

		scores = append(scores, SocketScore{
			Socket:     i,
			AvailWays:  availWays,
			AvailMem:   availMem,
			Score:      score,
			CanSatisfy: canSatisfy,
		})
	}

	sort.Slice(scores, func(i, j int) bool {
		if scores[i].CanSatisfy != scores[j].CanSatisfy {
			return scores[i].CanSatisfy
		}
		return scores[i].Score > scores[j].Score
	})

	return scores
}

// UpdateAllocation updates an existing allocation.
// This is used during probing to adjust resources without full release/allocate cycles.
func (m *RDTManager) UpdateAllocation(containerIndex int, ways int, mem float64, isProbing bool) (*AllocationHandle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing := m.allocations[containerIndex]
	if existing == nil {
		return nil, fmt.Errorf("no allocation for container %d", containerIndex)
	}

	// Enforce floor constraints unless probing
	if !isProbing {
		if ways < existing.FloorWays {
			ways = existing.FloorWays
		}
		if mem < existing.FloorMem {
			mem = existing.FloorMem
		}
	}

	// Release and reallocate on the same socket
	socket := existing.Socket
	containerKey := existing.ContainerKey
	m.releaseInternalLocked(existing)

	return m.allocateLocked(AllocationRequest{
		ContainerIndex: containerIndex,
		ContainerKey:   containerKey,
		Socket:         socket,
		L3Ways:         ways,
		MemBandwidth:   mem,
		IsProbing:      isProbing,
	})
}

// allocateLocked is the internal allocation without acquiring the lock.
func (m *RDTManager) allocateLocked(req AllocationRequest) (*AllocationHandle, error) {
	sr := &m.sockets[req.Socket]

	availWays := sr.AvailableWays()
	availMem := sr.AvailableMem()

	if req.L3Ways > availWays {
		return nil, fmt.Errorf("insufficient cache ways on socket %d: need %d, have %d",
			req.Socket, req.L3Ways, availWays)
	}
	if req.MemBandwidth > availMem {
		return nil, fmt.Errorf("insufficient memory bandwidth on socket %d: need %.2f, have %.2f",
			req.Socket, req.MemBandwidth, availMem)
	}

	newMask := m.computeNewMaskLocked(req.Socket, req.L3Ways)

	handle := &AllocationHandle{
		ContainerIndex: req.ContainerIndex,
		ContainerKey:   req.ContainerKey,
		ClassName:      m.containerClassName(req.ContainerIndex, req.ContainerKey),
		Socket:         req.Socket,
		L3Ways:         req.L3Ways,
		L3Mask:         newMask,
		MemBandwidth:   req.MemBandwidth,
		GrantedAt:      time.Now(),
	}

	if !req.IsProbing {
		handle.FloorWays = req.L3Ways
		handle.FloorMem = req.MemBandwidth
	}

	sr.AllocatedWays += req.L3Ways
	sr.AllocatedMem += req.MemBandwidth
	sr.AllocatedMask |= newMask
	m.recomputeSharedPoolLocked(req.Socket)
	m.allocations[req.ContainerIndex] = handle
	m.dirty = true

	return handle, nil
}

// CommitAllocation commits a probing allocation by setting the floor.
func (m *RDTManager) CommitAllocation(containerIndex int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	handle := m.allocations[containerIndex]
	if handle == nil {
		return fmt.Errorf("no allocation for container %d", containerIndex)
	}

	handle.FloorWays = handle.L3Ways
	handle.FloorMem = handle.MemBandwidth

	m.logger.WithFields(logrus.Fields{
		"container":  containerIndex,
		"floor_ways": handle.FloorWays,
		"floor_mem":  handle.FloorMem,
	}).Debug("RDT allocation committed")

	return nil
}

// Release releases a container's allocation or assignment and consolidates remaining allocations.
func (m *RDTManager) Release(containerIndex int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for socket assignment first (symbolic, no capacity consumed)
	if a := m.assignments[containerIndex]; a != nil {
		m.sockets[a.Socket].PendingContainers--
		if m.sockets[a.Socket].PendingContainers < 0 {
			m.sockets[a.Socket].PendingContainers = 0
		}
		delete(m.assignments, containerIndex)
		m.logger.WithField("container", containerIndex).Debug("Released socket assignment")
		return nil
	}

	// Check for allocation (actual RDT resources)
	handle := m.allocations[containerIndex]
	if handle == nil {
		// No allocation or assignment to release
		return nil
	}

	m.releaseInternalLocked(handle)
	delete(m.allocations, containerIndex)

	// Consolidate to avoid fragmentation
	return m.consolidateLocked()
}

// releaseInternalLocked releases resources without consolidation.
func (m *RDTManager) releaseInternalLocked(handle *AllocationHandle) {
	if handle == nil {
		return
	}

	sr := &m.sockets[handle.Socket]
	sr.AllocatedWays -= handle.L3Ways
	sr.AllocatedMem -= handle.MemBandwidth
	sr.AllocatedMask &^= handle.L3Mask

	if sr.AllocatedWays < 0 {
		sr.AllocatedWays = 0
	}
	if sr.AllocatedMem < 0 {
		sr.AllocatedMem = 0
	}

	m.recomputeSharedPoolLocked(handle.Socket)
	delete(m.allocations, handle.ContainerIndex)
	m.dirty = true
}

// GetAllocation returns the allocation for a container, or nil if none exists.
func (m *RDTManager) GetAllocation(containerIndex int) *AllocationHandle {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.allocations[containerIndex]
}

// GetAllAllocations returns a copy of all current allocations.
func (m *RDTManager) GetAllAllocations() []*AllocationHandle {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*AllocationHandle, 0, len(m.allocations))
	for _, h := range m.allocations {
		hCopy := *h
		result = append(result, &hCopy)
	}
	return result
}

// ApplyIfDirty applies pending configuration changes to the RDT backend.
func (m *RDTManager) ApplyIfDirty() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.dirty {
		return nil
	}
	return m.applyConfigurationLocked()
}

// Consolidate repacks all allocations to maintain contiguous bitmasks.
// This should be called after releasing allocations to avoid fragmentation.
func (m *RDTManager) Consolidate() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.consolidateLocked()
}

// consolidateLocked repacks allocations per socket.
func (m *RDTManager) consolidateLocked() error {
	for socket := 0; socket < m.config.Sockets; socket++ {
		if err := m.consolidateSocketLocked(socket); err != nil {
			return err
		}
	}
	return m.applyConfigurationLocked()
}

// consolidateSocketLocked repacks allocations on a single socket.
// Allocations are packed at the HIGH end, ordered by grant time (oldest first).
func (m *RDTManager) consolidateSocketLocked(socket int) error {
	// Collect allocations for this socket
	var socketAllocs []*AllocationHandle
	for _, h := range m.allocations {
		if h.Socket == socket {
			socketAllocs = append(socketAllocs, h)
		}
	}

	if len(socketAllocs) == 0 {
		// No allocations, reset to full shared pool
		fullMask := lowMask(m.config.TotalWays)
		m.sockets[socket].AllocatedMask = 0
		m.sockets[socket].SharedMask = fullMask
		m.sockets[socket].SharedMem = 100.0 - m.sockets[socket].ReservedMem
		return nil
	}

	// Sort by grant time (oldest first) to maintain deterministic packing
	sort.Slice(socketAllocs, func(i, j int) bool {
		return socketAllocs[i].GrantedAt.Before(socketAllocs[j].GrantedAt)
	})

	// Recompute masks by packing at the high end
	totalWays := m.config.TotalWays
	curPos := totalWays // Start at the top

	var combinedMask uint64
	for _, h := range socketAllocs {
		ways := h.L3Ways
		curPos -= ways
		newMask := lowMask(ways) << curPos
		h.L3Mask = newMask
		combinedMask |= newMask
	}

	m.sockets[socket].AllocatedMask = combinedMask
	m.recomputeSharedPoolLocked(socket)
	m.dirty = true

	m.logger.WithFields(logrus.Fields{
		"socket":      socket,
		"allocations": len(socketAllocs),
		"alloc_mask":  fmt.Sprintf("0x%x", combinedMask),
		"shared_mask": fmt.Sprintf("0x%x", m.sockets[socket].SharedMask),
	}).Debug("RDT allocations consolidated")

	return nil
}

// computeNewMaskLocked computes a contiguous mask for a new allocation.
// Takes ways from the high end of the remaining shared space.
func (m *RDTManager) computeNewMaskLocked(socket, ways int) uint64 {
	sr := &m.sockets[socket]

	// Find the current boundary between shared and allocated
	// Shared pool is always at the low end, so the boundary is after the last shared way
	sharedWays := m.config.TotalWays - sr.AllocatedWays

	// New allocation goes at the top of the shared space (becoming part of allocated)
	startPos := sharedWays - ways
	if startPos < 0 {
		startPos = 0
	}

	return lowMask(ways) << startPos
}

// recomputeSharedPoolLocked updates the shared pool mask based on allocations.
func (m *RDTManager) recomputeSharedPoolLocked(socket int) {
	sr := &m.sockets[socket]
	sharedWays := m.config.TotalWays - sr.AllocatedWays
	if sharedWays <= 0 {
		sr.SharedMask = 0
		sr.SharedMem = 0
	} else {
		sr.SharedMask = lowMask(sharedWays)
		sr.SharedMem = 100.0 - sr.ReservedMem - sr.AllocatedMem
		if sr.SharedMem < 0 {
			sr.SharedMem = 0
		}
	}
}

// applyConfiguration applies the current configuration to the RDT backend.
func (m *RDTManager) applyConfiguration() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applyConfigurationLocked()
}

// applyConfigurationLocked applies without locking.
func (m *RDTManager) applyConfigurationLocked() error {
	classes := make(map[string]struct {
		Socket0 *allocation.SocketAllocation
		Socket1 *allocation.SocketAllocation
	})

	// Shared pool class
	var sock0Shared, sock1Shared *allocation.SocketAllocation
	sock0Shared = &allocation.SocketAllocation{
		L3Bitmask:    fmt.Sprintf("0x%x", m.sockets[0].SharedMask),
		MemBandwidth: m.sockets[0].SharedMem,
	}
	if m.config.Sockets > 1 {
		sock1Shared = &allocation.SocketAllocation{
			L3Bitmask:    fmt.Sprintf("0x%x", m.sockets[1].SharedMask),
			MemBandwidth: m.sockets[1].SharedMem,
		}
	}
	classes[m.config.SharedClass] = struct {
		Socket0 *allocation.SocketAllocation
		Socket1 *allocation.SocketAllocation
	}{Socket0: sock0Shared, Socket1: sock1Shared}

	// Container allocations
	for _, h := range m.allocations {
		var sock0, sock1 *allocation.SocketAllocation
		if h.Socket == 0 {
			sock0 = &allocation.SocketAllocation{
				L3Bitmask:    fmt.Sprintf("0x%x", h.L3Mask),
				MemBandwidth: h.MemBandwidth,
			}
		} else {
			sock1 = &allocation.SocketAllocation{
				L3Bitmask:    fmt.Sprintf("0x%x", h.L3Mask),
				MemBandwidth: h.MemBandwidth,
			}
		}
		classes[h.ClassName] = struct {
			Socket0 *allocation.SocketAllocation
			Socket1 *allocation.SocketAllocation
		}{Socket0: sock0, Socket1: sock1}
	}

	if err := m.backend.CreateAllRDTClasses(classes); err != nil {
		return err
	}

	m.dirty = false
	m.logger.WithField("classes", len(classes)).Debug("RDT configuration applied")
	return nil
}

// MoveContainerToClass assigns all PIDs of a container to its dedicated class.
func (m *RDTManager) MoveContainerToClass(containerPID int, containerIndex int) error {
	m.mu.RLock()
	handle := m.allocations[containerIndex]
	m.mu.RUnlock()

	if handle == nil {
		// Move to shared class if no allocation
		return m.backend.AssignContainerToClass(containerPID, m.config.SharedClass)
	}

	return m.backend.AssignContainerToClass(containerPID, handle.ClassName)
}

// MoveContainerToShared moves a container to the shared pool class.
func (m *RDTManager) MoveContainerToShared(containerPID int) error {
	return m.backend.AssignContainerToClass(containerPID, m.config.SharedClass)
}

// containerClassName generates a class name for a container.
func (m *RDTManager) containerClassName(containerIndex int, containerKey string) string {
	if containerKey != "" {
		return fmt.Sprintf("dyn-%s", sanitizeName(containerKey))
	}
	return fmt.Sprintf("dyn-c%d", containerIndex)
}

// Cleanup releases all allocations and resets to initial state.
func (m *RDTManager) Cleanup() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.allocations = make(map[int]*AllocationHandle)
	for i := 0; i < m.config.Sockets; i++ {
		fullMask := lowMask(m.config.TotalWays)
		m.sockets[i].AllocatedWays = 0
		m.sockets[i].AllocatedMem = 0
		m.sockets[i].AllocatedMask = 0
		m.sockets[i].SharedMask = fullMask
		m.sockets[i].SharedMem = 100.0 - m.config.ReservedMem
	}
	m.dirty = true

	if err := m.applyConfigurationLocked(); err != nil {
		return err
	}

	return m.backend.Cleanup()
}

// --- Helper functions ---

// lowMask creates a contiguous bitmask of n bits starting from bit 0.
func lowMask(n int) uint64 {
	if n <= 0 {
		return 0
	}
	if n >= 64 {
		return ^uint64(0)
	}
	return (uint64(1) << n) - 1
}

// countBits counts the number of set bits in a mask.
func countBits(mask uint64) int {
	count := 0
	for mask != 0 {
		count += int(mask & 1)
		mask >>= 1
	}
	return count
}

// sanitizeName sanitizes a string for use in resctrl class names.
func sanitizeName(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_' {
			result = append(result, c)
		}
	}
	if len(result) == 0 {
		return "container"
	}
	return string(result)
}
