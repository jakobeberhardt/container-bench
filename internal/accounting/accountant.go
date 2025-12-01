package accounting

import (
	"fmt"
	"math"
	"os"
	"sync"

	"container-bench/internal/allocation"
	"container-bench/internal/host"
	"container-bench/internal/logging"

	"github.com/sirupsen/logrus"
)

// AllocationRequest represents a high-level resource allocation request
type AllocationRequest struct {
	// L3 Cache Bitmask > Ways > Bytes > Percent)
	L3Bitmask string
	L3Ways    string  // Cache way range (e.g., "0-5")
	L3Bytes   uint64  // Cache size in bytes (converted to ways using ceil)
	L3Percent float64 // Percentage of cache (0-100, converted to ways using ceil)

	MemBandwidth float64
}

// SocketState tracks the current allocation state for a socket
type SocketState struct {
	TotalWays        int
	AllocatedBitmask uint64 // Combined bitmask of all allocations
	MemBandwidthUsed float64
}

// ClassAllocation tracks the allocation for a specific RDT class
type ClassAllocation struct {
	ClassName  string
	Socket0    *allocation.SocketAllocation
	Socket1    *allocation.SocketAllocation
	Containers []int // PIDs assigned to this class
}

// RDTAccountant provides high-level resource accounting and allocation
// It wraps the RDT allocator and provides intelligent resource management
type RDTAccountant struct {
	allocator  allocation.RDTAllocator
	hostConfig *host.HostConfig
	logger     *logrus.Logger
	mu         sync.RWMutex

	// Per-socket state tracking
	socket0State SocketState
	socket1State SocketState

	// Class tracking
	classes        map[string]*ClassAllocation
	containerClass map[int]string // PID -> class name
}

// NewRDTAccountant creates a new RDT accountant instance
func NewRDTAccountant(allocator allocation.RDTAllocator, hostCfg *host.HostConfig) (*RDTAccountant, error) {
	var totalWays int
	var err error

	// Try to get ways from HostConfig first (for testability and consistency)
	if hostCfg != nil && hostCfg.L3Cache.WaysPerCache > 0 {
		totalWays = hostCfg.L3Cache.WaysPerCache
	} else {
		// Fall back to reading from resctrl filesystem
		totalWays, err = readL3Ways()
		if err != nil {
			return nil, fmt.Errorf("failed to read L3 cache info: %w", err)
		}
	}

	return &RDTAccountant{
		allocator:      allocator,
		hostConfig:     hostCfg,
		logger:         logging.GetSchedulerLogger(),
		socket0State:   SocketState{TotalWays: totalWays, AllocatedBitmask: 0, MemBandwidthUsed: 0},
		socket1State:   SocketState{TotalWays: totalWays, AllocatedBitmask: 0, MemBandwidthUsed: 0},
		classes:        make(map[string]*ClassAllocation),
		containerClass: make(map[int]string),
	}, nil
}

// Initialize initializes the accountant and underlying allocator
func (a *RDTAccountant) Initialize() error {
	return a.allocator.Initialize()
}

// CreateClass creates a new RDT class with the specified allocations
func (a *RDTAccountant) CreateClass(className string, socket0Req, socket1Req *AllocationRequest) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Check if class already exists
	if _, exists := a.classes[className]; exists {
		return fmt.Errorf("class %s already exists", className)
	}

	// Resolve requests to socket allocations (bitmasks)
	socket0Alloc, err := a.resolveAllocation(socket0Req, &a.socket0State, 0)
	if err != nil {
		return fmt.Errorf("failed to resolve socket0 allocation: %w", err)
	}

	socket1Alloc, err := a.resolveAllocation(socket1Req, &a.socket1State, 1)
	if err != nil {
		return fmt.Errorf("failed to resolve socket1 allocation: %w", err)
	}

	// Create the class in RDT
	if err := a.allocator.CreateRDTClass(className, socket0Alloc, socket1Alloc); err != nil {
		return err
	}

	// Track the allocation
	a.classes[className] = &ClassAllocation{
		ClassName:  className,
		Socket0:    socket0Alloc,
		Socket1:    socket1Alloc,
		Containers: make([]int, 0),
	}

	// Update socket states
	if socket0Alloc != nil && socket0Alloc.L3Bitmask != "" {
		bitmask := parseBitmask(socket0Alloc.L3Bitmask)
		a.socket0State.AllocatedBitmask |= bitmask
		a.socket0State.MemBandwidthUsed += socket0Alloc.MemBandwidth
	}
	if socket1Alloc != nil && socket1Alloc.L3Bitmask != "" {
		bitmask := parseBitmask(socket1Alloc.L3Bitmask)
		a.socket1State.AllocatedBitmask |= bitmask
		a.socket1State.MemBandwidthUsed += socket1Alloc.MemBandwidth
	}

	a.logger.WithField("class", className).Info("RDT class created and tracked in accountant")
	return nil
}

// UpdateClass updates an existing RDT class with new allocations
func (a *RDTAccountant) UpdateClass(className string, socket0Req, socket1Req *AllocationRequest) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Check if class exists
	classAlloc, exists := a.classes[className]
	if !exists {
		return fmt.Errorf("class %s does not exist", className)
	}

	// Free old allocations from socket states
	if classAlloc.Socket0 != nil && classAlloc.Socket0.L3Bitmask != "" {
		oldBitmask := parseBitmask(classAlloc.Socket0.L3Bitmask)
		a.socket0State.AllocatedBitmask &^= oldBitmask
		a.socket0State.MemBandwidthUsed -= classAlloc.Socket0.MemBandwidth
	}
	if classAlloc.Socket1 != nil && classAlloc.Socket1.L3Bitmask != "" {
		oldBitmask := parseBitmask(classAlloc.Socket1.L3Bitmask)
		a.socket1State.AllocatedBitmask &^= oldBitmask
		a.socket1State.MemBandwidthUsed -= classAlloc.Socket1.MemBandwidth
	}

	// Resolve new allocations
	socket0Alloc, err := a.resolveAllocation(socket0Req, &a.socket0State, 0)
	if err != nil {
		// Restore old allocations on error
		a.restoreSocketState(classAlloc)
		return fmt.Errorf("failed to resolve socket0 allocation: %w", err)
	}

	socket1Alloc, err := a.resolveAllocation(socket1Req, &a.socket1State, 1)
	if err != nil {
		// Restore old allocations on error
		a.restoreSocketState(classAlloc)
		return fmt.Errorf("failed to resolve socket1 allocation: %w", err)
	}

	// Update the class in RDT
	if err := a.allocator.UpdateRDTClass(className, socket0Alloc, socket1Alloc); err != nil {
		// Restore old allocations on error
		a.restoreSocketState(classAlloc)
		return err
	}

	// Update tracking
	classAlloc.Socket0 = socket0Alloc
	classAlloc.Socket1 = socket1Alloc

	// Update socket states with new allocations
	if socket0Alloc != nil && socket0Alloc.L3Bitmask != "" {
		bitmask := parseBitmask(socket0Alloc.L3Bitmask)
		a.socket0State.AllocatedBitmask |= bitmask
		a.socket0State.MemBandwidthUsed += socket0Alloc.MemBandwidth
	}
	if socket1Alloc != nil && socket1Alloc.L3Bitmask != "" {
		bitmask := parseBitmask(socket1Alloc.L3Bitmask)
		a.socket1State.AllocatedBitmask |= bitmask
		a.socket1State.MemBandwidthUsed += socket1Alloc.MemBandwidth
	}

	a.logger.WithField("class", className).Info("RDT class updated in accountant")
	return nil
}

// MoveContainer assigns a container to a specific RDT class
func (a *RDTAccountant) MoveContainer(containerPID int, className string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Check if class exists
	classAlloc, exists := a.classes[className]
	if !exists {
		return fmt.Errorf("class %s does not exist", className)
	}

	// Remove from old class if assigned
	if oldClassName, assigned := a.containerClass[containerPID]; assigned {
		if oldClass, exists := a.classes[oldClassName]; exists {
			oldClass.Containers = removeInt(oldClass.Containers, containerPID)
		}
	}

	// Assign to new class in RDT
	if err := a.allocator.AssignContainerToClass(containerPID, className); err != nil {
		return err
	}

	// Update tracking
	classAlloc.Containers = append(classAlloc.Containers, containerPID)
	a.containerClass[containerPID] = className

	a.logger.WithFields(logrus.Fields{
		"pid":   containerPID,
		"class": className,
	}).Debug("Container moved to RDT class")

	return nil
}

// DeleteClass removes an RDT class (moves containers to default first)
func (a *RDTAccountant) DeleteClass(className string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	classAlloc, exists := a.classes[className]
	if !exists {
		return fmt.Errorf("class %s does not exist", className)
	}

	// Move all containers to default class
	for _, pid := range classAlloc.Containers {
		if err := a.allocator.RemoveContainerFromClass(pid); err != nil {
			a.logger.WithError(err).WithField("pid", pid).Warn("Failed to remove container from class")
		}
		delete(a.containerClass, pid)
	}

	// Delete from RDT
	if err := a.allocator.DeleteRDTClass(className); err != nil {
		return err
	}

	// Free allocations from socket states
	if classAlloc.Socket0 != nil && classAlloc.Socket0.L3Bitmask != "" {
		bitmask := parseBitmask(classAlloc.Socket0.L3Bitmask)
		a.socket0State.AllocatedBitmask &^= bitmask
		a.socket0State.MemBandwidthUsed -= classAlloc.Socket0.MemBandwidth
	}
	if classAlloc.Socket1 != nil && classAlloc.Socket1.L3Bitmask != "" {
		bitmask := parseBitmask(classAlloc.Socket1.L3Bitmask)
		a.socket1State.AllocatedBitmask &^= bitmask
		a.socket1State.MemBandwidthUsed -= classAlloc.Socket1.MemBandwidth
	}

	// Remove from tracking
	delete(a.classes, className)

	a.logger.WithField("class", className).Info("RDT class deleted from accountant")
	return nil
}

// GetContainerClass returns the class name for a container
func (a *RDTAccountant) GetContainerClass(containerPID int) (string, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if className, exists := a.containerClass[containerPID]; exists {
		return className, nil
	}

	return a.allocator.GetContainerClass(containerPID)
}

// GetSocketState returns the current allocation state for a socket
func (a *RDTAccountant) GetSocketState(socketID int) (*SocketState, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	switch socketID {
	case 0:
		stateCopy := a.socket0State
		return &stateCopy, nil
	case 1:
		stateCopy := a.socket1State
		return &stateCopy, nil
	default:
		return nil, fmt.Errorf("invalid socket ID: %d", socketID)
	}
}

// Cleanup cleans up all RDT classes
func (a *RDTAccountant) Cleanup() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.logger.Info("Cleaning up RDT accountant")

	if err := a.allocator.Cleanup(); err != nil {
		return err
	}

	// Clear all tracking
	a.classes = make(map[string]*ClassAllocation)
	a.containerClass = make(map[int]string)
	a.socket0State.AllocatedBitmask = 0
	a.socket0State.MemBandwidthUsed = 0
	a.socket1State.AllocatedBitmask = 0
	a.socket1State.MemBandwidthUsed = 0

	return nil
}

// resolveAllocation converts an AllocationRequest to a SocketAllocation (bitmask)
func (a *RDTAccountant) resolveAllocation(req *AllocationRequest, state *SocketState, socketID int) (*allocation.SocketAllocation, error) {
	if req == nil {
		return nil, nil
	}

	alloc := &allocation.SocketAllocation{
		MemBandwidth: req.MemBandwidth,
	}

	// Validate memory bandwidth
	if req.MemBandwidth < 0 || req.MemBandwidth > 100 {
		return nil, fmt.Errorf("invalid memory bandwidth: %.2f%% (must be 0-100)", req.MemBandwidth)
	}

	// Check if total memory bandwidth would exceed 100%
	if state.MemBandwidthUsed+req.MemBandwidth > 100 {
		return nil, fmt.Errorf("memory bandwidth allocation would exceed 100%% on socket %d (used: %.2f%%, requested: %.2f%%)",
			socketID, state.MemBandwidthUsed, req.MemBandwidth)
	}

	// Priority: Bitmask > Ways > Bytes > Percentage
	if req.L3Bitmask != "" {
		// Direct bitmask - validate it doesn't overlap
		bitmask := parseBitmask(req.L3Bitmask)
		if state.AllocatedBitmask&bitmask != 0 {
			return nil, fmt.Errorf("L3 bitmask %s overlaps with existing allocations on socket %d", req.L3Bitmask, socketID)
		}
		alloc.L3Bitmask = req.L3Bitmask
		return alloc, nil
	}

	if req.L3Ways != "" {
		// Convert ways to bitmask
		bitmask, err := parseCacheWayRange(req.L3Ways, state.TotalWays)
		if err != nil {
			return nil, err
		}
		bitmaskInt := parseBitmask(bitmask)
		if state.AllocatedBitmask&bitmaskInt != 0 {
			return nil, fmt.Errorf("L3 ways %s overlaps with existing allocations on socket %d", req.L3Ways, socketID)
		}
		alloc.L3Bitmask = bitmask
		return alloc, nil
	}

	if req.L3Bytes > 0 {
		// Convert bytes to ways (ceil) and find best-fit bitmask
		if a.hostConfig == nil || a.hostConfig.L3Cache.BytesPerWay == 0 {
			return nil, fmt.Errorf("cannot convert bytes to cache ways: host config not available")
		}
		targetWays := int(math.Ceil(float64(req.L3Bytes) / float64(a.hostConfig.L3Cache.BytesPerWay)))
		if targetWays == 0 {
			targetWays = 1
		}
		if targetWays > state.TotalWays {
			return nil, fmt.Errorf("requested %d bytes requires %d cache ways but only %d available", req.L3Bytes, targetWays, state.TotalWays)
		}
		bitmask, err := a.findBestFitBitmaskForWays(targetWays, state)
		if err != nil {
			return nil, fmt.Errorf("failed to allocate %d bytes (%d ways) of L3 cache on socket %d: %w", req.L3Bytes, targetWays, socketID, err)
		}
		alloc.L3Bitmask = bitmask
		return alloc, nil
	}

	if req.L3Percent != 0 {
		// Validate percentage range (including negative check)
		if req.L3Percent < 0 || req.L3Percent > 100 {
			return nil, fmt.Errorf("invalid L3 percentage: %.2f%% (must be 0-100)", req.L3Percent)
		}
		// Find best-fit contiguous bitmask for percentage (with ceil rounding)
		bitmask, err := a.findBestFitBitmask(req.L3Percent, state)
		if err != nil {
			return nil, fmt.Errorf("failed to allocate %.2f%% of L3 cache on socket %d: %w", req.L3Percent, socketID, err)
		}
		alloc.L3Bitmask = bitmask
		return alloc, nil
	}

	return alloc, nil
}

// findBestFitBitmask finds the best contiguous bitmask for a percentage allocation
// Uses ceil rounding to ensure the allocation meets or exceeds the requested percentage
func (a *RDTAccountant) findBestFitBitmask(percent float64, state *SocketState) (string, error) {
	if percent < 0 || percent > 100 {
		return "", fmt.Errorf("invalid percentage: %.2f", percent)
	}

	// Use ceil to always round up - requesting 30% of 12 ways = ceil(3.6) = 4 ways
	targetWays := int(math.Ceil((float64(state.TotalWays) * percent) / 100.0))
	if targetWays == 0 {
		targetWays = 1
	}

	return a.findBestFitBitmaskForWays(targetWays, state)
}

// findBestFitBitmaskForWays finds the best contiguous bitmask for a specific number of ways
func (a *RDTAccountant) findBestFitBitmaskForWays(targetWays int, state *SocketState) (string, error) {
	if targetWays <= 0 {
		return "", fmt.Errorf("invalid target ways: %d (must be > 0)", targetWays)
	}
	if targetWays > state.TotalWays {
		return "", fmt.Errorf("requested %d ways but only %d available", targetWays, state.TotalWays)
	}

	// Find contiguous unallocated bits
	allocated := state.AllocatedBitmask
	bestStart := -1
	bestLength := 0

	for start := 0; start < state.TotalWays; start++ {
		if (allocated & (1 << start)) != 0 {
			continue // This way is allocated
		}

		// Count contiguous free ways starting from here
		length := 0
		for i := start; i < state.TotalWays && (allocated&(1<<i)) == 0; i++ {
			length++
		}

		// Check if this is a better fit (prefer smallest sufficient region)
		if length >= targetWays && (bestStart == -1 || length < bestLength) {
			bestStart = start
			bestLength = length
		}
	}

	if bestStart == -1 {
		return "", fmt.Errorf("no contiguous %d cache ways available", targetWays)
	}

	// Create bitmask for targetWays starting at bestStart
	mask := uint64(0)
	for i := 0; i < targetWays; i++ {
		mask |= (1 << (bestStart + i))
	}

	return fmt.Sprintf("0x%x", mask), nil
}

// restoreSocketState restores socket states after a failed update
func (a *RDTAccountant) restoreSocketState(classAlloc *ClassAllocation) {
	if classAlloc.Socket0 != nil && classAlloc.Socket0.L3Bitmask != "" {
		bitmask := parseBitmask(classAlloc.Socket0.L3Bitmask)
		a.socket0State.AllocatedBitmask |= bitmask
		a.socket0State.MemBandwidthUsed += classAlloc.Socket0.MemBandwidth
	}
	if classAlloc.Socket1 != nil && classAlloc.Socket1.L3Bitmask != "" {
		bitmask := parseBitmask(classAlloc.Socket1.L3Bitmask)
		a.socket1State.AllocatedBitmask |= bitmask
		a.socket1State.MemBandwidthUsed += classAlloc.Socket1.MemBandwidth
	}
}

// Helper functions

func readL3Ways() (int, error) {
	cbmMaskPath := "/sys/fs/resctrl/info/L3/cbm_mask"
	data, err := os.ReadFile(cbmMaskPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read L3 CBM mask: %w", err)
	}

	var cbmMask uint64
	if _, err := fmt.Sscanf(string(data), "%x", &cbmMask); err != nil {
		return 0, fmt.Errorf("failed to parse CBM mask: %w", err)
	}

	ways := 0
	for cbmMask > 0 {
		ways += int(cbmMask & 1)
		cbmMask >>= 1
	}

	return ways, nil
}

func parseCacheWayRange(wayRange string, totalWays int) (string, error) {
	var start, end int
	n, err := fmt.Sscanf(wayRange, "%d-%d", &start, &end)
	if err != nil || n != 2 {
		return "", fmt.Errorf("invalid cache way range format '%s', expected 'start-end' (e.g., '0-5')", wayRange)
	}

	if start < 0 || end >= totalWays || start > end {
		return "", fmt.Errorf("invalid cache way range %d-%d, must be within 0-%d and start <= end", start, end, totalWays-1)
	}

	mask := uint64(0)
	for i := start; i <= end; i++ {
		mask |= (1 << i)
	}

	return fmt.Sprintf("0x%x", mask), nil
}

func parseBitmask(bitmaskStr string) uint64 {
	var mask uint64
	// Handle both "0xff" and "ff" formats
	fmt.Sscanf(bitmaskStr, "0x%x", &mask)
	if mask == 0 {
		// Try without 0x prefix
		fmt.Sscanf(bitmaskStr, "%x", &mask)
	}
	return mask
}

func removeInt(slice []int, val int) []int {
	result := make([]int, 0, len(slice))
	for _, v := range slice {
		if v != val {
			result = append(result, v)
		}
	}
	return result
}
