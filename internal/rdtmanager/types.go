// Package rdtmanager provides a clean, socket-aware RDT resource manager for
// the dynamic scheduler. It handles:
// - Per-socket resource tracking (L3 ways and memory bandwidth)
// - Contiguous bitmask management (allocations packed at high end)
// - Automatic consolidation on release to avoid fragmentation
// - Simple allocation/release API with proper bookkeeping
package rdtmanager

import (
	"container-bench/internal/allocation"
	"fmt"
	"time"
)

// SocketAssignment represents a symbolic reservation - just socket placement.
// It does NOT consume any capacity. The container stays in system default.
// Actual resource allocation happens when probing starts/completes.
type SocketAssignment struct {
	ContainerIndex int
	ContainerKey   string
	Socket         int
	AssignedAt     time.Time
}

// AllocationHandle represents a container's active RDT resource allocation.
// An allocation has a real RDT COS and the container runs in it.
type AllocationHandle struct {
	ContainerIndex int
	ContainerKey   string
	ClassName      string
	Socket         int
	L3Ways         int
	L3Mask         uint64
	MemBandwidth   float64
	GrantedAt      time.Time
	// FloorWays/FloorMem represent committed minimums that cannot be reduced
	// (except via Release). Used for QoS guarantee enforcement.
	FloorWays int
	FloorMem  float64
}

// SocketResources tracks available resources on a single socket.
type SocketResources struct {
	TotalWays    int
	ReservedWays int     // System reserve (cannot be allocated to priority containers)
	ReservedMem  float64 // System reserve for memory bandwidth

	// Allocated tracks resources granted to priority containers with actual RDT classes.
	// The benchmark/shared pool gets whatever remains after allocations.
	AllocatedWays int
	AllocatedMem  float64

	// AllocatedMask is the combined bitmask of all priority allocations.
	// Priority allocations are packed at the HIGH end of the way space.
	AllocatedMask uint64

	// SharedMask is the bitmask for the shared/benchmark pool.
	// Always a contiguous low block.
	SharedMask uint64
	SharedMem  float64

	// PendingContainers counts containers assigned to this socket awaiting probing.
	// Used for load balancing decisions (symbolic, no capacity consumed).
	PendingContainers int
}

// AvailableWays returns the maximum ways available for a new allocation.
func (sr *SocketResources) AvailableWays() int {
	return sr.TotalWays - sr.ReservedWays - sr.AllocatedWays
}

// AvailableMem returns the memory bandwidth available for new allocations.
func (sr *SocketResources) AvailableMem() float64 {
	return 100.0 - sr.ReservedMem - sr.AllocatedMem
}

// AllocationRequest specifies a resource request for allocation.
type AllocationRequest struct {
	ContainerIndex int
	ContainerKey   string
	Socket         int
	L3Ways         int
	MemBandwidth   float64
	// IsProbing indicates this is a temporary probing allocation that may be
	// adjusted. If false, the allocation is committed and establishes a floor.
	IsProbing bool
}

// SocketScore represents the suitability of a socket for a new allocation.
type SocketScore struct {
	Socket       int
	AvailWays    int
	AvailMem     float64
	Score        float64 // Higher is better
	CanSatisfy   bool    // Can satisfy the requested minimum
}

// Config holds configuration for the RDT manager.
type Config struct {
	TotalWays      int
	Sockets        int
	ReservedWays   int     // Per-socket system reserve for shared pool
	ReservedMem    float64 // Per-socket system reserve for memory bandwidth
	SharedClass    string  // Class name for the shared/benchmark pool
}

// RDTBackend abstracts the underlying RDT allocation operations.
// This allows the manager to be tested without real RDT hardware.
type RDTBackend interface {
	Initialize() error
	CreateAllRDTClasses(classes map[string]struct {
		Socket0 *allocation.SocketAllocation
		Socket1 *allocation.SocketAllocation
	}) error
	AssignContainerToClass(pid int, className string) error
	Cleanup() error
}

// Validate checks that the config is valid.
func (c *Config) Validate() error {
	if c.TotalWays <= 0 {
		return fmt.Errorf("TotalWays must be > 0")
	}
	if c.Sockets <= 0 || c.Sockets > 2 {
		return fmt.Errorf("Sockets must be 1 or 2")
	}
	if c.ReservedWays < 0 || c.ReservedWays >= c.TotalWays {
		return fmt.Errorf("ReservedWays must be >= 0 and < TotalWays")
	}
	if c.ReservedMem < 0 || c.ReservedMem >= 100 {
		return fmt.Errorf("ReservedMem must be >= 0 and < 100")
	}
	if c.SharedClass == "" {
		return fmt.Errorf("SharedClass must be specified")
	}
	return nil
}
