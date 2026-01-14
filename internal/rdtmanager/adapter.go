package rdtmanager

import (
	"container-bench/internal/allocation"
)

// RDTAllocatorAdapter wraps an allocation.RDTAllocator to implement RDTBackend.
// This allows the RDTManager to use existing allocator implementations.
type RDTAllocatorAdapter struct {
	allocator allocation.RDTAllocator
}

// NewRDTAllocatorAdapter creates a new adapter.
func NewRDTAllocatorAdapter(allocator allocation.RDTAllocator) *RDTAllocatorAdapter {
	return &RDTAllocatorAdapter{allocator: allocator}
}

func (a *RDTAllocatorAdapter) Initialize() error {
	return a.allocator.Initialize()
}

func (a *RDTAllocatorAdapter) CreateAllRDTClasses(classes map[string]struct {
	Socket0 *allocation.SocketAllocation
	Socket1 *allocation.SocketAllocation
}) error {
	return a.allocator.CreateAllRDTClasses(classes)
}

func (a *RDTAllocatorAdapter) AssignContainerToClass(pid int, className string) error {
	return a.allocator.AssignContainerToClass(pid, className)
}

func (a *RDTAllocatorAdapter) Cleanup() error {
	return a.allocator.Cleanup()
}
