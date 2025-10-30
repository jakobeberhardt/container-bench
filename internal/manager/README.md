# Resource Manager

The ResourceManager provides a simple interface for schedulers to allocate Intel RDT resources (L3 cache) to containers dynamically during benchmark execution.

## Overview

The ResourceManager abstracts away the complexity of:
- Managing RDT partitions and classes
- Syncing container PIDs from cgroups to RDT classes
- Converting between percentage and cache ways
- Applying RDT configurations using the goresctrl library

## Interface

```go
type RDTAllocator interface {
    Initialize() error
    RegisterContainer(containerIndex int, pid int, cgroupPath string) error
    AllocateL3Cache(containerIndex int, percentage float64) error
    AllocateL3CacheWays(containerIndex int, ways int) error
    Reset() error
    Shutdown() error
}
```

## Usage Example

### In a Scheduler

```go
func (s *MyScheduler) Initialize(allocator RDTAllocator, containers []ContainerInfo) error {
    s.rdtAllocator = allocator
    s.containers = containers
    
    // Allocate 33% of L3 cache to each of 3 containers
    for _, container := range containers {
        if err := s.rdtAllocator.AllocateL3Cache(container.Index, 0.33); err != nil {
            return err
        }
    }
    
    return nil
}
```

### Allocating by Ways

```go
// Allocate exactly 5 cache ways to container 0
err := resourceManager.AllocateL3CacheWays(0, 5)
```

### Allocating by Percentage

```go
// Allocate 50% of available cache to container 1
err := resourceManager.AllocateL3Cache(1, 0.50)
```

## Fair Scheduler

The Fair Scheduler divides L3 cache equally among all containers:

- With 15 cache ways and 3 containers: each gets 5 ways
- With 14 cache ways and 3 containers: two get 5 ways, one gets 4 ways

To use the fair scheduler, set in your benchmark YAML:

```yaml
benchmark:
  scheduler:
    implementation: fair
    rdt: true
    log_level: info
```

## Implementation Details

### RDT Configuration Structure

The ResourceManager uses the goresctrl library's configuration format:

1. Creates a partition named "container-bench"
2. Within this partition, creates classes for each container (e.g., "container-0", "container-1")
3. Sets L3 allocation for each class using CacheProportion format (percentage strings like "33%")

### PID Syncing

The ResourceManager automatically handles PID syncing:

1. Reads all PIDs from the container's cgroup (`/sys/fs/cgroup/system.slice/docker-{containerID}.scope/cgroup.procs`)
2. Adds these PIDs to the appropriate RDT class
3. This happens automatically whenever an allocation is made

Schedulers don't need to worry about PID management - just call the allocation methods.

### Thread Safety

The ResourceManager uses mutexes to ensure thread-safe operation when multiple goroutines may call allocation methods.

## Notes

- All containers must be registered before allocations can be made
- The manager ensures allocations don't exceed available cache ways
- Percentage allocations are rounded to whole cache ways
- When dividing cache unevenly (e.g., 14 ways / 3 containers), the first containers get the extra ways
