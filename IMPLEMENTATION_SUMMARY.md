# ResourceManager and Fair Scheduler Implementation

## Summary

Implemented a robust ResourceManager for Intel RDT L3 cache allocation and a Fair Scheduler that uses it to equally distribute cache among containers.

## Files Created/Modified

### 1. `/internal/manager/resource_manager.go`
- **RDTAllocator Interface**: Defines the contract for resource allocation
  - `Initialize()`: Sets up RDT and creates initial configuration
  - `RegisterContainer()`: Registers a container with PID and cgroup path
  - `AllocateL3Cache(containerIndex, percentage)`: Allocates cache by percentage (0.0-1.0)
  - `AllocateL3CacheWays(containerIndex, ways)`: Allocates specific number of cache ways
  - `Reset()`: Resets all allocations to full cache
  - `Shutdown()`: Cleans up RDT configuration

- **ResourceManager Implementation**:
  - Uses goresctrl library properly with Config/Partitions/Classes structure
  - Handles PID syncing from cgroups automatically
  - Thread-safe with mutex protection
  - Creates "container-bench" partition with classes for each container
  - Uses CacheProportion format (percentage strings) for allocation

### 2. `/internal/scheduler/scheduler.go`
- Added **RDTAllocator interface** definition for use by schedulers
- Kept existing Scheduler interface with RDTAllocator parameter

### 3. `/internal/scheduler/fair_scheduler.go`
- **FairScheduler** implementation that divides L3 cache equally
- Handles uneven division (e.g., 14 ways / 3 containers = 5, 5, 4)
- Applies allocation immediately on initialization
- Logs performance metrics during execution
- Resets allocations on shutdown

### 4. `/cmd/main.go`
- Updated to use ResourceManager instead of non-existent `NewDefaultRDTAllocator()`
- Registers all containers with ResourceManager including PID and cgroup path
- Removed reference to non-existent `CacheAwareScheduler`
- Properly initializes scheduler with RDTAllocator

### 5. `/internal/manager/README.md`
- Documentation for ResourceManager usage
- Examples for schedulers
- Implementation details

## Key Features

### Simple Scheduler Interface
Schedulers only need to call:
```go
resourceManager.AllocateL3Cache(containerIndex, 0.33)  // 33% of cache
// OR
resourceManager.AllocateL3CacheWays(containerIndex, 5)  // Exactly 5 ways
```

### Automatic PID Management
- ResourceManager reads PIDs from container cgroups automatically
- Syncs PIDs to RDT classes on every allocation
- Schedulers don't need to handle PID tracking

### Fair Distribution
The Fair Scheduler:
- Calculates `baseWays = totalWays / numContainers`
- Distributes remainder to first containers
- Example with 15 ways, 3 containers: 5, 5, 5
- Example with 14 ways, 3 containers: 5, 5, 4

### Proper goresctrl Usage
- Uses `rdt.Config` with Partitions and Classes structure
- Uses `CacheProportion` type with percentage strings
- Calls `rdt.SetConfig()` to apply entire configuration
- Uses `rdt.GetClass()` to retrieve created classes for PID syncing

## Usage

### In Benchmark YAML:
```yaml
benchmark:
  scheduler:
    implementation: fair  # or "default"
    rdt: true            # Enable RDT allocation
    log_level: info
```

### Testing:
1. Run with fair scheduler: `./container-bench -c examples/fair_scheduler.yml`
2. Monitor RDT classes: `sudo cat /sys/fs/resctrl/container-bench/*/schemata`
3. Check logs for allocation messages

## Technical Details

### RDT Structure:
```
/sys/fs/resctrl/
├── container-bench/              # Partition
│   ├── container-0/              # Class for container 0
│   │   ├── schemata              # L3:0=33%;1=33%
│   │   └── tasks                 # PIDs assigned
│   ├── container-1/              # Class for container 1
│   └── container-2/              # Class for container 2
```

### Thread Safety:
- All ResourceManager methods use mutex locks
- Safe for concurrent calls from scheduler goroutines

### Error Handling:
- Validates percentage ranges (0.0-1.0)
- Validates cache ways (1 to totalWays)
- Returns detailed errors for debugging
- Logs warnings for non-critical failures (e.g., PID sync issues)

## Next Steps

The ResourceManager can be extended to support:
- Memory Bandwidth Allocation (MBA)
- L2 Cache Allocation
- Dynamic reallocation based on monitoring data
- More sophisticated scheduling policies
