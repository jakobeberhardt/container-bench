# Host Configuration - Single Source of Truth

## Overview

The `HostConfig` component provides a comprehensive, singleton-based single source of truth for all host-related information in container-bench. It uses reliable Go packages and system interfaces to gather accurate information about the host system.

## Architecture

### Singleton Pattern
`HostConfig` is initialized once using `sync.Once` and cached for the lifetime of the application, ensuring consistent information across all components.

```go
hostConfig, err := host.GetHostConfig()
```

## Data Sources

### 1. CPU Information
**Source:** `github.com/klauspost/cpuid/v2`

Provides:
- CPU vendor and model/brand name
- Physical and logical core counts
- Threads per core (hyperthreading detection)
- Cache hierarchy (L1, L2, L3) with sizes
- Cache line size

### 2. System Information
**Source:** `github.com/zcalusic/sysinfo`

Provides:
- Hostname
- OS name and architecture
- Kernel version
- Additional system metadata

### 3. CPU Topology Mapping
**Source:** `/sys/devices/system/cpu/cpu*/topology/`

Provides detailed core topology:
- Physical package/socket ID for each logical CPU
- Core ID within socket
- Hyperthread siblings mapping
- Die information

### 4. RDT Cache Information
**Source:** `/sys/fs/resctrl/info/L3/`

Provides:
- Cache bit mask (CBM) to determine cache ways
- Maximum number of CLOSIDs
- Cache allocation bitmask

### 5. Intel RDT
**Source:** `github.com/intel/goresctrl/pkg/rdt`

Provides:
- RDT support detection
- Monitoring capabilities
- Allocation capabilities
- Available RDT classes
- Monitoring features per resource

## Data Structures

### HostConfig
Main configuration structure containing all host information.

```go
type HostConfig struct {
    CPUVendor       string        // CPU vendor (e.g., "GenuineIntel")
    CPUModel        string        // CPU model name
    Topology        CPUTopology   // Detailed CPU topology
    L1Cache         CacheConfig   // L1 cache info
    L2Cache         CacheConfig   // L2 cache info
    L3Cache         L3CacheConfig // L3 cache info with RDT details
    RDT             RDTConfig     // Intel RDT configuration
    Hostname        string        // System hostname
    OSInfo          string        // OS name and architecture
    KernelVersion   string        // Kernel version
}
```

### CPUTopology
Detailed CPU topology information.

```go
type CPUTopology struct {
    PhysicalCores   int                    // Total physical cores
    LogicalCores    int                    // Total logical cores (with HT)
    ThreadsPerCore  int                    // Threads per core (1 or 2)
    Sockets         int                    // Number of CPU sockets
    CoresPerSocket  int                    // Cores per socket
    CoreMap         map[int]CoreInfo       // Logical CPU ID -> Core info
}
```

### CoreInfo
Information about a specific logical CPU.

```go
type CoreInfo struct {
    LogicalID       int        // Logical CPU ID (0, 1, 2, ...)
    PhysicalID      int        // Physical socket/package ID
    CoreID          int        // Core ID within socket
    Siblings        []int      // Hyperthread sibling IDs
}
```

### CacheConfig
Cache configuration for L1/L2 caches.

```go
type CacheConfig struct {
    SizeBytes       int64      // Size in bytes
    SizeKB          float64    // Size in KB
    SizeMB          float64    // Size in MB
    LineSize        int        // Cache line size in bytes
}
```

### L3CacheConfig
L3 cache configuration with RDT-specific information.

```go
type L3CacheConfig struct {
    SizeBytes       int64      // Total L3 cache size in bytes
    SizeKB          float64    // Size in KB
    SizeMB          float64    // Size in MB
    LineSize        int        // Cache line size in bytes
    CacheIDs        []uint64   // Available L3 cache IDs
    WaysPerCache    int        // Number of cache ways (from RDT)
    BytesPerWay     int64      // Bytes per cache way
    MaxBitmask      uint64     // Maximum allocation bitmask
}
```

### RDTConfig
Intel RDT configuration.

```go
type RDTConfig struct {
    Supported              bool
    MonitoringSupported    bool
    AllocationSupported    bool
    AvailableClasses       []string
    MonitoringFeatures     map[string][]string
    MaxCLOSIDs             int     // Max number of CLOSIDs
    
    // NYI: Memory bandwidth information
    // No reliable source for actual system memory bandwidth
}
```

## Usage Examples

### Basic Host Information
```go
hostConfig, err := host.GetHostConfig()
if err != nil {
    return err
}

fmt.Printf("CPU: %s %s\n", hostConfig.CPUVendor, hostConfig.CPUModel)
fmt.Printf("Cores: %d physical, %d logical\n", 
    hostConfig.Topology.PhysicalCores, 
    hostConfig.Topology.LogicalCores)
```

### CPU Topology
```go
// Get core mapping for CPU affinity
coreInfo := hostConfig.Topology.CoreMap[0]
fmt.Printf("CPU 0: Socket %d, Core %d, Siblings: %v\n",
    coreInfo.PhysicalID, coreInfo.CoreID, coreInfo.Siblings)
```

### Cache Information
```go
// Get L3 cache details
fmt.Printf("L3 Cache: %.2f MB\n", hostConfig.L3Cache.SizeMB)
fmt.Printf("Cache Ways: %d\n", hostConfig.L3Cache.WaysPerCache)
fmt.Printf("Bytes per Way: %d\n", hostConfig.L3Cache.BytesPerWay)
```

### RDT Capabilities
```go
if hostConfig.RDT.Supported {
    fmt.Printf("RDT Available\n")
    fmt.Printf("Max CLOSIDs: %d\n", hostConfig.RDT.MaxCLOSIDs)
    fmt.Printf("Monitoring: %v\n", hostConfig.RDT.MonitoringSupported)
    fmt.Printf("Allocation: %v\n", hostConfig.RDT.AllocationSupported)
}
```

### Utility Functions

#### Fair L3 Allocation
```go
// Calculate fair cache allocation for N containers
ways, bitmask := hostConfig.GetFairL3Allocation(4)
fmt.Printf("Each container gets %d ways (bitmask: 0x%x)\n", ways, bitmask)
```

#### Cache Utilization
```go
// Calculate cache utilization percentage
occupancyBytes := uint64(10 * 1024 * 1024) // 10 MB
utilization := hostConfig.GetL3CacheUtilizationPercent(occupancyBytes)
fmt.Printf("Cache utilization: %.2f%%\n", utilization)
```

## Integration with Components

### Schedulers
Schedulers receive the `HostConfig` to make informed decisions:

```go
type Scheduler interface {
    SetHostConfig(hostConfig *host.HostConfig)
    // ...
}
```

Example usage in scheduler:
```go
func (s *CacheAwareScheduler) SetHostConfig(hostConfig *host.HostConfig) {
    s.hostConfig = hostConfig
    s.totalCacheWays = hostConfig.L3Cache.WaysPerCache
}
```

### Metadata Collection
The database metadata collector uses `HostConfig` for benchmark metadata:

```go
metadata := &BenchmarkMetadata{
    CPUModel:      hostConfig.CPUModel,
    TotalCPUCores: hostConfig.Topology.PhysicalCores,
    CPUThreads:    hostConfig.Topology.LogicalCores,
    L3CacheSizeBytes: hostConfig.L3Cache.SizeBytes,
    // ...
}
```

### RDT Manager
The RDT manager uses host configuration for allocation:

```go
func (rm *DefaultRDTManager) Initialize(hostConfig *host.HostConfig) error {
    rm.hostConfig = hostConfig
    totalWays := hostConfig.L3Cache.WaysPerCache
    // Use ways for allocation decisions
}
```

## Not Yet Implemented (NYI)

### Memory Bandwidth
Maximum memory bandwidth is not currently available from a reliable source. It would require:
- Platform-specific detection
- Manual configuration
- Benchmark-based measurement

The field is commented out with NYI documentation:
```go
// NYI: Memory bandwidth information
// No reliable source for actual system memory bandwidth
// MaxMemoryBandwidthMBps int64
```

## Testing

Use the provided test utility:
```bash
go run test_hostconfig.go
```

This will display:
- System information
- CPU information and topology
- Core mappings with hyperthread siblings
- Cache hierarchy details
- L3/LLC specifics including ways and bitmasks
- RDT capabilities
- Utility function examples

## Benefits

1. **Single Source of Truth**: All components access the same host information
2. **Reliable Data**: Uses well-tested packages and kernel interfaces
3. **Comprehensive Topology**: Detailed core mapping for affinity decisions
4. **RDT Integration**: Direct support for Intel RDT capabilities
5. **Singleton Pattern**: Efficient, initialized once
6. **No Fake Data**: Only real, verifiable information
7. **Clear NYI**: Unimplemented features are documented, not fabricated

## Dependencies

```
github.com/klauspost/cpuid/v2  - CPU information and capabilities
github.com/zcalusic/sysinfo    - System information
github.com/intel/goresctrl/pkg/rdt - Intel RDT support
/sys/devices/system/cpu/*      - CPU topology from kernel
/sys/fs/resctrl/*              - RDT information from kernel
```
