# Container-Bench RDT and Fair Scheduler Implementation

## Overview

This document describes the implementation of unified host configuration, enhanced RDT metrics collection, and the new fair scheduler in container-bench.

## Host Configuration

### HostConfig Structure

A new `internal/host/hostconfig.go` module provides unified host system information:

```go
type HostConfig struct {
    // CPU Information
    CPUVendor       string
    CPUModel        string
    TotalCores      int
    TotalThreads    int
    NumSockets      int
    
    // Cache Information
    L3Cache         L3CacheConfig
    
    // RDT Information
    RDT             RDTConfig
    
    // System Information
    Hostname        string
    OSInfo          string
    KernelVersion   string
}
```

### Benefits

- **Single Source of Truth**: Host information is queried once at startup
- **Consistent Calculations**: Both scheduler and collectors use the same cache size data
- **Performance**: No repeated filesystem queries during runtime
- **Accuracy**: Proper cache utilization percentage calculations

## Enhanced RDT Metrics

### New Dataframe Fields

The RDT metrics in the dataframe now include:

```go
type RDTMetrics struct {
    // L3 Cache Monitoring
    L3CacheOccupancy     *uint64  // L3 cache occupancy in bytes
    
    // Memory Bandwidth Monitoring
    MemoryBandwidthTotal *uint64  // Total memory bandwidth in bytes/sec
    MemoryBandwidthLocal *uint64  // Local memory bandwidth in bytes/sec
    MemoryBandwidthMBps  *float64 // Memory bandwidth in MB/s
    
    // RDT Class Information
    RDTClassName         *string // Name of the RDT class/CLOS group
    MonGroupName         *string // Name of the monitoring group
    
    // Cache Allocation Information
    L3CacheAllocation    *uint64  // Allocated L3 cache ways/percentage
    L3CacheAllocationPct *float64 // L3 cache allocation percentage
    
    // Memory Bandwidth Allocation
    MBAThrottle          *uint64  // Memory bandwidth throttle percentage
    
    // Derived Metrics
    CacheLLCUtilizationPercent   *float64 // LLC cache utilization percentage
    BandwidthUtilizationPercent  *float64 // Bandwidth utilization percentage
}
```

### Database Fields

All new RDT metrics are properly stored in the InfluxDB with the following fields:

- `rdt_class_name` - RDT class name
- `rdt_mon_group_name` - Monitoring group name  
- `rdt_l3_cache_occupancy` - Cache occupancy in bytes
- `rdt_memory_bandwidth_total` - Total memory bandwidth
- `rdt_memory_bandwidth_local` - Local memory bandwidth
- `rdt_memory_bandwidth_mbps` - Memory bandwidth in MB/s
- `rdt_l3_cache_allocation` - Allocated cache ways
- `rdt_l3_cache_allocation_pct` - Cache allocation percentage
- `rdt_mba_throttle` - Memory bandwidth throttle
- `rdt_cache_llc_utilization_percent` - Cache utilization percentage
- `rdt_bandwidth_utilization_percent` - Bandwidth utilization percentage

## Fair Scheduler

### Purpose

The fair scheduler implements proportional LLC (Last Level Cache) allocation among containers:

- Divides available L3 cache ways equally among containers
- Monitors container performance metrics
- Logs recommendations for high cache miss scenarios
- Uses HostConfig for accurate calculations

### Configuration

```yaml
benchmark:
  scheduler:
    implementation: fair
    rdt: true
```

### Features

- **Proportional Allocation**: Each container gets equal L3 cache allocation
- **Real-time Monitoring**: Tracks cache miss rates and utilization
- **Performance Alerts**: Warns when containers exceed 40% cache miss rate
- **RDT Integration**: Uses Intel RDT for actual resource allocation

## Usage Examples

### Fair Scheduler Test

```yaml
benchmark:
  name: Fair Scheduler Test
  scheduler:
    implementation: fair
    rdt: true

container0:
  index: 0
  image: nginx:alpine
  data:
    frequency: 500
    perf: true
    docker: true
    rdt: true
```

### Available Schedulers

1. **default** - Basic monitoring only
2. **cache-aware** - Cache miss rate based decisions
3. **fair** - Proportional LLC allocation

## Implementation Details

### Separation of Concerns

- **Collectors**: Gather raw metrics (perf, docker, rdt)
- **Schedulers**: Make allocation decisions using RDTAllocator
- **HostConfig**: Provides system information to both

### RDT Collector Enhancements

- Uses HostConfig for cache size calculations
- Collects monitoring group names
- Calculates utilization percentages accurately
- Handles cache occupancy in multiple units (bytes, KB, MB)

### Data Flow

1. HostConfig initialized at startup
2. RDT collector uses HostConfig for calculations
3. Scheduler accesses HostConfig for allocation decisions
4. Datahandler processes all metrics
5. Database stores complete metric set

## Benefits

1. **Unified Configuration**: Single host config used throughout
2. **Complete Metrics**: All RDT fields properly collected and stored
3. **Fair Allocation**: Proportional resource distribution
4. **Performance Monitoring**: Real-time cache utilization tracking
5. **Scalability**: Easy to add new schedulers and metrics

## Dependencies

- Intel RDT supported hardware
- Linux resctrl filesystem
- `github.com/intel/goresctrl` package
- InfluxDB 2.7+ for metrics storage
