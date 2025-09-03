# Data Structure Documentation

## Overview

The RDT4NN driver uses a comprehensive data structure designed to support both real-time scheduling decisions and detailed analytical queries. Each row represents a single sampling point for one container at a specific time.

## Database Schema

### Primary Measurement: `benchmark_metrics`

#### Tags (Indexed for Fast Queries)
- `benchmark_id`: Unique benchmark identifier (string)
- `container_name`: Container name (e.g., "container0")
- `container_index`: Container index number (string)
- `container_image`: Docker image name
- `used_scheduler`: Scheduler implementation used

#### Core Fields (Benchmark Metadata)
| Field | Type | Description |
|-------|------|-------------|
| `benchmark_id` | int64 | Numeric benchmark identifier |
| `benchmark_started` | timestamp | Unix timestamp when benchmark started |
| `benchmark_finished` | timestamp | Unix timestamp when benchmark finished (optional) |
| `sampling_frequency` | int | Sampling frequency in milliseconds |
| `sampling_step` | int64 | Sequential step number (0, 1, 2, ...) |
| `relative_time_ms` | int64 | Milliseconds since benchmark start |
| `container_core` | int | Assigned CPU core for container |
| `cpu_executed_on` | int | Actual CPU where measurement was taken |

#### Docker Statistics Fields
| Field | Type | Description |
|-------|------|-------------|
| `docker_cpu_usage_percent` | float64 | CPU usage percentage (0-100) |
| `docker_cpu_usage_total` | uint64 | Total CPU usage in nanoseconds |
| `docker_cpu_usage_kernel` | uint64 | Kernel CPU usage in nanoseconds |
| `docker_cpu_usage_user` | uint64 | User CPU usage in nanoseconds |
| `docker_memory_usage` | uint64 | Memory usage in bytes |
| `docker_memory_limit` | uint64 | Memory limit in bytes |
| `docker_memory_usage_percent` | float64 | Memory usage percentage |
| `docker_memory_cache` | uint64 | Cached memory in bytes |
| `docker_memory_rss` | uint64 | Resident set size in bytes |
| `docker_network_rx_bytes` | uint64 | Network bytes received |
| `docker_network_tx_bytes` | uint64 | Network bytes transmitted |
| `docker_disk_read_bytes` | uint64 | Disk bytes read |
| `docker_disk_write_bytes` | uint64 | Disk bytes written |

#### Performance Counter Fields (Perf)
| Field | Type | Description |
|-------|------|-------------|
| `perf_cpu_cycles` | uint64 | CPU cycles consumed |
| `perf_instructions` | uint64 | Instructions executed |
| `perf_cache_references` | uint64 | Cache references |
| `perf_cache_misses` | uint64 | Cache misses |
| `perf_branch_misses` | uint64 | Branch prediction misses |
| `perf_page_faults` | uint64 | Page faults |
| `perf_context_switches` | uint64 | Context switches |
| `perf_cpu_migrations` | uint64 | CPU migrations |
| `perf_ipc` | float64 | Instructions per cycle |
| `perf_cache_miss_rate` | float64 | Cache miss rate (0-1) |

#### Intel RDT Fields
| Field | Type | Description |
|-------|------|-------------|
| `rdt_llc_occupancy` | uint64 | Last Level Cache occupancy in bytes |
| `rdt_local_mem_bw` | float64 | Local memory bandwidth in MB/s |
| `rdt_remote_mem_bw` | float64 | Remote memory bandwidth in MB/s |
| `rdt_total_mem_bw` | float64 | Total memory bandwidth in MB/s |
| `rdt_cache_allocation` | string | Cache allocation bitmask |
| `rdt_mb_allocation` | int | Memory bandwidth allocation percentage |
| `rdt_ipc_rate` | float64 | RDT-measured IPC rate |
| `rdt_misses_per_ki` | float64 | Cache misses per thousand instructions |

## Data Access Patterns

### 1. Real-Time Scheduling Interface

```go
// Interface for schedulers to receive real-time metrics
type MetricsConsumer interface {
    OnMetrics(ctx context.Context, metrics *storage.BenchmarkMetrics) error
    GetSchedulingDecisions(ctx context.Context) ([]SchedulingDecision, error)
}

// Real-time data access
func (metrics *BenchmarkMetrics) GetCPUUsage() float64
func (metrics *BenchmarkMetrics) GetMemoryUsage() uint64  
func (metrics *BenchmarkMetrics) GetCacheOccupancy() uint64
func (metrics *BenchmarkMetrics) GetMemoryBandwidth() float64
```

### 2. Analytical Queries

#### Get all data for a benchmark
```flux
from(bucket: "benchmarks")
|> range(start: -1h)
|> filter(fn: (r) => r.benchmark_id == "123")
|> sort(columns: ["_time", "container_name"])
```

#### Analyze busy neighbor effects
```flux
from(bucket: "benchmarks")
|> range(start: -30m)
|> filter(fn: (r) => r._field == "docker_cpu_usage_percent" or 
                     r._field == "rdt_llc_occupancy" or 
                     r._field == "rdt_total_mem_bw")
|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
```

#### Compare schedulers
```flux
from(bucket: "benchmarks")
|> range(start: -24h)
|> filter(fn: (r) => r._field == "perf_ipc")
|> group(columns: ["used_scheduler"])
|> mean()
```

## Benefits of This Structure

### 1. Real-Time Scheduling Support
- **Low Latency**: Direct interface for schedulers to receive metrics
- **Complete Context**: All container and system metrics in one structure
- **Buffering**: Recent metrics buffered for trend analysis
- **Decision Tracking**: Scheduling decisions logged with reasoning

### 2. Comprehensive Analysis
- **Wide Tables**: All metrics for a container at one timestamp in single row
- **Time Series**: Proper timestamps (UTC + relative) for time-based analysis
- **Metadata Rich**: Complete context (scheduler, frequency, image, etc.)
- **Cross-Container**: Easy correlation of metrics across containers

### 3. Performance Optimization
- **Indexed Tags**: Fast filtering by benchmark, container, scheduler
- **Batch Writes**: All metrics for a timestamp written together
- **Efficient Queries**: Wide rows reduce join operations
- **Data Locality**: Related metrics stored together

### 4. Extensibility
- **Pluggable Collectors**: Easy to add new metric sources
- **Flexible Fields**: Optional fields don't break existing queries
- **Versioning**: Benchmark ID increments support historical comparison
- **Metadata Evolution**: Easy to add new metadata fields

## Example Data Row

```json
{
  "benchmark_id": 42,
  "container_name": "container0",
  "container_index": 0,
  "container_image": "nginx:alpine",
  "container_core": 2,
  "used_scheduler": "adaptive",
  "sampling_frequency": 500,
  "sampling_step": 156,
  "relative_time_ms": 78000,
  "cpu_executed_on": 3,
  "benchmark_started": 1693046400,
  "docker_cpu_usage_percent": 45.2,
  "docker_memory_usage": 134217728,
  "docker_memory_usage_percent": 12.5,
  "perf_ipc": 1.8,
  "perf_cache_miss_rate": 0.035,
  "rdt_llc_occupancy": 2097152,
  "rdt_total_mem_bw": 1250.5,
  "timestamp": "2023-08-26T12:00:78.000Z"
}
```

This comprehensive structure enables both real-time decision making and deep analytical insights into container busy neighbor effects.
