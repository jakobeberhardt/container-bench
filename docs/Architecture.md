# RDT4NN Driver Architecture

## Overview

The RDT4NN Driver is designed as a modular, extensible system for conducting container-based performance benchmarks with hardware-level profiling capabilities.

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         CLI Layer                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Command   │  │ Validation  │  │   Configuration     │  │
│  │  Interface  │  │   Engine    │  │     Loading         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                    Benchmark Orchestration                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  Lifecycle  │  │   Timing    │  │    Signal          │  │
│  │  Manager    │  │  Controller │  │   Handling         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                     Core Components                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Container   │  │  Profiler   │  │    Scheduler       │  │
│  │  Manager    │  │  Manager    │  │     Manager        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                     Data Collection                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Docker    │  │    Perf     │  │       RDT          │  │
│  │ Statistics  │  │  Counters   │  │     Metrics        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                      Data Storage                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  InfluxDB   │  │   Query     │  │     Metadata       │  │
│  │  Client     │  │  Interface  │  │     Storage        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. CLI Layer (`cmd/`)

**Purpose**: Provides command-line interface for user interaction.

**Components**:
- `root.go`: Main CLI command handling, argument parsing, and validation
- Command routing and help system
- Configuration file validation

**Responsibilities**:
- Parse command-line arguments
- Load and validate configuration files  
- Initialize logging system
- Coordinate benchmark execution

### 2. Configuration Management (`internal/config/`)

**Purpose**: Handles YAML configuration parsing and validation.

**Key Structures**:
```go
type Config struct {
    Benchmark BenchmarkConfig
    Container map[string]ContainerConfig
}
```

**Features**:
- YAML configuration parsing
- Schema validation
- Default value assignment
- Type-safe configuration access

### 3. Benchmark Orchestration (`internal/benchmark/`)

**Purpose**: Coordinates the entire benchmark lifecycle.

**Key Components**:
- `Runner`: Main orchestration engine
- Lifecycle management (prepare → execute → cleanup)
- Signal handling for graceful shutdown
- Multi-goroutine coordination with sync.WaitGroup

**Execution Flow**:
1. **Preparation Phase**: Pull images, create containers, initialize profiling
2. **Execution Phase**: Start containers on schedule, begin profiling, monitor performance
3. **Cleanup Phase**: Stop containers, finalize data collection, clean up resources

### 4. Container Management (`internal/container/`)

**Purpose**: Docker container lifecycle management.

**Features**:
- Image pulling with authentication
- Container creation with resource constraints
- Scheduled container startup/shutdown
- CPU pinning and resource allocation
- Container statistics collection

**Key Methods**:
- `PullImages()`: Pull all required Docker images
- `CreateContainers()`: Create containers without starting them
- `StartContainer()`: Start specific container at scheduled time
- `GetContainerStats()`: Collect real-time container statistics

### 5. Profiling System (`internal/profiler/`)

**Purpose**: Multi-source hardware and software performance data collection.

**Architecture**:
```go
type Collector interface {
    Initialize(ctx context.Context) error
    Collect(ctx context.Context, timestamp time.Time) ([]storage.Measurement, error)
    Close() error
    Name() string
}
```

**Collectors**:
- **Docker Stats**: CPU, memory, network, and I/O statistics
- **Perf Counters**: Hardware performance counters (cycles, cache misses, etc.)
- **Intel RDT**: Cache occupancy and memory bandwidth metrics

**Collection Strategy**:
- High-frequency sampling (configurable from 50ms to seconds)
- Parallel collection from multiple sources
- Non-blocking data pipeline to storage
- Error resilience (continue collecting even if one collector fails)

### 6. Scheduling System (`internal/scheduler/`)

**Purpose**: Implements various strategies for mitigating busy neighbor interference.

**Scheduler Types**:

#### Default Scheduler
- Basic CPU pinning using taskset
- Simple resource limits
- No dynamic adjustments

#### RDT Scheduler  
- Intel RDT resource group management
- Cache Allocation Technology (CAT)
- Memory Bandwidth Allocation (MBA)
- Container isolation based on priority levels

#### Adaptive Scheduler
- Real-time performance monitoring
- Interference pattern detection
- Dynamic resource reallocation
- Machine learning-inspired optimization

**Interface**:
```go
type Scheduler interface {
    Initialize(ctx context.Context) error
    ScheduleContainer(ctx context.Context, containerID string, config config.ContainerConfig) error
    Monitor(ctx context.Context, containerIDs map[string]string) error
    Finalize(ctx context.Context) error
}
```

### 7. Storage System (`internal/storage/`)

**Purpose**: High-performance time-series data storage and retrieval.

**Features**:
- InfluxDB integration for time-series data
- Batch write operations for performance
- Automatic retries and error handling
- Metadata storage for benchmark context
- Query interface for data analysis

**Data Schema**:
```
measurement_name{
  benchmark_id="unique_id",
  container_id="short_id",
  container_name="name", 
  container_index="0",
  collector="source_type"
} field_name=value timestamp
```

## Data Flow

### Collection Pipeline
1. **Profiler Manager** coordinates multiple collectors
2. **Collectors** gather metrics from different sources in parallel
3. **Storage Manager** batches and writes measurements to InfluxDB
4. **Error handling** ensures data collection continues despite individual failures

### Scheduling Pipeline  
1. **Container events** trigger scheduler evaluation
2. **Performance metrics** feed back into scheduling decisions
3. **Resource adjustments** applied dynamically during runtime
4. **Interference mitigation** strategies activated based on detected patterns

## Extension Points

### Adding Custom Collectors
```go
type CustomCollector struct {
    benchmarkID string
    // custom fields
}

func (c *CustomCollector) Collect(ctx context.Context, timestamp time.Time) ([]storage.Measurement, error) {
    // Custom metric collection logic
    return measurements, nil
}
```

### Adding Custom Schedulers
```go
type CustomScheduler struct {
    // scheduler state
}

func (s *CustomScheduler) ScheduleContainer(ctx context.Context, containerID string, config config.ContainerConfig) error {
    // Custom scheduling logic
    return nil
}
```

## Performance Considerations

### Concurrent Design
- All major operations use goroutines with proper synchronization
- Non-blocking I/O for data collection and storage
- Configurable profiling frequency to balance resolution vs. overhead

### Resource Management
- Automatic cleanup on shutdown or error
- Resource pooling for database connections
- Memory-efficient metric batching

### Error Resilience
- Continue operation despite individual component failures
- Graceful degradation when optional features unavailable
- Comprehensive error logging and reporting

## Configuration Best Practices

### Profiling Frequency
- **High resolution** (50-100ms): For detailed interference studies
- **Medium resolution** (200-500ms): For general performance monitoring  
- **Low resolution** (1000ms+): For long-running benchmarks

### Container Scheduling
- **Immediate start** (start: 0): For baseline measurements
- **Staggered start** (start: 10, 20, 30): To observe interference onset
- **Overlapping periods**: To study busy neighbor effects

### Resource Allocation
- **CPU pinning**: Isolate containers to specific cores
- **Memory limits**: Control memory usage per container
- **I/O limits**: Prevent I/O interference between containers

This architecture provides a solid foundation for performance research while remaining extensible for future enhancements and specialized use cases.
