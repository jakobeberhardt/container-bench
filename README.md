# Container Bench

A comprehensive Go-based tool for defining, running, and profiling container-based benchmarks on Intel RDT-enabled machines to study the busy neighbor problem regarding memory bandwidth and caches.

## Features

- **YAML-based Configuration**: Easy-to-define benchmark specifications
- **Container Orchestration**: Docker-based container management with precise timing control
- **Hardware-level Profiling**: 
  - Intel RDT (Resource Director Technology) metrics
  - Linux perf performance counters
  - Docker container statistics
- **Data Storage**: InfluxDB integration for high-resolution time-series data
- **Pluggable Schedulers**: Multiple scheduling strategies to mitigate busy neighbor effects
  - Default: Basic CPU pinning and resource limits
  - RDT: Intel RDT-based resource isolation
  - Adaptive: Machine learning-inspired adaptive scheduling
- **Comprehensive Metadata Capture**: Automatic collection and storage of benchmark metadata
  - Complete configuration preservation (original YAML with secrets unexpanded)
  - Execution environment details (host, CPU, OS, kernel)
  - Container configuration and runtime information
  - Data collection statistics and performance metrics
  - Reproducibility and audit trail support

## Prerequisites

### System Requirements
- Linux system with Intel RDT support
- Docker installed and running
- InfluxDB server for data storage

### Intel RDT Setup
```bash
# Install Intel RDT tools
sudo apt-get install intel-pqos-tools

# Mount resctrl filesystem
sudo mkdir -p /sys/fs/resctrl
sudo mount -t resctrl resctrl /sys/fs/resctrl

# Verify RDT support
pqos -s
```

### Performance Tools
```bash
# Install perf tools
sudo apt-get install linux-tools-generic

# Allow non-root access to performance counters (optional)
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
```

## Installation

```bash
# Clone the repository
git clone https://github.com/jakobeberhardt/container-bench.git
cd container-bench

# Build the driver
go build -o container-bench

# Install (optional)
sudo cp container-bench /usr/local/bin/
```

## Usage

### Basic Usage
```bash
# Validate configuration
./container-bench validate -c examples/simple_test.yml

# Run benchmark
./container-bench -c examples/simple_test.yml

# Run with verbose logging
./container-bench -c examples/simple_test.yml -v

# Run with detailed metadata reporting
./container-bench -c examples/simple_test.yml --print-meta-data
```

### Configuration File Structure

```yaml
benchmark:
  name: "benchmark_name"              # Unique benchmark identifier
  max_t: 300                          # Duration in seconds (-1 for indefinite)
  log_level: "info"                   # Log level (trace, debug, info, warn, error)
  scheduler:
    implementation: "adaptive"        # Scheduler type (default, rdt, adaptive)
    rdt: true                         # Enable Intel RDT support
  data:
    profilefrequency: 100             # Profiling interval in milliseconds
    db:                               # InfluxDB configuration
      host: "localhost:8086"
      name: "benchmarks"
      user: "username"
      password: "password"
    rdt: true                         # Collect RDT metrics
    perf: true                        # Collect perf metrics
    dockerstats: true                 # Collect Docker stats
  docker:
    auth:                             # Docker registry authentication
      registry: "registry.example.com"
      username: "user"
      password: "pass"

# Container definitions
container0:
  index: 0                            # Container index for identification
  image: "nginx:alpine"               # Docker image
  port: "80:8080"                     # Port mapping (optional)
  start: 0                            # Start time in seconds
  stop: -1                            # Stop time (-1 for manual stop)
  core: 1                             # CPU core to pin to (optional)
  env:                                # Environment variables (optional)
    ENV_VAR: "value"
```

### Example Configurations

#### Simple Test
```bash
./container-bench -c examples/simple_test.yml
```
Runs a single NGINX container for basic functionality testing.

#### Memory-Intensive Benchmark
```bash
./container-bench -c examples/memory_intensive.yml
```
Runs multiple memory-intensive workloads to study cache and memory bandwidth interference.

#### Multi-Container Benchmark with Adaptive Scheduling
```bash
./container-bench -c examples/redis_nginx_benchmark.yml
```
Demonstrates adaptive scheduling with multiple Redis and NGINX containers.

## Schedulers

### Default Scheduler
- Basic CPU pinning using taskset
- Simple resource limit enforcement
- No dynamic adjustments

### RDT Scheduler
- Intel RDT resource group management
- Cache allocation technology (CAT)
- Memory bandwidth allocation (MBA)
- Container isolation based on priority

### Adaptive Scheduler
- Dynamic performance monitoring
- Interference detection algorithms
- Automatic resource reallocation
- Machine learning-inspired optimization

## Data Collection

The driver collects multiple types of performance data:

### Docker Statistics
- CPU usage percentage
- Memory usage (bytes and percentage)
- Network I/O (RX/TX bytes)
- Block I/O (read/write bytes)

### Performance Counters (perf)
- CPU cycles and instructions
- Cache references and misses
- Branch predictions and misses
- Page faults and context switches
- Last-level cache (LLC) metrics

### Intel RDT Metrics
- Last-level cache occupancy per core
- Memory bandwidth utilization (local and remote)
- Cache allocation enforcement
- Memory bandwidth throttling status

### Data Schema

All metrics are stored in InfluxDB with the following structure:
```
measurement_name{
  benchmark_id="unique_id",
  container_id="short_id", 
  container_name="name",
  container_index="0",
  collector="docker_stats|perf|rdt"
} value=123.45 timestamp
```

## Advanced Usage

### Custom Scheduler Implementation

Create a new scheduler by implementing the `Scheduler` interface:

```go
type CustomScheduler struct {
    // Your scheduler fields
}

func (cs *CustomScheduler) Initialize(ctx context.Context) error {
    // Initialization logic
}

func (cs *CustomScheduler) ScheduleContainer(ctx context.Context, containerID string, config config.ContainerConfig) error {
    // Container scheduling logic
}

func (cs *CustomScheduler) Monitor(ctx context.Context, containerIDs map[string]string) error {
    // Dynamic monitoring and adjustment logic
}

func (cs *CustomScheduler) Finalize(ctx context.Context) error {
    // Cleanup logic
}

func (cs *CustomScheduler) Name() string {
    return "custom"
}
```

### Custom Profiler Collector

Implement the `Collector` interface to add custom metrics:

```go
type CustomCollector struct {
    benchmarkID string
}

func (cc *CustomCollector) Initialize(ctx context.Context) error {
    // Setup custom monitoring
}

func (cc *CustomCollector) Collect(ctx context.Context, timestamp time.Time) ([]storage.Measurement, error) {
    // Collect custom metrics
    return measurements, nil
}

func (cc *CustomCollector) Close() error {
    // Cleanup resources
}

func (cc *CustomCollector) Name() string {
    return "custom_collector"
}
```

## Troubleshooting

### Common Issues

1. **Permission denied when accessing RDT**
   ```bash
   sudo mount -t resctrl resctrl /sys/fs/resctrl
   sudo chmod -R 755 /sys/fs/resctrl
   ```

2. **Docker permission denied**
   ```bash
   sudo usermod -aG docker $USER
   # Logout and login again
   ```

3. **InfluxDB connection failed**
   ```bash
   # Check InfluxDB is running
   sudo systemctl status influxdb
   
   # Test connection
   curl -I http://localhost:8086/ping
   ```

4. **perf events not available**
   ```bash
   # Enable perf events
   echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
   ```

### Debug Mode

Enable debug logging for detailed information:
```bash
./container-bench -c config.yml -v
```

Or set debug level in configuration:
```yaml
benchmark:
  log_level: debug
```