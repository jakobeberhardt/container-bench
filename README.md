# Container-Bench

A configurable and flexible tool for defining and profiling Docker-based container benchmarks. This tool provides performance monitoring using perf counters, Docker stats, and Intel Resource Director Technology (RDT), with data export to InfluxDB for analysis.

## Features

- **Multi-Container Benchmarking**: Define and run benchmarks with multiple containers
- **Performance Monitoring**: 
  - Hardware performance counters via perf (cgroup-based)
  - Docker container statistics
  - Intel RDT cache and memory bandwidth monitoring
- **Thread-Safe Data Collection**: Real-time data collection with concurrent goroutines
- **Flexible Scheduling**: Scheduler interface for implementing resource management policies
- **Database Integration**: Export time-series data to InfluxDB 2.7
- **Metadata Collection**: Automatic collection of benchmark metadata for reporting
- **YAML Configuration**: Easily define and run benchmarks

## Installation

```bash
# Clone the repository
git clone https://github.com/jakobeberhardt/container-bench
cd container-bench
# optional: install go
bash setup/install-go.sh
# Build the application
make build
./container-bench help
```
## Prerequisites

- Go 1.23 or later
- Docker
- Linux kernel with perf events support
- Intel RDT support (optional, for cache allocation monitoring)
- InfluxDB 2.7+ instance

## Configuration

### Environment Variables

The application automatically loads environment variables from a `.env` file in the current directory or the application directory. Create a `.env` file with your InfluxDB configuration:

```bash
# .env file
INFLUXDB_HOST=https://your-influxdb-host
INFLUXDB_USER=your-username
INFLUXDB_TOKEN=your-token
INFLUXDB_ORG=your-org
INFLUXDB_BUCKET=benchmarks
```

**Note**: The application will automatically detect and load this file - no need to manually export variables.

### Benchmark Configuration

Define benchmarks in YAML files. See `examples/simple_test.yml` for a complete example:

```yaml
benchmark:
  name: "My Benchmark"
  description: "Description of the benchmark"
  max_t: 30  # Maximum duration in seconds
  log_level: info
  scheduler:
    implementation: default
    rdt: false
  data:
    db:
      host: ${INFLUXDB_HOST}
      name: ${INFLUXDB_BUCKET}
      user: ${INFLUXDB_USER}
      password: ${INFLUXDB_TOKEN}
      org: ${INFLUXDB_ORG}

container0:
  index: 0
  image: nginx:alpine
  core: 0  # CPU core affinity
  command: "stress-ng --cpu 1 --timeout 25s"
  data:
    frequency: 100  # Collection frequency in ms
    perf: true      # Enable perf monitoring
    docker: true    # Enable Docker stats
    rdt: false      # Enable Intel RDT
```

## Usage

### Run a Benchmark

```bash
# The application automatically loads .env file
./container-bench run -c examples/simple_test.yml
```

### Validate Configuration

```bash
./container-bench validate -c examples/simple_test.yml
```

## Data Collection

The tool collects the following metrics:

### Perf Metrics (Hardware Counters)
- Cache misses and references
- Instructions and CPU cycles
- Branch instructions and misses
- L1/L3 cache metrics
- Derived metrics (cache miss rate, branch miss rate)

### Docker Metrics
- CPU usage (total, kernel, user, percentage)
- Memory usage (total, RSS, cache, swap)
- Network I/O (bytes and packets)
- Disk I/O (bytes and operations)

### RDT Metrics
- L3 cache occupancy
- Memory bandwidth usage
- CLOS group assignment

### Metadata Collection
Container-bench automatically collects comprehensive metadata for each benchmark run, including:
- Benchmark configuration and timing information
- Host system details (hostname, OS, CPU specifications)
- Data collection statistics (sampling steps, measurements, data size)
- Original configuration file content

Metadata is exported to the `benchmark_meta` table for generating detailed reports. 

## Architecture

```
container-bench/
├── cmd/                    # Main application entry point
├── internal/
│   ├── config/            # Configuration parsing and validation
│   ├── dataframe/         # Thread-safe data structures
│   ├── collectors/        # Data collection implementations
│   ├── scheduler/         # Scheduler interface and implementations
│   └── database/          # InfluxDB integration
├── examples/              # Example benchmark configurations
└── setup/                  # Host setup scripts
```

### Key Components

- **DataFrames**: Thread-safe data structures for storing time-series metrics
- **Collectors**: Goroutines that collect metrics at specified frequencies
- **Scheduler**: Interface for implementing resource management policies
- **Database**: InfluxDB integration for data export and analysis

## Data Access

The data frame provides structured access to collected metrics:

```go
// Access cache misses for container 0 at step 15
misses := dataframes.Container(0).Step(15).Perf.CacheMisses

// Get latest metrics for all containers
for containerIndex, containerDF := range dataframes.GetAllContainers() {
    latest := containerDF.GetLatestStep()
    if latest != nil && latest.Perf != nil {
        // Process performance data
    }
}
```

## Development

### Building
```bash
sudo setup/install-go.sh
```
```bash
make build
```

### Formatting

```bash
make fmt
make vet
```

## Host Setup

The `setup/` directory contains scripts for setting up Intel RDT and other system requirements:

```bash
cd setup/
sudo install-collectors.sh # Installs perf / RDT
sudo install-docker.sh 
```

## Database Schema

Data is exported to InfluxDB with the following structure:

### Time-Series Data
- **Measurement**: `benchmark_metrics`
- **Tags**: `benchmark_id`, `container_index`, `container_image`, `container_core`
- **Fields**: All collected metrics with prefixes:
  - `perf_*`: Performance counter metrics
  - `docker_*`: Docker statistics
  - `rdt_*`: Intel RDT metrics

### Metadata
- **Measurement**: `benchmark_meta`
- **Tags**: `benchmark_id`
- **Fields**: Comprehensive metadata including system info, configuration, and statistics

