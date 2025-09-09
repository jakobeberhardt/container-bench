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
- **YAML Configuration**: Human-readable benchmark definitions

## Prerequisites

- Go 1.23 or later
- Docker
- Linux kernel with perf events support
- Intel RDT support (optional, for cache allocation monitoring)
- InfluxDB 2.7+ instance

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd container-bench

# Build the application
make build

# Install system-wide (optional)
make install
```

## Configuration

### Environment Variables

Create a `.env` file with your InfluxDB configuration:

```bash
INFLUXDB_HOST=https://your-influxdb-host
INFLUXDB_USER=your-username
INFLUXDB_TOKEN=your-token
INFLUXDB_ORG=your-org
INFLUXDB_BUCKET=benchmarks
```

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
└── host/                  # Host setup scripts
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
make build
```

### Testing

```bash
make test
```

### Formatting

```bash
make fmt
make vet
```

## Host Setup

The `host/` directory contains scripts for setting up Intel RDT and other system requirements:

```bash
cd host
sudo ./setup-intel-rdt.sh
sudo ./fix-msr-access.sh
```

## Database Schema

Data is exported to InfluxDB with the following structure:

- **Measurement**: `benchmark_metrics`
- **Tags**: `benchmark_id`, `container_index`, `container_image`, `container_core`
- **Fields**: All collected metrics with prefixes:
  - `perf_*`: Performance counter metrics
  - `docker_*`: Docker statistics
  - `rdt_*`: Intel RDT metrics

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]
