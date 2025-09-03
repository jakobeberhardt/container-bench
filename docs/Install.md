# RDT4NN Driver Quick Reference

## Installation & Setup

```bash
# Quick setup (Ubuntu/Debian)
./setup.sh

# Manual build
go build -o rdt4nn-driver

# Install system-wide
sudo make install
```

## Basic Usage

```bash
# Validate configuration
./rdt4nn-driver validate -c config.yml

# Run benchmark
./rdt4nn-driver -c config.yml

# Run with verbose logging  
./rdt4nn-driver -c config.yml -v

# Show version
./rdt4nn-driver version
```

## Configuration Quick Start

### Minimal Configuration
```yaml
benchmark:
  name: my_benchmark
  max_t: 60
  log_level: info
  scheduler:
    implementation: default
  data:
    profilefrequency: 1000
    db:
      host: localhost:8086
      name: benchmarks
      user: user
      password: pass
    dockerstats: true
  docker:
    auth:
      registry: ""

container0:
  index: 0
  image: nginx:alpine
  start: 0
  stop: -1
```

### Full Configuration Template
```yaml
benchmark:
  name: "benchmark_name"
  max_t: 300                    # seconds, -1 for indefinite
  log_level: "info"            # trace|debug|info|warn|error
  scheduler:
    implementation: "adaptive"  # default|rdt|adaptive  
    rdt: true
  data:
    profilefrequency: 100       # milliseconds
    db:
      host: "localhost:8086"
      name: "database_name"
      user: "username" 
      password: "password"
    rdt: true                   # Intel RDT metrics
    perf: true                  # perf counters
    dockerstats: true           # Docker statistics
  docker:
    auth:
      registry: "registry.example.com"
      username: "user"
      password: "pass"

container0:
  index: 0                      # unique identifier
  image: "nginx:alpine"         # Docker image
  port: "80:8080"              # host:container (optional)
  start: 0                     # start time in seconds
  stop: 120                    # stop time, -1 for manual
  core: 1                      # CPU core to pin (optional)
  env:                         # environment variables
    ENV_VAR: "value"
```

## Command Reference

| Command | Description |
|---------|-------------|
| `rdt4nn-driver -c <config>` | Run benchmark |
| `rdt4nn-driver validate -c <config>` | Validate configuration |
| `rdt4nn-driver version` | Show version info |
| `rdt4nn-driver --help` | Show help |

## Common Patterns

### Memory Intensive Workload
```yaml
container0:
  index: 0
  image: "memory-stress:latest"
  start: 0
  stop: -1
  core: 0
  env:
    MEMORY_SIZE: "1GB"
    PATTERN: "random"
```

### CPU Intensive Workload  
```yaml
container1:
  index: 1
  image: "cpu-stress:latest"
  start: 10
  stop: -1
  core: 2
  env:
    CPU_THREADS: "1"
    DURATION: "300"
```

### Network Service
```yaml
container2:
  index: 2
  image: "nginx:alpine"
  port: "80:8080" 
  start: 0
  stop: -1
  core: 4
```

## Scheduler Configurations

### Default Scheduler
```yaml
scheduler:
  implementation: default
  rdt: false
```

### RDT-Based Scheduling
```yaml
scheduler:
  implementation: rdt
  rdt: true
```

### Adaptive Scheduling
```yaml
scheduler:
  implementation: adaptive
  rdt: true
```

## Profiling Configurations

### High Resolution (Research)
```yaml
data:
  profilefrequency: 50        # 50ms sampling
  rdt: true
  perf: true 
  dockerstats: true
```

### Standard Resolution (Development)
```yaml
data:
  profilefrequency: 200       # 200ms sampling
  rdt: false
  perf: true
  dockerstats: true
```

### Low Overhead (Production)
```yaml
data:
  profilefrequency: 1000      # 1s sampling
  rdt: false
  perf: false
  dockerstats: true
```

## Collected Metrics

### Docker Statistics
- CPU usage percentage
- Memory usage (bytes/percentage)
- Network I/O (RX/TX bytes)
- Block I/O (read/write bytes)

### Performance Counters (perf)
- CPU cycles and instructions
- Cache references and misses
- Branch predictions
- Page faults
- Context switches

### Intel RDT Metrics
- Last-level cache occupancy
- Memory bandwidth (local/remote)
- Cache allocation enforcement
- Memory bandwidth throttling

## Troubleshooting

### Common Issues

#### Permission Errors
```bash
# Docker permissions
sudo usermod -aG docker $USER
# Logout and login

# RDT permissions  
sudo mount -t resctrl resctrl /sys/fs/resctrl
sudo chmod -R 755 /sys/fs/resctrl

# Perf permissions
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
```

#### InfluxDB Connection
```bash
# Check InfluxDB status
sudo systemctl status influxdb

# Test connection
curl -I http://localhost:8086/ping
```

#### RDT Not Available
```bash
# Check CPU support
grep rdt /proc/cpuinfo

# Install tools (Ubuntu)
sudo apt install linux-perf

# For older systems without RDT tools
# Set rdt: false in configuration
```

### Debug Mode
```bash
# Enable debug logging
./rdt4nn-driver -c config.yml -v

# Or in configuration
log_level: debug
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DOCKER_HOST` | Docker daemon socket | `unix:///var/run/docker.sock` |
| `GOMAXPROCS` | Go runtime threads | Number of CPUs |

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error |
| 125 | Docker daemon error |
| 130 | Interrupted by user (Ctrl+C) |

## File Locations

```
rdt4nn-driver/
├── examples/               # Example configurations
├── docs/                  # Documentation
├── internal/              # Internal packages
│   ├── config/           # Configuration handling
│   ├── benchmark/        # Benchmark orchestration
│   ├── container/        # Container management  
│   ├── profiler/         # Data collection
│   ├── scheduler/        # Scheduling strategies
│   └── storage/          # Data storage
└── cmd/                  # CLI commands
```

## Quick Examples

### 5-Minute Redis Test
```bash
cat > quick-test.yml << EOF
benchmark:
  name: redis_quick_test
  max_t: 300
  log_level: info
  scheduler:
    implementation: default
  data:
    profilefrequency: 500
    db:
      host: localhost:8086
      name: benchmarks
      user: test
      password: test
    dockerstats: true
  docker:
    auth: {}

container0:
  index: 0
  image: redis:7-alpine
  port: "6379:6379"
  start: 0
  stop: -1
EOF

./rdt4nn-driver -c quick-test.yml
```

### Memory Interference Study
```bash
cat > memory-study.yml << EOF
benchmark:
  name: memory_interference
  max_t: 180
  log_level: info
  scheduler:
    implementation: adaptive
    rdt: true
  data:
    profilefrequency: 100
    db:
      host: localhost:8086
      name: benchmarks
      user: test
      password: test
    rdt: true
    perf: true
    dockerstats: true
  docker:
    auth: {}

container0:
  index: 0
  image: stress-ng:latest
  start: 0
  stop: -1
  core: 0
  env:
    STRESS_ARGS: "--vm 1 --vm-bytes 512M"

container1:
  index: 1  
  image: stress-ng:latest
  start: 30
  stop: -1
  core: 2
  env:
    STRESS_ARGS: "--vm 1 --vm-bytes 512M"
EOF

./rdt4nn-driver -c memory-study.yml
```
