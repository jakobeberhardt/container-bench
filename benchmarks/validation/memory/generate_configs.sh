#!/bin/bash

# Generator script for memory validation benchmark configurations
# Usage: bash generate_configs.sh [output_directory] [max_t_seconds]
# Example: bash generate_configs.sh ./short 15

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${1:-$SCRIPT_DIR}"
MAX_T="${2:-120}"
mkdir -p "$OUTPUT_DIR"

METRIC_LEVELS=("full" "ipc" "branch" "mixed" "docker")
FREQUENCIES=(100 1000 2000 5000)

echo "Generating memory validation configurations..."
echo "Output directory: $OUTPUT_DIR"
echo "Max duration (max_t): ${MAX_T}s"
echo ""

# Generate full metrics configs
for freq in "${FREQUENCIES[@]}"; do
    cat > "${OUTPUT_DIR}/full${freq}.yml" << EOF
benchmark:
  name: Memory Test Full Metrics @ ${freq}ms
  description: Memory validation with full metrics at ${freq}ms sampling
  max_t: ${MAX_T}
  log_level: info
  scheduler:
    implementation: default
    log_level: warn
    rdt: false
  data:
    db:
      host: \${INFLUXDB_HOST}
      name: \${INFLUXDB_BUCKET}
      user: \${INFLUXDB_USER}
      password: \${INFLUXDB_TOKEN}
      org: \${INFLUXDB_ORG}

container0:
  index: 0
  image: nginx:alpine
  core: 0
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf: true
    docker: true
    rdt: false

container1:
  index: 1
  image: nginx:alpine
  core: 2
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf: true
    docker: true
    rdt: false

container2:
  index: 2
  image: nginx:alpine
  core: 4
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf: true
    docker: true
    rdt: false
EOF
    echo "Created: full${freq}.yml"
done

# Generate IPC configs
for freq in "${FREQUENCIES[@]}"; do
    cat > "${OUTPUT_DIR}/ipc${freq}.yml" << EOF
benchmark:
  name: Memory Test IPC Only @ ${freq}ms
  description: Memory validation with IPC metrics at ${freq}ms sampling
  max_t: ${MAX_T}
  log_level: info
  scheduler:
    implementation: default
    log_level: warn
    rdt: false
  data:
    db:
      host: \${INFLUXDB_HOST}
      name: \${INFLUXDB_BUCKET}
      user: \${INFLUXDB_USER}
      password: \${INFLUXDB_TOKEN}
      org: \${INFLUXDB_ORG}

container0:
  index: 0
  image: nginx:alpine
  core: 0
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf:
      instructions: true
      cycles: true
    docker:
      cpu_usage_percent: true
      memory_usage: true
    rdt: false

container1:
  index: 1
  image: nginx:alpine
  core: 2
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf:
      instructions: true
      cycles: true
    docker:
      cpu_usage_percent: true
      memory_usage: true
    rdt: false

container2:
  index: 2
  image: nginx:alpine
  core: 4
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf:
      instructions: true
      cycles: true
    docker:
      cpu_usage_percent: true
      memory_usage: true
    rdt: false
EOF
    echo "Created: ipc${freq}.yml"
done

# Generate Branch configs
for freq in "${FREQUENCIES[@]}"; do
    cat > "${OUTPUT_DIR}/branch${freq}.yml" << EOF
benchmark:
  name: Memory Test Branch Only @ ${freq}ms
  description: Memory validation with branch metrics at ${freq}ms sampling
  max_t: ${MAX_T}
  log_level: info
  scheduler:
    implementation: default
    log_level: warn
    rdt: false
  data:
    db:
      host: \${INFLUXDB_HOST}
      name: \${INFLUXDB_BUCKET}
      user: \${INFLUXDB_USER}
      password: \${INFLUXDB_TOKEN}
      org: \${INFLUXDB_ORG}

container0:
  index: 0
  image: nginx:alpine
  core: 0
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf:
      branch_instructions: true
      branch_misses: true
    docker:
      cpu_usage_percent: true
      memory_usage: true
    rdt: false

container1:
  index: 1
  image: nginx:alpine
  core: 2
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf:
      branch_instructions: true
      branch_misses: true
    docker:
      cpu_usage_percent: true
      memory_usage: true
    rdt: false

container2:
  index: 2
  image: nginx:alpine
  core: 4
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf:
      branch_instructions: true
      branch_misses: true
    docker:
      cpu_usage_percent: true
      memory_usage: true
    rdt: false
EOF
    echo "Created: branch${freq}.yml"
done

# Generate Mixed configs (perf: instructions, cycles, stalls_total + docker: cpu, memory + rdt: l3)
for freq in "${FREQUENCIES[@]}"; do
    cat > "${OUTPUT_DIR}/mixed${freq}.yml" << EOF
benchmark:
  name: Memory Test Mixed Metrics @ ${freq}ms
  description: Memory validation with mixed metrics (perf IPC+stalls, docker cpu+mem, rdt L3) at ${freq}ms sampling
  max_t: ${MAX_T}
  log_level: info
  scheduler:
    implementation: default
    log_level: warn
    rdt: false
  data:
    db:
      host: \${INFLUXDB_HOST}
      name: \${INFLUXDB_BUCKET}
      user: \${INFLUXDB_USER}
      password: \${INFLUXDB_TOKEN}
      org: \${INFLUXDB_ORG}

container0:
  index: 0
  image: nginx:alpine
  core: 0
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf:
      instructions: true
      cycles: true
      stalls_total: true
    docker:
      cpu_usage_percent: true
      memory_usage: true
    rdt:
      l3_cache_occupancy: true

container1:
  index: 1
  image: nginx:alpine
  core: 2
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf:
      instructions: true
      cycles: true
      stalls_total: true
    docker:
      cpu_usage_percent: true
      memory_usage: true
    rdt:
      l3_cache_occupancy: true

container2:
  index: 2
  image: nginx:alpine
  core: 4
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf:
      instructions: true
      cycles: true
      stalls_total: true
    docker:
      cpu_usage_percent: true
      memory_usage: true
    rdt:
      l3_cache_occupancy: true
EOF
    echo "Created: mixed${freq}.yml"
done

# Generate Docker-only configs
for freq in "${FREQUENCIES[@]}"; do
    cat > "${OUTPUT_DIR}/docker${freq}.yml" << EOF
benchmark:
  name: Memory Test Docker Only @ ${freq}ms
  description: Memory validation with Docker metrics only at ${freq}ms sampling
  max_t: ${MAX_T}
  log_level: info
  scheduler:
    implementation: default
    log_level: warn
    rdt: false
  data:
    db:
      host: \${INFLUXDB_HOST}
      name: \${INFLUXDB_BUCKET}
      user: \${INFLUXDB_USER}
      password: \${INFLUXDB_TOKEN}
      org: \${INFLUXDB_ORG}

container0:
  index: 0
  image: nginx:alpine
  core: 0
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf: false
    docker:
      cpu_usage_percent: true
      memory_usage: true
    rdt: false

container1:
  index: 1
  image: nginx:alpine
  core: 2
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf: false
    docker:
      cpu_usage_percent: true
      memory_usage: true
    rdt: false

container2:
  index: 2
  image: nginx:alpine
  core: 4
  command: "while :; do dd if=/dev/zero of=/dev/null bs=1M count=100 2>/dev/null; done"
  data:
    frequency: ${freq}
    perf: false
    docker:
      cpu_usage_percent: true
      memory_usage: true
    rdt: false
EOF
    echo "Created: docker${freq}.yml"
done

echo ""
echo "Generated 20 configuration files (5 metric levels × 4 frequencies)"
echo "Location: $SCRIPT_DIR"
