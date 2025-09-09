# Intel RDT Setup Scripts for Ubuntu

This directory contains comprehensive scripts for setting up, testing, and managing Intel Resource Director Technology (RDT) on Ubuntu systems.

## Overview

Intel RDT provides hardware-based monitoring and allocation capabilities for shared system resources like cache and memory bandwidth. These scripts automate the installation, configuration, and management of Intel RDT on Ubuntu hosts.

## Files

- **`setup-intel-rdt.sh`** - Main installation and setup script
- **`test-intel-rdt.sh`** - Comprehensive testing and validation script
- **`manage-intel-rdt.sh`** - Management utility for daily operations
- **`README.md`** - This documentation file

## Prerequisites

### Hardware Requirements
- Intel CPU with RDT support (typically Xeon processors or newer Core processors)
- Supported features may include:
  - CMT (Cache Monitoring Technology)
  - MBM (Memory Bandwidth Monitoring)
  - CAT (Cache Allocation Technology)
  - CDP (Code and Data Prioritization)
  - MBA (Memory Bandwidth Allocation)

### Software Requirements
- Ubuntu 18.04 or later
- Linux kernel 4.10+ (recommended for full feature support)
- sudo privileges
- Internet connection for package downloads

## Quick Start

### 1. Setup Intel RDT

Run the main setup script to install and configure Intel RDT:

```bash
cd ~/container-bench/host
chmod +x setup-intel-rdt.sh
./setup-intel-rdt.sh
```

This script will:
- Check system compatibility
- Install required packages
- Download and build Intel RDT software
- Configure kernel modules and filesystem
- Set up systemd services
- Create documentation

### 2. Test Installation

Verify that Intel RDT is working correctly:

```bash
chmod +x test-intel-rdt.sh
./test-intel-rdt.sh
```

For comprehensive testing including stress tests:

```bash
./test-intel-rdt.sh --stress
```

### 3. Manage Intel RDT

Use the management utility for daily operations:

```bash
chmod +x manage-intel-rdt.sh

# Show system status
./manage-intel-rdt.sh status

# Monitor resources for 30 seconds
./manage-intel-rdt.sh monitor 30

# List all resource groups
./manage-intel-rdt.sh list-groups
```

## Detailed Usage

### Setup Script (`setup-intel-rdt.sh`)

The setup script performs a complete installation:

```bash
./setup-intel-rdt.sh
```

**What it does:**
1. **System Checks**: Verifies Ubuntu version, CPU compatibility, and kernel support
2. **Package Installation**: Installs build tools, MSR tools, and dependencies
3. **Software Build**: Downloads and compiles Intel RDT software from source
4. **Configuration**: Sets up kernel modules, mounts resctrl filesystem
5. **System Integration**: Creates systemd services for automatic startup
6. **Documentation**: Generates usage examples and documentation

**Output locations:**
- Intel RDT tools: `/opt/intel-rdt/bin/` (symlinked to `/usr/local/bin/`)
- Libraries: `/opt/intel-rdt/lib/`
- Documentation: `/opt/intel-rdt/docs/`

### Test Script (`test-intel-rdt.sh`)

Comprehensive testing and validation:

```bash
# Standard tests
./test-intel-rdt.sh

# Include stress testing
./test-intel-rdt.sh --stress
```

**Test categories:**
- **MSR Module**: Verifies MSR kernel module and device access
- **resctrl Filesystem**: Checks resctrl mount and structure
- **RDT Tools**: Tests pqos and rdtset availability and functionality
- **Feature Detection**: Identifies available RDT features
- **Monitoring**: Tests resource monitoring capabilities
- **Allocation**: Tests resource group creation and management
- **System Integration**: Verifies systemd services and library installation
- **Performance**: Measures monitoring overhead
- **Stress Testing**: Optional intensive testing with multiple processes

**Report generation:**
The script generates a detailed system report saved to `/tmp/rdt-system-report-YYYYMMDD-HHMMSS.txt`

### Management Script (`manage-intel-rdt.sh`)

Daily management and operations:

```bash
# Show system status and capabilities
./manage-intel-rdt.sh status

# Check installation integrity
./manage-intel-rdt.sh check

# Monitor all cores for 10 seconds (default)
./manage-intel-rdt.sh monitor

# Monitor specific cores for 30 seconds
./manage-intel-rdt.sh monitor 30 0-3

# List all resource groups
./manage-intel-rdt.sh list-groups

# Create a new resource group
./manage-intel-rdt.sh create-group web_servers

# Create group with specific cache allocation (50% of cache)
./manage-intel-rdt.sh create-group database "L3:0=0x0f"

# Add a process to a resource group
./manage-intel-rdt.sh add-process web_servers 1234

# Delete a resource group
./manage-intel-rdt.sh delete-group web_servers

# Reset all RDT configuration
./manage-intel-rdt.sh reset

# Show help
./manage-intel-rdt.sh help
```

## Common Use Cases

### 1. Container Resource Isolation

Isolate containers to specific cache allocations:

```bash
# Create resource group for critical containers
./manage-intel-rdt.sh create-group critical_containers "L3:0=0xf0"

# Find container PID and add to group
CONTAINER_PID=$(docker inspect --format '{{.State.Pid}}' my_container)
./manage-intel-rdt.sh add-process critical_containers $CONTAINER_PID
```

### 2. Application Performance Monitoring

Monitor specific applications:

```bash
# Monitor all resources for 60 seconds
./manage-intel-rdt.sh monitor 60

# Monitor in CSV format for analysis
pqos -m all -t 60 -u csv > monitoring_data.csv
```

### 3. Quality of Service (QoS)

Set up QoS for different application tiers:

```bash
# High priority applications (75% cache)
./manage-intel-rdt.sh create-group high_priority "L3:0=0xff0"

# Medium priority applications (25% cache)
./manage-intel-rdt.sh create-group medium_priority "L3:0=0x00f"

# Background tasks (minimal cache)
./manage-intel-rdt.sh create-group background "L3:0=0x001"
```

## Troubleshooting

### Common Issues

1. **CPU not supported**
   ```
   Error: Intel CPU required for Intel RDT support
   ```
   - Verify you have an Intel CPU with RDT features
   - Check `/proc/cpuinfo` for RDT-related flags

2. **MSR module not loaded**
   ```
   Error: MSR module is not loaded
   ```
   - Load manually: `sudo modprobe msr`
   - Check if MSR is blocked by UEFI Secure Boot

3. **resctrl not mounted**
   ```
   Error: resctrl filesystem is not mounted
   ```
   - Mount manually: `sudo mount -t resctrl resctrl /sys/fs/resctrl`
   - Check kernel RDT configuration

4. **Permission denied**
   ```
   Error: MSR read access failed
   ```
   - Ensure script is run with appropriate privileges
   - Check if MSR access is restricted by security policies

### Debug Steps

1. **Check system compatibility:**
   ```bash
   ./test-intel-rdt.sh
   ```

2. **Verify kernel support:**
   ```bash
   grep -E "CONFIG_(INTEL_RDT|X86_CPU_RESCTRL)" /boot/config-$(uname -r)
   ```

3. **Check CPU features:**
   ```bash
   grep -E "cqm|cat_l3|cat_l2|cdp_l3|cdp_l2|mba" /proc/cpuinfo
   ```

4. **Manual verification:**
   ```bash
   # Check MSR module
   lsmod | grep msr
   
   # Check resctrl mount
   mount | grep resctrl
   
   # Test pqos
   pqos -s
   ```

## Advanced Configuration

### Custom Resource Allocation

Resource allocation uses capability bitmasks (CBM). Understanding the format:

- **L3 Cache**: `L3:0=0xff` (allocate all cache ways)
- **Memory Bandwidth**: `MB:0=100` (allocate 100% bandwidth)
- **Multiple sockets**: `L3:0=0xf0;1=0x0f` (different allocation per socket)

### Integration with Container Orchestrators

For Kubernetes integration, consider:

1. **Node labeling** based on RDT capabilities
2. **Custom resource definitions** for RDT allocations
3. **Init containers** to set up resource groups
4. **Monitoring sidecars** using RDT metrics

### Performance Tuning

Optimize RDT for your workload:

1. **Profile applications** to understand cache usage patterns
2. **Monitor bandwidth** requirements
3. **Adjust allocations** based on performance metrics
4. **Use MBA controller** for dynamic bandwidth management

## References

- [Intel RDT Documentation](https://www.intel.com/content/www/us/en/architecture-and-technology/resource-director-technology.html)
- [Intel RDT Software Package](https://github.com/intel/intel-cmt-cat)
- [Linux Kernel RDT Documentation](https://www.kernel.org/doc/html/latest/x86/resctrl.html)
- [Intel Architecture Manual](https://www.intel.com/content/www/us/en/developer/articles/technical/intel-sdm.html)

## Support

For issues and questions:

1. Run the test script for diagnostic information
2. Check the generated system report
3. Review Intel RDT documentation
4. Consult kernel documentation for resctrl interface

## License

These scripts are provided as-is for the container-bench project. Please refer to Intel's licensing terms for the Intel RDT software package.
