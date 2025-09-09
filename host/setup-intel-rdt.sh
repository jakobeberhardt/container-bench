#!/bin/bash

# Intel RDT Setup Script for Ubuntu
# This script sets up Intel Resource Director Technology on Ubuntu hosts
# Author: Generated for container-bench project
# Date: September 2025

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
check_root() {
    if [[ $EUID -eq 0 ]]; then
        log_error "This script should not be run as root. Please run as a regular user with sudo privileges."
        exit 1
    fi
}

# Check Ubuntu version
check_ubuntu_version() {
    log "Checking Ubuntu version..."
    
    if ! command -v lsb_release &> /dev/null; then
        log_error "lsb_release not found. Please ensure you're running on Ubuntu."
        exit 1
    fi
    
    local ubuntu_version
    ubuntu_version=$(lsb_release -rs)
    local ubuntu_major
    ubuntu_major=$(echo "$ubuntu_version" | cut -d. -f1)
    
    log "Detected Ubuntu $ubuntu_version"
    
    if [[ "$ubuntu_major" -lt 18 ]]; then
        log_error "Ubuntu 18.04 or later is required for Intel RDT support."
        exit 1
    fi
    
    log_success "Ubuntu version check passed"
}

# Check CPU support for Intel RDT
check_cpu_support() {
    log "Checking CPU support for Intel RDT..."
    
    # Check if CPU is Intel
    if ! grep -q "vendor_id.*GenuineIntel" /proc/cpuinfo; then
        log_error "Intel CPU required for Intel RDT support."
        exit 1
    fi
    
    # Check for RDT features in CPUID
    local rdt_features=""
    
    # Check for CMT (Cache Monitoring Technology)
    if grep -q "cqm" /proc/cpuinfo; then
        rdt_features="$rdt_features CMT"
    fi
    
    # Check for CAT (Cache Allocation Technology)
    if grep -q "cat_l3" /proc/cpuinfo; then
        rdt_features="$rdt_features CAT_L3"
    fi
    
    if grep -q "cat_l2" /proc/cpuinfo; then
        rdt_features="$rdt_features CAT_L2"
    fi
    
    # Check for CDP (Code and Data Prioritization)
    if grep -q "cdp_l3" /proc/cpuinfo; then
        rdt_features="$rdt_features CDP_L3"
    fi
    
    if grep -q "cdp_l2" /proc/cpuinfo; then
        rdt_features="$rdt_features CDP_L2"
    fi
    
    # Check for MBA (Memory Bandwidth Allocation)
    if grep -q "mba" /proc/cpuinfo; then
        rdt_features="$rdt_features MBA"
    fi
    
    # Check CPU model for known RDT support
    local cpu_model
    cpu_model=$(grep -m1 "model name" /proc/cpuinfo | cut -d: -f2 | xargs)
    log "Detected CPU: $cpu_model"
    
    if [[ -n "$rdt_features" ]]; then
        log_success "Intel RDT features detected: $rdt_features"
    else
        log_warning "No Intel RDT features detected in /proc/cpuinfo. This might be normal for some processors."
        log "Continuing with setup - features will be verified after kernel configuration."
    fi
}

# Check and configure kernel support
check_kernel_support() {
    log "Checking kernel support for Intel RDT..."
    
    local kernel_version
    kernel_version=$(uname -r)
    local kernel_major
    kernel_major=$(echo "$kernel_version" | cut -d. -f1)
    local kernel_minor
    kernel_minor=$(echo "$kernel_version" | cut -d. -f2)
    
    log "Detected kernel version: $kernel_version"
    
    # Check if kernel version supports RDT
    if [[ "$kernel_major" -lt 4 ]] || [[ "$kernel_major" -eq 4 && "$kernel_minor" -lt 10 ]]; then
        log_warning "Kernel version $kernel_version has limited RDT support. Kernel 4.10+ recommended for full features."
    else
        log_success "Kernel version supports Intel RDT"
    fi
    
    # Check if resctrl is already mounted
    if mount | grep -q "resctrl"; then
        log_success "resctrl filesystem is already mounted"
    else
        log "resctrl filesystem not mounted. Will configure after package installation."
    fi
    
    # Check kernel configuration
    local config_file="/boot/config-$(uname -r)"
    if [[ -f "$config_file" ]]; then
        log "Checking kernel configuration..."
        
        # Check for RDT config options
        local rdt_configs=("CONFIG_INTEL_RDT" "CONFIG_INTEL_RDT_A" "CONFIG_X86_CPU_RESCTRL")
        local found_config=false
        
        for config in "${rdt_configs[@]}"; do
            if grep -q "^${config}=y" "$config_file" 2>/dev/null; then
                log_success "Found kernel config: $config"
                found_config=true
                break
            fi
        done
        
        if [[ "$found_config" == false ]]; then
            log_warning "Intel RDT kernel configuration not found. Some features may not be available."
        fi
    else
        log_warning "Kernel config file not found at $config_file"
    fi
}

# Install required packages
install_packages() {
    log "Installing required packages..."
    
    # Update package list
    log "Updating package list..."
    sudo apt-get update
    
    # Install build dependencies
    log "Installing build dependencies..."
    sudo apt-get install -y \
        build-essential \
        git \
        cmake \
        pkg-config \
        libpci-dev \
        msr-tools \
        cpuid \
        hwloc \
        numactl
    
    log_success "Required system packages installed"
}

# Download and build Intel RDT software
install_intel_rdt_software() {
    log "Installing Intel RDT software..."
    
    local install_dir="/opt/intel-rdt"
    local temp_dir="/tmp/intel-rdt-build"
    
    # Create installation directory
    sudo mkdir -p "$install_dir"
    
    # Clean up any existing build directory
    rm -rf "$temp_dir"
    mkdir -p "$temp_dir"
    cd "$temp_dir"
    
    # Clone the Intel RDT repository
    log "Downloading Intel RDT source code..."
    git clone https://github.com/intel/intel-cmt-cat.git
    cd intel-cmt-cat
    
    # Build the software
    log "Building Intel RDT software..."
    make
    
    # Install the software
    log "Installing Intel RDT software to $install_dir..."
    sudo make install PREFIX="$install_dir"
    
    # Create symlinks for easy access
    sudo ln -sf "$install_dir/bin/pqos" /usr/local/bin/pqos
    sudo ln -sf "$install_dir/bin/rdtset" /usr/local/bin/rdtset
    
    # Set up library path
    echo "$install_dir/lib" | sudo tee /etc/ld.so.conf.d/intel-rdt.conf
    sudo ldconfig
    
    log_success "Intel RDT software installed successfully"
    
    # Clean up
    cd /
    rm -rf "$temp_dir"
}

# Configure Intel RDT
configure_rdt() {
    log "Configuring Intel RDT..."
    
    # Load MSR module
    log "Loading MSR kernel module..."
    sudo modprobe msr
    
    # Ensure MSR module loads on boot
    echo "msr" | sudo tee -a /etc/modules
    
    # Mount resctrl filesystem if not already mounted
    if ! mount | grep -q "resctrl"; then
        log "Mounting resctrl filesystem..."
        sudo mkdir -p /sys/fs/resctrl
        sudo mount -t resctrl resctrl /sys/fs/resctrl
        
        # Add to fstab for automatic mounting
        if ! grep -q "resctrl" /etc/fstab; then
            echo "resctrl /sys/fs/resctrl resctrl defaults 0 0" | sudo tee -a /etc/fstab
        fi
        
        log_success "resctrl filesystem mounted"
    fi
    
    # Set appropriate permissions for tools
    sudo chmod +x /usr/local/bin/pqos /usr/local/bin/rdtset
    
    log_success "Intel RDT configuration completed"
}

# Test Intel RDT functionality
test_rdt_functionality() {
    log "Testing Intel RDT functionality..."
    
    # Test basic CPU information
    log "Testing CPU feature detection..."
    if command -v cpuid &> /dev/null; then
        cpuid -1 | grep -i rdt || log_warning "No RDT features reported by cpuid"
    fi
    
    # Test MSR access
    log "Testing MSR access..."
    if [[ -r /dev/cpu/0/msr ]]; then
        log_success "MSR access available"
    else
        log_error "MSR access not available. Check if MSR module is loaded."
        return 1
    fi
    
    # Test resctrl filesystem
    log "Testing resctrl filesystem..."
    if [[ -d /sys/fs/resctrl ]]; then
        log_success "resctrl filesystem available"
        
        # List available features
        if [[ -f /sys/fs/resctrl/info/last_level_cache/cbm_mask ]]; then
            local cbm_mask
            cbm_mask=$(cat /sys/fs/resctrl/info/last_level_cache/cbm_mask)
            log "Last Level Cache CBM mask: $cbm_mask"
        fi
        
        if [[ -f /sys/fs/resctrl/info/MB/bandwidth_gran ]]; then
            local mb_gran
            mb_gran=$(cat /sys/fs/resctrl/info/MB/bandwidth_gran)
            log "Memory Bandwidth granularity: $mb_gran"
        fi
    else
        log_warning "resctrl filesystem not available"
    fi
    
    # Test pqos tool
    log "Testing pqos tool..."
    if command -v pqos &> /dev/null; then
        log "Running pqos capability detection..."
        if pqos -s; then
            log_success "pqos tool working correctly"
        else
            log_warning "pqos tool encountered issues"
        fi
        
        log "Running pqos monitoring test..."
        if timeout 5 pqos -m all:0 -t 1; then
            log_success "pqos monitoring test successful"
        else
            log_warning "pqos monitoring test failed or timed out"
        fi
    else
        log_error "pqos tool not found"
        return 1
    fi
    
    # Test rdtset tool
    log "Testing rdtset tool..."
    if command -v rdtset &> /dev/null; then
        log_success "rdtset tool available"
    else
        log_error "rdtset tool not found"
        return 1
    fi
    
    log_success "Intel RDT functionality tests completed"
}

# Create systemd service for RDT management
create_systemd_service() {
    log "Creating systemd service for RDT management..."
    
    cat << 'EOF' | sudo tee /etc/systemd/system/intel-rdt.service > /dev/null
[Unit]
Description=Intel Resource Director Technology Setup
After=multi-user.target

[Service]
Type=oneshot
ExecStart=/bin/bash -c 'modprobe msr && mount -t resctrl resctrl /sys/fs/resctrl 2>/dev/null || true'
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
    
    sudo systemctl daemon-reload
    sudo systemctl enable intel-rdt.service
    sudo systemctl start intel-rdt.service
    
    log_success "Intel RDT systemd service created and enabled"
}

# Generate usage examples and documentation
create_documentation() {
    log "Creating documentation and usage examples..."
    
    local doc_dir="/opt/intel-rdt/docs"
    sudo mkdir -p "$doc_dir"
    
    cat << 'EOF' | sudo tee "$doc_dir/usage-examples.txt" > /dev/null
Intel RDT Usage Examples
========================

1. Monitor cache and memory bandwidth usage:
   pqos -m all -t 10

2. Show current RDT capabilities:
   pqos -s

3. Show detailed system information:
   pqos -d

4. Monitor specific cores (0-3):
   pqos -m all:0-3 -t 5

5. Set cache allocation for a process:
   rdtset -t 'l3=0xf' -c 0-3 your_application

6. Create a resource group with 50% cache allocation:
   mkdir /sys/fs/resctrl/test_group
   echo "0x0f" > /sys/fs/resctrl/test_group/schemata
   echo $$ > /sys/fs/resctrl/test_group/tasks

7. Monitor memory bandwidth for a specific process:
   pqos -p pid:1234 -t 10

8. Show available resource groups:
   ls /sys/fs/resctrl/

9. Reset RDT configuration:
   pqos -R

10. Show monitoring data in CSV format:
    pqos -m all -t 5 -u csv
EOF
    
    log_success "Documentation created at $doc_dir/usage-examples.txt"
}

# Main execution
main() {
    log "Starting Intel RDT setup for Ubuntu..."
    
    check_root
    check_ubuntu_version
    check_cpu_support
    check_kernel_support
    install_packages
    install_intel_rdt_software
    configure_rdt
    create_systemd_service
    test_rdt_functionality
    create_documentation
    
    log_success "Intel RDT setup completed successfully!"
    echo
    log "Next steps:"
    echo "1. Review the documentation at /opt/intel-rdt/docs/usage-examples.txt"
    echo "2. Test RDT functionality with: pqos -s"
    echo "3. Monitor system resources with: pqos -m all -t 10"
    echo "4. Check available resource groups: ls /sys/fs/resctrl/"
    echo
    log "Intel RDT tools are now available system-wide:"
    echo "- pqos: Intel RDT monitoring and configuration tool"
    echo "- rdtset: Task assignment tool for RDT resource groups"
    echo
}

# Trap errors and cleanup
trap 'log_error "Script failed at line $LINENO"' ERR

# Execute main function
main "$@"
