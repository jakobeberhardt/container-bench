#!/bin/bash

# Intel RDT MSR Access Fix Script
# This script fixes common MSR access issues for Intel RDT
# Author: Generated for container-bench project
# Date: September 2025

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
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

# Check if MSR module is loaded
check_msr_module() {
    log "Checking MSR module status..."
    
    if lsmod | grep -q "^msr"; then
        log_success "MSR module is loaded"
        return 0
    else
        log_error "MSR module is not loaded"
        return 1
    fi
}

# Load MSR module if not loaded
load_msr_module() {
    log "Loading MSR module..."
    
    if ! check_msr_module; then
        log "Attempting to load MSR module..."
        if sudo modprobe msr; then
            log_success "MSR module loaded successfully"
        else
            log_error "Failed to load MSR module"
            return 1
        fi
    fi
}

# Check MSR device files
check_msr_devices() {
    log "Checking MSR device files..."
    
    if [[ -c /dev/cpu/0/msr ]]; then
        log_success "MSR device files exist"
        
        # Check permissions
        local perms
        perms=$(stat -c "%a" /dev/cpu/0/msr)
        log "Current MSR device permissions: $perms"
        
        if [[ "$perms" == "600" ]]; then
            log_warning "MSR devices have root-only permissions"
            return 1
        elif [[ "$perms" == "640" ]] || [[ "$perms" == "644" ]]; then
            log_success "MSR devices have appropriate permissions"
            return 0
        fi
    else
        log_error "MSR device files do not exist"
        return 1
    fi
}

# Fix MSR device permissions
fix_msr_permissions() {
    log "Fixing MSR device permissions..."
    
    # Method 1: Change permissions to allow group read access
    log "Setting MSR device permissions to allow group access..."
    if sudo chmod 640 /dev/cpu/*/msr; then
        log_success "MSR device permissions updated"
    else
        log_error "Failed to update MSR device permissions"
        return 1
    fi
    
    # Verify the fix
    if check_msr_devices; then
        log_success "MSR permissions fix verified"
        return 0
    else
        log_warning "Permission fix may not have worked completely"
        return 1
    fi
}

# Create udev rule for persistent MSR permissions
create_udev_rule() {
    log "Creating udev rule for persistent MSR permissions..."
    
    local udev_rule="/etc/udev/rules.d/99-msr-permissions.rules"
    
    cat << 'EOF' | sudo tee "$udev_rule" > /dev/null
# Allow group read access to MSR devices for Intel RDT
KERNEL=="msr*", GROUP="root", MODE="0640"
EOF
    
    if [[ -f "$udev_rule" ]]; then
        log_success "Udev rule created: $udev_rule"
        
        # Reload udev rules
        log "Reloading udev rules..."
        sudo udevadm control --reload-rules
        sudo udevadm trigger
        
        log_success "Udev rules reloaded"
    else
        log_error "Failed to create udev rule"
        return 1
    fi
}

# Test MSR access
test_msr_access() {
    log "Testing MSR access..."
    
    # Test read access to MSR
    if timeout 5 bash -c 'dd if=/dev/cpu/0/msr bs=8 count=1 of=/dev/null 2>/dev/null'; then
        log_success "MSR read access working"
        return 0
    else
        log_error "MSR read access failed"
        return 1
    fi
}

# Alternative: Add user to appropriate group
add_user_to_groups() {
    local username
    username=$(whoami)
    
    log "Checking user groups for MSR access..."
    
    # Check if user is in root group (needed for MSR access)
    if groups | grep -q "root"; then
        log_success "User $username is in root group"
    else
        log_warning "User $username is not in root group"
        log "Adding user to root group..."
        if sudo usermod -a -G root "$username"; then
            log_success "User added to root group"
            log_warning "You may need to log out and log back in for group changes to take effect"
        else
            log_error "Failed to add user to root group"
            return 1
        fi
    fi
}

# Check for UEFI Secure Boot conflicts
check_secure_boot() {
    log "Checking for UEFI Secure Boot conflicts..."
    
    if [[ -d /sys/firmware/efi ]]; then
        log "UEFI system detected"
        
        # Check if Secure Boot is enabled
        local secureboot_status
        if [[ -f /sys/firmware/efi/efivars/SecureBoot-* ]]; then
            # This is a simplified check; in practice, checking Secure Boot status is complex
            log_warning "UEFI Secure Boot may be enabled"
            log "If MSR access continues to fail, you may need to:"
            log "1. Disable Secure Boot in BIOS/UEFI settings, or"
            log "2. Sign the MSR kernel module, or"
            log "3. Use a different approach for RDT access"
        else
            log "Secure Boot does not appear to be enforced"
        fi
    else
        log "Legacy BIOS system detected (no UEFI conflicts expected)"
    fi
}

# Check if Intel RDT tools can use OS interface instead
check_os_interface() {
    log "Checking if OS interface is available as alternative..."
    
    # Check if resctrl is mounted
    if mount | grep -q "resctrl"; then
        log_success "resctrl filesystem is available"
        log "You can use Intel RDT with OS interface instead of MSR interface"
        log "Set environment variable: export RDT_IFACE=OS"
        return 0
    else
        log_warning "resctrl filesystem not available"
        
        # Try to mount resctrl
        log "Attempting to mount resctrl filesystem..."
        if sudo mkdir -p /sys/fs/resctrl && sudo mount -t resctrl resctrl /sys/fs/resctrl; then
            log_success "resctrl filesystem mounted successfully"
            log "OS interface is now available as alternative to MSR"
            return 0
        else
            log_error "Failed to mount resctrl filesystem"
            return 1
        fi
    fi
}

# Main diagnostic and fix function
main() {
    log "Starting Intel RDT MSR access diagnostic and fix..."
    echo
    
    # Step 1: Check and load MSR module
    if ! check_msr_module; then
        load_msr_module || exit 1
    fi
    echo
    
    # Step 2: Check MSR device files and permissions
    if ! check_msr_devices; then
        fix_msr_permissions || log_warning "Permission fix failed, trying alternatives..."
    fi
    echo
    
    # Step 3: Create persistent udev rule
    create_udev_rule || log_warning "Udev rule creation failed"
    echo
    
    # Step 4: Check user groups
    add_user_to_groups || log_warning "Group modification failed"
    echo
    
    # Step 5: Test MSR access
    if test_msr_access; then
        log_success "MSR access is now working!"
    else
        log_error "MSR access still not working, checking alternatives..."
        echo
        
        # Step 6: Check for UEFI conflicts
        check_secure_boot
        echo
        
        # Step 7: Check OS interface alternative
        if check_os_interface; then
            log_success "OS interface available as alternative"
            echo
            log "To use OS interface instead of MSR interface:"
            echo "export RDT_IFACE=OS"
            echo "pqos -I -s  # Use OS interface"
            echo
        fi
    fi
    
    # Final recommendations
    echo "=========================================="
    echo "RECOMMENDATIONS"
    echo "=========================================="
    echo
    
    if test_msr_access &>/dev/null; then
        echo "✓ MSR access is working"
        echo "✓ You can use Intel RDT with MSR interface"
        echo
        echo "Test with: pqos -s"
    else
        echo "✗ MSR access is not working"
        echo
        echo "Try these solutions in order:"
        echo
        echo "1. Use OS interface instead:"
        echo "   export RDT_IFACE=OS"
        echo "   pqos -I -s"
        echo
        echo "2. Run as root (temporary solution):"
        echo "   sudo pqos -s"
        echo
        echo "3. If using UEFI Secure Boot:"
        echo "   - Disable Secure Boot in BIOS settings, or"
        echo "   - Use only the OS interface"
        echo
        echo "4. Check kernel configuration:"
        echo "   grep -E 'CONFIG_(INTEL_RDT|X86_CPU_RESCTRL)' /boot/config-\$(uname -r)"
        echo
    fi
    
    log "MSR diagnostic and fix completed"
}

# Show help
show_help() {
    echo "Intel RDT MSR Access Fix Script"
    echo
    echo "This script diagnoses and fixes common MSR access issues for Intel RDT."
    echo
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  --help, -h    Show this help message"
    echo
    echo "What this script does:"
    echo "1. Checks and loads MSR kernel module"
    echo "2. Fixes MSR device file permissions"
    echo "3. Creates persistent udev rules"
    echo "4. Checks user group memberships"
    echo "5. Tests MSR access"
    echo "6. Provides alternative solutions"
    echo
    echo "Common fixes applied:"
    echo "- Load MSR kernel module"
    echo "- Set MSR device permissions to 640"
    echo "- Create udev rule for persistent permissions"
    echo "- Add user to root group if needed"
    echo "- Configure OS interface as alternative"
    echo
}

# Handle command line arguments
case "${1:-}" in
    --help|-h)
        show_help
        exit 0
        ;;
    "")
        main
        ;;
    *)
        echo "Unknown option: $1"
        show_help
        exit 1
        ;;
esac
