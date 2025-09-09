#!/bin/bash

# Intel RDT Management Utility
# This script provides convenient management functions for Intel RDT
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

# Check if Intel RDT is properly installed
check_installation() {
    local errors=0
    
    log "Checking Intel RDT installation..."
    
    # Check tools
    if ! command -v pqos &> /dev/null; then
        log_error "pqos tool not found"
        ((errors++))
    fi
    
    if ! command -v rdtset &> /dev/null; then
        log_error "rdtset tool not found"
        ((errors++))
    fi
    
    # Check MSR module
    if ! lsmod | grep -q "^msr"; then
        log_error "MSR module not loaded"
        ((errors++))
    fi
    
    # Check resctrl mount
    if ! mount | grep -q "resctrl"; then
        log_error "resctrl filesystem not mounted"
        ((errors++))
    fi
    
    if [[ $errors -eq 0 ]]; then
        log_success "Intel RDT installation is complete and functional"
        return 0
    else
        log_error "Intel RDT installation has $errors issue(s)"
        return 1
    fi
}

# Show system status
show_status() {
    log "Intel RDT System Status"
    echo "======================="
    echo
    
    # CPU information
    echo "CPU Information:"
    echo "---------------"
    local cpu_model
    cpu_model=$(grep -m1 "model name" /proc/cpuinfo | cut -d: -f2 | xargs)
    echo "Model: $cpu_model"
    
    local cpu_features
    cpu_features=$(grep -o -E "cqm|cat_l3|cat_l2|cdp_l3|cdp_l2|mba" /proc/cpuinfo | sort -u | tr '\n' ' ')
    if [[ -n "$cpu_features" ]]; then
        echo "RDT Features: $cpu_features"
    else
        echo "RDT Features: None detected in /proc/cpuinfo"
    fi
    echo
    
    # Kernel information
    echo "Kernel Information:"
    echo "------------------"
    echo "Version: $(uname -r)"
    
    local kernel_config="/boot/config-$(uname -r)"
    if [[ -f "$kernel_config" ]]; then
        local rdt_config
        rdt_config=$(grep -E "CONFIG_(INTEL_RDT|X86_CPU_RESCTRL)" "$kernel_config" 2>/dev/null | head -1 || echo "Not found")
        echo "RDT Config: $rdt_config"
    fi
    echo
    
    # Module status
    echo "Module Status:"
    echo "-------------"
    if lsmod | grep -q "^msr"; then
        echo "MSR Module: Loaded ✓"
    else
        echo "MSR Module: Not loaded ✗"
    fi
    echo
    
    # resctrl status
    echo "resctrl Status:"
    echo "--------------"
    if mount | grep -q "resctrl"; then
        echo "Mounted: Yes ✓"
        echo "Mount point: $(mount | grep resctrl | awk '{print $3}')"
    else
        echo "Mounted: No ✗"
    fi
    echo
    
    # Tool status
    echo "Tool Status:"
    echo "-----------"
    if command -v pqos &> /dev/null; then
        echo "pqos: Available ✓ ($(which pqos))"
    else
        echo "pqos: Not available ✗"
    fi
    
    if command -v rdtset &> /dev/null; then
        echo "rdtset: Available ✓ ($(which rdtset))"
    else
        echo "rdtset: Not available ✗"
    fi
    echo
    
    # RDT capabilities
    echo "RDT Capabilities:"
    echo "----------------"
    if command -v pqos &> /dev/null && pqos -s &>/dev/null; then
        pqos -s
    else
        echo "Cannot detect RDT capabilities"
    fi
}

# Monitor system resources
monitor_resources() {
    local duration="${1:-10}"
    local cores="${2:-all}"
    
    log "Monitoring system resources for ${duration} seconds (cores: $cores)..."
    
    if ! command -v pqos &> /dev/null; then
        log_error "pqos tool not available"
        return 1
    fi
    
    # Start monitoring
    pqos -m "$cores" -t "$duration"
}

# List resource groups
list_groups() {
    log "Current Resource Groups:"
    echo "======================="
    
    if [[ ! -d /sys/fs/resctrl ]]; then
        log_error "resctrl filesystem not available"
        return 1
    fi
    
    echo "Default group: /sys/fs/resctrl/"
    echo "Tasks: $(wc -w < /sys/fs/resctrl/tasks) processes"
    echo "Schema: $(cat /sys/fs/resctrl/schemata 2>/dev/null | head -1 || echo "N/A")"
    echo
    
    # List custom groups
    local custom_groups
    custom_groups=$(find /sys/fs/resctrl -maxdepth 1 -type d -name "*" ! -path "/sys/fs/resctrl" ! -path "*/info" | sort)
    
    if [[ -n "$custom_groups" ]]; then
        echo "Custom groups:"
        while IFS= read -r group; do
            local group_name
            group_name=$(basename "$group")
            local task_count
            task_count=$(wc -w < "$group/tasks" 2>/dev/null || echo "0")
            local schema
            schema=$(cat "$group/schemata" 2>/dev/null | head -1 || echo "N/A")
            
            echo "  $group_name:"
            echo "    Tasks: $task_count processes"
            echo "    Schema: $schema"
            echo
        done <<< "$custom_groups"
    else
        echo "No custom resource groups found"
    fi
}

# Create a new resource group
create_group() {
    local group_name="$1"
    local schema="${2:-}"
    
    if [[ -z "$group_name" ]]; then
        log_error "Group name is required"
        echo "Usage: $0 create-group <group_name> [schema]"
        return 1
    fi
    
    local group_path="/sys/fs/resctrl/$group_name"
    
    if [[ -d "$group_path" ]]; then
        log_error "Group '$group_name' already exists"
        return 1
    fi
    
    log "Creating resource group: $group_name"
    
    if mkdir "$group_path"; then
        log_success "Group '$group_name' created successfully"
        
        if [[ -n "$schema" ]]; then
            log "Setting schema: $schema"
            if echo "$schema" > "$group_path/schemata"; then
                log_success "Schema applied successfully"
            else
                log_error "Failed to apply schema"
                return 1
            fi
        fi
        
        echo "Group created at: $group_path"
        echo "Add processes with: echo PID > $group_path/tasks"
    else
        log_error "Failed to create group '$group_name'"
        return 1
    fi
}

# Delete a resource group
delete_group() {
    local group_name="$1"
    
    if [[ -z "$group_name" ]]; then
        log_error "Group name is required"
        echo "Usage: $0 delete-group <group_name>"
        return 1
    fi
    
    local group_path="/sys/fs/resctrl/$group_name"
    
    if [[ ! -d "$group_path" ]]; then
        log_error "Group '$group_name' does not exist"
        return 1
    fi
    
    log "Deleting resource group: $group_name"
    
    # Move processes back to default group
    local tasks
    tasks=$(cat "$group_path/tasks" 2>/dev/null || true)
    if [[ -n "$tasks" ]]; then
        log "Moving processes back to default group..."
        for pid in $tasks; do
            echo "$pid" > /sys/fs/resctrl/tasks 2>/dev/null || true
        done
    fi
    
    # Remove the group
    if rmdir "$group_path"; then
        log_success "Group '$group_name' deleted successfully"
    else
        log_error "Failed to delete group '$group_name'"
        return 1
    fi
}

# Add process to resource group
add_process() {
    local group_name="$1"
    local pid="$2"
    
    if [[ -z "$group_name" || -z "$pid" ]]; then
        log_error "Group name and PID are required"
        echo "Usage: $0 add-process <group_name> <pid>"
        return 1
    fi
    
    local group_path="/sys/fs/resctrl/$group_name"
    
    if [[ ! -d "$group_path" ]]; then
        log_error "Group '$group_name' does not exist"
        return 1
    fi
    
    if ! kill -0 "$pid" 2>/dev/null; then
        log_error "Process $pid does not exist"
        return 1
    fi
    
    log "Adding process $pid to group $group_name"
    
    if echo "$pid" > "$group_path/tasks"; then
        log_success "Process $pid added to group $group_name"
    else
        log_error "Failed to add process $pid to group $group_name"
        return 1
    fi
}

# Reset RDT configuration
reset_config() {
    log "Resetting RDT configuration..."
    
    if command -v pqos &> /dev/null; then
        if pqos -R; then
            log_success "RDT configuration reset successfully"
        else
            log_error "Failed to reset RDT configuration with pqos"
            return 1
        fi
    else
        log_warning "pqos not available, manually resetting..."
        
        # Move all processes to default group
        if [[ -d /sys/fs/resctrl ]]; then
            find /sys/fs/resctrl -maxdepth 1 -type d -name "*" ! -path "/sys/fs/resctrl" ! -path "*/info" | while read -r group; do
                local tasks
                tasks=$(cat "$group/tasks" 2>/dev/null || true)
                if [[ -n "$tasks" ]]; then
                    for pid in $tasks; do
                        echo "$pid" > /sys/fs/resctrl/tasks 2>/dev/null || true
                    done
                fi
                rmdir "$group" 2>/dev/null || true
            done
            log_success "Manual reset completed"
        fi
    fi
}

# Show help
show_help() {
    echo "Intel RDT Management Utility"
    echo
    echo "Usage: $0 <command> [arguments]"
    echo
    echo "Commands:"
    echo "  status                          Show system status and capabilities"
    echo "  check                          Check installation integrity"
    echo "  monitor [duration] [cores]     Monitor resources (default: 10s, all cores)"
    echo "  list-groups                    List all resource groups"
    echo "  create-group <name> [schema]   Create a new resource group"
    echo "  delete-group <name>            Delete a resource group"
    echo "  add-process <group> <pid>      Add process to resource group"
    echo "  reset                          Reset RDT configuration"
    echo "  help                           Show this help message"
    echo
    echo "Examples:"
    echo "  $0 status                      # Show system status"
    echo "  $0 monitor 30 0-3              # Monitor cores 0-3 for 30 seconds"
    echo "  $0 create-group web_servers    # Create group for web servers"
    echo "  $0 add-process web_servers 1234 # Add process 1234 to web_servers group"
    echo "  $0 list-groups                 # Show all resource groups"
    echo "  $0 reset                       # Reset all RDT settings"
    echo
}

# Main execution
main() {
    case "${1:-help}" in
        status)
            show_status
            ;;
        check)
            check_installation
            ;;
        monitor)
            monitor_resources "${2:-10}" "${3:-all}"
            ;;
        list-groups)
            list_groups
            ;;
        create-group)
            if [[ $# -lt 2 ]]; then
                log_error "Group name is required"
                echo "Usage: $0 create-group <group_name> [schema]"
                exit 1
            fi
            create_group "$2" "${3:-}"
            ;;
        delete-group)
            if [[ $# -lt 2 ]]; then
                log_error "Group name is required"
                echo "Usage: $0 delete-group <group_name>"
                exit 1
            fi
            delete_group "$2"
            ;;
        add-process)
            if [[ $# -lt 3 ]]; then
                log_error "Group name and PID are required"
                echo "Usage: $0 add-process <group_name> <pid>"
                exit 1
            fi
            add_process "$2" "$3"
            ;;
        reset)
            reset_config
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "Unknown command: ${1:-}"
            echo
            show_help
            exit 1
            ;;
    esac
}

# Execute main function
main "$@"
