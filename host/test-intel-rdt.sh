#!/bin/bash

# Intel RDT Testing and Validation Script
# This script provides comprehensive testing for Intel RDT functionality
# Author: Generated for container-bench project
# Date: September 2025

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test results tracking
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_TOTAL=0

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

test_pass() {
    ((TESTS_PASSED++))
    ((TESTS_TOTAL++))
    log_success "PASS: $1"
}

test_fail() {
    ((TESTS_FAILED++))
    ((TESTS_TOTAL++))
    log_error "FAIL: $1"
}

test_skip() {
    ((TESTS_TOTAL++))
    log_warning "SKIP: $1"
}

# Test MSR module availability
test_msr_module() {
    log "Testing MSR module availability..."
    
    if lsmod | grep -q "^msr"; then
        test_pass "MSR module is loaded"
    else
        test_fail "MSR module is not loaded"
        return 1
    fi
    
    if [[ -c /dev/cpu/0/msr ]]; then
        test_pass "MSR device file exists"
    else
        test_fail "MSR device file does not exist"
        return 1
    fi
    
    # Test MSR read access
    if timeout 5 bash -c 'dd if=/dev/cpu/0/msr bs=8 count=1 of=/dev/null 2>/dev/null'; then
        test_pass "MSR read access working"
    else
        test_fail "MSR read access failed"
        return 1
    fi
}

# Test resctrl filesystem
test_resctrl_filesystem() {
    log "Testing resctrl filesystem..."
    
    if mount | grep -q "resctrl"; then
        test_pass "resctrl filesystem is mounted"
    else
        test_fail "resctrl filesystem is not mounted"
        return 1
    fi
    
    if [[ -d /sys/fs/resctrl ]]; then
        test_pass "resctrl directory exists"
    else
        test_fail "resctrl directory does not exist"
        return 1
    fi
    
    # Test resctrl structure
    local resctrl_files=("info" "tasks" "schemata")
    for file in "${resctrl_files[@]}"; do
        if [[ -e "/sys/fs/resctrl/$file" ]]; then
            test_pass "resctrl $file exists"
        else
            test_fail "resctrl $file missing"
        fi
    done
}

# Test Intel RDT tools
test_rdt_tools() {
    log "Testing Intel RDT tools..."
    
    # Test pqos availability
    if command -v pqos &> /dev/null; then
        test_pass "pqos tool is available"
    else
        test_fail "pqos tool is not available"
        return 1
    fi
    
    # Test rdtset availability
    if command -v rdtset &> /dev/null; then
        test_pass "rdtset tool is available"
    else
        test_fail "rdtset tool is not available"
        return 1
    fi
    
    # Test pqos capabilities detection
    if pqos -s &>/dev/null; then
        test_pass "pqos capability detection working"
    else
        test_fail "pqos capability detection failed"
        return 1
    fi
}

# Test RDT feature detection
test_rdt_features() {
    log "Testing RDT feature detection..."
    
    local output
    if ! output=$(pqos -s 2>&1); then
        test_fail "Cannot detect RDT features with pqos"
        return 1
    fi
    
    # Check for specific features
    local features=("CMT" "MBM" "CAT" "CDP" "MBA")
    local found_features=0
    
    for feature in "${features[@]}"; do
        if echo "$output" | grep -qi "$feature"; then
            test_pass "$feature feature detected"
            ((found_features++))
        else
            log_warning "$feature feature not detected"
        fi
    done
    
    if [[ $found_features -gt 0 ]]; then
        test_pass "At least one RDT feature detected"
    else
        test_fail "No RDT features detected"
        return 1
    fi
}

# Test monitoring functionality
test_monitoring() {
    log "Testing monitoring functionality..."
    
    # Test basic monitoring
    local monitor_output
    if monitor_output=$(timeout 10 pqos -m all:0 -t 2 2>&1); then
        test_pass "Basic monitoring test successful"
    else
        test_fail "Basic monitoring test failed"
        echo "$monitor_output"
        return 1
    fi
    
    # Test CSV output format
    if timeout 10 pqos -m all:0 -t 1 -u csv &>/dev/null; then
        test_pass "CSV output format working"
    else
        test_fail "CSV output format failed"
    fi
    
    # Test specific core monitoring
    if timeout 10 pqos -m all:0-1 -t 1 &>/dev/null; then
        test_pass "Specific core monitoring working"
    else
        test_fail "Specific core monitoring failed"
    fi
}

# Test allocation functionality
test_allocation() {
    log "Testing allocation functionality..."
    
    # Create a test resource group
    local test_group="/sys/fs/resctrl/rdt_test_$$"
    
    if mkdir "$test_group" 2>/dev/null; then
        test_pass "Created test resource group"
        
        # Test adding current process to the group
        if echo $$ > "$test_group/tasks" 2>/dev/null; then
            test_pass "Added process to resource group"
        else
            test_fail "Failed to add process to resource group"
        fi
        
        # Test reading schemata
        if [[ -r "$test_group/schemata" ]]; then
            test_pass "Resource group schemata is readable"
            local schemata_content
            schemata_content=$(cat "$test_group/schemata")
            log "Current schemata: $schemata_content"
        else
            test_fail "Resource group schemata is not readable"
        fi
        
        # Cleanup
        echo $$ > /sys/fs/resctrl/tasks 2>/dev/null || true
        rmdir "$test_group" 2>/dev/null || true
        test_pass "Cleaned up test resource group"
    else
        test_fail "Failed to create test resource group"
    fi
}

# Test rdtset functionality
test_rdtset() {
    log "Testing rdtset functionality..."
    
    # Test rdtset with a simple command
    if timeout 10 rdtset -c 0 echo "RDT test successful" &>/dev/null; then
        test_pass "rdtset basic functionality working"
    else
        test_fail "rdtset basic functionality failed"
    fi
    
    # Test rdtset help
    if rdtset -h &>/dev/null; then
        test_pass "rdtset help available"
    else
        test_fail "rdtset help not available"
    fi
}

# Test system integration
test_system_integration() {
    log "Testing system integration..."
    
    # Test systemd service
    if systemctl is-enabled intel-rdt.service &>/dev/null; then
        test_pass "Intel RDT systemd service is enabled"
    else
        test_fail "Intel RDT systemd service is not enabled"
    fi
    
    if systemctl is-active intel-rdt.service &>/dev/null; then
        test_pass "Intel RDT systemd service is active"
    else
        test_fail "Intel RDT systemd service is not active"
    fi
    
    # Test library installation
    if ldconfig -p | grep -q "libpqos"; then
        test_pass "Intel RDT library is properly installed"
    else
        test_warning "Intel RDT library not found in ldconfig"
    fi
    
    # Test documentation
    if [[ -f /opt/intel-rdt/docs/usage-examples.txt ]]; then
        test_pass "Documentation is available"
    else
        test_fail "Documentation is missing"
    fi
}

# Performance test
test_performance() {
    log "Running performance tests..."
    
    # Test monitoring overhead
    local start_time end_time duration
    start_time=$(date +%s.%N)
    
    if timeout 30 pqos -m all:0 -t 5 &>/dev/null; then
        end_time=$(date +%s.%N)
        duration=$(echo "$end_time - $start_time" | bc)
        
        if (( $(echo "$duration < 35" | bc -l) )); then
            test_pass "Monitoring performance test (${duration}s) within acceptable range"
        else
            test_fail "Monitoring performance test took too long (${duration}s)"
        fi
    else
        test_fail "Performance monitoring test failed"
    fi
}

# Generate detailed system report
generate_system_report() {
    log "Generating detailed system report..."
    
    local report_file="/tmp/rdt-system-report-$(date +%Y%m%d-%H%M%S).txt"
    
    {
        echo "Intel RDT System Report"
        echo "======================"
        echo "Generated: $(date)"
        echo "Hostname: $(hostname)"
        echo "Kernel: $(uname -r)"
        echo "OS: $(lsb_release -d 2>/dev/null | cut -f2 || echo "Unknown")"
        echo
        
        echo "CPU Information:"
        echo "==============="
        grep -E "model name|cpu MHz|cache size|processor" /proc/cpuinfo | head -20
        echo
        
        echo "RDT Features in /proc/cpuinfo:"
        echo "=============================="
        grep -E "cqm|cat_l3|cat_l2|cdp_l3|cdp_l2|mba" /proc/cpuinfo || echo "No RDT features found"
        echo
        
        echo "Kernel Modules:"
        echo "==============="
        lsmod | grep -E "msr|resctrl" || echo "No relevant modules loaded"
        echo
        
        echo "resctrl Mount:"
        echo "=============="
        mount | grep resctrl || echo "resctrl not mounted"
        echo
        
        echo "resctrl Information:"
        echo "==================="
        if [[ -d /sys/fs/resctrl/info ]]; then
            find /sys/fs/resctrl/info -name "*.txt" -o -name "*_mask" -o -name "*_gran" | while read -r file; do
                echo "$(basename "$file"): $(cat "$file" 2>/dev/null || echo "Cannot read")"
            done
        else
            echo "resctrl info directory not available"
        fi
        echo
        
        echo "pqos Capabilities:"
        echo "=================="
        pqos -s 2>/dev/null || echo "pqos not available or failed"
        echo
        
        echo "Test Results Summary:"
        echo "===================="
        echo "Tests Passed: $TESTS_PASSED"
        echo "Tests Failed: $TESTS_FAILED"
        echo "Total Tests: $TESTS_TOTAL"
        
    } > "$report_file"
    
    log_success "System report generated: $report_file"
    echo "Report location: $report_file"
}

# Stress test for RDT functionality
stress_test() {
    log "Running RDT stress test..."
    
    if ! command -v stress-ng &> /dev/null; then
        log_warning "stress-ng not available. Skipping stress test."
        log "Install with: sudo apt-get install stress-ng"
        return 0
    fi
    
    # Create multiple resource groups and monitor them
    local pids=()
    local groups=()
    
    for i in {1..3}; do
        local group="/sys/fs/resctrl/stress_test_$i"
        if mkdir "$group" 2>/dev/null; then
            groups+=("$group")
            
            # Start a stress process in background
            stress-ng --cpu 1 --timeout 30s &
            local pid=$!
            pids+=("$pid")
            
            # Add process to resource group
            echo "$pid" > "$group/tasks" 2>/dev/null || true
            
            log "Started stress test $i (PID: $pid) in group $group"
        fi
    done
    
    # Monitor the stress test
    if [[ ${#pids[@]} -gt 0 ]]; then
        log "Monitoring stress test for 30 seconds..."
        timeout 35 pqos -m all -t 30 &>/dev/null || true
        test_pass "RDT stress test completed"
    else
        test_fail "Failed to start stress test processes"
    fi
    
    # Cleanup
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    
    for group in "${groups[@]}"; do
        rmdir "$group" 2>/dev/null || true
    done
    
    log "Stress test cleanup completed"
}

# Main test execution
main() {
    log "Starting Intel RDT comprehensive testing..."
    echo
    
    # Check if running as root for some tests
    if [[ $EUID -eq 0 ]]; then
        log_warning "Running as root. Some tests may behave differently."
    fi
    
    # Run all tests
    test_msr_module || true
    echo
    test_resctrl_filesystem || true
    echo
    test_rdt_tools || true
    echo
    test_rdt_features || true
    echo
    test_monitoring || true
    echo
    test_allocation || true
    echo
    test_rdtset || true
    echo
    test_system_integration || true
    echo
    test_performance || true
    echo
    
    # Optional stress test
    if [[ "${1:-}" == "--stress" ]]; then
        stress_test || true
        echo
    fi
    
    # Generate system report
    generate_system_report
    echo
    
    # Final summary
    log "Testing completed!"
    echo
    echo "=========================================="
    echo "TEST SUMMARY"
    echo "=========================================="
    echo "Tests Passed: $TESTS_PASSED"
    echo "Tests Failed: $TESTS_FAILED"
    echo "Total Tests:  $TESTS_TOTAL"
    echo
    
    if [[ $TESTS_FAILED -eq 0 ]]; then
        log_success "All tests passed! Intel RDT is working correctly."
        exit 0
    else
        log_error "Some tests failed. Please check the output above."
        echo
        echo "Common solutions:"
        echo "1. Ensure you have an Intel CPU with RDT support"
        echo "2. Check that the kernel supports RDT (4.10+)"
        echo "3. Verify MSR module is loaded: sudo modprobe msr"
        echo "4. Check resctrl mount: sudo mount -t resctrl resctrl /sys/fs/resctrl"
        echo "5. Run the setup script again: ./setup-intel-rdt.sh"
        exit 1
    fi
}

# Help function
show_help() {
    echo "Intel RDT Testing Script"
    echo
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  --stress    Run additional stress tests (requires stress-ng)"
    echo "  --help      Show this help message"
    echo
    echo "Examples:"
    echo "  $0                # Run standard tests"
    echo "  $0 --stress      # Run tests with stress testing"
    echo
}

# Handle command line arguments
case "${1:-}" in
    --help|-h)
        show_help
        exit 0
        ;;
    --stress)
        main "$@"
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
