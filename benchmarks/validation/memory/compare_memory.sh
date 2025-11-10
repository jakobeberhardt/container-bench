#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$PROJECT_DIR"

if [ "$EUID" -ne 0 ]; then 
    echo "Error: This script must be run as root (use sudo)"
    exit 1
fi

CONFIG_FOLDER="${1:-benchmarks/validation/memory}"

if [ ! -d "$CONFIG_FOLDER" ]; then
    echo "Error: Config folder not found: $CONFIG_FOLDER"
    exit 1
fi

CONFIG_FILES=($(find "$CONFIG_FOLDER" -maxdepth 1 -name "*.yml" -type f | sort))

if [ ${#CONFIG_FILES[@]} -eq 0 ]; then
    echo "Error: No YAML configuration files found in $CONFIG_FOLDER"
    exit 1
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${BLUE}=========================================="
echo "Memory Validation"
echo -e "==========================================${NC}"
echo ""
echo "Config Folder: $CONFIG_FOLDER"
echo "Total Tests: ${#CONFIG_FILES[@]}"
echo ""
echo "Configurations to test:"
for config in "${CONFIG_FILES[@]}"; do
    echo "  - $(basename $config)"
done
echo ""
read -p "Press Enter to start validation..."
echo ""

# Arrays to store results
declare -A memory_kb
declare -A elapsed_time

test_num=0
total_tests=${#CONFIG_FILES[@]}

for config_file in "${CONFIG_FILES[@]}"; do
    test_num=$((test_num + 1))
    config_name=$(basename "$config_file" .yml)
    
    echo -e "${CYAN}=========================================="
    echo "Test ${test_num}/${total_tests}: $config_name"
    echo -e "==========================================${NC}"
    echo "Config: $config_file"
    echo ""
    
    # Clean up containers
    docker rm -f $(docker ps -aq) 2>/dev/null || true
    sleep 2
    
    # Run benchmark with memory tracking
    log_file="/tmp/memory_validation_${config_name}.log"
    
    if /usr/bin/time -v ./container-bench run -c "$config_file" 2>&1 | tee "$log_file"; then
            # Extract metrics
            memory_kb[$config_name]=$(grep "Maximum resident set size" "$log_file" | awk '{print $6}')
            elapsed_time[$config_name]=$(grep "Elapsed (wall clock)" "$log_file" | awk '{print $8}')
            
            mem_mb=$(echo "scale=2; ${memory_kb[$config_name]} / 1024" | bc)
            echo ""
            echo -e "${GREEN}✓ Completed: ${mem_mb} MB, ${elapsed_time[$config_name]}${NC}"
        else
            memory_kb[$config_name]="N/A"
            elapsed_time[$config_name]="N/A"
            echo -e "${RED}✗ Test failed${NC}"
        fi
        
        echo ""
        sleep 2
done

# Clean up
docker rm -f $(docker ps -aq) 2>/dev/null || true

# Generate results report
echo ""
echo -e "${BLUE}=========================================="
echo "MEMORY VALIDATION RESULTS"
echo -e "==========================================${NC}"
echo ""

# Simple table of results
printf "%-40s | %12s | %12s\n" "Configuration" "Peak Memory" "Runtime"
printf "%s\n" "---------------------------------------------+-------------+-------------"

# Find baseline (first config or one with "full" in name)
baseline_kb=""
baseline_name=""
for config_name in "${!memory_kb[@]}"; do
    if [[ "$config_name" == *"full"* ]] && [ -z "$baseline_kb" ]; then
        baseline_kb=${memory_kb[$config_name]}
        baseline_name=$config_name
        break
    fi
done

# If no "full" config, use first one as baseline
if [ -z "$baseline_kb" ]; then
    for config_name in "${!memory_kb[@]}"; do
        baseline_kb=${memory_kb[$config_name]}
        baseline_name=$config_name
        break
    done
fi

# Sort and display results
for config_file in "${CONFIG_FILES[@]}"; do
    config_name=$(basename "$config_file" .yml)
    mem_kb=${memory_kb[$config_name]}
    time=${elapsed_time[$config_name]}
    
    if [ "$mem_kb" != "N/A" ]; then
        mem_mb=$(echo "scale=2; $mem_kb / 1024" | bc)
        printf "%-40s | %10s MB | %12s\n" "$config_name" "$mem_mb" "$time"
    else
        printf "%-40s | %12s | %12s\n" "$config_name" "FAILED" "$time"
    fi
done

echo ""
echo -e "${BLUE}=========================================="
echo "SUMMARY"
echo -e "==========================================${NC}"
echo ""

# Calculate statistics
if [ -n "$baseline_kb" ] && [ "$baseline_kb" != "N/A" ]; then
    baseline_mb=$(echo "scale=2; $baseline_kb / 1024" | bc)
    echo "Baseline ($baseline_name): $baseline_mb MB"
    echo ""
fi

# Find best and worst configurations
best_mem_config=""
best_mem_kb=999999999
worst_mem_config=""
worst_mem_kb=0

for config_name in "${!memory_kb[@]}"; do
    mem_kb=${memory_kb[$config_name]}
    if [ "$mem_kb" != "N/A" ] && [ "$mem_kb" -ne 0 ]; then
        if [ "$mem_kb" -lt "$best_mem_kb" ]; then
            best_mem_kb=$mem_kb
            best_mem_config=$config_name
        fi
        if [ "$mem_kb" -gt "$worst_mem_kb" ]; then
            worst_mem_kb=$mem_kb
            worst_mem_config=$config_name
        fi
    fi
done

if [ -n "$best_mem_config" ]; then
    best_mem_mb=$(echo "scale=2; $best_mem_kb / 1024" | bc)
    echo "Lowest Memory: $best_mem_config ($best_mem_mb MB)"
    
    if [ -n "$baseline_kb" ] && [ "$baseline_kb" != "N/A" ]; then
        savings=$(echo "scale=1; (1 - $best_mem_kb / $baseline_kb) * 100" | bc)
        echo "  Reduction from baseline: ${savings}%"
    fi
fi

if [ -n "$worst_mem_config" ]; then
    worst_mem_mb=$(echo "scale=2; $worst_mem_kb / 1024" | bc)
    echo ""
    echo "Highest Memory: $worst_mem_config ($worst_mem_mb MB)"
fi

if [ -n "$best_mem_kb" ] && [ -n "$worst_mem_kb" ] && [ "$worst_mem_kb" -gt 0 ]; then
    range=$(echo "scale=2; ($worst_mem_kb - $best_mem_kb) / 1024" | bc)
    range_pct=$(echo "scale=1; (1 - $best_mem_kb / $worst_mem_kb) * 100" | bc)
    echo ""
    echo "Memory Range: $range MB difference (${range_pct}% reduction from highest to lowest)"
fi

echo ""
echo "Key Insights:"
echo "- Tested ${total_tests} configurations in $CONFIG_FOLDER"
echo "- Memory usage varies based on metrics and sampling frequency"
echo "- Lower sampling frequency = fewer samples = less memory"
echo "- Selective metrics reduce memory footprint significantly"

echo ""
echo -e "${BLUE}=========================================="
echo "DETAILED COMPARISON"
echo -e "==========================================${NC}"
echo ""

# Find best configurations
echo ""
echo -e "${BLUE}=========================================="
echo "DETAILED COMPARISON"
echo -e "==========================================${NC}"
echo ""

# Show comparison with baseline if available
if [ -n "$baseline_kb" ] && [ "$baseline_kb" != "N/A" ]; then
    echo "Comparing all configs to baseline ($baseline_name):"
    echo ""
    printf "%-40s | %12s | %12s\n" "Configuration" "Memory" "vs Baseline"
    printf "%s\n" "---------------------------------------------+-------------+-------------"
    
    for config_file in "${CONFIG_FILES[@]}"; do
        config_name=$(basename "$config_file" .yml)
        mem_kb=${memory_kb[$config_name]}
        
        if [ "$mem_kb" != "N/A" ]; then
            mem_mb=$(echo "scale=2; $mem_kb / 1024" | bc)
            reduction=$(echo "scale=1; (1 - $mem_kb / $baseline_kb) * 100" | bc)
            saved_mb=$(echo "scale=2; ($baseline_kb - $mem_kb) / 1024" | bc)
            
            if [ "$config_name" == "$baseline_name" ]; then
                printf "%-40s | %10s MB | %12s\n" "$config_name" "$mem_mb" "(baseline)"
            else
                printf "%-40s | %10s MB | %+10s%% \n" "$config_name" "$mem_mb" "$reduction"
            fi
        fi
    done
fi

echo ""

# Save detailed report
report_dir="$CONFIG_FOLDER"
report_file="${report_dir}/validation_report_$(date +%Y%m%d_%H%M%S).txt"
{
    echo "container-bench Memory Validation Report"
    echo "Generated: $(date)"
    echo "Config Folder: $CONFIG_FOLDER"
    echo "=========================================="
    echo ""
    echo "Results:"
    echo ""
    printf "%-40s | %12s | %12s\n" "Configuration" "Peak Memory" "Runtime"
    printf "%s\n" "---------------------------------------------+-------------+-------------"
    
    for config_file in "${CONFIG_FILES[@]}"; do
        config_name=$(basename "$config_file" .yml)
        mem_kb=${memory_kb[$config_name]}
        time=${elapsed_time[$config_name]}
        
        if [ "$mem_kb" != "N/A" ]; then
            mem_mb=$(echo "scale=2; $mem_kb / 1024" | bc)
            printf "%-40s | %10s MB | %12s\n" "$config_name" "$mem_mb" "$time"
        else
            printf "%-40s | %12s | %12s\n" "$config_name" "FAILED" "$time"
        fi
    done
    
    echo ""
    if [ -n "$baseline_kb" ] && [ "$baseline_kb" != "N/A" ]; then
        baseline_mb=$(echo "scale=2; $baseline_kb / 1024" | bc)
        echo "Baseline: $baseline_mb MB ($baseline_name)"
    fi
    if [ -n "$best_mem_kb" ] && [ -n "$best_mem_config" ]; then
        best_mem_mb=$(echo "scale=2; $best_mem_kb / 1024" | bc)
        echo "Best: $best_mem_mb MB ($best_mem_config)"
    fi
    if [ -n "$worst_mem_kb" ] && [ -n "$worst_mem_config" ]; then
        worst_mem_mb=$(echo "scale=2; $worst_mem_kb / 1024" | bc)
        echo "Worst: $worst_mem_mb MB ($worst_mem_config)"
    fi
    
} > "$report_file"

echo ""
echo -e "${GREEN}Validation complete!${NC}"
echo "Detailed report: $report_file"
echo ""
