#!/bin/bash
set -e

echo "Starting Docker daemon..."
dockerd-entrypoint.sh &

echo "Waiting for Docker daemon to be ready..."
timeout=30
counter=0
while ! docker info >/dev/null 2>&1; do
    if [ $counter -ge $timeout ]; then
        echo "ERROR: Docker daemon failed to start within ${timeout} seconds"
        exit 1
    fi
    echo "Waiting for Docker daemon... ($counter/$timeout)"
    sleep 1
    counter=$((counter + 1))
done

echo "Docker daemon is ready"

echo "Setting up perf permissions..."
sysctl -w kernel.perf_event_paranoid=-1 2>/dev/null || echo "Warning: Could not set perf_event_paranoid"

# RDT support commented out - requires specific Intel hardware and kernel support
# echo "Loading MSR module..."
# modprobe msr 2>/dev/null || echo "Warning: Could not load msr module"
# 
# echo "Setting up resctrl filesystem..."
# umount /sys/fs/resctrl 2>/dev/null || true
# mount -t resctrl resctrl /sys/fs/resctrl 2>/dev/null || echo "Warning: Could not mount resctrl"
# 
# export RDT_IFACE=OS
# 
# echo "Cleaning up RDT lock files..."
# rm -f /var/lock/libpqos* /run/lock/libpqos* 2>/dev/null || true
# 
# echo "Cleaning up empty resctrl groups..."
# for g in /sys/fs/resctrl/*; do
#   case "$g" in
#     */info|*/mon_groups|*/mon_data) continue ;;
#   esac
#   [ -d "$g" ] && [ ! -s "$g/tasks" ] && rmdir "$g" 2>/dev/null || true
# done

echo "Container-bench environment ready!"
echo ""
echo "Available commands:"
echo "  ./container-bench run -c <config.yml>    - Run a benchmark"
echo "  ./container-bench validate -c <config.yml> - Validate a config"
echo ""

exec "$@"
