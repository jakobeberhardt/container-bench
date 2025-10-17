# RDT Monitoring Fix: Zero LLC Occupancy Issue

## Problem Description

RDT (Intel Resource Director Technology) monitoring was showing **zero values for LLC (Last Level Cache) occupancy** even though LLC allocation percentages were correctly reported in the database.

## Root Cause Analysis

The issue was caused by the timing optimization that separated container startup from command execution:

### Previous Flow (Working)
1. Container created WITH command in config
2. Container started → workload runs immediately with PID X
3. RDT monitoring group created and PID X added
4. ✅ Monitoring captures workload activity

### New Flow (Broken)
1. Container created WITHOUT command
2. Container started → only entrypoint runs with PID X
3. RDT monitoring group created and PID X added
4. Command executed via `docker exec` → **new PID Y created**
5. ❌ PID Y not in RDT monitoring group → **zero occupancy**

### Technical Details

- **Docker Exec Behavior**: When you execute a command using `docker exec`, Docker creates a **new process** with a different PID inside the container
- **RDT Monitoring Scope**: The RDT monitoring group was only tracking the original container PID, not the exec'd PID
- **Result**: The actual workload (running under the exec'd PID) was not being monitored, leading to zero LLC occupancy values

## Solution Implementation

### 1. Track Container Cgroup Path

Modified `RDTCollector` to store the cgroup path in addition to the PID:

```go
type RDTCollector struct {
    pid          int
    pidStr       string
    cgroupPath   string // NEW: Path to container's cgroup
    monGroupName string
    className    string
    // ... other fields
}
```

### 2. Sync All Cgroup PIDs to Monitoring Group

Added `syncCGroupPIDs()` method that:
- Reads all PIDs from the container's `cgroup.procs` file
- Adds ALL PIDs to the RDT monitoring group
- Ensures both the main container process AND exec'd processes are monitored

```go
func (rc *RDTCollector) syncCGroupPIDs() error {
    // Read cgroup.procs to get all PIDs in the container
    cgroupProcsPath := fmt.Sprintf("%s/cgroup.procs", rc.cgroupPath)
    data, err := ioutil.ReadFile(cgroupProcsPath)
    // ...
    
    // Add all PIDs to the RDT monitoring group
    rc.monGroup.AddPids(pids...)
}
```

### 3. Continuous PID Synchronization

Modified `Collect()` to sync PIDs **before each collection**:
- Picks up any new PIDs created by `docker exec`
- Ensures monitoring coverage is complete
- Minimal overhead (only reads one small file)

```go
func (rc *RDTCollector) Collect() *dataframe.RDTMetrics {
    // Sync PIDs before collecting
    rc.syncCGroupPIDs()
    
    // Get monitoring data
    monData := rc.monGroup.GetMonData()
    // ...
}
```

### 4. Updated Initialization

Modified `NewRDTCollector()` to:
- Accept `cgroupPath` parameter
- Perform initial PID sync during setup
- Log cgroup path for debugging

## How It Works

### PID Discovery Flow
```
Container Cgroup: /sys/fs/cgroup/system.slice/docker-abc123.scope/
    │
    ├─ cgroup.procs (contains all PIDs in container)
    │   ├─ 12345 (main container process)
    │   ├─ 12389 (exec'd workload process)
    │   └─ 12401 (child process)
    │
    └─ Sync to RDT monitoring group: container-bench-mon-12345
        └─ Now monitors: 12345, 12389, 12401
```

### Timing
```
t0: Container starts (PID 12345)
t1: RDT monitoring group created
t2: Initial sync adds PID 12345
t3: Collectors start
t4: Scheduler starts
t5: docker exec runs command (new PID 12389)
t6: First collection → syncCGroupPIDs() discovers PID 12389
t7: PID 12389 added to monitoring group
t8: Subsequent collections capture workload activity ✅
```

## Benefits

### Complete Monitoring Coverage
- ✅ Main container process monitored
- ✅ Exec'd processes monitored
- ✅ Child processes monitored
- ✅ Dynamic PID discovery

### Accurate Metrics
- LLC occupancy now reflects actual workload activity
- Memory bandwidth measurements are complete
- Cache miss rates are accurate

### Minimal Overhead
- Only reads small `cgroup.procs` file
- Happens once per collection interval (e.g., every 100ms)
- `AddPids()` is idempotent (safe to add same PID multiple times)

## Testing Recommendations

### Verify the Fix
1. Run a benchmark with RDT monitoring enabled
2. Check that LLC occupancy is non-zero during workload execution
3. Verify that values match expected cache usage patterns

### Debug Commands
```bash
# Check cgroup PIDs
cat /sys/fs/cgroup/system.slice/docker-<container_id>.scope/cgroup.procs

# Check RDT monitoring group
ls /sys/fs/resctrl/mon_groups/

# Check monitoring data
cat /sys/fs/resctrl/mon_groups/container-bench-mon-<pid>/mon_data/mon_L3_00/llc_occupancy
```

### Expected Behavior
- **Before command execution**: LLC occupancy ≈ 0 (idle container)
- **After command execution**: LLC occupancy > 0 (active workload)
- **Allocation %**: Should remain constant (based on RDT class)

## Files Modified

1. `internal/collectors/rdt.go`
   - Added `cgroupPath` field
   - Added `syncCGroupPIDs()` method
   - Modified `NewRDTCollector()` to accept cgroup path
   - Modified `Collect()` to sync PIDs before collecting

2. `internal/collectors/collector.go`
   - Updated `NewRDTCollector()` call to pass cgroup path

## Related Issues

This fix complements the timing optimization (separating container start from command execution) by ensuring that RDT monitoring adapts to the new execution model where workloads run under different PIDs than the main container process.

## References

- Intel RDT Documentation: Monitoring requires PIDs to be explicitly added to monitoring groups
- Docker Exec Behavior: Creates new processes with different PIDs
- Linux Cgroups: `cgroup.procs` contains all PIDs in a cgroup
