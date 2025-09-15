# DataFrame Field Management Guide

This document explains how to add, remove, or modify fields in the container-bench data pipeline, which consists of: **Collectors** to **DataFrames** to **Data Handler** to**Database**.

## Architecture Overview

1. **Collectors** (`internal/collectors/`) - Collect raw metrics from Docker, perf, and RDT
2. **DataFrames** (`internal/dataframe/`) - Store metrics in structured format during benchmark
3. **Data Handler** (`internal/datahandeling/`) - Process and derive metrics after benchmark completion
4. **Database** (`internal/database/`) - Export processed data to InfluxDB

## Field Types and Locations

### Core Metric Structures

All metric structures are defined in `internal/dataframe/dataframe.go`:

- `PerfMetrics` - Performance counters from perf events
- `DockerMetrics` - Container resource usage from Docker API
- `RDTMetrics` - Intel RDT cache and bandwidth metrics

### Field Naming Convention

- Use lowercase with underscores for JSON tags: `"field_name"`
- Use CamelCase for Go struct fields: `FieldName`
- Database field names follow pattern: `<source>_<metric>_<unit>`
  - Examples: `perf_cache_miss_rate`, `docker_memory_usage`, `rdt_l3_cache_occupancy`

## Adding New Fields

### Step 1: Update DataFrame Structure

Add the new field to the appropriate metrics struct in `internal/dataframe/dataframe.go`:

```go
type PerfMetrics struct {
    // Existing fields...
    NewMetric *uint64 `json:"new_metric,omitempty"`
}
```

### Step 2: Update Collector

Modify the appropriate collector to populate the new field:

**For Docker metrics** (`internal/collectors/docker.go`):
```go
if someCondition {
    value := extractValue()
    metrics.NewMetric = &value
}
```

**For Perf metrics** (`internal/collectors/perf.go`):
```go
if perfEvent.Value > 0 {
    value := perfEvent.Value
    metrics.NewMetric = &value
}
```

**For RDT metrics** (`internal/collectors/rdt.go`):
```go
if rdtValue, exists := rdtData["new_metric"]; exists {
    metrics.NewMetric = &rdtValue
}
```

### Step 3: Update Data Handler (Optional)

If the field needs processing or derived calculations, add logic in `internal/datahandeling/datahandeling.go`:

```go
// In processContainer or addDerivedMetrics
if step.Source.NewMetric != nil {
    // Perform calculations
    derivedValue := calculateDerived(*step.Source.NewMetric)
    step.Source.DerivedField = &derivedValue
}
```

### Step 4: Update Database Export

Add the field to InfluxDB export in `internal/database/influxdb.go`:

```go
// In createFieldsFromStep function
if step.Source.NewMetric != nil {
    fields["source_new_metric"] = *step.Source.NewMetric
}
```

### Step 5: Update Metadata (If Hardware-Related)

For system hardware information, update `SystemInfo` struct and collection logic in `internal/database/influxdb.go`.

## Removing Fields

### Step 1: Remove from DataFrame

Remove the field from the metrics struct in `internal/dataframe/dataframe.go`.

### Step 2: Remove from Collectors

Remove any code that populates the field from the relevant collectors.

### Step 3: Remove from Data Handler

Remove any processing logic for the field from `internal/datahandeling/datahandeling.go`.

### Step 4: Remove from Database

Remove the field export from `internal/database/influxdb.go`.

### Step 5: Remove from Schedulers (If Used)

Check scheduler implementations for any usage of the removed field.

## Renaming Fields

### Step 1: Update DataFrame Structure

Change the field name and JSON tag:

```go
// Old
OldName *uint64 `json:"old_name,omitempty"`

// New  
NewName *uint64 `json:"new_name,omitempty"`
```

### Step 2: Update All References

Update all references across:
- Collectors (`internal/collectors/`)
- Data handler (`internal/datahandeling/`)
- Database export (`internal/database/`)
- Schedulers (`internal/scheduler/`)

## Database Schema Considerations

### Field Types in InfluxDB

- `*uint64` → `unsigned` in InfluxDB
- `*float64` → `float` in InfluxDB  
- `*string` → `string` in InfluxDB
- `*int` → `integer` in InfluxDB

### Tags vs Fields

**Tags** (indexed, for grouping):
- `benchmark_id`
- `container_index` 
- `container_image`
- `container_core`

**Fields** (actual metrics):
- All performance metrics
- Resource usage metrics
- Derived calculations

## Testing Changes

### 1. Compilation Test
```bash
make clean && make build
```

### 2. Validation Test
```bash
./container-bench validate -c examples/simple_test.yml
```

### 3. End-to-End Test
```bash
sudo ./container-bench run -c examples/simple_test.yml
```

### 4. Database Verification
Query InfluxDB to verify new fields appear correctly:
```sql
SELECT * FROM benchmark_metrics WHERE benchmark_id = 'latest'
```

## Common Patterns

### Optional Fields with Nil Checks
```go
if metrics.SomeField != nil {
    fields["some_field"] = *metrics.SomeField
}
```

### Derived Calculations
```go
if metrics.A != nil && metrics.B != nil && *metrics.B > 0 {
    rate := float64(*metrics.A) / float64(*metrics.B)
    metrics.ARate = &rate
}
```

### Unit Conversions
```go
// Convert bytes to MB
if metrics.Bytes != nil {
    mb := float64(*metrics.Bytes) / (1024 * 1024)
    metrics.MB = &mb
}
```

## Recent Changes

### Removed Fields
- `BranchMissRate` from PerfMetrics (derived at query time)
- `NetworkRxPackets`, `NetworkTxPackets` from DockerMetrics
- `DiskReadOps`, `DiskWriteOps` from DockerMetrics  
- `L3CacheOccupancyKB`, `L3CacheOccupancyMB` from RDTMetrics

### Renamed Fields
- `CacheUtilization` → `CacheLLCUtilizationPercent`
- `BandwidthUtilization` → `BandwidthUtilizationPercent`

### Added Metadata Fields
- `L3CacheSizeBytes` - L3 cache size from `/sys/devices/system/cpu/cpu0/cache/index3/size`
- `MaxMemoryBandwidthMBps` - Estimated memory bandwidth based on CPU model

## Best Practices

1. **Always use pointers** for optional metrics to distinguish between "not collected" and "zero value"
2. **Add fields incrementally** - test each step before proceeding
3. **Maintain consistency** in naming across all layers
4. **Document units** in field comments (bytes, MB, percentage, etc.)
5. **Test with real benchmarks** to ensure data flows correctly
6. **Consider backwards compatibility** when modifying existing fields

## Troubleshooting

### Compilation Errors
- Check for typos in field names across all files
- Ensure all references are updated when renaming
- Verify import statements are correct

### Runtime Errors
- Check for nil pointer dereferences
- Verify collectors actually populate the fields
- Ensure data handler processes all required fields

### Database Issues
- Verify field names match between code and InfluxDB queries
- Check data types are compatible
- Ensure tags vs fields are correctly categorized
