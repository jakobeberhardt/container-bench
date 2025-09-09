package database

import (
	"context"
	"fmt"
	"time"

	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

type InfluxDBClient struct {
	client   influxdb2.Client
	writeAPI api.WriteAPIBlocking
	queryAPI api.QueryAPI
	bucket   string
	org      string
}

func NewInfluxDBClient(config config.DatabaseConfig) (*InfluxDBClient, error) {
	client := influxdb2.NewClient(config.Host, config.Password)
	
	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	health, err := client.Health(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to InfluxDB: %w", err)
	}
	
	if health.Status != "pass" {
		return nil, fmt.Errorf("InfluxDB health check failed: %s", health.Message)
	}

	writeAPI := client.WriteAPIBlocking(config.Org, config.Name)
	queryAPI := client.QueryAPI(config.Org)

	return &InfluxDBClient{
		client:   client,
		writeAPI: writeAPI,
		queryAPI: queryAPI,
		bucket:   config.Name,
		org:      config.Org,
	}, nil
}

func (idb *InfluxDBClient) GetLastBenchmarkID() (int, error) {
	ctx := context.Background()
	
	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: -30d)
		|> filter(fn: (r) => r._measurement == "benchmark_metrics")
		|> distinct(column: "benchmark_id")
		|> map(fn: (r) => ({_value: int(v: r.benchmark_id)}))
		|> max()
		|> yield(name: "max_benchmark_id")
	`, idb.bucket)

	result, err := idb.queryAPI.Query(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to query last benchmark ID: %w", err)
	}
	defer result.Close()

	maxID := 0
	for result.Next() {
		if result.Record().Value() != nil {
			if id, ok := result.Record().Value().(int64); ok {
				maxID = int(id)
			}
		}
	}

	if result.Err() != nil {
		return 0, fmt.Errorf("error reading query results: %w", result.Err())
	}

	return maxID, nil
}

func (idb *InfluxDBClient) WriteDataFrames(benchmarkID int, benchmarkConfig *config.BenchmarkConfig, dataframes *dataframe.DataFrames, startTime, endTime time.Time) error {
	ctx := context.Background()

	// Create points for all container data
	var points []*write.Point

	containers := dataframes.GetAllContainers()
	for containerIndex, containerDF := range containers {
		containerConfig := idb.getContainerConfig(benchmarkConfig, containerIndex)
		if containerConfig == nil {
			continue
		}

		steps := containerDF.GetAllSteps()
		fmt.Printf("📊 Container %d: Collected %d data points\n", containerIndex, len(steps))
		for stepNumber, step := range steps {
			if step == nil {
				continue
			}

			// Create base point with common tags
			point := influxdb2.NewPoint("benchmark_metrics",
				map[string]string{
					"benchmark_id":     fmt.Sprintf("%d", benchmarkID),
					"container_index":  fmt.Sprintf("%d", containerIndex),
					"container_image":  containerConfig.Image,
					"container_core":   fmt.Sprintf("%d", containerConfig.Core),
					"benchmark_started": startTime.Format(time.RFC3339),
					"benchmark_finished": endTime.Format(time.RFC3339),
				},
				idb.createFields(step, stepNumber),
				step.Timestamp)

			points = append(points, point)
		}
	}

	// Write all points
	if len(points) > 0 {
		if err := idb.writeAPI.WritePoint(ctx, points...); err != nil {
			return fmt.Errorf("failed to write data points: %w", err)
		}
	}

	return nil
}

func (idb *InfluxDBClient) getContainerConfig(benchmarkConfig *config.BenchmarkConfig, containerIndex int) *config.ContainerConfig {
	for _, container := range benchmarkConfig.Containers {
		if container.Index == containerIndex {
			return &container
		}
	}
	return nil
}

func (idb *InfluxDBClient) createFields(step *dataframe.SamplingStep, stepNumber int) map[string]interface{} {
	fields := make(map[string]interface{})

	// Add step number
	fields["step_number"] = stepNumber

	// Add Perf metrics
	if step.Perf != nil {
		if step.Perf.CacheMisses != nil {
			fields["perf_cache_misses"] = *step.Perf.CacheMisses
		}
		if step.Perf.CacheReferences != nil {
			fields["perf_cache_references"] = *step.Perf.CacheReferences
		}
		if step.Perf.Instructions != nil {
			fields["perf_instructions"] = *step.Perf.Instructions
		}
		if step.Perf.Cycles != nil {
			fields["perf_cycles"] = *step.Perf.Cycles
		}
		if step.Perf.BranchInstructions != nil {
			fields["perf_branch_instructions"] = *step.Perf.BranchInstructions
		}
		if step.Perf.BranchMisses != nil {
			fields["perf_branch_misses"] = *step.Perf.BranchMisses
		}
		if step.Perf.BusCycles != nil {
			fields["perf_bus_cycles"] = *step.Perf.BusCycles
		}
		if step.Perf.CacheMissRate != nil {
			fields["perf_cache_miss_rate"] = *step.Perf.CacheMissRate
		}
		if step.Perf.BranchMissRate != nil {
			fields["perf_branch_miss_rate"] = *step.Perf.BranchMissRate
		}
	}

	// Add Docker metrics
	if step.Docker != nil {
		if step.Docker.CPUUsageTotal != nil {
			fields["docker_cpu_usage_total"] = *step.Docker.CPUUsageTotal
		}
		if step.Docker.CPUUsageKernel != nil {
			fields["docker_cpu_usage_kernel"] = *step.Docker.CPUUsageKernel
		}
		if step.Docker.CPUUsageUser != nil {
			fields["docker_cpu_usage_user"] = *step.Docker.CPUUsageUser
		}
		if step.Docker.CPUUsagePercent != nil {
			fields["docker_cpu_usage_percent"] = *step.Docker.CPUUsagePercent
		}
		if step.Docker.CPUThrottling != nil {
			fields["docker_cpu_throttling"] = *step.Docker.CPUThrottling
		}
		if step.Docker.MemoryUsage != nil {
			fields["docker_memory_usage"] = *step.Docker.MemoryUsage
		}
		if step.Docker.MemoryLimit != nil {
			fields["docker_memory_limit"] = *step.Docker.MemoryLimit
		}
		if step.Docker.MemoryCache != nil {
			fields["docker_memory_cache"] = *step.Docker.MemoryCache
		}
		if step.Docker.MemoryRSS != nil {
			fields["docker_memory_rss"] = *step.Docker.MemoryRSS
		}
		if step.Docker.MemorySwap != nil {
			fields["docker_memory_swap"] = *step.Docker.MemorySwap
		}
		if step.Docker.MemoryUsagePercent != nil {
			fields["docker_memory_usage_percent"] = *step.Docker.MemoryUsagePercent
		}
		if step.Docker.NetworkRxBytes != nil {
			fields["docker_network_rx_bytes"] = *step.Docker.NetworkRxBytes
		}
		if step.Docker.NetworkTxBytes != nil {
			fields["docker_network_tx_bytes"] = *step.Docker.NetworkTxBytes
		}
		if step.Docker.NetworkRxPackets != nil {
			fields["docker_network_rx_packets"] = *step.Docker.NetworkRxPackets
		}
		if step.Docker.NetworkTxPackets != nil {
			fields["docker_network_tx_packets"] = *step.Docker.NetworkTxPackets
		}
		if step.Docker.DiskReadBytes != nil {
			fields["docker_disk_read_bytes"] = *step.Docker.DiskReadBytes
		}
		if step.Docker.DiskWriteBytes != nil {
			fields["docker_disk_write_bytes"] = *step.Docker.DiskWriteBytes
		}
		if step.Docker.DiskReadOps != nil {
			fields["docker_disk_read_ops"] = *step.Docker.DiskReadOps
		}
		if step.Docker.DiskWriteOps != nil {
			fields["docker_disk_write_ops"] = *step.Docker.DiskWriteOps
		}
	}

	// Add RDT metrics
	if step.RDT != nil {
		if step.RDT.L3CacheUsage != nil {
			fields["rdt_l3_cache_usage"] = *step.RDT.L3CacheUsage
		}
		if step.RDT.MemoryBandwidth != nil {
			fields["rdt_memory_bandwidth"] = *step.RDT.MemoryBandwidth
		}
		if step.RDT.CLOSGroup != nil {
			fields["rdt_clos_group"] = *step.RDT.CLOSGroup
		}
	}

	return fields
}

func (idb *InfluxDBClient) Close() {
	if idb.client != nil {
		idb.client.Close()
	}
}
