package profiler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/jakobeberhardt/container-bench/internal/storage"
	log "github.com/sirupsen/logrus"
)

type DockerStatsCollector struct {
	metadataProvider *MetadataProvider
	client           *client.Client
}

func NewDockerStatsCollector(metadataProvider *MetadataProvider) *DockerStatsCollector {
	return &DockerStatsCollector{
		metadataProvider: metadataProvider,
	}
}

func (d *DockerStatsCollector) SetContainerIDs(containerIDs map[string]string) {
	// This method is kept for backward compatibility but is no longer needed
	// Container IDs are now managed by the metadata provider
}

func (d *DockerStatsCollector) Initialize(ctx context.Context) error {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("failed to create Docker client: %w", err)
	}
	d.client = cli
	return nil
}

func (d *DockerStatsCollector) Collect(ctx context.Context, timestamp time.Time) ([]storage.Measurement, error) {
	var measurements []storage.Measurement

	// Get container names from metadata provider
	containerNames := d.metadataProvider.GetAllContainerNames()

	// Use the container metadata that was provided to us
	for _, containerName := range containerNames {
		metadata, err := d.metadataProvider.GetContainerMetadata(containerName)
		if err != nil {
			log.WithError(err).WithField("container", containerName).Warn("Failed to get container metadata")
			continue
		}

		log.WithFields(log.Fields{
			"container_name": containerName,
			"container_id":   metadata.ShortID,
		}).Debug("Collecting stats for container")

		// Create a separate context with timeout for each container stats request
		// This prevents the request from being cancelled if the main context is cancelled
		statsCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		// Get container stats directly by ID
		stats, err := d.client.ContainerStats(statsCtx, metadata.ID, false)
		cancel() // Cancel immediately after the request
		
		if err != nil {
			log.WithError(err).WithField("container", metadata.ShortID).Warn("Failed to get container stats")
			continue
		}

		var statsJSON types.StatsJSON
		if err := json.NewDecoder(stats.Body).Decode(&statsJSON); err != nil {
			stats.Body.Close()
			log.WithError(err).WithField("container", metadata.ShortID).Warn("Failed to decode container stats")
			continue
		}
		stats.Body.Close()

		log.WithFields(log.Fields{
			"container":    metadata.ShortID,
			"cpu_usage":    statsJSON.CPUStats.CPUUsage.TotalUsage,
			"memory_usage": statsJSON.MemoryStats.Usage,
		}).Debug("Successfully collected container stats")

		// Convert to measurements using metadata
		containerMeasurements := d.convertStatsToMeasurements(metadata, statsJSON, timestamp)
		measurements = append(measurements, containerMeasurements...)
	}

	return measurements, nil
}

func (d *DockerStatsCollector) convertStatsToMeasurements(metadata *ContainerMetadata, stats types.StatsJSON, timestamp time.Time) []storage.Measurement {
	// Get configuration for benchmark ID
	config := d.metadataProvider.GetConfig()

	tags := map[string]string{
		"benchmark_id":     config.BenchmarkID,
		"container_id":     metadata.ShortID,
		"container_name":   metadata.Name,
		"container_index":  metadata.Index,
		"collector":        "docker_stats",
	}

	var measurements []storage.Measurement

	// CPU metrics
	cpuUsage := float64(0)
	numCPUs := len(stats.CPUStats.CPUUsage.PercpuUsage)
	
	// Fallback: if PercpuUsage is empty, use the total number of CPUs from the system
	// This can happen in some Docker environments where per-CPU stats aren't available
	if numCPUs == 0 {
		if stats.CPUStats.OnlineCPUs > 0 {
			numCPUs = int(stats.CPUStats.OnlineCPUs)
		} else {
			// Last resort: assume single CPU
			numCPUs = 1
		}
	}
	
	if stats.PreCPUStats.CPUUsage.TotalUsage > 0 && stats.PreCPUStats.SystemUsage > 0 && numCPUs > 0 {
		cpuDelta := float64(stats.CPUStats.CPUUsage.TotalUsage - stats.PreCPUStats.CPUUsage.TotalUsage)
		systemDelta := float64(stats.CPUStats.SystemUsage - stats.PreCPUStats.SystemUsage)
		
		if systemDelta > 0 {
			cpuUsage = (cpuDelta / systemDelta) * float64(numCPUs) * 100.0
		}
	}

	measurements = append(measurements, storage.Measurement{
		Name:      "cpu_usage_percent",
		Tags:      copyTags(tags),
		Fields:    map[string]interface{}{"value": cpuUsage},
		Timestamp: timestamp,
	})

	// Additional CPU metrics
	measurements = append(measurements, storage.Measurement{
		Name:      "cpu_usage_total",
		Tags:      copyTags(tags),
		Fields:    map[string]interface{}{"value": float64(stats.CPUStats.CPUUsage.TotalUsage)},
		Timestamp: timestamp,
	})

	measurements = append(measurements, storage.Measurement{
		Name:      "cpu_usage_kernel",
		Tags:      copyTags(tags),
		Fields:    map[string]interface{}{"value": float64(stats.CPUStats.CPUUsage.UsageInKernelmode)},
		Timestamp: timestamp,
	})

	measurements = append(measurements, storage.Measurement{
		Name:      "cpu_usage_user",
		Tags:      copyTags(tags),
		Fields:    map[string]interface{}{"value": float64(stats.CPUStats.CPUUsage.UsageInUsermode)},
		Timestamp: timestamp,
	})

	measurements = append(measurements, storage.Measurement{
		Name:      "cpu_throttling",
		Tags:      copyTags(tags),
		Fields:    map[string]interface{}{"value": float64(stats.CPUStats.ThrottlingData.ThrottledTime)},
		Timestamp: timestamp,
	})

	// Memory metrics
	memUsage := float64(stats.MemoryStats.Usage)
	memLimit := float64(stats.MemoryStats.Limit)
	memPercent := float64(0)
	if memLimit > 0 {
		memPercent = (memUsage / memLimit) * 100.0
	}

	measurements = append(measurements, storage.Measurement{
		Name:      "memory_usage_bytes",
		Tags:      copyTags(tags),
		Fields:    map[string]interface{}{"value": memUsage},
		Timestamp: timestamp,
	})

	measurements = append(measurements, storage.Measurement{
		Name:      "memory_limit",
		Tags:      copyTags(tags),
		Fields:    map[string]interface{}{"value": memLimit},
		Timestamp: timestamp,
	})

	measurements = append(measurements, storage.Measurement{
		Name:      "memory_usage_percent",
		Tags:      copyTags(tags),
		Fields:    map[string]interface{}{"value": memPercent},
		Timestamp: timestamp,
	})

	// Additional memory metrics
	measurements = append(measurements, storage.Measurement{
		Name:      "memory_cache",
		Tags:      copyTags(tags),
		Fields:    map[string]interface{}{"value": float64(stats.MemoryStats.Stats["cache"])},
		Timestamp: timestamp,
	})

	measurements = append(measurements, storage.Measurement{
		Name:      "memory_rss",
		Tags:      copyTags(tags),
		Fields:    map[string]interface{}{"value": float64(stats.MemoryStats.Stats["rss"])},
		Timestamp: timestamp,
	})

	measurements = append(measurements, storage.Measurement{
		Name:      "memory_swap",
		Tags:      copyTags(tags),
		Fields:    map[string]interface{}{"value": float64(stats.MemoryStats.Stats["swap"])},
		Timestamp: timestamp,
	})

	// Network metrics
	if len(stats.Networks) > 0 {
		var rxBytes, txBytes, rxPackets, txPackets uint64
		for _, network := range stats.Networks {
			rxBytes += network.RxBytes
			txBytes += network.TxBytes
			rxPackets += network.RxPackets
			txPackets += network.TxPackets
		}

		measurements = append(measurements, storage.Measurement{
			Name:      "network_rx_bytes",
			Tags:      copyTags(tags),
			Fields:    map[string]interface{}{"value": float64(rxBytes)},
			Timestamp: timestamp,
		})

		measurements = append(measurements, storage.Measurement{
			Name:      "network_tx_bytes",
			Tags:      copyTags(tags),
			Fields:    map[string]interface{}{"value": float64(txBytes)},
			Timestamp: timestamp,
		})

		measurements = append(measurements, storage.Measurement{
			Name:      "network_rx_packets",
			Tags:      copyTags(tags),
			Fields:    map[string]interface{}{"value": float64(rxPackets)},
			Timestamp: timestamp,
		})

		measurements = append(measurements, storage.Measurement{
			Name:      "network_tx_packets",
			Tags:      copyTags(tags),
			Fields:    map[string]interface{}{"value": float64(txPackets)},
			Timestamp: timestamp,
		})
	}

	// Block I/O metrics
	if len(stats.BlkioStats.IoServiceBytesRecursive) > 0 {
		var readBytes, writeBytes uint64
		for _, blkio := range stats.BlkioStats.IoServiceBytesRecursive {
			if blkio.Op == "Read" {
				readBytes += blkio.Value
			} else if blkio.Op == "Write" {
				writeBytes += blkio.Value
			}
		}

		measurements = append(measurements, storage.Measurement{
			Name:      "disk_read_bytes",
			Tags:      copyTags(tags),
			Fields:    map[string]interface{}{"value": float64(readBytes)},
			Timestamp: timestamp,
		})

		measurements = append(measurements, storage.Measurement{
			Name:      "disk_write_bytes",
			Tags:      copyTags(tags),
			Fields:    map[string]interface{}{"value": float64(writeBytes)},
			Timestamp: timestamp,
		})
	}

	// Block I/O operations
	if len(stats.BlkioStats.IoServicedRecursive) > 0 {
		var readOps, writeOps uint64
		for _, blkio := range stats.BlkioStats.IoServicedRecursive {
			if blkio.Op == "Read" {
				readOps += blkio.Value
			} else if blkio.Op == "Write" {
				writeOps += blkio.Value
			}
		}

		measurements = append(measurements, storage.Measurement{
			Name:      "disk_read_ops",
			Tags:      copyTags(tags),
			Fields:    map[string]interface{}{"value": float64(readOps)},
			Timestamp: timestamp,
		})

		measurements = append(measurements, storage.Measurement{
			Name:      "disk_write_ops",
			Tags:      copyTags(tags),
			Fields:    map[string]interface{}{"value": float64(writeOps)},
			Timestamp: timestamp,
		})
	}

	return measurements
}

func copyTags(original map[string]string) map[string]string {
	copy := make(map[string]string)
	for k, v := range original {
		copy[k] = v
	}
	return copy
}

func (d *DockerStatsCollector) Close() error {
	if d.client != nil {
		return d.client.Close()
	}
	return nil
}

func (d *DockerStatsCollector) Name() string {
	return "docker_stats"
}
