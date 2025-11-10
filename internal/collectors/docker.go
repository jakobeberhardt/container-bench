package collectors

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"container-bench/internal/config"
	"container-bench/internal/dataframe"
	"container-bench/internal/logging"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

type DockerCollector struct {
	client         *client.Client
	containerID    string
	containerIndex int
	config         *config.DockerMetricsConfig

	lastCPUStats  *types.CPUStats
	lastTimestamp time.Time
	mutex         sync.Mutex
}

func NewDockerCollector(containerID string, containerIndex int, dockerConfig *config.DockerMetricsConfig) (*DockerCollector, error) {
	logger := logging.GetLogger()

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		logger.WithField("container_index", containerIndex).WithError(err).Error("Failed to create Docker client")
		return nil, err
	}

	collector := &DockerCollector{
		client:         cli,
		containerID:    containerID,
		containerIndex: containerIndex,
		config:         dockerConfig,
	}

	return collector, nil
}

func (dc *DockerCollector) Collect() *dataframe.DockerMetrics {
	logger := logging.GetLogger()

	// Use one-shot
	// TODO: Use the actual context
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stats, err := dc.client.ContainerStatsOneShot(ctx, dc.containerID)
	if err != nil {
		logger.WithField("container_id", dc.containerID[:12]).WithError(err).Warn("Failed to collect Docker stats")
		return nil
	}
	defer stats.Body.Close()

	// Decode the stats
	var dockerStats types.StatsJSON
	decoder := json.NewDecoder(stats.Body)
	if err := decoder.Decode(&dockerStats); err != nil {
		logger.WithField("container_id", dc.containerID[:12]).WithError(err).Warn("Failed to decode Docker stats")
		return nil
	}

	// Parse and return metrics
	currentTime := time.Now()
	metrics := dc.parseDockerStats(&dockerStats, currentTime)

	return metrics
}

func (dc *DockerCollector) parseDockerStats(dockerStats *types.StatsJSON, currentTime time.Time) *dataframe.DockerMetrics {
	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	metrics := &dataframe.DockerMetrics{}

	// If no config, collect all metrics (backward compatibility)
	collectAll := dc.config == nil

	// CPU metrics
	if (collectAll || dc.config.CPUUsageTotal) && dockerStats.CPUStats.CPUUsage.TotalUsage > 0 {
		totalUsage := dockerStats.CPUStats.CPUUsage.TotalUsage
		metrics.CPUUsageTotal = &totalUsage
	}

	if (collectAll || dc.config.CPUUsageKernel) && dockerStats.CPUStats.CPUUsage.UsageInKernelmode > 0 {
		kernelUsage := dockerStats.CPUStats.CPUUsage.UsageInKernelmode
		metrics.CPUUsageKernel = &kernelUsage
	}

	if (collectAll || dc.config.CPUUsageUser) && dockerStats.CPUStats.CPUUsage.UsageInUsermode > 0 {
		userUsage := dockerStats.CPUStats.CPUUsage.UsageInUsermode
		metrics.CPUUsageUser = &userUsage
	}

	// CPU percentage
	if (collectAll || dc.config.CPUUsagePercent) && dc.lastCPUStats != nil && dockerStats.CPUStats.CPUUsage.TotalUsage > 0 {
		cpuDelta := float64(dockerStats.CPUStats.CPUUsage.TotalUsage - dc.lastCPUStats.CPUUsage.TotalUsage)
		systemDelta := float64(dockerStats.CPUStats.SystemUsage - dc.lastCPUStats.SystemUsage)

		if systemDelta > 0 && cpuDelta >= 0 {
			cpuPercent := (cpuDelta / systemDelta) * float64(dockerStats.CPUStats.OnlineCPUs) * 100.0
			metrics.CPUUsagePercent = &cpuPercent
		}
	}

	// Store current stats for next calculation (always needed for CPU percent)
	if dockerStats.CPUStats.CPUUsage.TotalUsage > 0 {
		dc.lastCPUStats = &dockerStats.CPUStats
		dc.lastTimestamp = currentTime
	}

	// CPU throttling
	if (collectAll || dc.config.CPUThrottling) && dockerStats.CPUStats.ThrottlingData.ThrottledTime > 0 {
		throttling := dockerStats.CPUStats.ThrottlingData.ThrottledTime
		metrics.CPUThrottling = &throttling
	}

	// Memory metrics
	if (collectAll || dc.config.MemoryUsage) && dockerStats.MemoryStats.Usage > 0 {
		memUsage := dockerStats.MemoryStats.Usage
		metrics.MemoryUsage = &memUsage
	}

	if (collectAll || dc.config.MemoryLimit) && dockerStats.MemoryStats.Limit > 0 {
		memLimit := dockerStats.MemoryStats.Limit
		metrics.MemoryLimit = &memLimit
	}

	if collectAll || dc.config.MemoryCache {
		if cache, ok := dockerStats.MemoryStats.Stats["cache"]; ok {
			metrics.MemoryCache = &cache
		}
	}

	if collectAll || dc.config.MemoryRSS {
		if rss, ok := dockerStats.MemoryStats.Stats["rss"]; ok {
			metrics.MemoryRSS = &rss
		}
	}

	if collectAll || dc.config.MemorySwap {
		if swap, ok := dockerStats.MemoryStats.Stats["swap"]; ok {
			metrics.MemorySwap = &swap
		}
	}

	// Memory percentage
	if (collectAll || dc.config.MemoryUsagePercent) && dockerStats.MemoryStats.Usage > 0 && dockerStats.MemoryStats.Limit > 0 {
		memPercent := float64(dockerStats.MemoryStats.Usage) / float64(dockerStats.MemoryStats.Limit) * 100.0
		metrics.MemoryUsagePercent = &memPercent
	}

	// Network metrics
	if collectAll || dc.config.NetworkRxBytes || dc.config.NetworkTxBytes {
		for _, netStats := range dockerStats.Networks {
			if (collectAll || dc.config.NetworkRxBytes) && netStats.RxBytes > 0 {
				rxBytes := netStats.RxBytes
				metrics.NetworkRxBytes = &rxBytes
			}
			if (collectAll || dc.config.NetworkTxBytes) && netStats.TxBytes > 0 {
				txBytes := netStats.TxBytes
				metrics.NetworkTxBytes = &txBytes
			}
			break // Uses first network interface
		}
	}

	// Block I/O metrics
	if collectAll || dc.config.DiskReadBytes || dc.config.DiskWriteBytes {
		for _, blkioStats := range dockerStats.BlkioStats.IoServiceBytesRecursive {
			if (collectAll || dc.config.DiskReadBytes) && blkioStats.Op == "Read" && blkioStats.Value > 0 {
				readBytes := blkioStats.Value
				metrics.DiskReadBytes = &readBytes
			}
			if (collectAll || dc.config.DiskWriteBytes) && blkioStats.Op == "Write" && blkioStats.Value > 0 {
				writeBytes := blkioStats.Value
				metrics.DiskWriteBytes = &writeBytes
			}
		}
	}

	return metrics
}

func (dc *DockerCollector) Close() {
	if dc.client != nil {
		dc.client.Close()
	}
}
