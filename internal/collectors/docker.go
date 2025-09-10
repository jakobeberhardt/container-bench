package collectors

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"container-bench/internal/dataframe"
	"container-bench/internal/logging"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

type DockerCollector struct {
	client         *client.Client
	containerID    string
	containerIndex int
	
	// Store previous stats for CPU percentage calculation
	lastCPUStats   *types.CPUStats
	lastTimestamp  time.Time
	mutex          sync.Mutex
}

func NewDockerCollector(containerID string, containerIndex int) (*DockerCollector, error) {
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
	}

	return collector, nil
}

func (dc *DockerCollector) Collect() *dataframe.DockerMetrics {
	// Use one-shot stats collection for fresh data every call
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	stats, err := dc.client.ContainerStatsOneShot(ctx, dc.containerID)
	if err != nil {
		fmt.Printf("⚠️  Failed to collect Docker stats for %s: %v\n", dc.containerID[:12], err)
		return nil
	}
	defer stats.Body.Close()

	// Decode the stats
	var dockerStats types.StatsJSON
	decoder := json.NewDecoder(stats.Body)
	if err := decoder.Decode(&dockerStats); err != nil {
		fmt.Printf("⚠️  Failed to decode Docker stats for %s: %v\n", dc.containerID[:12], err)
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

	// CPU metrics
	if dockerStats.CPUStats.CPUUsage.TotalUsage > 0 {
		totalUsage := dockerStats.CPUStats.CPUUsage.TotalUsage
		metrics.CPUUsageTotal = &totalUsage
	}

	if dockerStats.CPUStats.CPUUsage.UsageInKernelmode > 0 {
		kernelUsage := dockerStats.CPUStats.CPUUsage.UsageInKernelmode
		metrics.CPUUsageKernel = &kernelUsage
	}

	if dockerStats.CPUStats.CPUUsage.UsageInUsermode > 0 {
		userUsage := dockerStats.CPUStats.CPUUsage.UsageInUsermode
		metrics.CPUUsageUser = &userUsage
	}

	// CPU percentage calculation using our own previous stats
	if dc.lastCPUStats != nil && dockerStats.CPUStats.CPUUsage.TotalUsage > 0 {
		cpuDelta := float64(dockerStats.CPUStats.CPUUsage.TotalUsage - dc.lastCPUStats.CPUUsage.TotalUsage)
		systemDelta := float64(dockerStats.CPUStats.SystemUsage - dc.lastCPUStats.SystemUsage)
		
		if systemDelta > 0 && cpuDelta >= 0 {
			cpuPercent := (cpuDelta / systemDelta) * float64(dockerStats.CPUStats.OnlineCPUs) * 100.0
			metrics.CPUUsagePercent = &cpuPercent
		}
	}

	// Store current stats for next calculation
	if dockerStats.CPUStats.CPUUsage.TotalUsage > 0 {
		dc.lastCPUStats = &dockerStats.CPUStats
		dc.lastTimestamp = currentTime
	}

	// CPU throttling
	if dockerStats.CPUStats.ThrottlingData.ThrottledTime > 0 {
		throttling := dockerStats.CPUStats.ThrottlingData.ThrottledTime
		metrics.CPUThrottling = &throttling
	}

	// Memory metrics
	if dockerStats.MemoryStats.Usage > 0 {
		memUsage := dockerStats.MemoryStats.Usage
		metrics.MemoryUsage = &memUsage
	}

	if dockerStats.MemoryStats.Limit > 0 {
		memLimit := dockerStats.MemoryStats.Limit
		metrics.MemoryLimit = &memLimit
	}

	if cache, ok := dockerStats.MemoryStats.Stats["cache"]; ok {
		metrics.MemoryCache = &cache
	}

	if rss, ok := dockerStats.MemoryStats.Stats["rss"]; ok {
		metrics.MemoryRSS = &rss
	}

	if swap, ok := dockerStats.MemoryStats.Stats["swap"]; ok {
		metrics.MemorySwap = &swap
	}

	// Memory percentage
	if dockerStats.MemoryStats.Usage > 0 && dockerStats.MemoryStats.Limit > 0 {
		memPercent := float64(dockerStats.MemoryStats.Usage) / float64(dockerStats.MemoryStats.Limit) * 100.0
		metrics.MemoryUsagePercent = &memPercent
	}

	// Network metrics
	for _, netStats := range dockerStats.Networks {
		if netStats.RxBytes > 0 {
			rxBytes := netStats.RxBytes
			metrics.NetworkRxBytes = &rxBytes
		}
		if netStats.TxBytes > 0 {
			txBytes := netStats.TxBytes
			metrics.NetworkTxBytes = &txBytes
		}
		if netStats.RxPackets > 0 {
			rxPackets := netStats.RxPackets
			metrics.NetworkRxPackets = &rxPackets
		}
		if netStats.TxPackets > 0 {
			txPackets := netStats.TxPackets
			metrics.NetworkTxPackets = &txPackets
		}
		break // Use first network interface
	}

	// Block I/O metrics
	for _, blkioStats := range dockerStats.BlkioStats.IoServiceBytesRecursive {
		if blkioStats.Op == "Read" && blkioStats.Value > 0 {
			readBytes := blkioStats.Value
			metrics.DiskReadBytes = &readBytes
		}
		if blkioStats.Op == "Write" && blkioStats.Value > 0 {
			writeBytes := blkioStats.Value
			metrics.DiskWriteBytes = &writeBytes
		}
	}

	for _, blkioStats := range dockerStats.BlkioStats.IoServicedRecursive {
		if blkioStats.Op == "Read" && blkioStats.Value > 0 {
			readOps := blkioStats.Value
			metrics.DiskReadOps = &readOps
		}
		if blkioStats.Op == "Write" && blkioStats.Value > 0 {
			writeOps := blkioStats.Value
			metrics.DiskWriteOps = &writeOps
		}
	}

	return metrics
}

func (dc *DockerCollector) Close() {
	if dc.client != nil {
		dc.client.Close()
	}
}
