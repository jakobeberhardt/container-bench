package collectors

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"container-bench/internal/dataframe"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

type DockerCollector struct {
	client         *client.Client
	containerID    string
	latestMetrics  *dataframe.DockerMetrics
	metricsMutex   sync.RWMutex
	streamCtx      context.Context
	streamCancel   context.CancelFunc
	streamStarted  bool
}

func NewDockerCollector(containerID string) (*DockerCollector, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	collector := &DockerCollector{
		client:       cli,
		containerID:  containerID,
		streamCtx:    ctx,
		streamCancel: cancel,
	}

	// Start streaming stats in background goroutine
	go collector.streamStats()

	return collector, nil
}

func (dc *DockerCollector) streamStats() {
	stats, err := dc.client.ContainerStats(dc.streamCtx, dc.containerID, true) // stream=true
	if err != nil {
		return
	}
	defer stats.Body.Close()

	decoder := json.NewDecoder(stats.Body)
	for {
		select {
		case <-dc.streamCtx.Done():
			return
		default:
			var dockerStats types.StatsJSON
			if err := decoder.Decode(&dockerStats); err != nil {
				return
			}

			// Parse and store the latest metrics
			metrics := dc.parseDockerStats(&dockerStats)
			
			dc.metricsMutex.Lock()
			dc.latestMetrics = metrics
			dc.streamStarted = true
			dc.metricsMutex.Unlock()
		}
	}
}

func (dc *DockerCollector) Collect() *dataframe.DockerMetrics {
	dc.metricsMutex.RLock()
	defer dc.metricsMutex.RUnlock()
	
	if !dc.streamStarted {
		fmt.Printf("⏳ Docker stats stream not ready for %s\n", dc.containerID[:12])
		return nil
	}
	
	if dc.latestMetrics == nil {
		fmt.Printf("� No Docker stats available for %s\n", dc.containerID[:12])
		return nil
	}
	
	fmt.Printf("✅ Docker stats collected from stream for %s\n", dc.containerID[:12])
	
	// Return a copy of the latest metrics
	return dc.copyMetrics(dc.latestMetrics)
}

func (dc *DockerCollector) copyMetrics(original *dataframe.DockerMetrics) *dataframe.DockerMetrics {
	if original == nil {
		return nil
	}
	
	// Create a copy to avoid race conditions
	copy := &dataframe.DockerMetrics{}
	
	if original.CPUUsageTotal != nil {
		val := *original.CPUUsageTotal
		copy.CPUUsageTotal = &val
	}
	if original.CPUUsageKernel != nil {
		val := *original.CPUUsageKernel
		copy.CPUUsageKernel = &val
	}
	if original.CPUUsageUser != nil {
		val := *original.CPUUsageUser
		copy.CPUUsageUser = &val
	}
	if original.CPUUsagePercent != nil {
		val := *original.CPUUsagePercent
		copy.CPUUsagePercent = &val
	}
	if original.CPUThrottling != nil {
		val := *original.CPUThrottling
		copy.CPUThrottling = &val
	}
	if original.MemoryUsage != nil {
		val := *original.MemoryUsage
		copy.MemoryUsage = &val
	}
	if original.MemoryLimit != nil {
		val := *original.MemoryLimit
		copy.MemoryLimit = &val
	}
	if original.MemoryCache != nil {
		val := *original.MemoryCache
		copy.MemoryCache = &val
	}
	if original.MemoryRSS != nil {
		val := *original.MemoryRSS
		copy.MemoryRSS = &val
	}
	if original.MemorySwap != nil {
		val := *original.MemorySwap
		copy.MemorySwap = &val
	}
	if original.MemoryUsagePercent != nil {
		val := *original.MemoryUsagePercent
		copy.MemoryUsagePercent = &val
	}
	if original.NetworkRxBytes != nil {
		val := *original.NetworkRxBytes
		copy.NetworkRxBytes = &val
	}
	if original.NetworkTxBytes != nil {
		val := *original.NetworkTxBytes
		copy.NetworkTxBytes = &val
	}
	if original.NetworkRxPackets != nil {
		val := *original.NetworkRxPackets
		copy.NetworkRxPackets = &val
	}
	if original.NetworkTxPackets != nil {
		val := *original.NetworkTxPackets
		copy.NetworkTxPackets = &val
	}
	if original.DiskReadBytes != nil {
		val := *original.DiskReadBytes
		copy.DiskReadBytes = &val
	}
	if original.DiskWriteBytes != nil {
		val := *original.DiskWriteBytes
		copy.DiskWriteBytes = &val
	}
	if original.DiskReadOps != nil {
		val := *original.DiskReadOps
		copy.DiskReadOps = &val
	}
	if original.DiskWriteOps != nil {
		val := *original.DiskWriteOps
		copy.DiskWriteOps = &val
	}
	
	return copy
}

func (dc *DockerCollector) parseDockerStats(dockerStats *types.StatsJSON) *dataframe.DockerMetrics {
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

	// CPU percentage calculation
	if dockerStats.CPUStats.CPUUsage.TotalUsage > 0 && dockerStats.PreCPUStats.CPUUsage.TotalUsage > 0 {
		cpuDelta := float64(dockerStats.CPUStats.CPUUsage.TotalUsage - dockerStats.PreCPUStats.CPUUsage.TotalUsage)
		systemDelta := float64(dockerStats.CPUStats.SystemUsage - dockerStats.PreCPUStats.SystemUsage)
		
		if systemDelta > 0 {
			cpuPercent := (cpuDelta / systemDelta) * float64(dockerStats.CPUStats.OnlineCPUs) * 100.0
			metrics.CPUUsagePercent = &cpuPercent
		}
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
	if dc.streamCancel != nil {
		dc.streamCancel()
	}
	if dc.client != nil {
		dc.client.Close()
	}
}
