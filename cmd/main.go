package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"container-bench/internal/collectors"
	"container-bench/internal/config"
	"container-bench/internal/database"
	"container-bench/internal/dataframe"
	"container-bench/internal/scheduler"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/spf13/cobra"
)

type ContainerBench struct {
	config       *config.BenchmarkConfig
	dockerClient *client.Client
	dbClient     *database.InfluxDBClient
	dataframes   *dataframe.DataFrames
	scheduler    scheduler.Scheduler
	collectors   []*collectors.ContainerCollector
	benchmarkID  int
	startTime    time.Time
	endTime      time.Time
}

func main() {
	var configFile string

	rootCmd := &cobra.Command{
		Use:   "container-bench",
		Short: "Container performance benchmarking tool",
		Long:  "A configurable tool for profiling Docker containers with perf, docker stats, and Intel RDT",
	}

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Run a benchmark",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runBenchmark(configFile)
		},
	}

	validateCmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate a benchmark configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			return validateConfig(configFile)
		},
	}

	runCmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to benchmark configuration file")
	runCmd.MarkFlagRequired("config")

	validateCmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to benchmark configuration file")
	validateCmd.MarkFlagRequired("config")

	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(validateCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func validateConfig(configFile string) error {
	_, err := config.LoadConfig(configFile)
	if err != nil {
		fmt.Printf("Configuration validation failed: %v\n", err)
		return err
	}
	fmt.Println("Configuration is valid")
	return nil
}

func runBenchmark(configFile string) error {
	bench := &ContainerBench{
		dataframes: dataframe.NewDataFrames(),
	}

	// Load configuration
	var err error
	bench.config, err = config.LoadConfig(configFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Initialize Docker client
	bench.dockerClient, err = client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("failed to create Docker client: %w", err)
	}
	defer bench.dockerClient.Close()

	// Initialize database client
	bench.dbClient, err = database.NewInfluxDBClient(bench.config.Benchmark.Data.DB)
	if err != nil {
		return fmt.Errorf("failed to create database client: %w", err)
	}
	defer bench.dbClient.Close()

	// Get next benchmark ID
	lastID, err := bench.dbClient.GetLastBenchmarkID()
	if err != nil {
		return fmt.Errorf("failed to get last benchmark ID: %w", err)
	}
	bench.benchmarkID = lastID + 1

	// Initialize scheduler
	bench.scheduler = scheduler.NewDefaultScheduler()
	if err := bench.scheduler.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize scheduler: %w", err)
	}
	defer bench.scheduler.Shutdown()

	fmt.Printf("Starting benchmark %d: %s\n", bench.benchmarkID, bench.config.Benchmark.Name)

	// Setup containers
	if err := bench.setupContainers(); err != nil {
		return fmt.Errorf("failed to setup containers: %w", err)
	}

	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nReceived interrupt signal, shutting down...")
		cancel()
	}()

	// Start benchmark
	bench.startTime = time.Now()
	if err := bench.startBenchmark(ctx); err != nil {
		return fmt.Errorf("benchmark failed: %w", err)
	}

	return nil
}

func (cb *ContainerBench) setupContainers() error {
	containers := cb.config.GetContainersSorted()

	for _, containerConfig := range containers {
		fmt.Printf("Setting up container %d: %s\n", containerConfig.Index, containerConfig.Image)

		// Pull image
		ctx := context.Background()
		_, err := cb.dockerClient.ImagePull(ctx, containerConfig.Image, types.ImagePullOptions{})
		if err != nil {
			return fmt.Errorf("failed to pull image %s: %w", containerConfig.Image, err)
		}

		// Create container
		containerName := fmt.Sprintf("bench-%d-container-%d", cb.benchmarkID, containerConfig.Index)
		
		config := &container.Config{
			Image: containerConfig.Image,
		}

		if containerConfig.Command != "" {
			config.Cmd = []string{"sh", "-c", containerConfig.Command}
		}

		hostConfig := &container.HostConfig{}

		if containerConfig.Port != "" {
			// Parse port mapping (simplified)
			hostConfig.PortBindings = map[nat.Port][]nat.PortBinding{
				nat.Port(containerConfig.Port): {{HostPort: containerConfig.Port}},
			}
		}

		// Set CPU affinity
		hostConfig.CpusetCpus = strconv.Itoa(containerConfig.Core)

		resp, err := cb.dockerClient.ContainerCreate(ctx, config, hostConfig, nil, nil, containerName)
		if err != nil {
			return fmt.Errorf("failed to create container %s: %w", containerName, err)
		}

		// Setup data frame for this container
		containerDF := cb.dataframes.AddContainer(containerConfig.Index)

		// Create collector
		collectorConfig := collectors.CollectorConfig{
			Frequency:    time.Duration(containerConfig.Data.Frequency) * time.Millisecond,
			EnablePerf:   containerConfig.Data.Perf,
			EnableDocker: containerConfig.Data.Docker,
			EnableRDT:    containerConfig.Data.RDT,
		}

		collector := collectors.NewContainerCollector(containerConfig.Index, resp.ID, collectorConfig, containerDF)
		cb.collectors = append(cb.collectors, collector)

		fmt.Printf("Container %d created with ID: %s\n", containerConfig.Index, resp.ID[:12])
	}

	return nil
}

func (cb *ContainerBench) startBenchmark(ctx context.Context) error {
	// Start all containers and get their PIDs
	for i, collector := range cb.collectors {
		containerConfig := cb.config.GetContainersSorted()[i]
		
		// Start container
		if err := cb.dockerClient.ContainerStart(ctx, collector.ContainerID, container.StartOptions{}); err != nil {
			return fmt.Errorf("failed to start container %d: %w", containerConfig.Index, err)
		}

		// Get container info
		info, err := cb.dockerClient.ContainerInspect(ctx, collector.ContainerID)
		if err != nil {
			return fmt.Errorf("failed to inspect container %d: %w", containerConfig.Index, err)
		}

		pid := info.State.Pid
		cgroupPath := filepath.Join("/sys/fs/cgroup", info.HostConfig.CgroupParent, info.ID)

		collector.SetContainerInfo(pid, cgroupPath)

		// Start collector
		if err := collector.Start(ctx); err != nil {
			return fmt.Errorf("failed to start collector for container %d: %w", containerConfig.Index, err)
		}

		fmt.Printf("Container %d started with PID %d\n", containerConfig.Index, pid)
	}

	// Start scheduler updates
	schedulerTicker := time.NewTicker(1 * time.Second)
	defer schedulerTicker.Stop()

	// Set up timeout
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, cb.config.GetMaxDuration())
	defer timeoutCancel()

	fmt.Printf("Benchmark running for up to %v...\n", cb.config.GetMaxDuration())

	// Main benchmark loop
	for {
		select {
		case <-timeoutCtx.Done():
			if timeoutCtx.Err() == context.DeadlineExceeded {
				fmt.Println("Benchmark timeout reached")
			} else {
				fmt.Println("Benchmark interrupted")
			}
			cb.endTime = time.Now()
			return cb.finalizeBenchmark()
		case <-schedulerTicker.C:
			// Update scheduler with current data
			if err := cb.scheduler.ProcessDataFrames(cb.dataframes); err != nil {
				fmt.Printf("Scheduler error: %v\n", err)
			}
		}
	}
}

func (cb *ContainerBench) finalizeBenchmark() error {
	fmt.Println("Finalizing benchmark...")

	// Stop all collectors
	for _, collector := range cb.collectors {
		if err := collector.Stop(); err != nil {
			fmt.Printf("Error stopping collector: %v\n", err)
		}
	}

	// Stop and remove containers
	ctx := context.Background()
	for _, collector := range cb.collectors {
		if err := cb.dockerClient.ContainerStop(ctx, collector.ContainerID, container.StopOptions{}); err != nil {
			fmt.Printf("Error stopping container %s: %v\n", collector.ContainerID[:12], err)
		}

		if err := cb.dockerClient.ContainerRemove(ctx, collector.ContainerID, container.RemoveOptions{}); err != nil {
			fmt.Printf("Error removing container %s: %v\n", collector.ContainerID[:12], err)
		}
	}

	// Export data to database
	fmt.Println("Exporting data to database...")
	if err := cb.dbClient.WriteDataFrames(cb.benchmarkID, cb.config, cb.dataframes, cb.startTime, cb.endTime); err != nil {
		return fmt.Errorf("failed to export data: %w", err)
	}

	duration := cb.endTime.Sub(cb.startTime)
	fmt.Printf("Benchmark %d completed in %v\n", cb.benchmarkID, duration)

	return nil
}
