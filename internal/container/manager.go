package container

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/jakobeberhardt/container-bench/internal/config"
	log "github.com/sirupsen/logrus"
)

// Manager handles Docker container operations
type Manager struct {
	client      *client.Client
	containers  map[string]string // name -> container ID mapping
	authConfig  config.AuthConfig
}

// NewManager creates a new container manager
func NewManager(authConfig config.AuthConfig) (*Manager, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	return &Manager{
		client:     cli,
		containers: make(map[string]string),
		authConfig: authConfig,
	}, nil
}

// PullImages pulls all required Docker images
func (m *Manager) PullImages(ctx context.Context, containers map[string]config.ContainerConfig) error {
	log.Info("Pulling Docker images")

	// Create authentication if configured
	var authStr string
	if m.authConfig.Username != "" && m.authConfig.Password != "" {
		authConfig := registry.AuthConfig{
			Username:      m.authConfig.Username,
			Password:      m.authConfig.Password,
			ServerAddress: m.authConfig.Registry,
		}
		encodedJSON, err := json.Marshal(authConfig)
		if err != nil {
			return fmt.Errorf("failed to encode auth config: %w", err)
		}
		authStr = base64.URLEncoding.EncodeToString(encodedJSON)
	}

	// Get unique images
	images := make(map[string]bool)
	for _, containerCfg := range containers {
		images[containerCfg.Image] = true
	}

	// Pull each unique image
	for image := range images {
		log.WithField("image", image).Info("Pulling image")
		
		reader, err := m.client.ImagePull(ctx, image, types.ImagePullOptions{
			RegistryAuth: authStr,
		})
		if err != nil {
			return fmt.Errorf("failed to pull image %s: %w", image, err)
		}

		// Read the pull response to completion
		_, err = io.Copy(io.Discard, reader)
		reader.Close()
		if err != nil {
			return fmt.Errorf("failed to read pull response for image %s: %w", image, err)
		}

		log.WithField("image", image).Info("Image pulled successfully")
	}

	return nil
}

// CreateContainers creates all containers but doesn't start them
func (m *Manager) CreateContainers(ctx context.Context, containers map[string]config.ContainerConfig, benchmarkID string) error {
	log.Info("Creating containers")

	for name, containerCfg := range containers {
		if err := m.createContainer(ctx, name, containerCfg, benchmarkID); err != nil {
			return fmt.Errorf("failed to create container %s: %w", name, err)
		}
	}

	log.WithField("count", len(containers)).Info("All containers created successfully")
	return nil
}

// createContainer creates a single container
func (m *Manager) createContainer(ctx context.Context, name string, cfg config.ContainerConfig, benchmarkID string) error {
	// Prepare port bindings
	var portBindings nat.PortMap
	var exposedPorts nat.PortSet
	
	if cfg.Port != "" {
		parts := strings.Split(cfg.Port, ":")
		if len(parts) == 2 {
			containerPort := nat.Port(parts[1] + "/tcp")
			hostPort := parts[0]
			
			portBindings = nat.PortMap{
				containerPort: []nat.PortBinding{{HostPort: hostPort}},
			}
			exposedPorts = nat.PortSet{
				containerPort: struct{}{},
			}
		}
	}

	// Prepare environment variables
	env := make([]string, 0)
	for key, value := range cfg.Env {
		env = append(env, fmt.Sprintf("%s=%s", key, value))
	}

	// Add benchmark metadata
	env = append(env,
		fmt.Sprintf("BENCHMARK_ID=%s", benchmarkID),
		fmt.Sprintf("CONTAINER_INDEX=%d", cfg.Index),
		fmt.Sprintf("CONTAINER_NAME=%s", name),
	)

	// Configure CPU resources
	var resources container.Resources
	if cfg.Core >= 0 {
		// Pin to specific CPU core
		resources.CpusetCpus = strconv.Itoa(cfg.Core)
	}

	config := &container.Config{
		Image:        cfg.Image,
		Env:          env,
		ExposedPorts: exposedPorts,
		Labels: map[string]string{
			"benchmark.id":    benchmarkID,
			"benchmark.index": strconv.Itoa(cfg.Index),
			"benchmark.name":  name,
		},
	}

	hostConfig := &container.HostConfig{
		PortBindings: portBindings,
		Resources:    resources,
		AutoRemove:   false, // We'll handle cleanup manually
	}

	// Create container
	containerName := fmt.Sprintf("%s_%s", benchmarkID, name)
	resp, err := m.client.ContainerCreate(ctx, config, hostConfig, nil, nil, containerName)
	if err != nil {
		return fmt.Errorf("failed to create container: %w", err)
	}

	m.containers[name] = resp.ID
	
	log.WithFields(log.Fields{
		"name":         name,
		"container_id": resp.ID[:12],
		"image":        cfg.Image,
		"core":         cfg.Core,
	}).Info("Container created successfully")

	return nil
}

// StartContainer starts a specific container
func (m *Manager) StartContainer(ctx context.Context, name string) error {
	containerID, exists := m.containers[name]
	if !exists {
		return fmt.Errorf("container %s not found", name)
	}

	if err := m.client.ContainerStart(ctx, containerID, types.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("failed to start container %s: %w", name, err)
	}

	log.WithFields(log.Fields{
		"name":         name,
		"container_id": containerID[:12],
	}).Info("Container started successfully")

	return nil
}

// StopContainer stops a specific container
func (m *Manager) StopContainer(ctx context.Context, name string) error {
	containerID, exists := m.containers[name]
	if !exists {
		return fmt.Errorf("container %s not found", name)
	}

	timeout := 10 // 10 seconds timeout
	if err := m.client.ContainerStop(ctx, containerID, container.StopOptions{
		Timeout: &timeout,
	}); err != nil {
		return fmt.Errorf("failed to stop container %s: %w", name, err)
	}

	log.WithFields(log.Fields{
		"name":         name,
		"container_id": containerID[:12],
	}).Info("Container stopped successfully")

	return nil
}

// StopAndCleanup stops and removes all containers
func (m *Manager) StopAndCleanup(ctx context.Context) error {
	log.Info("Stopping and cleaning up containers")

	// Use a fresh context for cleanup to avoid cancellation issues
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var errors []error

	for name, containerID := range m.containers {
		// First, try to stop container gracefully
		timeout := 10
		stopErr := m.client.ContainerStop(cleanupCtx, containerID, container.StopOptions{
			Timeout: &timeout,
		})
		
		// If graceful stop failed, we'll still try to force remove
		if stopErr != nil && !isContainerNotFoundError(stopErr) {
			log.WithFields(log.Fields{
				"name":         name,
				"container_id": containerID[:12],
				"error":        stopErr,
			}).Warn("Failed to stop container gracefully, will try force removal")
		}

		// Remove container (force removal)
		removeErr := m.client.ContainerRemove(cleanupCtx, containerID, types.ContainerRemoveOptions{
			RemoveVolumes: true,
			Force:         true, // Force removal even if container is running
		})

		if removeErr != nil {
			if isContainerNotFoundError(removeErr) {
				log.WithField("name", name).Info("Container already removed")
			} else {
				errors = append(errors, fmt.Errorf("failed to remove container %s: %w", name, removeErr))
			}
		} else {
			log.WithField("name", name).Info("Container cleaned up successfully")
		}
	}

	// Clear the container map
	m.containers = make(map[string]string)

	if len(errors) > 0 {
		// Log all errors but don't fail the cleanup completely
		for _, err := range errors {
			log.WithError(err).Warn("Container cleanup error")
		}
		return fmt.Errorf("encountered %d errors during cleanup", len(errors))
	}

	return nil
}

// isContainerNotFoundError checks if the error indicates the container was not found
func isContainerNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "No such container") || 
		   strings.Contains(err.Error(), "not found")
}

// GetContainerIDs returns a map of container names to IDs
func (m *Manager) GetContainerIDs() map[string]string {
	result := make(map[string]string)
	for name, id := range m.containers {
		result[name] = id
	}
	return result
}

// GetContainerID returns the Docker ID for a specific container name
func (m *Manager) GetContainerID(name string) string {
	return m.containers[name]
}

// GetContainerStats returns real-time stats for a container
func (m *Manager) GetContainerStats(ctx context.Context, containerID string) (*types.StatsJSON, error) {
	stats, err := m.client.ContainerStats(ctx, containerID, false)
	if err != nil {
		return nil, err
	}
	defer stats.Body.Close()

	var statsJSON types.StatsJSON
	if err := json.NewDecoder(stats.Body).Decode(&statsJSON); err != nil {
		return nil, err
	}

	return &statsJSON, nil
}

// IsContainerRunning checks if a container is currently running
func (m *Manager) IsContainerRunning(ctx context.Context, name string) (bool, error) {
	containerID, exists := m.containers[name]
	if !exists {
		return false, fmt.Errorf("container %s not found", name)
	}

	inspect, err := m.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return false, err
	}

	return inspect.State.Running, nil
}

// ExecuteCommand executes a command in a running container
func (m *Manager) ExecuteCommand(ctx context.Context, containerName, command string) error {
	containerID, exists := m.containers[containerName]
	if !exists {
		return fmt.Errorf("container %s not found", containerName)
	}

	// Wait a moment for container to fully start and check if it's running
	maxAttempts := 5
	for attempt := 0; attempt < maxAttempts; attempt++ {
		inspect, err := m.client.ContainerInspect(ctx, containerID)
		if err != nil {
			return fmt.Errorf("failed to inspect container %s: %w", containerName, err)
		}

		if inspect.State.Running {
			break
		}
		
		if attempt < maxAttempts-1 {
			log.WithFields(log.Fields{
				"container": containerName,
				"attempt":   attempt + 1,
			}).Debug("Container not running yet, waiting...")
			time.Sleep(500 * time.Millisecond)
		} else {
			return fmt.Errorf("container %s is not running after %d attempts", containerName, maxAttempts)
		}
	}

	// Create exec instance
	execConfig := types.ExecConfig{
		Cmd:          []string{"sh", "-c", command},
		AttachStdout: true,
		AttachStderr: true,
		Detach:       true, // Run in background
	}

	exec, err := m.client.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return fmt.Errorf("failed to create exec for container %s: %w", containerName, err)
	}

	// Start the exec
	if err := m.client.ContainerExecStart(ctx, exec.ID, types.ExecStartCheck{Detach: true}); err != nil {
		return fmt.Errorf("failed to start exec in container %s: %w", containerName, err)
	}

	log.WithFields(log.Fields{
		"container_name": containerName,
		"container_id":   containerID[:12],
		"command":        command,
		"exec_id":        exec.ID[:12],
	}).Info("Command executed in container")

	return nil
}

// Close closes the Docker client connection
func (m *Manager) Close() error {
	if m.client != nil {
		return m.client.Close()
	}
	return nil
}
