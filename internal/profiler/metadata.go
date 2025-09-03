package profiler

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// ContainerMetadata holds all the metadata for a specific container
type ContainerMetadata struct {
	Name        string    `json:"name"`
	ID          string    `json:"id"`
	ShortID     string    `json:"short_id"`
	PID         int       `json:"pid"`
	Cgroup      string    `json:"cgroup"`
	Index       string    `json:"index"`
	LastUpdated time.Time `json:"last_updated"`
}

// ProfilerConfig holds configuration that is static throughout the benchmark
type ProfilerConfig struct {
	BenchmarkID      string                           `json:"benchmark_id"`
	SamplingRate     int                              `json:"sampling_rate_ms"`
	ContainerMetadata map[string]*ContainerMetadata   `json:"containers"`
	EnabledProfilers  map[string]bool                 `json:"enabled_profilers"`
}

// MetadataProvider manages container metadata and provides it to profilers
type MetadataProvider struct {
	config           *ProfilerConfig
	containerIDs     map[string]string // name -> container ID mapping  
	mutex            sync.RWMutex
	refreshInterval  time.Duration
	stopChan         chan struct{}
	wg               sync.WaitGroup
	initialized      bool
}

// NewMetadataProvider creates a new metadata provider
func NewMetadataProvider(benchmarkID string, samplingRate int) *MetadataProvider {
	return &MetadataProvider{
		config: &ProfilerConfig{
			BenchmarkID:       benchmarkID,
			SamplingRate:      samplingRate,
			ContainerMetadata: make(map[string]*ContainerMetadata),
			EnabledProfilers:  make(map[string]bool),
		},
		containerIDs:    make(map[string]string),
		refreshInterval: 2 * time.Second, // Refresh every 2s to catch containers starting quickly
		stopChan:        make(chan struct{}),
	}
}

// SetContainerIDs sets the container IDs and initializes metadata
func (mp *MetadataProvider) SetContainerIDs(containerIDs map[string]string) error {
	mp.mutex.Lock()
	defer mp.mutex.Unlock()

	mp.containerIDs = make(map[string]string)
	for name, id := range containerIDs {
		mp.containerIDs[name] = id
	}

	// Initialize metadata for all containers
	return mp.refreshContainerMetadata()
}

// SetEnabledProfilers sets which profilers are enabled
func (mp *MetadataProvider) SetEnabledProfilers(profilers map[string]bool) {
	mp.mutex.Lock()
	defer mp.mutex.Unlock()

	for name, enabled := range profilers {
		mp.config.EnabledProfilers[name] = enabled
	}
}

// Initialize starts the metadata provider and begins monitoring
func (mp *MetadataProvider) Initialize(ctx context.Context) error {
	mp.mutex.Lock()
	defer mp.mutex.Unlock()

	if mp.initialized {
		return nil
	}

	// Start background refresh goroutine
	mp.wg.Add(1)
	go mp.backgroundRefresh(ctx)

	mp.initialized = true
	log.WithFields(log.Fields{
		"refresh_interval": mp.refreshInterval,
		"containers":       len(mp.containerIDs),
	}).Info("Container metadata provider initialized")

	return nil
}

// GetConfig returns the current profiler configuration
func (mp *MetadataProvider) GetConfig() *ProfilerConfig {
	mp.mutex.RLock()
	defer mp.mutex.RUnlock()

	// Create a deep copy to avoid race conditions
	configCopy := &ProfilerConfig{
		BenchmarkID:       mp.config.BenchmarkID,
		SamplingRate:      mp.config.SamplingRate,
		ContainerMetadata: make(map[string]*ContainerMetadata),
		EnabledProfilers:  make(map[string]bool),
	}

	for name, metadata := range mp.config.ContainerMetadata {
		metadataCopy := *metadata
		configCopy.ContainerMetadata[name] = &metadataCopy
	}

	for name, enabled := range mp.config.EnabledProfilers {
		configCopy.EnabledProfilers[name] = enabled
	}

	return configCopy
}

// GetContainerMetadata returns metadata for a specific container
func (mp *MetadataProvider) GetContainerMetadata(containerName string) (*ContainerMetadata, error) {
	mp.mutex.RLock()
	defer mp.mutex.RUnlock()

	metadata, exists := mp.config.ContainerMetadata[containerName]
	if !exists {
		return nil, fmt.Errorf("container metadata not found for: %s", containerName)
	}

	// Return a copy to avoid external modifications
	metadataCopy := *metadata
	return &metadataCopy, nil
}

// GetAllContainerNames returns all container names
func (mp *MetadataProvider) GetAllContainerNames() []string {
	mp.mutex.RLock()
	defer mp.mutex.RUnlock()

	names := make([]string, 0, len(mp.config.ContainerMetadata))
	for name := range mp.config.ContainerMetadata {
		names = append(names, name)
	}
	return names
}

// RefreshContainerMetadata forces a refresh of container metadata (useful when containers are started)
func (mp *MetadataProvider) RefreshContainerMetadata() error {
	mp.mutex.Lock()
	defer mp.mutex.Unlock()
	return mp.refreshContainerMetadata()
}

// backgroundRefresh periodically refreshes container metadata
func (mp *MetadataProvider) backgroundRefresh(ctx context.Context) {
	defer mp.wg.Done()

	ticker := time.NewTicker(mp.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Debug("Metadata provider stopped due to context cancellation")
			return
		case <-mp.stopChan:
			log.Debug("Metadata provider stopped")
			return
		case <-ticker.C:
			mp.mutex.Lock()
			if err := mp.refreshContainerMetadata(); err != nil {
				log.WithError(err).Warn("Failed to refresh container metadata")
			}
			mp.mutex.Unlock()
		}
	}
}

// refreshContainerMetadata updates the metadata for all containers
func (mp *MetadataProvider) refreshContainerMetadata() error {
	now := time.Now()
	errors := make([]string, 0)

	for containerName, containerID := range mp.containerIDs {
		if containerID == "" {
			continue
		}

		// Get container PID
		pid, err := mp.getContainerPID(containerID)
		if err != nil {
			// If container is not running yet, create placeholder metadata
			mp.config.ContainerMetadata[containerName] = &ContainerMetadata{
				Name:        containerName,
				ID:          containerID,
				ShortID:     containerID[:12],
				PID:         0, // Will be updated when container starts
				Cgroup:      "", // Will be updated when container starts  
				Index:       mp.extractContainerIndex(containerName),
				LastUpdated: now,
			}
			log.WithFields(log.Fields{
				"container": containerName,
				"error":     err.Error(),
			}).Debug("Container not running yet, created placeholder metadata")
			continue
		}

		// Get container cgroup
		cgroup, err := mp.getContainerCgroup(pid)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to get cgroup for %s: %v", containerName, err))
			continue
		}

		// Update metadata
		mp.config.ContainerMetadata[containerName] = &ContainerMetadata{
			Name:        containerName,
			ID:          containerID,
			ShortID:     containerID[:12],
			PID:         pid,
			Cgroup:      cgroup,
			Index:       mp.extractContainerIndex(containerName),
			LastUpdated: now,
		}

		log.WithFields(log.Fields{
			"container": containerName,
			"pid":       pid,
			"cgroup":    cgroup,
		}).Debug("Updated container metadata")
	}

	if len(errors) > 0 {
		return fmt.Errorf("metadata refresh errors: %s", strings.Join(errors, "; "))
	}

	log.WithField("containers", len(mp.config.ContainerMetadata)).Debug("Container metadata refreshed successfully")
	return nil
}

// extractContainerIndex extracts the index from a container name (e.g., "container0" -> "0")
func (mp *MetadataProvider) extractContainerIndex(containerName string) string {
	if len(containerName) > 9 && containerName[:9] == "container" {
		return containerName[9:]
	}
	return ""
}

// getContainerPID gets the PID of a container
func (mp *MetadataProvider) getContainerPID(containerID string) (int, error) {
	cmd := exec.Command("docker", "inspect", "-f", "{{.State.Pid}}", containerID)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("failed to get container PID: %w (output: %s)", err, string(output))
	}

	pidStr := strings.TrimSpace(string(output))
	if pidStr == "0" {
		return 0, fmt.Errorf("container is not running (PID is 0)")
	}

	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse PID: %w", err)
	}

	return pid, nil
}

// getContainerCgroup gets the cgroup path for a container process
func (mp *MetadataProvider) getContainerCgroup(pid int) (string, error) {
	cmd := exec.Command("awk", "-F:", "$1==\"0\"{print $3}", fmt.Sprintf("/proc/%d/cgroup", pid))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to get cgroup: %w (output: %s)", err, string(output))
	}

	cgroup := strings.TrimSpace(string(output))
	if cgroup == "" {
		return "", fmt.Errorf("empty cgroup path for PID %d", pid)
	}

	return cgroup, nil
}

// Stop stops the metadata provider
func (mp *MetadataProvider) Stop() error {
	mp.mutex.Lock()
	defer mp.mutex.Unlock()

	if !mp.initialized {
		return nil
	}

	close(mp.stopChan)
	mp.wg.Wait()
	mp.initialized = false

	log.Info("Container metadata provider stopped")
	return nil
}
