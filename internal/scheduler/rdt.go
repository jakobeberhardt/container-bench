package scheduler

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/jakobeberhardt/container-bench/internal/config"
	log "github.com/sirupsen/logrus"
)

// RDTScheduler implements Intel RDT-based scheduling strategies
type RDTScheduler struct {
	resourceGroups map[string]string // containerID -> resource group mapping
}

// NewRDTScheduler creates a new RDT scheduler
func NewRDTScheduler() *RDTScheduler {
	return &RDTScheduler{
		resourceGroups: make(map[string]string),
	}
}

// Initialize initializes the RDT scheduler
func (rs *RDTScheduler) Initialize(ctx context.Context) error {
	log.Info("Initializing RDT scheduler")
	
	// Check if RDT is available
	if err := rs.checkRDTAvailability(); err != nil {
		return fmt.Errorf("RDT not available: %w", err)
	}

	// Initialize RDT resource groups
	if err := rs.initializeResourceGroups(); err != nil {
		return fmt.Errorf("failed to initialize RDT resource groups: %w", err)
	}

	log.Info("RDT scheduler initialized successfully")
	return nil
}

// checkRDTAvailability checks if Intel RDT is available
func (rs *RDTScheduler) checkRDTAvailability() error {
	// Check if rdtset command is available
	if _, err := exec.LookPath("rdtset"); err != nil {
		return fmt.Errorf("rdtset command not found: %w", err)
	}

	// Check if resctrl filesystem is mounted
	cmd := exec.Command("mountpoint", "-q", "/sys/fs/resctrl")
	if err := cmd.Run(); err != nil {
		log.Warn("resctrl filesystem not mounted, attempting to mount")
		cmd = exec.Command("sudo", "mount", "-t", "resctrl", "resctrl", "/sys/fs/resctrl")
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to mount resctrl filesystem: %w", err)
		}
	}

	return nil
}

// initializeResourceGroups creates RDT resource groups for isolation
func (rs *RDTScheduler) initializeResourceGroups() error {
	// Create resource groups for different container classes
	groups := []string{"high_priority", "medium_priority", "low_priority"}
	
	for _, group := range groups {
		groupPath := fmt.Sprintf("/sys/fs/resctrl/%s", group)
		
		// Create directory for resource group
		cmd := exec.Command("sudo", "mkdir", "-p", groupPath)
		if err := cmd.Run(); err != nil {
			log.WithError(err).WithField("group", group).Warn("Failed to create resource group directory")
			continue
		}

		// Set cache allocation for the group (example: different cache ways)
		var schemata string
		switch group {
		case "high_priority":
			schemata = "L3:0=0xfff;1=0xfff" // Full cache access
		case "medium_priority":
			schemata = "L3:0=0x0ff;1=0x0ff" // Half cache access
		case "low_priority":
			schemata = "L3:0=0x00f;1=0x00f" // Quarter cache access
		}

		cmd = exec.Command("sudo", "sh", "-c", fmt.Sprintf("echo '%s' > %s/schemata", schemata, groupPath))
		if err := cmd.Run(); err != nil {
			log.WithError(err).WithField("group", group).Warn("Failed to set cache schemata")
		}

		log.WithField("group", group).Info("RDT resource group initialized")
	}

	return nil
}

// ScheduleContainer applies RDT policies to a container
func (rs *RDTScheduler) ScheduleContainer(ctx context.Context, containerID string, cfg config.ContainerConfig) error {
	log.WithFields(log.Fields{
		"container_id": containerID[:12],
		"index":        cfg.Index,
	}).Info("Applying RDT scheduling policy")

	// Determine resource group based on container index/priority
	var resourceGroup string
	switch {
	case cfg.Index == 0:
		resourceGroup = "high_priority"
	case cfg.Index <= 2:
		resourceGroup = "medium_priority"
	default:
		resourceGroup = "low_priority"
	}

	// Get container PID
	cmd := exec.Command("docker", "inspect", "-f", "{{.State.Pid}}", containerID)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to get container PID: %w", err)
	}

	pidStr := strings.TrimSpace(string(output))
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		return fmt.Errorf("invalid PID: %w", err)
	}

	// Assign container to resource group
	if err := rs.assignToResourceGroup(pid, resourceGroup); err != nil {
		return fmt.Errorf("failed to assign container to resource group: %w", err)
	}

	rs.resourceGroups[containerID] = resourceGroup

	log.WithFields(log.Fields{
		"container_id":    containerID[:12],
		"pid":             pid,
		"resource_group":  resourceGroup,
	}).Info("Container assigned to RDT resource group")

	return nil
}

// assignToResourceGroup assigns a PID to an RDT resource group
func (rs *RDTScheduler) assignToResourceGroup(pid int, group string) error {
	tasksFile := fmt.Sprintf("/sys/fs/resctrl/%s/tasks", group)
	
	cmd := exec.Command("sudo", "sh", "-c", fmt.Sprintf("echo %d >> %s", pid, tasksFile))
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to assign PID %d to resource group %s: %w", pid, group, err)
	}

	return nil
}

// Monitor monitors containers and adjusts RDT policies dynamically
func (rs *RDTScheduler) Monitor(ctx context.Context, containerIDs map[string]string) error {
	log.Info("Starting RDT scheduler monitoring")

	// This is where you would implement dynamic RDT policy adjustments
	// based on real-time performance metrics

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("RDT scheduler monitoring stopped")
			return ctx.Err()
		case <-ticker.C:
			rs.adjustResourceGroups(ctx, containerIDs)
		}
	}
}

// adjustResourceGroups dynamically adjusts RDT resource groups based on performance
func (rs *RDTScheduler) adjustResourceGroups(ctx context.Context, containerIDs map[string]string) {
	// This is a placeholder for dynamic adjustment logic
	// In a real implementation, you would:
	// 1. Collect performance metrics (cache misses, memory bandwidth)
	// 2. Analyze busy neighbor effects
	// 3. Adjust cache allocation and memory bandwidth throttling
	// 4. Move containers between resource groups if needed

	log.Debug("Checking RDT resource group adjustments")
	
	// Example: Check if any containers are experiencing high cache miss rates
	// and move them to different resource groups
	for name, containerID := range containerIDs {
		if group, exists := rs.resourceGroups[containerID]; exists {
			log.WithFields(log.Fields{
				"container":       name,
				"resource_group":  group,
			}).Debug("Container resource group status")
		}
	}
}

// Finalize cleans up RDT scheduler resources
func (rs *RDTScheduler) Finalize(ctx context.Context) error {
	log.Info("Finalizing RDT scheduler")

	// Clean up resource groups
	groups := []string{"high_priority", "medium_priority", "low_priority"}
	
	for _, group := range groups {
		groupPath := fmt.Sprintf("/sys/fs/resctrl/%s", group)
		cmd := exec.Command("sudo", "rmdir", groupPath)
		if err := cmd.Run(); err != nil {
			log.WithError(err).WithField("group", group).Warn("Failed to remove resource group")
		}
	}

	return nil
}

// Name returns the scheduler name
func (rs *RDTScheduler) Name() string {
	return "rdt"
}
