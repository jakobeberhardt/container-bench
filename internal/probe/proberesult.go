package probe

import (
	"time"
)

// ProbeResult contains sensitivity analysis results for a container under probing conditions
type ProbeResult struct {
	BenchmarkID int `json:"benchmark_id"`

	UsedProbeKernel string `json:"used_probe_kernel"`

	// Victim container info
	ContainerID      string `json:"container_id"`
	ContainerName    string `json:"container_name"`
	ContainerIndex   int    `json:"container_index"`
	ContainerCores   string `json:"container_cores"`
	ContainerSocket  int    `json:"container_socket,omitempty"`
	ContainerImage   string `json:"container_image"`
	ContainerCommand string `json:"container_command,omitempty"`

	// Probing container information (the container creating interference)
	ProbingContainerID     string `json:"probing_container_id"`
	ProbingContainerName   string `json:"probing_container_name"`
	ProbingContainerCores  string `json:"probing_container_cores"`
	ProbingContainerSocket int    `json:"probing_container_socket,omitempty"`

	// Probing execution metadata
	ProbeTime time.Duration `json:"probe_time"`
	Isolated  bool          `json:"isolated"`
	Aborted   bool          `json:"aborted"`

	Started   time.Time  `json:"started"`
	Finished  time.Time  `json:"finished"`
	AbortedAt *time.Time `json:"aborted_at,omitempty"`

	// Dataframe references
	FirstDataframeStep int `json:"first_dataframe_step"`
	LastDataframeStep  int `json:"last_dataframe_step"`

	Sensitivities map[string]*SensitivityMetrics `json:"sensitivities,omitempty"`
}

// SensitivityMetrics holds sensitivity measurements for a specific metric type
type SensitivityMetrics struct {
	LLC      *float64 `json:"llc,omitempty"`
	MemRead  *float64 `json:"mem_read,omitempty"`
	MemWrite *float64 `json:"mem_write,omitempty"`
	SysCall  *float64 `json:"syscall,omitempty"`
	Prefetch *float64 `json:"prefetch,omitempty"`
}
