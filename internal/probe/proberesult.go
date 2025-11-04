package probe

import (
	"time"
)

// ProbeResult contains sensitivity analysis results for a container under probing conditions
type ProbeResult struct {
	// Benchmark metadata
	BenchmarkID int `json:"benchmark_id"`

	// Probe kernel used
	UsedProbeKernel string `json:"used_probe_kernel"`

	// Target container information (the container being probed)
	ContainerID     string `json:"container_id"`
	ContainerName   string `json:"container_name"`
	ContainerIndex  int    `json:"container_index"`
	ContainerCores  string `json:"container_cores"`
	ContainerSocket int    `json:"container_socket,omitempty"`
	ContainerImage  string `json:"container_image"`
	ContainerCommand string `json:"container_command,omitempty"`

	// Probing container information (the container creating interference)
	ProbingContainerID     string `json:"probing_container_id"`
	ProbingContainerName   string `json:"probing_container_name"`
	ProbingContainerCores  string `json:"probing_container_cores"`
	ProbingContainerSocket int    `json:"probing_container_socket,omitempty"`

	// Probing execution metadata
	ProbeTime time.Duration `json:"probe_time"` // Duration of the probing
	Isolated  bool          `json:"isolated"`   // Whether the probe ran isolated
	Aborted   bool          `json:"aborted"`    // Whether the probe was aborted

	// Timestamps
	Started  time.Time  `json:"started"`
	Finished time.Time  `json:"finished"`
	AbortedAt *time.Time `json:"aborted_at,omitempty"`

	// Dataframe references (sampling steps used for analysis)
	FirstDataframeStep int `json:"first_dataframe_step"`
	LastDataframeStep  int `json:"last_dataframe_step"`

	// Sensitivity metrics (0.0 = no impact, 1.0 = complete degradation)
	// These are computed by the ProbeKernel implementation
	CPUInteger   *float64 `json:"cpu_integer,omitempty"`    // Sensitivity to integer computation interference
	CPUFloat     *float64 `json:"cpu_float,omitempty"`      // Sensitivity to floating-point computation interference
	LLC          *float64 `json:"llc,omitempty"`            // Sensitivity to last-level cache pressure
	MemRead      *float64 `json:"mem_read,omitempty"`       // Sensitivity to memory read bandwidth contention
	MemWrite     *float64 `json:"mem_write,omitempty"`      // Sensitivity to memory write bandwidth contention
	StoreBuffer  *float64 `json:"store_buffer,omitempty"`   // Sensitivity to store buffer contention
	Scoreboard   *float64 `json:"scoreboard,omitempty"`     // Sensitivity to instruction scoreboard stalls
	NetworkRead  *float64 `json:"network_read,omitempty"`   // Sensitivity to network read contention
	NetworkWrite *float64 `json:"network_write,omitempty"`  // Sensitivity to network write contention
	SysCall      *float64 `json:"syscall,omitempty"`        // Sensitivity to system call overhead
}
