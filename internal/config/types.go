package config

import (
	"fmt"
	"os"
	"time"
)

type BenchmarkConfig struct {
	Benchmark  BenchmarkInfo              `yaml:"benchmark"`
	Arrival    *ArrivalConfig             `yaml:"arrival,omitempty"`
	Data       *CollectorConfig           `yaml:"data,omitempty"` // shared container data defaults (generated-trace mode)
	Workloads  map[string]WorkloadConfig  `yaml:"workloads,omitempty"`
	Containers map[string]ContainerConfig `yaml:",inline"` // static containers (or generated containers after expansion)
}

// ArrivalConfig configures seed-based trace generation.
// Mean/Sigma control inter-arrival time in seconds.
type ArrivalConfig struct {
	Seed  int64   `yaml:"seed,omitempty"`
	Mean  float64 `yaml:"mean"`
	Sigma float64 `yaml:"sigma"`

	// Length controls job length distribution (seconds).
	// If omitted, defaults are used.
	Length *NormalDistConfig `yaml:"length,omitempty"`

	// Split selects which kind of workload arrives.
	// If Split.Random is true, split weights are ignored.
	Split WeightedSet `yaml:"split,omitempty"`

	// Sensitivities selects an interference sensitivity bucket for arriving jobs.
	// If Random is true, weights are ignored.
	Sensitivities WeightedSet `yaml:"sensitivities,omitempty"`
	Sensetivities WeightedSet `yaml:"sensetivities,omitempty"` // backward/typo compatibility
}

type NormalDistConfig struct {
	Mean  float64 `yaml:"mean"`
	Sigma float64 `yaml:"sigma"`
	Min   float64 `yaml:"min,omitempty"`
	Max   float64 `yaml:"max,omitempty"`
}

// WeightedSet supports a YAML mapping like:
//
//	split:
//	  random: false
//	  single-thread: 80
//	  multi-thread: 10
//
// The non-"random" keys are treated as weights.
type WeightedSet struct {
	Random  bool               `yaml:"random,omitempty"`
	Weights map[string]float64 `yaml:",inline"`
}

// WorkloadConfig describes an entry in the workload pool for generated traces.
// The command must run indefinitely (the orchestrator stops it at stop_t).
type WorkloadConfig struct {
	Image    string `yaml:"image"`
	Command  string `yaml:"command"`
	NumCores int    `yaml:"num_cores,omitempty"`

	// Semantic scheduling hints (optional): copied into generated ContainerConfig entries.
	Critical       bool     `yaml:"critical,omitempty"`
	IPC            *float64 `yaml:"ipc,omitempty"`
	IPCEfficiency  *float64 `yaml:"ipce,omitempty"` // optimal IPC efficiency (0..1) target/label

	// Used only for generation-time selection (never passed to scheduler as semantic labels).
	Kind        string `yaml:"kind,omitempty"`        // single-thread|multi-thread|multi-programmed|iobound
	Sensitivity string `yaml:"sensitivity,omitempty"` // low|medium|high
}

type BenchmarkInfo struct {
	Name            string          `yaml:"name"`
	Description     string          `yaml:"description"`
	Drain           bool            `yaml:"drain,omitempty"`
	MaxT            int             `yaml:"max_t"`
	PIDSyncInterval int             `yaml:"pid_sync_interval,omitempty"` // Interval in ms for syncing PIDs to RDT groups (default: 100ms)
	LogLevel        string          `yaml:"log_level"`
	Accounting      *LoggingConfig  `yaml:"accounting,omitempty"`
	Scheduler       SchedulerConfig `yaml:"scheduler"`
	Data            DataConfig      `yaml:"data"`
	Docker          *DockerConfig   `yaml:"docker,omitempty"`
}

type LoggingConfig struct {
	LogLevel string `yaml:"log_level,omitempty"`
}

type DockerConfig struct {
	Registry *RegistryConfig `yaml:"registry,omitempty"`
}

type RegistryConfig struct {
	Host     string `yaml:"host"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type SchedulerConfig struct {
	Implementation string `yaml:"implementation"`
	RDT            bool   `yaml:"rdt"`
	LogLevel       string `yaml:"log_level,omitempty"`

	// dynamic-v3: when admitting priority containers, allow best-effort force-moves (CPU moves)
	// of non-priority containers to free cores on the preferred socket.
	// - nil (omitted): enabled (default)
	// - false: disabled
	// - true: enabled
	ForceMoveForPriorityAdmission *bool `yaml:"force_move_for_priority_admission,omitempty"`

	// Backward-compat alias (deprecated): prefer force_move_for_priority_admission.
	EvictForPriorityAdmission *bool `yaml:"evict_for_priority_admission,omitempty"`

	// Socket rebalancing (used by implementation: "dynamic")
	// - rebalance: enables/disables the feature (default: disabled)
	// - rebalance_batch: when rebalancing is enabled, controls whether we move multiple
	//   containers per tick (true) or at most one (false).
	//
	// Backward-compatibility: if rebalance is omitted, rebalance_batch=true enables
	// rebalancing in batch mode; rebalance_batch=false keeps it disabled.
	Rebalance      *bool `yaml:"rebalance,omitempty"`
	RebalanceBatch *bool `yaml:"rebalance_batch,omitempty"`

	// Interference-aware scheduler (implementation: "interference_aware")
	MaxL3                      int      `yaml:"max_l3,omitempty"`                        // max exclusive cache ways to probe/allocate
	MaxMem                     float64  `yaml:"max_mem,omitempty"`                       // max memory bandwidth percentage to probe/allocate
	ProbingT                   float64  `yaml:"probing_t,omitempty"`                     // total probing time budget (seconds)
	BreakCondition             float64  `yaml:"break_condition,omitempty"`               // stop probing when avg IPCE (Perf.IPCEfficancy) >= this
	BreakCPULoad               *float64 `yaml:"break_cpu_load,omitempty"`                // stop probing when avg Docker CPU usage percent <= this (-1 or nil disables)
	BreakLLCOccupancy          *float64 `yaml:"break_llc_occupancy,omitempty"`           // stop probing when avg LLC utilization percent <= this (-1 or nil disables)
	BreakImprovement           float64  `yaml:"break_improvement,omitempty"`             // stop when relative improvement below this
	GreedyAllocation           bool     `yaml:"greedy_allocation,omitempty"`             // if true, stop probe early on diminishing returns; if false, classify diminishing returns post-mortem
	Reallocate                 bool     `yaml:"reallocate,omitempty"`                    // opportunistic reallocation when jobs finish
	SkipAllocationAfterProbing *bool    `yaml:"skip_allocation_after_probing,omitempty"` // if true, never keep allocations after probing (default: true)
	AllocateUnbound            bool     `yaml:"allocate_unbound,omitempty"`              // keep allocation for unbound containers even if skip_allocation_after_probing is true
	WarmupT                    int      `yaml:"warmup_t,omitempty"`                      // seconds after container start before it can be probed (default: 5)
	CooldownT                  int      `yaml:"cooldown_t,omitempty"`                    // minimum seconds between probes (default: 2)

	Prober    *ProberConfig    `yaml:"prober,omitempty"`
	Allocator *AllocatorConfig `yaml:"allocator,omitempty"`
}

type AllocatorConfig struct {
	Implementation    string   `yaml:"implementation"`
	WarmupT           int      `yaml:"warmup_t,omitempty"`
	Duration          int      `yaml:"duration,omitempty"`
	Target            float64  `yaml:"target,omitempty"`
	StepSizeL3        int      `yaml:"step_size_l3,omitempty"`
	StepSizeMB        int      `yaml:"step_size_mb,omitempty"`
	MinL3Ways         int      `yaml:"min_l3_ways,omitempty"`
	MaxL3Ways         int      `yaml:"max_l3_ways,omitempty"`
	MinMemBandwidth   float64  `yaml:"min_mem_bandwidth,omitempty"`
	MaxMemBandwidth   float64  `yaml:"max_mem_bandwidth,omitempty"`
	ForceReallocation bool     `yaml:"force_reallocation,omitempty"`
	IsolateOthers     bool     `yaml:"isolate_others,omitempty"`
	Order             string   `yaml:"order,omitempty"`
	BreakOnEfficiency *float64 `yaml:"break_on_efficiency,omitempty"` // Stop probe when IPC efficiency exceeds this (nil = never break)
}

type ProberConfig struct {
	// Log level for the prober subsystem (allocation probing + optional kernel probing)
	LogLevel string `yaml:"log_level,omitempty"`

	// Allocate controls whether the prober actually allocates RDT resources during probing.
	// If true (default): full allocation probing is performed, taking/releasing L3 cache ways
	// and memory bandwidth, which may impact other containers on the socket.
	// If false: lightweight sampling-only probing using StallsL3MissPercent to estimate
	// interference potential without allocating any RDT resources.
	Allocate *bool `yaml:"allocate,omitempty"`

	// GreedyAllocation controls whether probing stops early on diminishing returns.
	// If true: stop probe early when improvement is below threshold.
	// If false: run full probe and classify diminishing returns post-mortem.
	GreedyAllocation bool `yaml:"greedy_allocation,omitempty"`

	// Allocation prober parameters (used by schedulers doing allocation probing).
	MinL3Ways        int     `yaml:"min_l3,omitempty"`
	MaxL3Ways        int     `yaml:"max_l3,omitempty"`
	StepL3Ways       int     `yaml:"step_l3,omitempty"`
	MinMemBandwidth  float64 `yaml:"min_mem,omitempty"`
	MaxMemBandwidth  float64 `yaml:"max_mem,omitempty"`
	StepMemBandwidth float64 `yaml:"mem_step,omitempty"`
	Order            string  `yaml:"order,omitempty"` // "asc" (start smallest) or "desc" (start largest)
	ProbingT         float64 `yaml:"probing_t,omitempty"`

	// BufferWays / BufferMemory add headroom beyond a guarantee-satisfying allocation.
	// If > 0 and a probe finds an allocation meeting the IPCE guarantee, the scheduler will
	// attempt to commit an allocation with extra resources (best-effort).
	// BufferWays is additional L3 ways.
	// BufferMemory is additional memory steps (multiples of mem_step / MBA granularity).
	BufferWays   int `yaml:"buffer_ways,omitempty"`
	BufferMemory int `yaml:"buffer_memory,omitempty"`

	// ProbingFrequency temporarily overrides the container collector sampling frequency (ms)
	// while the allocation prober is running for a container. If 0: disabled.
	ProbingFrequency int `yaml:"probing_frequency,omitempty"`

	// DropOutliers controls outlier trimming for allocation probing metrics.
	// If > 0: sort samples and drop N lowest and N highest values (keep the inner values).
	// If <= 0: keep all values (-1 disables trimming; 0 means keep all).
	DropOutliers int `yaml:"drop_outliers,omitempty"`

	// Kernel prober configuration (legacy/optional; used by ProbeScheduler and similar).
	Implementation    string `yaml:"implementation"`
	Abortable         bool   `yaml:"abortable"`
	Isolated          bool   `yaml:"isolated"`
	DefaultT          int    `yaml:"default_t,omitempty"`
	WarmupT           int    `yaml:"warmup_t,omitempty"`
	CooldownT         int    `yaml:"cooldown_t,omitempty"`
	DefaultProbeCores string `yaml:"default_probe_cores,omitempty"`
	ProbeImage        string `yaml:"probe_image,omitempty"`
}

type DataConfig struct {
	DB DatabaseConfig `yaml:"db"`
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Name     string `yaml:"name"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Org      string `yaml:"org"`
}

type ContainerConfig struct {
	Index       int               `yaml:"index"`
	Name        string            `yaml:"name,omitempty"`
	KeyName     string            `yaml:"-"`
	Image       string            `yaml:"image"`
	Port        string            `yaml:"port,omitempty"`
	Core        string            `yaml:"core,omitempty"`
	NumCores    int               `yaml:"num_cores,omitempty"`
	CPUCores    []int             `yaml:"-"`
	StartT      *int              `yaml:"start_t,omitempty"`
	StopT       *int              `yaml:"stop_t,omitempty"`
	ExpectedT   *int              `yaml:"expected_t,omitempty"`
	Command     string            `yaml:"command,omitempty"`

	// Semantic scheduling hints (optional).
	Critical      bool     `yaml:"critical,omitempty"`
	Priority      bool     `yaml:"priority,omitempty"`
	IPC           *float64 `yaml:"ipc,omitempty"`
	IPCEfficiency *float64 `yaml:"ipce,omitempty"` // optimal IPC efficiency (0..1) target/label

	Privileged  bool              `yaml:"privileged,omitempty"`
	Environment map[string]string `yaml:"environment,omitempty"`
	Volumes     []string          `yaml:"volumes,omitempty"`
	Data        CollectorConfig   `yaml:"data"`
}

func (c *ContainerConfig) GetTargetIPC() (float64, bool) {
	if c == nil || c.IPC == nil {
		return 0, false
	}
	return *c.IPC, true
}

func (c *ContainerConfig) GetIPCEfficancy() (float64, bool) {
	if c == nil || c.IPCEfficiency == nil {
		return 0, false
	}
	return *c.IPCEfficiency, true
}

func (c *ContainerConfig) GetRequestedNumCores() int {
	if c.NumCores <= 0 {
		return 1
	}
	return c.NumCores
}

func (c *ContainerConfig) GetStartSeconds() int {
	if c.StartT == nil {
		return 0
	}
	return *c.StartT
}

func (c *ContainerConfig) GetStopSeconds(maxT int) int {
	if c.StopT == nil {
		return maxT
	}
	return *c.StopT
}

func (c *ContainerConfig) GetExpectedSeconds() (int, bool) {
	if c.ExpectedT == nil {
		return 0, false
	}
	return *c.ExpectedT, true
}

type CollectorConfig struct {
	Frequency int         `yaml:"frequency"`
	Perf      interface{} `yaml:"perf"`
	Docker    interface{} `yaml:"docker"`
	RDT       interface{} `yaml:"rdt"`
}

type PerfConfig struct {
	Enabled                  bool `yaml:"-"`
	CacheMisses              bool `yaml:"cache_misses,omitempty"`
	CacheReferences          bool `yaml:"cache_references,omitempty"`
	Instructions             bool `yaml:"instructions,omitempty"`
	Cycles                   bool `yaml:"cycles,omitempty"`
	BranchInstructions       bool `yaml:"branch_instructions,omitempty"`
	BranchMisses             bool `yaml:"branch_misses,omitempty"`
	BusCycles                bool `yaml:"bus_cycles,omitempty"`
	StallsTotal              bool `yaml:"stalls_total,omitempty"`
	StallsL3Miss             bool `yaml:"stalls_l3_miss,omitempty"`
	StallsL2Miss             bool `yaml:"stalls_l2_miss,omitempty"`
	StallsL1dMiss            bool `yaml:"stalls_l1d_miss,omitempty"`
	StallsMemAny             bool `yaml:"stalls_mem_any,omitempty"`
	ResourceStallsSB         bool `yaml:"resource_stalls_sb,omitempty"`
	ResourceStallsScoreboard bool `yaml:"resource_stalls_scoreboard,omitempty"`
	L1DCacheLoadMisses       bool `yaml:"l1d_cache_load_misses,omitempty"`
	L1DCacheLoads            bool `yaml:"l1d_cache_loads,omitempty"`
	L1DCacheStores           bool `yaml:"l1d_cache_stores,omitempty"`
	L1ICacheLoadMisses       bool `yaml:"l1i_cache_load_misses,omitempty"`
	LLCLoadMisses            bool `yaml:"llc_load_misses,omitempty"`
	LLCLoads                 bool `yaml:"llc_loads,omitempty"`
	LLCStoreMisses           bool `yaml:"llc_store_misses,omitempty"`
	LLCStores                bool `yaml:"llc_stores,omitempty"`
	CacheMissRate            bool `yaml:"cache_miss_rate,omitempty"`
	InstructionsPerCycle     bool `yaml:"instructions_per_cycle,omitempty"`
	StalledCyclesPercent     bool `yaml:"stalled_cycles_percent,omitempty"`
}

type DockerMetricsConfig struct {
	Enabled            bool `yaml:"-"`
	AssignedCores      bool `yaml:"assigned_cores,omitempty"`
	CPUUsageTotal      bool `yaml:"cpu_usage_total,omitempty"`
	CPUUsageKernel     bool `yaml:"cpu_usage_kernel,omitempty"`
	CPUUsageUser       bool `yaml:"cpu_usage_user,omitempty"`
	CPUUsagePercent    bool `yaml:"cpu_usage_percent,omitempty"`
	CPUThrottling      bool `yaml:"cpu_throttling,omitempty"`
	MemoryUsage        bool `yaml:"memory_usage,omitempty"`
	MemoryLimit        bool `yaml:"memory_limit,omitempty"`
	MemoryCache        bool `yaml:"memory_cache,omitempty"`
	MemoryRSS          bool `yaml:"memory_rss,omitempty"`
	MemorySwap         bool `yaml:"memory_swap,omitempty"`
	MemoryUsagePercent bool `yaml:"memory_usage_percent,omitempty"`
	NetworkRxBytes     bool `yaml:"network_rx_bytes,omitempty"`
	NetworkTxBytes     bool `yaml:"network_tx_bytes,omitempty"`
	DiskReadBytes      bool `yaml:"disk_read_bytes,omitempty"`
	DiskWriteBytes     bool `yaml:"disk_write_bytes,omitempty"`
}

type RDTConfig struct {
	Enabled                     bool `yaml:"-"`
	L3CacheOccupancy            bool `yaml:"l3_cache_occupancy,omitempty"`
	MemoryBandwidthTotal        bool `yaml:"memory_bandwidth_total,omitempty"`
	MemoryBandwidthLocal        bool `yaml:"memory_bandwidth_local,omitempty"`
	RDTClassName                bool `yaml:"rdt_class_name,omitempty"`
	MonGroupName                bool `yaml:"mon_group_name,omitempty"`
	L3CacheAllocation           bool `yaml:"l3_cache_allocation,omitempty"`
	L3CacheAllocationPct        bool `yaml:"l3_cache_allocation_pct,omitempty"`
	MBAThrottle                 bool `yaml:"mba_throttle,omitempty"`
	CacheLLCUtilizationPercent  bool `yaml:"cache_llc_utilization_percent,omitempty"`
	BandwidthUtilizationPercent bool `yaml:"bandwidth_utilization_percent,omitempty"`
}

func (c *BenchmarkConfig) GetMaxDuration() time.Duration {
	return time.Duration(c.Benchmark.MaxT) * time.Second
}

func (c *BenchmarkConfig) GetRegistryConfig() *RegistryConfig {
	// First check if registry is configured in the YAML file
	if c.Benchmark.Docker != nil && c.Benchmark.Docker.Registry != nil {
		return c.Benchmark.Docker.Registry
	}

	// Otherwise, try to get from environment variables (legacy support)
	host := os.Getenv("DOCKER_REGISTRY_HOST")
	username := os.Getenv("DOCKER_REGISTRY_USERNAME")
	password := os.Getenv("DOCKER_REGISTRY_PASSWORD")

	if host != "" && username != "" && password != "" {
		return &RegistryConfig{
			Host:     host,
			Username: username,
			Password: password,
		}
	}

	return nil
}

func (c *BenchmarkConfig) GetContainersSorted() []ContainerConfig {
	var containers []ContainerConfig
	for _, container := range c.Containers {
		containers = append(containers, container)
	}

	// Sort by index
	for i := 0; i < len(containers)-1; i++ {
		for j := i + 1; j < len(containers); j++ {
			if containers[i].Index > containers[j].Index {
				containers[i], containers[j] = containers[j], containers[i]
			}
		}
	}

	return containers
}

// returns the effective container name for a given container config
func (c *ContainerConfig) GetContainerName(benchmarkID int) string {
	if c.Name != "" {
		// Use explicit name if specified in yml
		return c.Name
	}
	if c.KeyName != "" {
		// Use the YAML key name directly
		return c.KeyName
	}
	// Fallback to generated name if neither is available
	return fmt.Sprintf("bench-%d-container-%d", benchmarkID, c.Index)
}

// GetPerfConfig parses the Perf field and returns a PerfConfig
// If Perf is a bool true, returns config with all metrics enabled
// If Perf is a bool false, returns nil
// If Perf is a map, returns config with only specified metrics enabled
func (cc *CollectorConfig) GetPerfConfig() *PerfConfig {
	if cc.Perf == nil {
		return nil
	}

	// Handle bool case
	if enabled, ok := cc.Perf.(bool); ok {
		if !enabled {
			return nil
		}
		// All metrics enabled
		return &PerfConfig{
			Enabled:                  true,
			CacheMisses:              true,
			CacheReferences:          true,
			Instructions:             true,
			Cycles:                   true,
			BranchInstructions:       true,
			BranchMisses:             true,
			BusCycles:                true,
			StallsTotal:              true,
			StallsL3Miss:             true,
			StallsL2Miss:             true,
			StallsL1dMiss:            true,
			StallsMemAny:             true,
			ResourceStallsSB:         true,
			ResourceStallsScoreboard: true,
			L1DCacheLoadMisses:       true,
			L1DCacheLoads:            true,
			L1DCacheStores:           true,
			L1ICacheLoadMisses:       true,
			LLCLoadMisses:            true,
			LLCLoads:                 true,
			LLCStoreMisses:           true,
			LLCStores:                true,
			CacheMissRate:            true,
			InstructionsPerCycle:     true,
			StalledCyclesPercent:     true,
		}
	}

	// Handle map case - parse from interface{}
	if perfMap, ok := cc.Perf.(map[string]interface{}); ok {
		config := &PerfConfig{Enabled: true}

		// Parse each field
		if v, exists := perfMap["cache_misses"]; exists {
			if b, ok := v.(bool); ok {
				config.CacheMisses = b
			}
		}
		if v, exists := perfMap["cache_references"]; exists {
			if b, ok := v.(bool); ok {
				config.CacheReferences = b
			}
		}
		if v, exists := perfMap["instructions"]; exists {
			if b, ok := v.(bool); ok {
				config.Instructions = b
			}
		}
		if v, exists := perfMap["cycles"]; exists {
			if b, ok := v.(bool); ok {
				config.Cycles = b
			}
		}
		if v, exists := perfMap["branch_instructions"]; exists {
			if b, ok := v.(bool); ok {
				config.BranchInstructions = b
			}
		}
		if v, exists := perfMap["branch_misses"]; exists {
			if b, ok := v.(bool); ok {
				config.BranchMisses = b
			}
		}
		if v, exists := perfMap["bus_cycles"]; exists {
			if b, ok := v.(bool); ok {
				config.BusCycles = b
			}
		}
		if v, exists := perfMap["stalls_total"]; exists {
			if b, ok := v.(bool); ok {
				config.StallsTotal = b
			}
		}
		if v, exists := perfMap["stalls_l3_miss"]; exists {
			if b, ok := v.(bool); ok {
				config.StallsL3Miss = b
			}
		}
		if v, exists := perfMap["stalls_l2_miss"]; exists {
			if b, ok := v.(bool); ok {
				config.StallsL2Miss = b
			}
		}
		if v, exists := perfMap["stalls_l1d_miss"]; exists {
			if b, ok := v.(bool); ok {
				config.StallsL1dMiss = b
			}
		}
		if v, exists := perfMap["stalls_mem_any"]; exists {
			if b, ok := v.(bool); ok {
				config.StallsMemAny = b
			}
		}
		if v, exists := perfMap["resource_stalls_sb"]; exists {
			if b, ok := v.(bool); ok {
				config.ResourceStallsSB = b
			}
		}
		if v, exists := perfMap["resource_stalls_scoreboard"]; exists {
			if b, ok := v.(bool); ok {
				config.ResourceStallsScoreboard = b
			}
		}
		if v, exists := perfMap["l1d_cache_load_misses"]; exists {
			if b, ok := v.(bool); ok {
				config.L1DCacheLoadMisses = b
			}
		}
		if v, exists := perfMap["l1d_cache_loads"]; exists {
			if b, ok := v.(bool); ok {
				config.L1DCacheLoads = b
			}
		}
		if v, exists := perfMap["l1d_cache_stores"]; exists {
			if b, ok := v.(bool); ok {
				config.L1DCacheStores = b
			}
		}
		if v, exists := perfMap["l1i_cache_load_misses"]; exists {
			if b, ok := v.(bool); ok {
				config.L1ICacheLoadMisses = b
			}
		}
		if v, exists := perfMap["llc_load_misses"]; exists {
			if b, ok := v.(bool); ok {
				config.LLCLoadMisses = b
			}
		}
		if v, exists := perfMap["llc_loads"]; exists {
			if b, ok := v.(bool); ok {
				config.LLCLoads = b
			}
		}
		if v, exists := perfMap["llc_store_misses"]; exists {
			if b, ok := v.(bool); ok {
				config.LLCStoreMisses = b
			}
		}
		if v, exists := perfMap["llc_stores"]; exists {
			if b, ok := v.(bool); ok {
				config.LLCStores = b
			}
		}
		if v, exists := perfMap["cache_miss_rate"]; exists {
			if b, ok := v.(bool); ok {
				config.CacheMissRate = b
			}
		}
		if v, exists := perfMap["instructions_per_cycle"]; exists {
			if b, ok := v.(bool); ok {
				config.InstructionsPerCycle = b
			}
		}
		if v, exists := perfMap["stalled_cycles_percent"]; exists {
			if b, ok := v.(bool); ok {
				config.StalledCyclesPercent = b
			}
		}

		return config
	}

	return nil
}

// GetDockerConfig parses the Docker field and returns a DockerMetricsConfig
func (cc *CollectorConfig) GetDockerConfig() *DockerMetricsConfig {
	if cc.Docker == nil {
		return nil
	}

	// Handle bool case
	if enabled, ok := cc.Docker.(bool); ok {
		if !enabled {
			return nil
		}
		// All metrics enabled
		return &DockerMetricsConfig{
			Enabled:            true,
			AssignedCores:      true,
			CPUUsageTotal:      true,
			CPUUsageKernel:     true,
			CPUUsageUser:       true,
			CPUUsagePercent:    true,
			CPUThrottling:      true,
			MemoryUsage:        true,
			MemoryLimit:        true,
			MemoryCache:        true,
			MemoryRSS:          true,
			MemorySwap:         true,
			MemoryUsagePercent: true,
			NetworkRxBytes:     true,
			NetworkTxBytes:     true,
			DiskReadBytes:      true,
			DiskWriteBytes:     true,
		}
	}

	// Handle map case
	if dockerMap, ok := cc.Docker.(map[string]interface{}); ok {
		config := &DockerMetricsConfig{Enabled: true}

		if v, exists := dockerMap["assigned_cores"]; exists {
			if b, ok := v.(bool); ok {
				config.AssignedCores = b
			}
		}

		if v, exists := dockerMap["cpu_usage_total"]; exists {
			if b, ok := v.(bool); ok {
				config.CPUUsageTotal = b
			}
		}
		if v, exists := dockerMap["cpu_usage_kernel"]; exists {
			if b, ok := v.(bool); ok {
				config.CPUUsageKernel = b
			}
		}
		if v, exists := dockerMap["cpu_usage_user"]; exists {
			if b, ok := v.(bool); ok {
				config.CPUUsageUser = b
			}
		}
		if v, exists := dockerMap["cpu_usage_percent"]; exists {
			if b, ok := v.(bool); ok {
				config.CPUUsagePercent = b
			}
		}
		if v, exists := dockerMap["cpu_throttling"]; exists {
			if b, ok := v.(bool); ok {
				config.CPUThrottling = b
			}
		}
		if v, exists := dockerMap["memory_usage"]; exists {
			if b, ok := v.(bool); ok {
				config.MemoryUsage = b
			}
		}
		if v, exists := dockerMap["memory_limit"]; exists {
			if b, ok := v.(bool); ok {
				config.MemoryLimit = b
			}
		}
		if v, exists := dockerMap["memory_cache"]; exists {
			if b, ok := v.(bool); ok {
				config.MemoryCache = b
			}
		}
		if v, exists := dockerMap["memory_rss"]; exists {
			if b, ok := v.(bool); ok {
				config.MemoryRSS = b
			}
		}
		if v, exists := dockerMap["memory_swap"]; exists {
			if b, ok := v.(bool); ok {
				config.MemorySwap = b
			}
		}
		if v, exists := dockerMap["memory_usage_percent"]; exists {
			if b, ok := v.(bool); ok {
				config.MemoryUsagePercent = b
			}
		}
		if v, exists := dockerMap["network_rx_bytes"]; exists {
			if b, ok := v.(bool); ok {
				config.NetworkRxBytes = b
			}
		}
		if v, exists := dockerMap["network_tx_bytes"]; exists {
			if b, ok := v.(bool); ok {
				config.NetworkTxBytes = b
			}
		}
		if v, exists := dockerMap["disk_read_bytes"]; exists {
			if b, ok := v.(bool); ok {
				config.DiskReadBytes = b
			}
		}
		if v, exists := dockerMap["disk_write_bytes"]; exists {
			if b, ok := v.(bool); ok {
				config.DiskWriteBytes = b
			}
		}

		return config
	}

	return nil
}

// GetRDTConfig parses the RDT field and returns an RDTConfig
func (cc *CollectorConfig) GetRDTConfig() *RDTConfig {
	if cc.RDT == nil {
		return nil
	}

	// Handle bool case
	if enabled, ok := cc.RDT.(bool); ok {
		if !enabled {
			return nil
		}
		// All metrics enabled
		return &RDTConfig{
			Enabled:                     true,
			L3CacheOccupancy:            true,
			MemoryBandwidthTotal:        true,
			MemoryBandwidthLocal:        true,
			RDTClassName:                true,
			MonGroupName:                true,
			L3CacheAllocation:           true,
			L3CacheAllocationPct:        true,
			MBAThrottle:                 true,
			CacheLLCUtilizationPercent:  true,
			BandwidthUtilizationPercent: true,
		}
	}

	// Handle map case
	if rdtMap, ok := cc.RDT.(map[string]interface{}); ok {
		config := &RDTConfig{Enabled: true}

		if v, exists := rdtMap["l3_cache_occupancy"]; exists {
			if b, ok := v.(bool); ok {
				config.L3CacheOccupancy = b
			}
		}
		if v, exists := rdtMap["memory_bandwidth_total"]; exists {
			if b, ok := v.(bool); ok {
				config.MemoryBandwidthTotal = b
			}
		}
		if v, exists := rdtMap["memory_bandwidth_local"]; exists {
			if b, ok := v.(bool); ok {
				config.MemoryBandwidthLocal = b
			}
		}
		if v, exists := rdtMap["rdt_class_name"]; exists {
			if b, ok := v.(bool); ok {
				config.RDTClassName = b
			}
		}
		if v, exists := rdtMap["mon_group_name"]; exists {
			if b, ok := v.(bool); ok {
				config.MonGroupName = b
			}
		}
		if v, exists := rdtMap["l3_cache_allocation"]; exists {
			if b, ok := v.(bool); ok {
				config.L3CacheAllocation = b
			}
		}
		if v, exists := rdtMap["l3_cache_allocation_pct"]; exists {
			if b, ok := v.(bool); ok {
				config.L3CacheAllocationPct = b
			}
		}
		if v, exists := rdtMap["mba_throttle"]; exists {
			if b, ok := v.(bool); ok {
				config.MBAThrottle = b
			}
		}
		if v, exists := rdtMap["cache_llc_utilization_percent"]; exists {
			if b, ok := v.(bool); ok {
				config.CacheLLCUtilizationPercent = b
			}
		}
		if v, exists := rdtMap["bandwidth_utilization_percent"]; exists {
			if b, ok := v.(bool); ok {
				config.BandwidthUtilizationPercent = b
			}
		}

		return config
	}

	return nil
}
