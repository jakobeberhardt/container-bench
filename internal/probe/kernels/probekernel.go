package kernels

import (
	"container-bench/internal/config"
	"container-bench/internal/dataframe"
)

// ProbeKernel defines the interface for probe kernel implementations
// A probe kernel analyzes dataframes to compute sensitivity metrics
type ProbeKernel interface {
	// GetName returns the name of the probe kernel implementation
	GetName() string

	// GetVersion returns the version of the probe kernel implementation
	GetVersion() string

	// AnalyzeSensitivity analyzes dataframes and computes sensitivity metrics
	// baselineSteps: dataframe steps before probing started (baseline behavior)
	// probingSteps: dataframe steps during probing (with interference)
	// containerConfig: configuration of the target container
	// Returns pointers to sensitivity values (0.0-1.0), nil if metric not applicable
	AnalyzeSensitivity(
		baselineSteps map[int]*dataframe.SamplingStep,
		probingSteps map[int]*dataframe.SamplingStep,
		containerConfig *config.ContainerConfig,
	) (
		cpuInteger *float64,
		cpuFloat *float64,
		llc *float64,
		memRead *float64,
		memWrite *float64,
		storeBuffer *float64,
		scoreboard *float64,
		networkRead *float64,
		networkWrite *float64,
		sysCall *float64,
	)
}
