package kernels

import (
"container-bench/internal/config"
"container-bench/internal/dataframe"
"math"
)

// DefaultProbeKernel is a basic implementation that analyzes IPC and cache miss rates
type DefaultProbeKernel struct {
name    string
version string
}

func NewDefaultProbeKernel() *DefaultProbeKernel {
return &DefaultProbeKernel{
name:    "default",
version: "1.0.0",
}
}

func (dpk *DefaultProbeKernel) GetName() string {
return dpk.name
}

func (dpk *DefaultProbeKernel) GetVersion() string {
return dpk.version
}

// AnalyzeSensitivity computes sensitivity metrics based on performance degradation
func (dpk *DefaultProbeKernel) AnalyzeSensitivity(
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
) {
// Compute baseline metrics
baselineIPC := computeAverageIPC(baselineSteps)
baselineCacheMissRate := computeAverageCacheMissRate(baselineSteps)
baselineStalledCycles := computeAverageStalledCycles(baselineSteps)

// Compute probing metrics
probingIPC := computeAverageIPC(probingSteps)
probingCacheMissRate := computeAverageCacheMissRate(probingSteps)
probingStalledCycles := computeAverageStalledCycles(probingSteps)

// LLC sensitivity: based on cache miss rate increase
if baselineCacheMissRate > 0 {
missRateIncrease := (probingCacheMissRate - baselineCacheMissRate) / baselineCacheMissRate
llcValue := clampSensitivity(missRateIncrease)
llc = &llcValue
}

// CPU Integer sensitivity: based on IPC degradation
if baselineIPC > 0 {
ipcDegradation := (baselineIPC - probingIPC) / baselineIPC
cpuIntValue := clampSensitivity(ipcDegradation)
cpuInteger = &cpuIntValue
}

// Store buffer / Scoreboard: based on stalled cycles increase
if baselineStalledCycles > 0 {
stalledIncrease := (probingStalledCycles - baselineStalledCycles) / baselineStalledCycles
sbValue := clampSensitivity(stalledIncrease * 0.5)
storeBuffer = &sbValue
}

return cpuInteger, cpuFloat, llc, memRead, memWrite, storeBuffer, scoreboard, networkRead, networkWrite, sysCall
}

// Helper functions

func computeAverageIPC(steps map[int]*dataframe.SamplingStep) float64 {
var totalIPC float64
var count int

for _, step := range steps {
if step.Perf != nil && step.Perf.InstructionsPerCycle != nil {
totalIPC += *step.Perf.InstructionsPerCycle
count++
}
}

if count == 0 {
return 0
}
return totalIPC / float64(count)
}

func computeAverageCacheMissRate(steps map[int]*dataframe.SamplingStep) float64 {
var totalMissRate float64
var count int

for _, step := range steps {
if step.Perf != nil && step.Perf.CacheMissRate != nil {
totalMissRate += *step.Perf.CacheMissRate
count++
}
}

if count == 0 {
return 0
}
return totalMissRate / float64(count)
}

func computeAverageStalledCycles(steps map[int]*dataframe.SamplingStep) float64 {
var totalStalled float64
var count int

for _, step := range steps {
if step.Perf != nil && step.Perf.StalledCyclesPercent != nil {
totalStalled += *step.Perf.StalledCyclesPercent
count++
}
}

if count == 0 {
return 0
}
return totalStalled / float64(count)
}

// clampSensitivity ensures sensitivity values are in [0.0, 1.0] range
func clampSensitivity(value float64) float64 {
if value < 0 {
return 0
}
if value > 1.0 {
return 1.0
}
if math.IsNaN(value) || math.IsInf(value, 0) {
return 0
}
return value
}
