package resources

import (
	"container-bench/internal/dataframe"
	"container-bench/internal/logging"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

type AllocationProbeBreakPolicy struct {
	// AcceptableIPCEfficiency stops probing when a candidate reaches or exceeds this.
	// If nil: disabled.
	// If < 0: disabled.
	AcceptableIPCEfficiency *float64

	// DiminishingReturnsThreshold stops probing when the relative improvement of the
	// current observed IPCEfficiency over the previous IPCEfficiency (within the same
	// L3Ways memory sweep) is below this threshold.
	// If nil: disabled.
	// If < 0: disabled.
	DiminishingReturnsThreshold *float64

	// MaxCPUUsagePercent stops probing when the candidate's average Docker CPU usage
	// percent is less than or equal to this threshold.
	// If nil: disabled.
	// If < 0: disabled.
	MaxCPUUsagePercent *float64

	// MaxL3UtilizationPct stops probing when the candidate's average LLC/L3 utilization
	// percent (RDT L3 utilization pct per socket) is less than or equal to this threshold.
	// If nil: disabled.
	// If < 0: disabled.
	MaxL3UtilizationPct *float64
}

type AllocationProbeOptions struct {
	// GreedyAllocation stops early when diminishing returns are detected.
	// If false, the probe runs the full candidate set (unless break_condition hits) and
	// diminishing returns is classified post-mortem based on the last within-ways mem step.
	GreedyAllocation bool

	// ProbingFrequency, if > 0, requests a temporary increase of the target container's
	// container collector frequency for the duration of this allocation probe.
	// The orchestration/scheduler must provide a callback to apply this.
	ProbingFrequency time.Duration

	// OutlierDrop controls outlier trimming for allocation probe metric computation.
	// If > 0: sort samples and drop N lowest and N highest values.
	// If <= 0: keep all values.
	OutlierDrop int

	// BaselineFirst prepends a baseline (no-allocation-change) candidate before applying
	// any RDT allocations. This allows checking break conditions without harming other
	// workloads.
	BaselineFirst bool
}

type AllocationProbeTarget struct {
	BenchmarkID int

	ContainerID      string
	ContainerName    string
	ContainerIndex   int
	ContainerCores   string
	ContainerSocket  int
	ContainerImage   string
	ContainerCommand string
}

type AllocationProbeCallbacks struct {
	ApplyAllocation  func(ways int, mem float64) error
	ResetToBenchmark func() error
	LatestStepNumber func(dfs *dataframe.DataFrames, containerIndex int) int

	// OverrideContainerCollectorFrequency temporarily overrides the sampling frequency of
	// the container collector for the probe target and returns a restore function.
	// If nil, probing runs with the configured baseline collection frequency.
	OverrideContainerCollectorFrequency func(containerIndex int, freq time.Duration) (restore func(), err error)
}

// AllocationProbeRunner incrementally runs an allocation probe using a caller-provided
// apply/reset interface and computes metrics from the provided dataframes.
type AllocationProbeRunner struct {
	target       AllocationProbeTarget
	rangeCfg     AllocationRange
	breaks       AllocationProbeBreakPolicy
	candidates   []AllocationSpec
	candidateDur time.Duration
	cb           AllocationProbeCallbacks

	result *AllocationProbeResult
	opts   AllocationProbeOptions

	started      bool
	finished     bool
	startStep    int
	startedAt    time.Time
	candidateIdx int

	bestIdx     int
	bestEff     float64
	bestWays    int
	bestMem     float64
	prevBestEff float64

	stopReason string
	unbound    bool
	startedAlloc bool

	restoreCollectorFrequency func()
}

func NewAllocationProbeRunner(
	target AllocationProbeTarget,
	rangeCfg AllocationRange,
	candidates []AllocationSpec,
	candidateDur time.Duration,
	breaks AllocationProbeBreakPolicy,
	opts AllocationProbeOptions,
	cb AllocationProbeCallbacks,
) *AllocationProbeRunner {
	res := &AllocationProbeResult{
		BenchmarkID:      target.BenchmarkID,
		ContainerID:      target.ContainerID,
		ContainerName:    target.ContainerName,
		ContainerIndex:   target.ContainerIndex,
		ContainerCores:   target.ContainerCores,
		ContainerSocket:  target.ContainerSocket,
		ContainerImage:   target.ContainerImage,
		ContainerCommand: target.ContainerCommand,
		Started:          time.Now(),
		Range:            rangeCfg,
		Allocations:      make([]AllocationResult, 0, len(candidates)),
	}

	return &AllocationProbeRunner{
		target:       target,
		rangeCfg:     rangeCfg,
		breaks:       breaks,
		candidates:   candidates,
		candidateDur: candidateDur,
		cb:           cb,
		result:       res,
		opts:         opts,
		started:      false,
		finished:     false,
		candidateIdx: 0,
		bestIdx:      -1,
		bestEff:      -1,
		stopReason:   "",
		unbound:      false,
	}
}

func NewAllocationProbeRunnerFromRange(
	target AllocationProbeTarget,
	rangeCfg AllocationRange,
	totalBudget time.Duration,
	minCandidateDur time.Duration,
	breaks AllocationProbeBreakPolicy,
	opts AllocationProbeOptions,
	cb AllocationProbeCallbacks,
) *AllocationProbeRunner {
	candidates := GenerateAllocationSequence(rangeCfg)
	if opts.BaselineFirst {
		// Sentinel candidate: (0,0) means "baseline"; runner will skip ApplyAllocation.
		candidates = append([]AllocationSpec{{L3Ways: 0, MemBandwidth: 0}}, candidates...)
	}
	if len(candidates) == 0 {
		return NewAllocationProbeRunner(target, rangeCfg, nil, 0, breaks, opts, cb)
	}

	candidateDur := time.Duration(float64(totalBudget) / float64(len(candidates)))
	if candidateDur < minCandidateDur {
		candidateDur = minCandidateDur
	}

	rangeCfg.DurationPerAlloc = int(candidateDur / time.Millisecond)
	if rangeCfg.DurationPerAlloc <= 0 {
		rangeCfg.DurationPerAlloc = 1
	}
	if totalBudget > 0 {
		rangeCfg.MaxTotalDuration = int(totalBudget.Seconds())
	}

	return NewAllocationProbeRunner(target, rangeCfg, candidates, candidateDur, breaks, opts, cb)
}

func (r *AllocationProbeRunner) diminishingThreshold() (float64, bool) {
	if r.breaks.DiminishingReturnsThreshold == nil {
		return 0, false
	}
	th := *r.breaks.DiminishingReturnsThreshold
	if th < 0 {
		return 0, false
	}
	return th, true
}

func (r *AllocationProbeRunner) postMortemDiminishingReturns() bool {
	th, ok := r.diminishingThreshold()
	if !ok {
		return false
	}
	if len(r.candidates) < 2 || len(r.result.Allocations) < 2 {
		return false
	}
	// Find the last adjacent pair within the same L3Ways group.
	for i := len(r.candidates) - 1; i > 0; i-- {
		if r.candidates[i].L3Ways != r.candidates[i-1].L3Ways {
			continue
		}
		curr := r.result.Allocations[i].IPCEfficiency
		prev := r.result.Allocations[i-1].IPCEfficiency
		if curr < 0 || prev <= 0 {
			return false
		}
		rel := (curr - prev) / prev
		return rel >= 0 && rel < th
	}
	return false
}

func (r *AllocationProbeRunner) IsRunning() bool                  { return r.started && !r.finished }
func (r *AllocationProbeRunner) IsFinished() bool                 { return r.finished }
func (r *AllocationProbeRunner) Result() *AllocationProbeResult   { return r.result }
func (r *AllocationProbeRunner) BestWays() int                    { return r.bestWays }
func (r *AllocationProbeRunner) BestMem() float64                 { return r.bestMem }
func (r *AllocationProbeRunner) BestEff() float64                 { return r.bestEff }
func (r *AllocationProbeRunner) StopReason() string               { return r.stopReason }
func (r *AllocationProbeRunner) Unbound() bool                    { return r.unbound }
func (r *AllocationProbeRunner) StartedAllocating() bool          { return r.startedAlloc }
func (r *AllocationProbeRunner) NumCandidates() int               { return len(r.candidates) }
func (r *AllocationProbeRunner) CandidateDuration() time.Duration { return r.candidateDur }

// Abort stops the probe early and performs cleanup (e.g., restoring collector frequency).
// It is safe to call multiple times.
func (r *AllocationProbeRunner) Abort(reason string) {
	if reason == "" {
		reason = "aborted"
	}
	r.finish(reason)
}

func (r *AllocationProbeRunner) Start(dfs *dataframe.DataFrames) error {
	if r.started {
		return nil
	}
	r.started = true
	r.result.Started = time.Now()

	logger := logging.GetProberLogger()
	logger.WithFields(logrus.Fields{
		"container":        r.target.ContainerName,
		"container_index":  r.target.ContainerIndex,
		"socket":           r.rangeCfg.SocketID,
		"candidates":       len(r.candidates),
		"candidate_dur_ms": int(r.candidateDur / time.Millisecond),
		"min_l3_ways":      r.rangeCfg.MinL3Ways,
		"max_l3_ways":      r.rangeCfg.MaxL3Ways,
		"step_l3_ways":     r.rangeCfg.StepL3Ways,
		"min_mem_bw":       r.rangeCfg.MinMemBandwidth,
		"max_mem_bw":       r.rangeCfg.MaxMemBandwidth,
		"step_mem_bw":      r.rangeCfg.StepMemBandwidth,
	}).Info("Allocation probe started")

	if r.opts.ProbingFrequency > 0 && r.cb.OverrideContainerCollectorFrequency != nil {
		restore, err := r.cb.OverrideContainerCollectorFrequency(r.target.ContainerIndex, r.opts.ProbingFrequency)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"container":       r.target.ContainerName,
				"container_index": r.target.ContainerIndex,
				"freq_ms":         int(r.opts.ProbingFrequency / time.Millisecond),
			}).WithError(err).Warn("Failed to override container collector frequency for allocation probing")
		} else {
			r.restoreCollectorFrequency = restore
			logger.WithFields(logrus.Fields{
				"container":       r.target.ContainerName,
				"container_index": r.target.ContainerIndex,
				"freq_ms":         int(r.opts.ProbingFrequency / time.Millisecond),
			}).Debug("Overrode container collector frequency for allocation probing")
		}
	}

	if len(r.candidates) == 0 {
		r.finish("no_candidates")
		return nil
	}

	if r.cb.ResetToBenchmark != nil {
		_ = r.cb.ResetToBenchmark()
	}

	r.candidateIdx = 0
	r.startedAt = time.Now()
	if r.cb.LatestStepNumber != nil {
		r.startStep = r.cb.LatestStepNumber(dfs, r.target.ContainerIndex)
	}

	first := r.candidates[0]
	if r.cb.ApplyAllocation != nil && !(first.L3Ways <= 0 && first.MemBandwidth <= 0) {
		r.startedAlloc = true
		logger.WithFields(logrus.Fields{
			"container":        r.target.ContainerName,
			"container_index":  r.target.ContainerIndex,
			"socket":           r.rangeCfg.SocketID,
			"allocation":       1,
			"total":            len(r.candidates),
			"l3_ways":          first.L3Ways,
			"mem_bw":           first.MemBandwidth,
			"candidate_dur_ms": int(r.candidateDur / time.Millisecond),
		}).Debug("Testing allocation")
		if err := r.cb.ApplyAllocation(first.L3Ways, first.MemBandwidth); err != nil {
			r.finish("apply_failed")
			return err
		}
	} else if first.L3Ways <= 0 && first.MemBandwidth <= 0 {
		logger.WithFields(logrus.Fields{
			"container":        r.target.ContainerName,
			"container_index":  r.target.ContainerIndex,
			"socket":           r.rangeCfg.SocketID,
			"allocation":       1,
			"total":            len(r.candidates),
			"candidate_dur_ms": int(r.candidateDur / time.Millisecond),
		}).Debug("Testing baseline (no allocation change)")
	}

	return nil
}

func (r *AllocationProbeRunner) Step(dfs *dataframe.DataFrames) (done bool, err error) {
	if !r.started {
		if err := r.Start(dfs); err != nil {
			return true, err
		}
	}
	if r.finished {
		return true, nil
	}

	logger := logging.GetProberLogger()

	if time.Since(r.startedAt) < r.candidateDur {
		return false, nil
	}

	endStep := 0
	if r.cb.LatestStepNumber != nil {
		endStep = r.cb.LatestStepNumber(dfs, r.target.ContainerIndex)
	}

	cand := r.candidates[r.candidateIdx]
	isBaseline := (cand.L3Ways <= 0 && cand.MemBandwidth <= 0)
	allocRes := AllocationResult{
		L3Ways:         cand.L3Ways,
		MemBandwidth:   cand.MemBandwidth,
		SocketID:       r.rangeCfg.SocketID,
		IsolatedOthers: r.rangeCfg.IsolateOthers,
		Started:        r.startedAt,
		Duration:       time.Since(r.startedAt),
		DataFrameSteps: make([]int, 0),
	}
	if isBaseline {
		// Baseline is a pre-allocation precheck. We intentionally compute from the latest
		// available samples (no time-window filtering) so we can skip allocating even when
		// the collector frequency is lower than candidateDur (and no new steps arrive).
		//
		// We look back a small number of steps to smooth noise.
		baselineLookbackSteps := 3
		start := endStep - baselineLookbackSteps
		if start < -1 {
			start = -1
		}
		ComputeAllocationMetricsWithOutlierDrop(&allocRes, dfs, r.target.ContainerIndex, start, endStep, r.opts.OutlierDrop)
	} else if r.candidateDur > 0 {
		ComputeAllocationMetricsWithOutlierDropAndWindow(
			&allocRes,
			dfs,
			r.target.ContainerIndex,
			r.startStep,
			endStep,
			r.opts.OutlierDrop,
			r.startedAt,
			r.startedAt.Add(r.candidateDur),
		)
	} else {
		ComputeAllocationMetricsWithOutlierDrop(&allocRes, dfs, r.target.ContainerIndex, r.startStep, endStep, r.opts.OutlierDrop)
	}
	r.result.Allocations = append(r.result.Allocations, allocRes)

	eff := allocRes.IPCEfficiency

	if eff >= 0 {
		if r.bestIdx == -1 || eff > r.bestEff {
			r.prevBestEff = r.bestEff
			r.bestIdx = r.candidateIdx
			r.bestEff = eff
			r.bestWays = cand.L3Ways
			r.bestMem = cand.MemBandwidth
		}
	}

	logger.WithFields(logrus.Fields{
		"container":       r.target.ContainerName,
		"container_index": r.target.ContainerIndex,
		"socket":          r.rangeCfg.SocketID,
		"allocation":      r.candidateIdx + 1,
		"total":           len(r.candidates),
		"l3_ways":         cand.L3Ways,
		"mem_bw":          cand.MemBandwidth,
		"ipc_eff":         fmt.Sprintf("%.4f", allocRes.IPCEfficiency),
		"avg_ipc":         fmt.Sprintf("%.4f", allocRes.AvgIPC),
		"avg_theo_ipc":    fmt.Sprintf("%.4f", allocRes.AvgTheoreticalIPC),
		"best_l3_ways":    r.bestWays,
		"best_mem_bw":     r.bestMem,
		"best_eff":        fmt.Sprintf("%.4f", r.bestEff),
	}).Debug("Allocation result")

	// Break: acceptable threshold
	if thr := r.breaks.AcceptableIPCEfficiency; thr != nil {
		if *thr >= 0 && r.bestIdx != -1 && r.bestEff >= *thr {
			r.finish("break_condition")
			return true, nil
		}
	}

	// For the initial baseline candidate (no RDT changes), we may use additional
	// break conditions to avoid expensive probing for containers that are already
	// "not worth probing" (e.g., idle/low LLC usage). After baseline, only IPCE
	// (and diminishing-returns logic) governs early stopping.
	if isBaseline {
		// Break: low CPU usage
		if thr := r.breaks.MaxCPUUsagePercent; thr != nil {
			if *thr >= 0 && allocRes.AvgCPUUsagePercent >= 0 && allocRes.AvgCPUUsagePercent <= *thr {
				r.finish("break_condition_cpu")
				return true, nil
			}
		}

		// Break: low LLC/L3 utilization
		if thr := r.breaks.MaxL3UtilizationPct; thr != nil {
			if *thr >= 0 && allocRes.AvgL3UtilizationPct >= 0 && allocRes.AvgL3UtilizationPct <= *thr {
				r.finish("break_condition_llc")
				return true, nil
			}
		}
	}

	// Break: diminishing returns (relative improvement)
	// Greedy mode: stop early when diminishing returns are detected.
	// Non-greedy mode: run full probe and classify diminishing returns post-mortem.
	if r.opts.GreedyAllocation {
		th, ok := r.diminishingThreshold()
		if ok && r.candidateIdx > 0 {
			prevCand := r.candidates[r.candidateIdx-1]
			if prevCand.L3Ways == cand.L3Ways && cand.MemBandwidth > prevCand.MemBandwidth {
				prevRes := r.result.Allocations[len(r.result.Allocations)-2]
				prevEff := prevRes.IPCEfficiency
				if eff >= 0 && prevEff > 0 {
					rel := (eff - prevEff) / prevEff
					if rel >= 0 && rel < th {
						r.finish("diminishing_returns")
						return true, nil
					}
				}
			}
		}
	}

	// Advance candidate
	r.candidateIdx++
	if r.candidateIdx >= len(r.candidates) {
		r.finish("completed")
		return true, nil
	}

	next := r.candidates[r.candidateIdx]
	r.startedAt = time.Now()
	if r.cb.LatestStepNumber != nil {
		r.startStep = r.cb.LatestStepNumber(dfs, r.target.ContainerIndex)
	}
	if r.cb.ApplyAllocation != nil && !(next.L3Ways <= 0 && next.MemBandwidth <= 0) {
		r.startedAlloc = true
		logger.WithFields(logrus.Fields{
			"container":        r.target.ContainerName,
			"container_index":  r.target.ContainerIndex,
			"socket":           r.rangeCfg.SocketID,
			"allocation":       r.candidateIdx + 1,
			"total":            len(r.candidates),
			"l3_ways":          next.L3Ways,
			"mem_bw":           next.MemBandwidth,
			"candidate_dur_ms": int(r.candidateDur / time.Millisecond),
		}).Debug("Testing allocation")
		if err := r.cb.ApplyAllocation(next.L3Ways, next.MemBandwidth); err != nil {
			r.finish("apply_failed")
			return true, err
		}
	} else if next.L3Ways <= 0 && next.MemBandwidth <= 0 {
		logger.WithFields(logrus.Fields{
			"container":        r.target.ContainerName,
			"container_index":  r.target.ContainerIndex,
			"socket":           r.rangeCfg.SocketID,
			"allocation":       r.candidateIdx + 1,
			"total":            len(r.candidates),
			"candidate_dur_ms": int(r.candidateDur / time.Millisecond),
		}).Debug("Testing baseline (no allocation change)")
	}

	return false, nil
}

func (r *AllocationProbeRunner) finish(reason string) {
	if r.finished {
		return
	}
	r.finished = true
	r.stopReason = reason
	if r.restoreCollectorFrequency != nil {
		r.restoreCollectorFrequency()
		r.restoreCollectorFrequency = nil
	}

	r.result.Finished = time.Now()
	r.result.TotalProbeTime = r.result.Finished.Sub(r.result.Started)

	logger := logging.GetProberLogger()

	// Determine unbound semantics (matches interference-aware behavior)
	if r.bestIdx == -1 {
		r.unbound = true
		r.bestWays = r.rangeCfg.MaxL3Ways
		r.bestMem = r.rangeCfg.MaxMemBandwidth
		// Mark as aborted due to missing data
		r.result.Aborted = true
		r.result.AbortReason = "no_data"
		now := time.Now()
		r.result.AbortedAt = &now
		logger.WithFields(logrus.Fields{
			"container":       r.target.ContainerName,
			"container_index": r.target.ContainerIndex,
			"socket":          r.rangeCfg.SocketID,
			"precheck":        !r.startedAlloc,
			"reason":          r.stopReason,
			"total_ms":        int(r.result.TotalProbeTime / time.Millisecond),
		}).Info("Allocation probe finished")
		return
	}

	if reason != "completed" {
		r.result.Aborted = true
		r.result.AbortReason = reason
		now := time.Now()
		r.result.AbortedAt = &now
	} else {
		r.result.Aborted = false
		r.result.AbortReason = ""
		r.result.AbortedAt = nil
	}

	// Classification for completed probes.
	if reason == "completed" {
		if !r.opts.GreedyAllocation && r.postMortemDiminishingReturns() {
			// Completed full probe, but the last relevant step indicates diminishing returns.
			r.stopReason = "diminishing_returns"
			r.unbound = false
		} else {
			// Completed full probe without a break or diminishing-returns signal.
			r.unbound = true
		}
	}

	logger.WithFields(logrus.Fields{
		"container":       r.target.ContainerName,
		"container_index": r.target.ContainerIndex,
		"socket":          r.rangeCfg.SocketID,
		"precheck":        !r.startedAlloc,
		"reason":          r.stopReason,
		"aborted":         r.result.Aborted,
		"unbound":         r.unbound,
		"best_l3_ways":    r.bestWays,
		"best_mem_bw":     r.bestMem,
		"best_eff":        fmt.Sprintf("%.4f", r.bestEff),
		"total_ms":        int(r.result.TotalProbeTime / time.Millisecond),
		"candidates":      len(r.candidates),
	}).Info("Allocation probe finished")
}
