package datahandeling

import (
	"math"
	"sort"
	"time"

	"container-bench/internal/config"
)

type QoSMetric string

const (
	QoSMetricIPCE QoSMetric = "ipce"
	QoSMetricIPC  QoSMetric = "ipc"
)

type QoSResult struct {
	ContainerIndex int
	Metric         QoSMetric
	Guarantee      float64
	FractionMet    float64
	Samples        int
}

// ComputePriorityQoSMetFractions derives, for each priority container with a configured
// QoS guarantee, the fraction of time its measured performance stayed above the guarantee.
//
// Guarantees are interpreted as:
// - if `ipce` is set: compare against processed `PerfIPCEfficancy`
// - else if `ipc` is set: compare against processed `PerfInstructionsPerCycle`
//
// The fraction is computed time-weighted based on sampling timestamps when possible.
func ComputePriorityQoSMetFractions(benchmarkCfg *config.BenchmarkConfig, metrics *BenchmarkMetrics) map[int]QoSResult {
	results := make(map[int]QoSResult)
	if benchmarkCfg == nil || metrics == nil {
		return results
	}

	// Build guarantees for priority containers.
	type guarantee struct {
		metric    QoSMetric
		threshold float64
	}
	guarantees := make(map[int]guarantee)
	for _, c := range benchmarkCfg.GetContainersSorted() {
		if !(c.Priority || c.Critical) {
			continue
		}
		if c.IPCEfficiency != nil {
			guarantees[c.Index] = guarantee{metric: QoSMetricIPCE, threshold: *c.IPCEfficiency}
			continue
		}
		if c.IPC != nil {
			guarantees[c.Index] = guarantee{metric: QoSMetricIPC, threshold: *c.IPC}
			continue
		}
	}
	if len(guarantees) == 0 {
		return results
	}

	for _, cm := range metrics.ContainerMetrics {
		g, ok := guarantees[cm.ContainerIndex]
		if !ok {
			continue
		}

		// Extract valid samples.
		type sample struct {
			ts    time.Time
			meets bool
		}
		samples := make([]sample, 0, len(cm.Steps))
		for _, s := range cm.Steps {
			var v *float64
			switch g.metric {
			case QoSMetricIPCE:
				v = s.PerfIPCEfficancy
			case QoSMetricIPC:
				v = s.PerfInstructionsPerCycle
			default:
				v = nil
			}
			if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) {
				continue
			}
			samples = append(samples, sample{ts: s.Timestamp, meets: *v >= g.threshold})
		}
		if len(samples) == 0 {
			continue
		}

		sort.Slice(samples, func(i, j int) bool { return samples[i].ts.Before(samples[j].ts) })

		// Time-weighted fraction over intervals.
		var total float64
		var met float64
		for i := 0; i+1 < len(samples); i++ {
			dt := samples[i+1].ts.Sub(samples[i].ts).Seconds()
			if dt <= 0 {
				continue
			}
			total += dt
			if samples[i].meets {
				met += dt
			}
		}

		fraction := 0.0
		if total > 0 {
			fraction = met / total
		} else {
			// Fallback: simple step fraction.
			metCount := 0
			for _, s := range samples {
				if s.meets {
					metCount++
				}
			}
			fraction = float64(metCount) / float64(len(samples))
		}
		if fraction < 0 {
			fraction = 0
		}
		if fraction > 1 {
			fraction = 1
		}

		results[cm.ContainerIndex] = QoSResult{
			ContainerIndex: cm.ContainerIndex,
			Metric:         g.metric,
			Guarantee:      g.threshold,
			FractionMet:    fraction,
			Samples:        len(samples),
		}
	}

	return results
}
