package allocation

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"text/template"
	"time"

	allocationMappings "container-bench/internal/plot/allocation/mappings"
	groupTemplate "container-bench/internal/plot/allocation/templates/group"
	plotTemplate "container-bench/internal/plot/allocation/templates/plot"
	wrapperTemplate "container-bench/internal/plot/allocation/templates/wrapper"
	"container-bench/internal/plot/database"
	"container-bench/internal/plot/timeseries/mappings"

	"github.com/sirupsen/logrus"
)

type AllocationPlotGenerator struct {
	dbClient *database.PlotDBClient
	logger   *logrus.Logger
}

func NewAllocationPlotGenerator(dbClient *database.PlotDBClient, logger *logrus.Logger) *AllocationPlotGenerator {
	return &AllocationPlotGenerator{
		dbClient: dbClient,
		logger:   logger,
	}
}

type PlotOptions struct {
	BenchmarkID          int
	AllocationProbeIndex int
	Metric               string
	IncludeLegend        bool // Whether to include the named legend in this plot
}

func (g *AllocationPlotGenerator) Generate(ctx context.Context, opts PlotOptions) (string, string, error) {
	// Default to including legend
	opts.IncludeLegend = true
	return g.GenerateWithOptions(ctx, opts)
}

func (g *AllocationPlotGenerator) GenerateWithOptions(ctx context.Context, opts PlotOptions) (string, string, error) {
	g.logger.WithFields(logrus.Fields{
		"benchmark_id":           opts.BenchmarkID,
		"allocation_probe_index": opts.AllocationProbeIndex,
		"metric":                 opts.Metric,
	}).Info("Generating allocation probe plot")

	allocations, err := g.dbClient.QueryAllocationData(ctx, opts.BenchmarkID, opts.AllocationProbeIndex)
	if err != nil {
		return "", "", fmt.Errorf("failed to query allocation data: %w", err)
	}

	if len(allocations) == 0 {
		return "", "", fmt.Errorf("no allocation data found for benchmark %d, probe %d", opts.BenchmarkID, opts.AllocationProbeIndex)
	}

	meta, err := g.dbClient.QueryMetaData(ctx, opts.BenchmarkID)
	if err != nil {
		g.logger.WithError(err).Warn("Failed to query metadata, continuing without it")
	}

	plotData, err := g.preparePlotData(allocations, opts)
	if err != nil {
		return "", "", fmt.Errorf("failed to prepare plot data: %w", err)
	}

	wrapperData := g.prepareWrapperData(opts, allocations[0].ContainerName, meta)

	plotOutput, err := g.renderPlot(plotData)
	if err != nil {
		return "", "", fmt.Errorf("failed to render plot: %w", err)
	}

	wrapperOutput, err := g.renderWrapper(wrapperData)
	if err != nil {
		return "", "", fmt.Errorf("failed to render wrapper: %w", err)
	}

	g.logger.Info("Allocation probe plot generated successfully")
	return plotOutput, wrapperOutput, nil
}

func (g *AllocationPlotGenerator) preparePlotData(
	allocations []database.AllocationData,
	opts PlotOptions,
) (*plotTemplate.PlotData, error) {

	bandwidthMap := make(map[float64][]database.AllocationData)
	for _, alloc := range allocations {
		bandwidthMap[alloc.MemBandwidth] = append(bandwidthMap[alloc.MemBandwidth], alloc)
	}

	var bandwidths []float64
	for bw := range bandwidthMap {
		bandwidths = append(bandwidths, bw)
	}
	sort.Float64s(bandwidths)

	var plotSeries []plotTemplate.PlotSeries
	yMin := math.Inf(1)
	yMax := math.Inf(-1)
	xMin := math.Inf(1)
	xMax := math.Inf(-1)

	for idx, bw := range bandwidths {
		allocsForBW := bandwidthMap[bw]

		sort.Slice(allocsForBW, func(i, j int) bool {
			return allocsForBW[i].L3Ways < allocsForBW[j].L3Ways
		})

		style := mappings.GetContainerStyle(idx)
		series := plotTemplate.PlotSeries{
			MemBandwidth: bw,
			Style:        style.ToTikzOptions(),
			LegendEntry:  fmt.Sprintf("%.0f\\%%", bw),
			Coordinates:  []string{},
		}

		for _, alloc := range allocsForBW {
			xVal := float64(alloc.L3Ways)
			yVal := g.getMetricValue(alloc, opts.Metric)

			if yVal == nil {
				continue
			}

			yFloat := *yVal

			if xVal < xMin {
				xMin = xVal
			}
			if xVal > xMax {
				xMax = xVal
			}
			if yFloat < yMin {
				yMin = yFloat
			}
			if yFloat > yMax {
				yMax = yFloat
			}

			series.Coordinates = append(series.Coordinates, fmt.Sprintf("(%d,%.6f)", alloc.L3Ways, yFloat))
		}

		if len(series.Coordinates) > 0 {
			plotSeries = append(plotSeries, series)
		}
	}

	firstAlloc := allocations[0]

	// Use exact L3 ways range from data (no padding)
	// xMin and xMax already contain the actual min/max L3 ways values

	ylabel := opts.Metric
	metricMapping, exists := allocationMappings.GetMetricMapping(opts.Metric)
	if exists {
		ylabel = metricMapping.Label

		// Apply min/max from mappings for y-axis
		if metricMapping.Min != nil {
			if minVal, ok := metricMapping.Min.(float64); ok {
				yMin = minVal
			}
		}

		if metricMapping.Max != nil {
			if maxStr, ok := metricMapping.Max.(string); ok && maxStr == "auto" {
				// Keep data-driven yMax, add padding
				yPadding := (yMax - yMin) * 0.1
				if yPadding == 0 {
					yPadding = yMax * 0.1
				}
				yMax = yMax + yPadding
			} else if maxVal, ok := metricMapping.Max.(float64); ok {
				yMax = maxVal
			}
		}
	} else {
		// No mapping found, add padding to data-driven bounds
		yPadding := (yMax - yMin) * 0.1
		if yPadding == 0 {
			yPadding = yMax * 0.1
		}
		yMax = yMax + yPadding
	}

	// For multi-metric plots, use a shared labelID without the metric suffix
	labelID := fmt.Sprintf("%d-%d", opts.BenchmarkID, opts.AllocationProbeIndex)
	if !opts.IncludeLegend {
		// For single-metric plots or when not including legend, keep the metric in the ID
		labelID = fmt.Sprintf("%d-%d-%s", opts.BenchmarkID, opts.AllocationProbeIndex, opts.Metric)
	}

	// Find baseline value (max L3 ways + max bandwidth allocation)
	// For metrics where higher is better: 90% of max value
	// For metrics where lower is better: 110% of min value
	var baselineValue *float64
	lowerIsBetter := opts.Metric == "cache_miss_rate" || opts.Metric == "stalled_cycles" || opts.Metric == "stalls_l3_miss_percent"

	for _, alloc := range allocations {
		if alloc.L3Ways == firstAlloc.RangeMaxL3Ways && alloc.MemBandwidth == firstAlloc.RangeMaxMemBandwidth {
			val := g.getMetricValue(alloc, opts.Metric)
			if val != nil {
				var baselineVal float64
				if lowerIsBetter {
					// For metrics where lower is better, baseline is 110% of the best (lowest) value
					baselineVal = *val * 1.1
				} else {
					// For metrics where higher is better, baseline is 90% of the best (highest) value
					baselineVal = *val * 0.9
				}
				baselineValue = &baselineVal
				break
			}
		}
	}

	plotData := &plotTemplate.PlotData{
		GeneratedDate:                time.Now().Format(time.RFC3339),
		BenchmarkID:                  opts.BenchmarkID,
		AllocationProbeIndex:         opts.AllocationProbeIndex,
		LabelID:                      labelID,
		IncludeLegend:                opts.IncludeLegend,
		BaselineValue:                baselineValue,
		ContainerIndex:               firstAlloc.ContainerIndex,
		ContainerName:                firstAlloc.ContainerName,
		ContainerImage:               firstAlloc.ContainerImage,
		ContainerCores:               firstAlloc.ContainerCores,
		ProbeStarted:                 firstAlloc.ProbeStarted,
		ProbeFinished:                firstAlloc.ProbeFinished,
		TotalProbeTimeSeconds:        float64(firstAlloc.TotalProbeTime) / 1e9,
		ProbeAborted:                 firstAlloc.ProbeAborted,
		RangeMinL3Ways:               firstAlloc.RangeMinL3Ways,
		RangeMaxL3Ways:               firstAlloc.RangeMaxL3Ways,
		RangeMinMemBandwidth:         firstAlloc.RangeMinMemBandwidth,
		RangeMaxMemBandwidth:         firstAlloc.RangeMaxMemBandwidth,
		RangeStepL3Ways:              firstAlloc.RangeStepL3Ways,
		RangeStepMemBandwidth:        firstAlloc.RangeStepMemBandwidth,
		RangeOrder:                   firstAlloc.RangeOrder,
		RangeDurationPerAlloc:        firstAlloc.RangeDurationPerAlloc,
		RangeDurationPerAllocSeconds: float64(firstAlloc.RangeDurationPerAlloc) / 1000.0,
		RangeIsolateOthers:           firstAlloc.RangeIsolateOthers,
		XLabel:                       "L3 Cache Ways",
		YLabel:                       ylabel,
		XMin:                         fmt.Sprintf("%.0f", xMin),
		XMax:                         fmt.Sprintf("%.0f", xMax),
		YMin:                         fmt.Sprintf("%.6f", yMin),
		YMax:                         fmt.Sprintf("%.6f", yMax),
		Plots:                        plotSeries,
	}

	return plotData, nil
}

func (g *AllocationPlotGenerator) getMetricValue(alloc database.AllocationData, metric string) *float64 {
	var val float64
	switch metric {
	case "ipc":
		val = alloc.AvgIPC
	case "ipc_efficiency":
		val = normalizePercentMetric(alloc.IPCEfficiency)
	case "cache_miss_rate":
		val = normalizePercentMetric(alloc.AvgCacheMissRate)
	case "stalled_cycles":
		val = alloc.AvgStalledCycles
	case "stalls_l3_miss_percent":
		val = alloc.AvgStallsL3MissPercent
	case "l3_occupancy":
		val = float64(alloc.AvgL3Occupancy)
	case "mem_bandwidth_used":
		val = float64(alloc.AvgMemBandwidthUsed)
	default:
		return nil
	}
	return &val
}

// normalizePercentMetric converts fraction-like values (0..~1) to percent (0..100)
// while leaving already-percent values untouched.
func normalizePercentMetric(v float64) float64 {
	if v < 0 {
		return v
	}
	// Treat values up to 1.5 as fractions to be robust to slight >1.0 noise.
	if v <= 1.5 {
		return v * 100.0
	}
	return v
}

func (g *AllocationPlotGenerator) prepareWrapperData(
	opts PlotOptions,
	containerName string,
	meta *database.MetaData,
) *wrapperTemplate.WrapperData {

	return &wrapperTemplate.WrapperData{
		GeneratedDate:        time.Now().Format(time.RFC3339),
		BenchmarkID:          opts.BenchmarkID,
		AllocationProbeIndex: opts.AllocationProbeIndex,
		Metric:               opts.Metric,
		LabelID:              fmt.Sprintf("%d-%d-%s", opts.BenchmarkID, opts.AllocationProbeIndex, opts.Metric),
		PlotFileName:         fmt.Sprintf("%s.tikz", opts.Metric),
		ContainerName:        containerName,
	}
}

func (g *AllocationPlotGenerator) renderPlot(data *plotTemplate.PlotData) (string, error) {
	tmpl, err := template.New("plot").Parse(plotTemplate.PlotTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to parse plot template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute plot template: %w", err)
	}

	return buf.String(), nil
}

func (g *AllocationPlotGenerator) renderWrapper(data *wrapperTemplate.WrapperData) (string, error) {
	tmpl, err := template.New("wrapper").Parse(wrapperTemplate.WrapperTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to parse wrapper template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute wrapper template: %w", err)
	}

	return buf.String(), nil
}

func (g *AllocationPlotGenerator) GenerateGroupWrapper(
	ctx context.Context,
	benchmarkID int,
	allocationProbeIndex int,
	metrics []string,
	labelID string,
	containerName string,
	meta *database.MetaData,
) (string, error) {
	g.logger.WithFields(logrus.Fields{
		"benchmark_id":           benchmarkID,
		"allocation_probe_index": allocationProbeIndex,
		"metrics":                metrics,
	}).Info("Generating allocation probe group wrapper")

	var subfigures []groupTemplate.SubfigureData
	for _, metric := range metrics {
		metricLabel := metric
		if metricMapping, exists := allocationMappings.GetMetricMapping(metric); exists {
			metricLabel = metricMapping.ShortLabel
		}

		subfigures = append(subfigures, groupTemplate.SubfigureData{
			PlotFileName: fmt.Sprintf("%s.tikz", metric),
			Caption:      metricLabel,
		})
	}

	groupData := &groupTemplate.GroupWrapperData{
		GeneratedDate:        time.Now().Format(time.RFC3339),
		BenchmarkID:          benchmarkID,
		AllocationProbeIndex: allocationProbeIndex,
		Metrics:              metrics,
		LabelID:              labelID,
		Subfigures:           subfigures,
		ShortCaption:         fmt.Sprintf("Allocation Probe for %s", containerName),
		Caption:              fmt.Sprintf("Metrics for different allocation combinations for %s", containerName),
	}

	tmpl, err := template.New("group").Parse(groupTemplate.GroupWrapperTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to parse group wrapper template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, groupData); err != nil {
		return "", fmt.Errorf("failed to execute group wrapper template: %w", err)
	}

	g.logger.Info("Allocation probe group wrapper generated successfully")
	return buf.String(), nil
}
