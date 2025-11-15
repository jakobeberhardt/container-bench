package timeseries

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"text/template"
	"time"

	"container-bench/internal/plot/database"
	"container-bench/internal/plot/timeseries/mappings"
	plotTemplate "container-bench/internal/plot/timeseries/templates/plot"
	wrapperTemplate "container-bench/internal/plot/timeseries/templates/wrapper"

	"github.com/sirupsen/logrus"
)

type TimeseriesPlotGenerator struct {
	dbClient *database.PlotDBClient
	logger   *logrus.Logger
}

func NewTimeseriesPlotGenerator(dbClient *database.PlotDBClient, logger *logrus.Logger) *TimeseriesPlotGenerator {
	return &TimeseriesPlotGenerator{
		dbClient: dbClient,
		logger:   logger,
	}
}

type PlotOptions struct {
	BenchmarkID int
	XField      string
	YField      string
	Interval    float64
	MinOverride *float64
	MaxOverride *float64
}

func (g *TimeseriesPlotGenerator) Generate(ctx context.Context, opts PlotOptions) (string, string, error) {
	g.logger.WithFields(logrus.Fields{
		"benchmark_id": opts.BenchmarkID,
		"x_field":      opts.XField,
		"y_field":      opts.YField,
		"interval":     opts.Interval,
	}).Info("Generating timeseries plot")

	meta, err := g.dbClient.QueryMetaData(ctx, opts.BenchmarkID)
	if err != nil {
		return "", "", fmt.Errorf("failed to query metadata: %w", err)
	}

	metrics, err := g.dbClient.QueryMetrics(ctx, opts.BenchmarkID, opts.YField, opts.Interval)
	if err != nil {
		return "", "", fmt.Errorf("failed to query metrics: %w", err)
	}

	if len(metrics) == 0 {
		return "", "", fmt.Errorf("no data found for benchmark %d and field %s", opts.BenchmarkID, opts.YField)
	}

	xMapping, xExists := mappings.GetFieldMapping(opts.XField)
	if !xExists {
		return "", "", fmt.Errorf("unknown X field: %s", opts.XField)
	}

	yMapping, yExists := mappings.GetFieldMapping(opts.YField)
	if !yExists {
		return "", "", fmt.Errorf("unknown Y field: %s", opts.YField)
	}

	plotData, err := g.preparePlotData(meta, metrics, opts, xMapping, yMapping)
	if err != nil {
		return "", "", fmt.Errorf("failed to prepare plot data: %w", err)
	}

	wrapperData := g.prepareWrapperData(opts, yMapping)

	plotOutput, err := g.renderPlot(plotData)
	if err != nil {
		return "", "", fmt.Errorf("failed to render plot: %w", err)
	}

	wrapperOutput, err := g.renderWrapper(wrapperData)
	if err != nil {
		return "", "", fmt.Errorf("failed to render wrapper: %w", err)
	}

	g.logger.Info("Timeseries plot generated successfully")
	return plotOutput, wrapperOutput, nil
}

func (g *TimeseriesPlotGenerator) preparePlotData(
	meta *database.MetaData,
	metrics []database.MetricsDataPoint,
	opts PlotOptions,
	xMapping, yMapping mappings.FieldMapping,
) (*plotTemplate.PlotData, error) {

	containerMap := make(map[int][]database.MetricsDataPoint)
	for _, m := range metrics {
		containerMap[m.ContainerIndex] = append(containerMap[m.ContainerIndex], m)
	}

	var containerIndices []int
	for idx := range containerMap {
		containerIndices = append(containerIndices, idx)
	}
	sort.Ints(containerIndices)

	var plotSeries []plotTemplate.PlotSeries
	yMin := math.Inf(1)
	yMax := math.Inf(-1)
	xMin := math.Inf(1)
	xMax := math.Inf(-1)

	for _, containerIdx := range containerIndices {
		dataPoints := containerMap[containerIdx]
		if len(dataPoints) == 0 {
			continue
		}

		style := mappings.GetContainerStyle(containerIdx)
		series := plotTemplate.PlotSeries{
			ContainerIndex: containerIdx,
			ContainerName:  dataPoints[0].ContainerName,
			ContainerImage: dataPoints[0].ContainerImage,
			ContainerCore:  dataPoints[0].ContainerCore,
			Style:          style.ToTikzOptions(),
			LegendEntry:    dataPoints[0].ContainerName,
			Coordinates:    []string{},
		}

		aggregated := g.aggregateData(dataPoints, opts)

		for _, point := range aggregated {
			xVal := g.getFieldValue(point, opts.XField)
			yVal := g.getFieldValue(point, opts.YField)

			if xVal == nil || yVal == nil {
				continue
			}

			xFloat := g.toFloat64(xVal)
			yFloat := g.toFloat64(yVal)

			if opts.XField == "relative_time" {
				xFloat = xFloat / 1e9
			}

			coord := fmt.Sprintf("(%.6f,%.6f)", xFloat, yFloat)
			series.Coordinates = append(series.Coordinates, coord)

			if xFloat < xMin {
				xMin = xFloat
			}
			if xFloat > xMax {
				xMax = xFloat
			}
			if yFloat < yMin {
				yMin = yFloat
			}
			if yFloat > yMax {
				yMax = yFloat
			}
		}

		if len(series.Coordinates) > 0 {
			plotSeries = append(plotSeries, series)
		}
	}

	xMinStr, xMaxStr := g.determineAxisLimits(xMapping, nil, nil, xMin, xMax)
	yMinStr, yMaxStr := g.determineAxisLimits(yMapping, opts.MinOverride, opts.MaxOverride, yMin, yMax)

	plotData := &plotTemplate.PlotData{
		GeneratedDate:            time.Now().Format("2006-01-02 15:04:05"),
		BenchmarkID:              meta.BenchmarkID,
		BenchmarkName:            meta.BenchmarkName,
		Description:              meta.Description,
		BenchmarkStarted:         meta.BenchmarkStarted,
		BenchmarkFinished:        meta.BenchmarkFinished,
		DurationSeconds:          meta.DurationSeconds,
		MaxDurationSeconds:       meta.MaxDurationSeconds,
		SamplingFrequencyMs:      meta.SamplingFrequencyMs,
		TotalContainers:          meta.TotalContainers,
		TotalSamplingSteps:       meta.TotalSamplingSteps,
		TotalMeasurements:        meta.TotalMeasurements,
		UsedScheduler:            meta.UsedScheduler,
		SchedulerVersion:         meta.SchedulerVersion,
		DriverVersion:            meta.DriverVersion,
		Hostname:                 meta.Hostname,
		ExecutionHost:            meta.ExecutionHost,
		CPUVendor:                meta.CPUVendor,
		CPUModel:                 meta.CPUModel,
		TotalCPUCores:            meta.TotalCPUCores,
		CPUSockets:               meta.CPUSockets,
		CPUThreads:               meta.CPUThreads,
		L1CacheSizeKB:            meta.L1CacheSizeKB,
		L2CacheSizeKB:            meta.L2CacheSizeKB,
		L3CacheSizeMB:            meta.L3CacheSizeMB,
		L3CacheWays:              meta.L3CacheWays,
		MaxMemoryBandwidthMbps:   meta.MaxMemoryBandwidthMbps,
		KernelVersion:            meta.KernelVersion,
		OSInfo:                   meta.OSInfo,
		PerfEnabled:              meta.PerfEnabled,
		DockerStatsEnabled:       meta.DockerStatsEnabled,
		RDTEnabled:               meta.RDTEnabled,
		RDTSupported:             meta.RDTSupported,
		RDTAllocationSupported:   meta.RDTAllocationSupported,
		RDTMonitoringSupported:   meta.RDTMonitoringSupported,
		ProberEnabled:            meta.ProberEnabled,
		ProberImplementation:     meta.ProberImplementation,
		ProberIsolated:           meta.ProberIsolated,
		Title:                    yMapping.Label,
		XLabel:                   xMapping.Label,
		YLabel:                   yMapping.Label,
		XMin:                     xMinStr,
		XMax:                     xMaxStr,
		YMin:                     yMinStr,
		YMax:                     yMaxStr,
		Plots:                    plotSeries,
	}

	return plotData, nil
}

func (g *TimeseriesPlotGenerator) aggregateData(
	dataPoints []database.MetricsDataPoint,
	opts PlotOptions,
) []database.MetricsDataPoint {
	if opts.Interval <= 0 {
		return dataPoints
	}

	intervalNs := int64(opts.Interval * 1e9)
	buckets := make(map[int64][]database.MetricsDataPoint)

	for _, dp := range dataPoints {
		bucket := dp.RelativeTime / intervalNs
		buckets[bucket] = append(buckets[bucket], dp)
	}

	var aggregated []database.MetricsDataPoint
	for bucket := range buckets {
		points := buckets[bucket]
		if len(points) == 0 {
			continue
		}

		avgPoint := points[0]
		avgPoint.RelativeTime = bucket * intervalNs

		if yVal := g.getFieldValue(points[0], opts.YField); yVal != nil {
			sum := 0.0
			count := 0
			for _, p := range points {
				if val := g.getFieldValue(p, opts.YField); val != nil {
					sum += g.toFloat64(val)
					count++
				}
			}
			if count > 0 {
				avgPoint.Fields[opts.YField] = sum / float64(count)
			}
		}

		aggregated = append(aggregated, avgPoint)
	}

	sort.Slice(aggregated, func(i, j int) bool {
		return aggregated[i].RelativeTime < aggregated[j].RelativeTime
	})

	return aggregated
}

func (g *TimeseriesPlotGenerator) getFieldValue(dp database.MetricsDataPoint, field string) interface{} {
	switch field {
	case "relative_time":
		return dp.RelativeTime
	case "step_number":
		return dp.StepNumber
	default:
		return dp.Fields[field]
	}
}

func (g *TimeseriesPlotGenerator) toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case uint64:
		return float64(v)
	case int:
		return float64(v)
	default:
		return 0
	}
}

func (g *TimeseriesPlotGenerator) determineAxisLimits(
	mapping mappings.FieldMapping,
	minOverride, maxOverride *float64,
	dataMin, dataMax float64,
) (string, string) {
	var minStr, maxStr string

	if minOverride != nil {
		minStr = fmt.Sprintf("%.2f", *minOverride)
	} else if minVal, ok := mapping.Min.(float64); ok {
		minStr = fmt.Sprintf("%.2f", minVal)
	} else if mapping.Min == "auto" {
		minStr = fmt.Sprintf("%.2f", dataMin*0.95)
	} else {
		minStr = "0"
	}

	if maxOverride != nil {
		maxStr = fmt.Sprintf("%.2f", *maxOverride)
	} else if maxVal, ok := mapping.Max.(float64); ok {
		maxStr = fmt.Sprintf("%.2f", maxVal)
	} else if mapping.Max == "auto" {
		maxStr = fmt.Sprintf("%.2f", dataMax*1.05)
	} else {
		maxStr = "100"
	}

	return minStr, maxStr
}

func (g *TimeseriesPlotGenerator) prepareWrapperData(opts PlotOptions, yMapping mappings.FieldMapping) *wrapperTemplate.WrapperData {
	return &wrapperTemplate.WrapperData{
		GeneratedDate: time.Now().Format("2006-01-02 15:04:05"),
		BenchmarkID:   opts.BenchmarkID,
		YField:        opts.YField,
		PlotFileName:  fmt.Sprintf("benchmark-%d-%s.tikz", opts.BenchmarkID, opts.YField),
		ShortCaption:  yMapping.ShortLabel,
		Caption:       fmt.Sprintf("The %s per container", yMapping.Label),
	}
}

func (g *TimeseriesPlotGenerator) renderPlot(data *plotTemplate.PlotData) (string, error) {
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

func (g *TimeseriesPlotGenerator) renderWrapper(data *wrapperTemplate.WrapperData) (string, error) {
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
