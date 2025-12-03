package plot

import (
	"context"
	"fmt"

	"container-bench/internal/logging"
	"container-bench/internal/plot/allocation"
	"container-bench/internal/plot/database"
	"container-bench/internal/plot/polar"
	"container-bench/internal/plot/timeseries"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

type PlotType string

const (
	PlotTypeTimeseries PlotType = "timeseries"
	PlotTypePolar      PlotType = "polar"
	PlotTypeAllocation PlotType = "allocation"
)

type PlotManager struct {
	dbClient             *database.PlotDBClient
	timeseriesGenerator  *timeseries.TimeseriesPlotGenerator
	polarGenerator       *polar.PolarPlotGenerator
	allocationGenerator  *allocation.AllocationPlotGenerator
	logger               *logrus.Logger
}

func NewPlotManager() (*PlotManager, error) {
	logger := logging.GetLogger()

	godotenv.Load(".env")

	dbClient, err := database.NewPlotDBClient(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create database client: %w", err)
	}

	return &PlotManager{
		dbClient:            dbClient,
		timeseriesGenerator: timeseries.NewTimeseriesPlotGenerator(dbClient, logger),
		polarGenerator:      polar.NewPolarPlotGenerator(dbClient, logger),
		allocationGenerator: allocation.NewAllocationPlotGenerator(dbClient, logger),
		logger:              logger,
	}, nil
}

func (pm *PlotManager) Close() {
	if pm.dbClient != nil {
		pm.dbClient.Close()
	}
}

func (pm *PlotManager) GenerateTimeseriesPlot(
	benchmarkID int,
	xField, yField string,
	interval float64,
	minOverride, maxOverride *float64,
) (plotTikz, wrapperTex string, err error) {
	ctx := context.Background()

	opts := timeseries.PlotOptions{
		BenchmarkID: benchmarkID,
		XField:      xField,
		YField:      yField,
		Interval:    interval,
		MinOverride: minOverride,
		MaxOverride: maxOverride,
	}

	return pm.timeseriesGenerator.Generate(ctx, opts)
}

func (pm *PlotManager) GeneratePolarPlot(probeIndices []int, metricType string) (plotTikz, wrapperTex string, err error) {
	ctx := context.Background()

	opts := polar.PlotOptions{
		ProbeIndices: probeIndices,
		MetricType:   metricType,
	}

	return pm.polarGenerator.Generate(ctx, opts)
}

func (pm *PlotManager) GenerateAllocationPlot(
	benchmarkID int,
	allocationProbeIndex int,
	metric string,
) (plotTikz, wrapperTex string, err error) {
	ctx := context.Background()

	opts := allocation.PlotOptions{
		BenchmarkID:          benchmarkID,
		AllocationProbeIndex: allocationProbeIndex,
		Metric:               metric,
	}

	return pm.allocationGenerator.Generate(ctx, opts)
}

func (pm *PlotManager) GenerateAllocationPlotWithLegend(
	benchmarkID int,
	allocationProbeIndex int,
	metric string,
	includeLegend bool,
) (plotTikz, wrapperTex string, err error) {
	ctx := context.Background()

	opts := allocation.PlotOptions{
		BenchmarkID:          benchmarkID,
		AllocationProbeIndex: allocationProbeIndex,
		Metric:               metric,
		IncludeLegend:        includeLegend,
	}

	return pm.allocationGenerator.GenerateWithOptions(ctx, opts)
}

func (pm *PlotManager) GenerateAllocationPlotGroupWrapper(
	benchmarkID int,
	allocationProbeIndex int,
	metrics []string,
	labelID string,
) (groupWrapperTex string, err error) {
	ctx := context.Background()

	// Get meta data and container name from first metric
	allocations, err := pm.dbClient.QueryAllocationData(ctx, benchmarkID, allocationProbeIndex)
	if err != nil {
		return "", fmt.Errorf("failed to query allocation data: %w", err)
	}

	if len(allocations) == 0 {
		return "", fmt.Errorf("no allocation data found for benchmark %d, probe %d", benchmarkID, allocationProbeIndex)
	}

	meta, err := pm.dbClient.QueryMetaData(ctx, benchmarkID)
	if err != nil {
		pm.logger.WithError(err).Warn("Failed to query metadata, continuing without it")
	}

	containerName := allocations[0].ContainerName

	return pm.allocationGenerator.GenerateGroupWrapper(
		ctx,
		benchmarkID,
		allocationProbeIndex,
		metrics,
		labelID,
		containerName,
		meta,
	)
}
