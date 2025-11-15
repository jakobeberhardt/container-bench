package plot

import (
	"context"
	"fmt"

	"container-bench/internal/logging"
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
)

type PlotManager struct {
	dbClient            *database.PlotDBClient
	timeseriesGenerator *timeseries.TimeseriesPlotGenerator
	polarGenerator      *polar.PolarPlotGenerator
	logger              *logrus.Logger
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

func (pm *PlotManager) GeneratePolarPlot(probeIndices []int) (plotTikz, wrapperTex string, err error) {
	ctx := context.Background()

	opts := polar.PlotOptions{
		ProbeIndices: probeIndices,
	}

	return pm.polarGenerator.Generate(ctx, opts)
}
