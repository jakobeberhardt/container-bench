package polar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"text/template"
	"time"

	"container-bench/internal/plot/database"
	"container-bench/internal/plot/polar/mappings"
	plotTemplate "container-bench/internal/plot/polar/templates/plot"
	wrapperTemplate "container-bench/internal/plot/polar/templates/wrapper"

	"github.com/sirupsen/logrus"
)

type PolarPlotGenerator struct {
	dbClient *database.PlotDBClient
	logger   *logrus.Logger
}

func NewPolarPlotGenerator(dbClient *database.PlotDBClient, logger *logrus.Logger) *PolarPlotGenerator {
	return &PolarPlotGenerator{
		dbClient: dbClient,
		logger:   logger,
	}
}

type PlotOptions struct {
	ProbeIndices []int
}

func (g *PolarPlotGenerator) Generate(ctx context.Context, opts PlotOptions) (string, string, error) {
	g.logger.WithField("probe_indices", opts.ProbeIndices).Info("Generating polar plot")

	if len(opts.ProbeIndices) == 0 {
		return "", "", fmt.Errorf("no probe indices specified")
	}

	probes, err := g.dbClient.QueryProbes(ctx, opts.ProbeIndices)
	if err != nil {
		return "", "", fmt.Errorf("failed to query probes: %w", err)
	}

	if len(probes) == 0 {
		return "", "", fmt.Errorf("no probe data found for specified indices")
	}

	plotData := g.preparePlotData(probes, opts)
	wrapperData := g.prepareWrapperData(probes, opts)

	plotOutput, err := g.renderPlot(plotData)
	if err != nil {
		return "", "", fmt.Errorf("failed to render plot: %w", err)
	}

	wrapperOutput, err := g.renderWrapper(wrapperData)
	if err != nil {
		return "", "", fmt.Errorf("failed to render wrapper: %w", err)
	}

	g.logger.Info("Polar plot generated successfully")
	return plotOutput, wrapperOutput, nil
}

func (g *PolarPlotGenerator) preparePlotData(probes []database.ProbeData, opts PlotOptions) *plotTemplate.PlotData {
	sort.Slice(probes, func(i, j int) bool {
		return probes[i].ContainerIndex < probes[j].ContainerIndex
	})

	var probeSeries []plotTemplate.ProbeSeries
	probeKernel := ""
	probeVersion := ""
	if len(probes) > 0 {
		fullKernel := probes[0].UsedProbeKernel
		if idx := strings.Index(fullKernel, " v"); idx != -1 {
			probeKernel = fullKernel[:idx]
			probeVersion = fullKernel[idx+1:]
		} else {
			probeKernel = fullKernel
		}
	}

	for i, probe := range probes {
		style := mappings.GetProbeStyle(i)

		legendEntry := probe.ContainerCommand
		if len(legendEntry) > 50 {
			legendEntry = legendEntry[:47] + "..."
		}

		series := plotTemplate.ProbeSeries{
			BenchmarkID:           probe.BenchmarkID,
			ContainerIndex:        probe.ContainerIndex,
			ContainerName:         probe.ContainerName,
			ContainerImage:        probe.ContainerImage,
			ContainerCores:        probe.ContainerCores,
			ContainerCommand:      probe.ContainerCommand,
			ProbingContainerCores: probe.ProbingContainerCores,
			UsedProbeKernel:       probe.UsedProbeKernel,
			LLC:                   probe.LLC,
			MemRead:               probe.MemRead,
			MemWrite:              probe.MemWrite,
			Prefetch:              probe.Prefetch,
			Syscall:               probe.Syscall,
			ProbeTimeNs:           probe.ProbeTimeNs,
			Isolated:              probe.Isolated,
			Aborted:               probe.Aborted,
			Style:                 style.ToTikzOptions(),
			LegendEntry:           legendEntry,
		}

		probeSeries = append(probeSeries, series)
	}

	var indicesStr []string
	for _, idx := range opts.ProbeIndices {
		indicesStr = append(indicesStr, fmt.Sprintf("%d", idx))
	}

	labelID := g.generateLabelID(opts.ProbeIndices)

	return &plotTemplate.PlotData{
		GeneratedDate: time.Now().Format("2006-01-02 15:04:05"),
		ProbeKernel:   probeKernel,
		ProbeVersion:  probeVersion,
		ProbeIndices:  strings.Join(indicesStr, ", "),
		LabelID:       labelID,
		Probes:        probeSeries,
	}
}

func (g *PolarPlotGenerator) prepareWrapperData(probes []database.ProbeData, opts PlotOptions) *wrapperTemplate.WrapperData {
	probeKernel := ""
	probeVersion := ""
	if len(probes) > 0 {
		fullKernel := probes[0].UsedProbeKernel
		if idx := strings.Index(fullKernel, " v"); idx != -1 {
			probeKernel = fullKernel[:idx]
			probeVersion = fullKernel[idx+1:]
		} else {
			probeKernel = fullKernel
		}
	}

	labelID := g.generateLabelID(opts.ProbeIndices)

	return &wrapperTemplate.WrapperData{
		GeneratedDate: time.Now().Format("2006-01-02 15:04:05"),
		ProbeKernel:   probeKernel,
		ProbeVersion:  probeVersion,
		PlotFilePath:  fmt.Sprintf("probe-sensitivity-%s-%s.tikz.tex", probeKernel, labelID),
		ShortCaption:  fmt.Sprintf("Example of Probes"),
		Caption:       fmt.Sprintf("Sensitivity using the \\texttt{%s} probe kernel of different applications.", probeKernel),
		LabelID:       labelID,
	}
}

func (g *PolarPlotGenerator) generateLabelID(indices []int) string {
	var parts []string
	for _, idx := range indices {
		parts = append(parts, fmt.Sprintf("%d", idx))
	}
	input := strings.Join(parts, "-")
	hash := sha256.Sum256([]byte(input))
	return fmt.Sprintf("%x", hash[:6])
}

func (g *PolarPlotGenerator) renderPlot(data *plotTemplate.PlotData) (string, error) {
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

func (g *PolarPlotGenerator) renderWrapper(data *wrapperTemplate.WrapperData) (string, error) {
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
