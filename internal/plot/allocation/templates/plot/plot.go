package templates

const PlotTemplate = `% Generated on {{.GeneratedDate}}
%
% Benchmark ID: {{.BenchmarkID}}
% Allocation Probe Index: {{.AllocationProbeIndex}}
% Container: {{.ContainerName}} (index {{.ContainerIndex}}, image: {{.ContainerImage}})
% Probe Started: {{.ProbeStarted}}
% Probe Finished: {{.ProbeFinished}}
% Total Probe Time: {{.TotalProbeTimeSeconds}}s
% Probe Aborted: {{.ProbeAborted}}
%
% Allocation Range:
% L3 Ways: {{.RangeMinL3Ways}}-{{.RangeMaxL3Ways}} (step: {{.RangeStepL3Ways}})
% Memory Bandwidth: {{.RangeMinMemBandwidth}}%-{{.RangeMaxMemBandwidth}}% (step: {{.RangeStepMemBandwidth}}%)
% Duration per Allocation: {{.RangeDurationPerAlloc}}s
% Order: {{.RangeOrder}}
% Isolated Others: {{.RangeIsolateOthers}}
%
\begin{tikzpicture}
\begin{axis}[
xlabel={ {{.XLabel}} },
ylabel={ {{.YLabel}} },
width=\textwidth,
height=0.4\textwidth,
xmin={{.XMin}}, xmax={{.XMax}},
ymin={{.YMin}}, ymax={{.YMax}},
ymajorgrids,
grid style=dashed,
legend columns=5,
legend pos=south east,
legend style={font=\small},{{if .IncludeLegend}}
legend style={font=\scriptsize, column sep=6pt},
legend to name=allocation-legend-{{.LabelID}},{{else}}
every axis legend/.code={\let\addlegendentry\relax},{{end}}
]

{{range .Plots}}
% Memory Bandwidth: {{.MemBandwidth}}%
\addplot+[{{.Style}}]
  coordinates {
{{range .Coordinates}}    {{.}}
{{end}}  };
\addlegendentry{ {{.LegendEntry}} }

{{end}}{{if .BaselineValue}}
% Baseline reference (90% of max allocation)
\addplot[black,dashed,thick,forget plot]
  coordinates {({{.XMin}},{{.BaselineValue}}) ({{.XMax}},{{.BaselineValue}})};
{{end}}
\end{axis}
\end{tikzpicture}
`

type PlotData struct {
	GeneratedDate         string
	BenchmarkID           int
	AllocationProbeIndex  int
	LabelID               string
	IncludeLegend         bool
	BaselineValue         *float64
	ContainerIndex        int
	ContainerName         string
	ContainerImage        string
	ContainerCores        string
	ProbeStarted          string
	ProbeFinished         string
	TotalProbeTimeSeconds float64
	ProbeAborted          bool
	RangeMinL3Ways        int
	RangeMaxL3Ways        int
	RangeMinMemBandwidth  float64
	RangeMaxMemBandwidth  float64
	RangeStepL3Ways       int
	RangeStepMemBandwidth float64
	RangeOrder            string
	RangeDurationPerAlloc int
	RangeIsolateOthers    bool
	XLabel                string
	YLabel                string
	XMin                  string
	XMax                  string
	YMin                  string
	YMax                  string
	Plots                 []PlotSeries
}

type PlotSeries struct {
	MemBandwidth float64
	Style        string
	LegendEntry  string
	Coordinates  []string
}
