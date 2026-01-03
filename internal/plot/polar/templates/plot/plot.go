package templates

const PlotTemplate = `% Generated on {{.GeneratedDate}}
% Probe Kernel: {{.ProbeKernel}} {{.ProbeVersion}}
% Metric Type: {{.MetricName}} ({{.MetricFullName}})
\begin{tikzpicture}
\begin{polaraxis}[
  width=9cm,
  grid=both,
  ymin=0, ymax=0.3,
  xtick={0,72,144,216,288},
  xticklabels={
    {\hspace{1ex}LLC},
    Mem Read,
    Mem Write,
    Syscall,
    Prefetch
  },
  legend columns=2,
  % legend style={font=\scriptsize, column sep=6pt},
  % legend to name=polar-legend-{{.LabelID}},
  every axis legend/.code={\let\addlegendentry\relax},
]

{{range .Probes}}
% Probe Index: {{.ContainerIndex}}, Benchmark ID: {{.BenchmarkID}}
% Probe Kernel: {{.UsedProbeKernel}}, Duration: {{.ProbeTimeNs}}ns
% Probe Cores: {{.ProbingContainerCores}}, Isolated: {{.Isolated}}, Aborted: {{.Aborted}}
%
% Probed Container: {{.ContainerName}} (index {{.ContainerIndex}})
% Container Image: {{.ContainerImage}}
% Container Command: {{.ContainerCommand}}
% Container Cores: {{.ContainerCores}}
% addplot source: benchmark_id={{.BenchmarkID}} probe_container_index={{.ContainerIndex}} metric={{$.MetricName}}
\addplot+[{{.Style}}] coordinates {
  (0,   {{.LLC}})      % LLC sensitivity
  (72,  {{.MemRead}})  % Mem Read sensitivity
  (144, {{.MemWrite}}) % Mem Write sensitivity
  (216, {{.Syscall}})  % Syscall sensitivity
  (288, {{.Prefetch}}) % Prefetch sensitivity
  (0,   {{.LLC}})      % Close the loop
};
% TODO: Add command
\addlegendentry{ {{.LegendEntry}} };

{{end}}
\end{polaraxis}
\end{tikzpicture}
`

type PlotData struct {
	GeneratedDate  string
	ProbeKernel    string
	ProbeVersion   string
	ProbeIndices   string
	LabelID        string
	MetricName     string
	MetricFullName string
	Probes         []ProbeSeries
}

type ProbeSeries struct {
	BenchmarkID           int
	ContainerIndex        int
	ContainerName         string
	ContainerImage        string
	ContainerCores        string
	ContainerCommand      string
	ProbingContainerCores string
	UsedProbeKernel       string
	LLC                   float64
	MemRead               float64
	MemWrite              float64
	Prefetch              float64
	Syscall               float64
	ProbeTimeNs           int64
	Isolated              bool
	Aborted               bool
	Style                 string
	LegendEntry           string
}
