package templates

const PlotTemplate = `% Generated on {{.GeneratedDate}}
%
% Probe Kernel: {{.ProbeKernel}}
% Probe Indices: {{.ProbeIndices}}
%

\begin{tikzpicture}
\begin{polaraxis}[
  width=9cm,
  grid=both,
  ymin=0, ymax=1,
  xtick={0,72,144,216,288},
  xticklabels={
    LLC,
    Mem Read,
    Mem Write,
    Syscall,
    Prefetch
  },
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

\addplot+[{{.Style}}] coordinates {
  (0,   {{.LLC}})      % LLC
  (72,  {{.MemRead}})  % Mem Read
  (144, {{.MemWrite}}) % Mem Write
  (216, {{.Syscall}})  % Syscall
  (288, {{.Prefetch}}) % Prefetch
  (0,   {{.LLC}})      % Close the loop
};
% TODO: Add command
\addlegendentry{ {{.LegendEntry}} };

{{end}}
\end{polaraxis}
\end{tikzpicture}
`

type PlotData struct {
	GeneratedDate string
	ProbeKernel   string
	ProbeIndices  string
	Probes        []ProbeSeries
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
