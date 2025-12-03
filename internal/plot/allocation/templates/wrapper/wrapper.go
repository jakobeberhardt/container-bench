package templates

const WrapperTemplate = `% Generated on {{.GeneratedDate}}
% Benchmark ID: {{.BenchmarkID}}
% Allocation Probe Index: {{.AllocationProbeIndex}}
% Metric: {{.Metric}}
\begin{center}
\begin{figure}[H]
    \centering
    \resizebox{1\linewidth}{!}{\input{ {{.PlotFileName}} }}
    \caption[Allocation Probe for {{.ContainerName}}]{Metrics for different allocation combinations for {{.ContainerName}}}
    \label{fig:benchmark-{{.BenchmarkID}}-allocation-{{.AllocationProbeIndex}}-{{.Metric}}}
    \end{figure}
\end{center}
`

type WrapperData struct {
	GeneratedDate        string
	BenchmarkID          int
	AllocationProbeIndex int
	Metric               string
	LabelID              string
	PlotFileName         string
	ContainerName        string
}
