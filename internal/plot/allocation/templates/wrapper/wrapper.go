package templates

const WrapperTemplate = `% Generated on {{.GeneratedDate}}
% Benchmark ID: {{.BenchmarkID}}
% Allocation Probe Index: {{.AllocationProbeIndex}}
% Metric: {{.Metric}}
\begin{center}
\begin{figure}[H]
    \centering
    \resizebox{1\linewidth}{!}{\input{ {{.PlotFileName}} }}
    % \vspace{0.5em}
    % \pgfplotslegendfromname{allocation-legend-{{.LabelID}}}
    % TODO: Add Caption
    \caption[{{.ShortCaption}}]{ {{.Caption}} }
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
	ShortCaption         string
	Caption              string
}
