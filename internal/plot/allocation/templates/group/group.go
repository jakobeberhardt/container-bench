package group

const GroupWrapperTemplate = `% Generated on {{.GeneratedDate}}
% Benchmark ID: {{.BenchmarkID}}
% Allocation Probe Index: {{.AllocationProbeIndex}}
% Metrics: {{range $i, $m := .Metrics}}{{if $i}}, {{end}}{{$m}}{{end}}

\begin{figure}[htbp]
    \centering
    \pgfplotslegendfromname{allocation-legend-{{.LabelID}}}
    
    \vspace{0.5em}
    
{{range .Subfigures}}    \begin{subfigure}{\linewidth}
        \centering
        \resizebox{\linewidth}{!}{\input{{"{"}}\currfiledir {{.PlotFileName}}{{"}"}} }
        \caption{{"{"}}{{.Caption}}{{"}"}}
    \end{subfigure}
    
{{end}}    % TODO: Add caption
	\caption[{{.ShortCaption}}]{{"{"}}{{.Caption}}{{"}"}}
    \label{fig:benchmark-{{.BenchmarkID}}-allocation-{{.AllocationProbeIndex}}}
	
\end{figure}
`

type GroupWrapperData struct {
	GeneratedDate        string
	BenchmarkID          int
	AllocationProbeIndex int
	Metrics              []string
	LabelID              string
	Subfigures           []SubfigureData
	ShortCaption         string
	Caption              string
}

type SubfigureData struct {
	PlotFileName string
	Caption      string
}
