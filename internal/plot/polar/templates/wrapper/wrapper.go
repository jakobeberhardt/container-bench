package templates

const WrapperTemplate = `% Generated on {{.GeneratedDate}}
% Probe Kernel: {{.ProbeKernel}} {{.ProbeVersion}}
% Metric Type: {{.MetricName}} ({{.MetricFullName}})
\begin{figure}[H]
    \centering
	\resizebox{1\linewidth}{!}{\input{{"{"}}\currfiledir/{{.PlotFileName}}{{"}"}} }
    % \vspace{0.5em}
    % \pgfplotslegendfromname{polar-legend-{{.LabelID}}}
    \caption[{{.ShortCaption}}]{ {{.Caption}} }
    \label{fig:sensitivity-{{.ProbeKernel}}-{{.LabelID}}}
\end{figure}
`

type WrapperData struct {
	GeneratedDate  string
	ProbeKernel    string
	ProbeVersion   string
	PlotFileName   string
	ShortCaption   string
	Caption        string
	LabelID        string
	MetricName     string
	MetricFullName string
}
