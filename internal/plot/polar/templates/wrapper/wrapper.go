package templates

const WrapperTemplate = `% Generated on {{.GeneratedDate}}
% Probe Kernel: {{.ProbeKernel}} {{.ProbeVersion}}
% Metric Type: {{.MetricName}} ({{.MetricFullName}})
\begin{figure}[htbp]
    \centering
	\pgfplotslegendfromname{polar-legend-{{.LabelID}}} % chktex 8
    
	\vspace{0.5em}
    
	\resizebox{1\linewidth}{!}{\input{{"{"}}\currfiledir/{{.PlotFileName}}{{"}"}} } % chktex 27
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
