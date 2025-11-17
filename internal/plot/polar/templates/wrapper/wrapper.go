package templates

const WrapperTemplate = `% Generated on {{.GeneratedDate}}
% Probe Kernel: {{.ProbeKernel}} {{.ProbeVersion}}
\begin{figure}[H]
    \centering
    \resizebox{1\linewidth}{!}{\input{ {{.PlotFilePath}} }}
    \vspace{0.5em}
    \pgfplotslegendfromname{polar-legend-{{.LabelID}}}
    \caption[{{.ShortCaption}}]{ {{.Caption}} }
    \label{fig:sensitivity-{{.ProbeKernel}}-{{.LabelID}}}
\end{figure}
`

type WrapperData struct {
	GeneratedDate string
	ProbeKernel   string
	ProbeVersion  string
	PlotFilePath  string
	ShortCaption  string
	Caption       string
	LabelID       string
}
