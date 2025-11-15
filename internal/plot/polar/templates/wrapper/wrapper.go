package templates

const WrapperTemplate = `% Generated on {{.GeneratedDate}}
% Probe Kernel: {{.ProbeKernel}}

\begin{figure}[H]
    \centering
    \resizebox{1\linewidth}{!}{\input{./{{.PlotFileName}} }}
    \caption[{{.ShortCaption}}]{ {{.Caption}} }
    \label{fig:sensitivity-{{.LabelID}}}
\end{figure}
`

type WrapperData struct {
	GeneratedDate string
	ProbeKernel   string
	PlotFileName  string
	ShortCaption  string
	Caption       string
	LabelID       string
}
