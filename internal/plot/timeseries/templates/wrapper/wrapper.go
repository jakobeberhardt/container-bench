package templates

const WrapperTemplate = `% Generated on {{.GeneratedDate}}
% Benchmark ID: {{.BenchmarkID}}
% Field: {{.YField}}
\begin{center}
    \begin{figure}[H]
    \centering
    \resizebox{1\linewidth}{!}{\input{./{{.PlotFileName}} }}
    % TODO: Add short and long caption
    \caption[{{.ShortCaption}}]{ {{.Caption}} }
    \label{fig:benchmark-{{.BenchmarkID}}-{{.YField}}}
    \end{figure}
\end{center}
`

type WrapperData struct {
	GeneratedDate string
	BenchmarkID   int
	YField        string
	PlotFileName  string
	ShortCaption  string
	Caption       string
}
