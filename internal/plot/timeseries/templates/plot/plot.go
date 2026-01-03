package templates

const PlotTemplate = `% Generated on {{.GeneratedDate}}
%
% Benchmark ID: {{.BenchmarkID}}
% Benchmark Name: {{.BenchmarkName}}
% Description: {{.Description}}
% Started: {{.BenchmarkStarted}}
% Finished: {{.BenchmarkFinished}}
% Duration: {{.DurationSeconds}}s (max: {{.MaxDurationSeconds}}s)
% Sampling Frequency: {{.SamplingFrequencyMs}}ms
% Total Containers: {{.TotalContainers}}
% Total Sampling Steps: {{.TotalSamplingSteps}}
% Total Measurements: {{.TotalMeasurements}}
% Scheduler: {{.UsedScheduler}} (v{{.SchedulerVersion}})
% Driver Version: {{.DriverVersion}}
%
% Host Information:
% Hostname: {{.Hostname}} (exec: {{.ExecutionHost}})
% CPU: {{.CPUVendor}} {{.CPUModel}} ({{.TotalCPUCores}} cores, {{.CPUThreads}} threads, {{.CPUSockets}} sockets)
% L1 Cache: {{.L1CacheSizeKB}}KB
% L2 Cache: {{.L2CacheSizeKB}}KB
% L3 Cache: {{.L3CacheSizeMB}}MB ({{.L3CacheWays}} ways)
% Max Memory Bandwidth: {{.MaxMemoryBandwidthMbps}}MB/s
% Kernel: {{.KernelVersion}}
% OS: {{.OSInfo}}
%
% Features Enabled:
% Perf: {{.PerfEnabled}}
% Docker Stats: {{.DockerStatsEnabled}}
% RDT: {{.RDTEnabled}} (supported: {{.RDTSupported}}, alloc: {{.RDTAllocationSupported}}, mon: {{.RDTMonitoringSupported}})
% Prober: {{.ProberEnabled}} (impl: {{.ProberImplementation}}, isolated: {{.ProberIsolated}})
%
\begin{tikzpicture}
	\begin{axis}[
		% title={ {{.Title}} },
		xlabel={ {{.XLabel}} },
		ylabel={ {{.YLabel}} },
		width=\textwidth,
		height=1\textwidth,
		xmin={{.XMin}}, xmax={{.XMax}},
		ymin={{.YMin}}, ymax={{.YMax}},
		ymajorgrids,
		grid style=dashed,
		legend columns=2,
		legend pos=north east,
		% legend style={font=\scriptsize, column sep=6pt},
		% legend to name=legend-{{.BenchmarkID}}-{{.Fieldname}}
		% every axis legend/.code={\let\addlegendentry\relax},
	]

{{range .Plots}}
% Container: {{.ContainerName}} (index {{.ContainerIndex}}, image: {{.ContainerImage}}, core: {{.ContainerCore}})
% addplot source: benchmark_id={{$.BenchmarkID}} field={{$.Fieldname}} container_index={{.ContainerIndex}}
\addplot+[{{.Style}}]
  coordinates {
{{range .Coordinates}}    {{.}}
{{end}}  };
\addlegendentry{ {{.LegendEntry}} }

{{end}}
	\end{axis}
\end{tikzpicture}
`

type PlotData struct {
	GeneratedDate            string
	BenchmarkID              int
	BenchmarkName            string
	Description              string
	BenchmarkStarted         string
	BenchmarkFinished        string
	DurationSeconds          int64
	MaxDurationSeconds       int64
	SamplingFrequencyMs      int64
	TotalContainers          int64
	TotalSamplingSteps       int64
	TotalMeasurements        int64
	UsedScheduler            string
	SchedulerVersion         string
	DriverVersion            string
	Hostname                 string
	ExecutionHost            string
	CPUVendor                string
	CPUModel                 string
	TotalCPUCores            int64
	CPUSockets               int64
	CPUThreads               int64
	L1CacheSizeKB            float64
	L2CacheSizeKB            float64
	L3CacheSizeMB            float64
	L3CacheWays              int64
	MaxMemoryBandwidthMbps   int64
	KernelVersion            string
	OSInfo                   string
	PerfEnabled              bool
	DockerStatsEnabled       bool
	RDTEnabled               bool
	RDTSupported             bool
	RDTAllocationSupported   bool
	RDTMonitoringSupported   bool
	ProberEnabled            bool
	ProberImplementation     string
	ProberIsolated           bool
	Title                    string
	XLabel                   string
	YLabel                   string
	Fieldname                string
	XMin                     string
	XMax                     string
	YMin                     string
	YMax                     string
	Plots                    []PlotSeries
}

type PlotSeries struct {
	ContainerIndex int
	ContainerName  string
	ContainerImage string
	ContainerCore  string
	Style          string
	LegendEntry    string
	Coordinates    []string
}
