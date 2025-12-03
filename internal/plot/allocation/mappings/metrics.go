package mappings

type MetricMapping struct {
	Label      string
	ShortLabel string
	Min        interface{}
	Max        interface{}
	Unit       string
}

var MetricMappings = map[string]MetricMapping{
	"ipc": {
		Label:      "Instructions Per Cycle (IPC)",
		ShortLabel: "IPC",
		Min:        0.0,
		Max:        "auto",
		Unit:       "",
	},
	"ipc_efficiency": {
		Label:      "IPC Efficiency (\\%)",
		ShortLabel: "IPC Efficiency (\\%)",
		Min:        0.0,
		Max:        100.0,
		Unit:       "\\%",
	},
	"cache_miss_rate": {
		Label:      "Cache Miss Rate (\\%)",
		ShortLabel: "Cache Miss Rate (\\%)",
		Min:        0.0,
		Max:        100.0,
		Unit:       "\\%",
	},
	"stalled_cycles": {
		Label:      "Stalled Cycles (\\%)",
		ShortLabel: "Stalled Cycles (\\%)",
		Min:        0.0,
		Max:        100.0,
		Unit:       "\\%",
	},
	"stalls_l3_miss_percent": {
		Label:      "L3 Stalls (\\%)",
		ShortLabel: "L3 Stalls (\\%)",
		Min:        0.0,
		Max:        100.0,
		Unit:       "\\%",
	},
	"l3_occupancy": {
		Label:      "L3 Cache Occupancy (MB)",
		ShortLabel: "L3 Occ.",
		Min:        0.0,
		Max:        "auto",
		Unit:       "MB",
	},
	"l3_occupancy_kb": {
		Label:      "L3 Cache Occupancy (KB)",
		ShortLabel: "L3 Occ.",
		Min:        0.0,
		Max:        "auto",
		Unit:       "KB",
	},
	"l3_occupancy_bytes": {
		Label:      "L3 Cache Occupancy (Bytes)",
		ShortLabel: "L3 Occ.",
		Min:        0.0,
		Max:        "auto",
		Unit:       "Bytes",
	},
	"mem_bandwidth_used": {
		Label:      "Memory Bandwidth Used (MB/s)",
		ShortLabel: "Mem BW",
		Min:        0.0,
		Max:        "auto",
		Unit:       "MB/s",
	},
	"mem_bandwidth_mbps": {
		Label:      "Memory Bandwidth (MB/s)",
		ShortLabel: "Mem BW",
		Min:        0.0,
		Max:        "auto",
		Unit:       "MB/s",
	},
	"mem_bandwidth_total": {
		Label:      "Total Memory Bandwidth (Bytes/s)",
		ShortLabel: "Total BW",
		Min:        0.0,
		Max:        "auto",
		Unit:       "Bytes/s",
	},
	"mem_bandwidth_local": {
		Label:      "Local Memory Bandwidth (Bytes/s)",
		ShortLabel: "Local BW",
		Min:        0.0,
		Max:        "auto",
		Unit:       "Bytes/s",
	},
	"cycles": {
		Label:      "CPU Cycles",
		ShortLabel: "Cycles",
		Min:        0.0,
		Max:        "auto",
		Unit:       "",
	},
	"instructions": {
		Label:      "Instructions",
		ShortLabel: "Instr.",
		Min:        0.0,
		Max:        "auto",
		Unit:       "",
	},
	"cache_references": {
		Label:      "Cache References",
		ShortLabel: "Cache Refs",
		Min:        0.0,
		Max:        "auto",
		Unit:       "",
	},
	"cache_misses": {
		Label:      "Cache Misses",
		ShortLabel: "Cache Miss",
		Min:        0.0,
		Max:        "auto",
		Unit:       "",
	},
	"branch_instructions": {
		Label:      "Branch Instructions",
		ShortLabel: "Branches",
		Min:        0.0,
		Max:        "auto",
		Unit:       "",
	},
	"branch_misses": {
		Label:      "Branch Misses",
		ShortLabel: "Br. Miss",
		Min:        0.0,
		Max:        "auto",
		Unit:       "",
	},
	"branch_miss_rate": {
		Label:      "Branch Miss Rate (\\%)",
		ShortLabel: "Br. Miss",
		Min:        0.0,
		Max:        100.0,
		Unit:       "\\%",
	},
}

func GetMetricMapping(metricName string) (MetricMapping, bool) {
	mapping, exists := MetricMappings[metricName]
	return mapping, exists
}
