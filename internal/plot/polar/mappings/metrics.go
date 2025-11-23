package mappings

// display information for a probe metric type
type MetricInfo struct {
	Name        string
	FullName    string
	Description string
}

var metricMappings = map[string]MetricInfo{
	"ipc": {
		Name:        "IPC",
		FullName:    "Instructions Per Cycle",
		Description: "Instructions Per Cycle (IPC) sensitivity",
	},
	"scp": {
		Name:        "SCP",
		FullName:    "Stalled Cycles Percentage",
		Description: "Stalled Cycles Percentage (SCP) sensitivity",
	},
}

// GetMetricInfo returns the display information for a metric type
func GetMetricInfo(metricType string) MetricInfo {
	if info, ok := metricMappings[metricType]; ok {
		return info
	}
	// Default fallback
	return MetricInfo{
		Name:        metricType,
		FullName:    metricType,
		Description: metricType + " sensitivity",
	}
}
