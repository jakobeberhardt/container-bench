package version

import "fmt"

const (
	Version = "1.2.1"
	
	BuildDate = "2025-08-29"
	
	DefaultSchedulerVersion = "1.0.0"
	RDTSchedulerVersion     = "1.1.0"
	AdaptiveSchedulerVersion = "2.0.0"
)

func GetVersionInfo() string {
	return fmt.Sprintf("container-bench v%s (built %s)", Version, BuildDate)
}

func GetSchedulerVersion(implementation string) string {
	switch implementation {
	case "default":
		return DefaultSchedulerVersion
	case "rdt":
		return RDTSchedulerVersion
	case "adaptive":
		return AdaptiveSchedulerVersion
	default:
		return "unknown"
	}
}
