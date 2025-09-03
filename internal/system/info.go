package system

import (
	"bufio"
	"os"
	"runtime"
	"strings"
)

type CPUInfo struct {
	Model     string
	Cores     int
	Threads   int
	Vendor    string
	Family    string
	Stepping  string
}

type OSInfo struct {
	OS           string
	Architecture string
	Kernel       string
	Hostname     string
}

func GetCPUInfo() *CPUInfo {
	info := &CPUInfo{
		Cores:   runtime.NumCPU(),
		Threads: runtime.GOMAXPROCS(0),
	}

	if runtime.GOOS == "linux" {
		if cpuInfo := parseCPUInfo(); cpuInfo != nil {
			info.Model = cpuInfo.Model
			info.Vendor = cpuInfo.Vendor
			info.Family = cpuInfo.Family
			info.Stepping = cpuInfo.Stepping
		}
	} else {
		info.Model = "Unknown CPU Model"
		info.Vendor = "Unknown Vendor"
	}

	return info
}

// GetOSInfo retrieves operating system information
func GetOSInfo() *OSInfo {
	hostname, _ := os.Hostname()
	
	info := &OSInfo{
		OS:           runtime.GOOS,
		Architecture: runtime.GOARCH,
		Hostname:     hostname,
	}

	if runtime.GOOS == "linux" {
		info.Kernel = getLinuxKernelVersion()
	}

	return info
}

// parseCPUInfo parses /proc/cpuinfo on Linux systems
func parseCPUInfo() *CPUInfo {
	file, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return nil
	}
	defer file.Close()

	info := &CPUInfo{}
	scanner := bufio.NewScanner(file)
	
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				continue
			}
			
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			
			switch key {
			case "model name":
				if info.Model == "" { 
					info.Model = value
				}
			case "vendor_id":
				if info.Vendor == "" {
					info.Vendor = value
				}
			case "cpu family":
				if info.Family == "" {
					info.Family = value
				}
			case "stepping":
				if info.Stepping == "" {
					info.Stepping = value
				}
			}
		}
	}

	return info
}

func getLinuxKernelVersion() string {
	data, err := os.ReadFile("/proc/version")
	if err != nil {
		return "Unknown"
	}
	
	version := string(data)
	if parts := strings.Fields(version); len(parts) >= 3 {
		return parts[2] 
	}
	
	return strings.TrimSpace(version)
}
