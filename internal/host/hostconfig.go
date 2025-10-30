package host

import (
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"container-bench/internal/logging"

	"github.com/intel/goresctrl/pkg/rdt"
	"github.com/klauspost/cpuid/v2"
	"github.com/sirupsen/logrus"
	"github.com/zcalusic/sysinfo"
)

// HostConfig contains comprehensive host system configuration information.
// This is the single source of truth for all host-related information.
type HostConfig struct {
	// CPU Information
	CPUVendor string
	CPUModel  string

	// CPU Topology
	Topology CPUTopology

	// Cache Hierarchy
	L1Cache CacheConfig
	L2Cache CacheConfig
	L3Cache L3CacheConfig

	// RDT Configuration
	RDT RDTConfig

	// System Information
	Hostname      string
	OSInfo        string
	KernelVersion string

	logger *logrus.Logger
}

type CPUTopology struct {
	PhysicalCores  int
	LogicalCores   int // with hyperthreading
	ThreadsPerCore int
	Sockets        int              // Number of CPU sockets
	CoresPerSocket int              // Physical cores per socket
	CoreMap        map[int]CoreInfo // Mapping of logical CPU ID to detailed core info
}

type CoreInfo struct {
	LogicalID  int   // Logical CPU ID (0, 1, 2, ...)
	PhysicalID int   // Physical package/socket ID
	CoreID     int   // Core ID within the socket
	Siblings   []int // Hyperthread sibling logical CPU IDs
}

// CacheConfig contains cache configuration for L1/L2 caches
type CacheConfig struct {
	SizeBytes int64
	SizeKB    float64
	SizeMB    float64
	LineSize  int
}

// L3CacheConfig contains L3 (LLC) cache configuration with RDT-specific information
type L3CacheConfig struct {
	SizeBytes    int64
	SizeKB       float64
	SizeMB       float64
	LineSize     int      // Cache line size in bytes
	CacheIDs     []uint64 // Available L3 cache IDs
	WaysPerCache int      // Number of ways per cache (from RDT)
	BytesPerWay  int64
	MaxBitmask   uint64 // Maximum allocation bitmask (from RDT)
}

// RDTConfig contains Intel RDT system configuration
type RDTConfig struct {
	Supported           bool
	MonitoringSupported bool
	AllocationSupported bool
	AvailableClasses    []string
	MonitoringFeatures  map[string][]string // MonResource -> features
	MaxCLOSIDs          int                 // Maximum number of CLOSIDs

	// NYI: Memory bandwidth information
	// No reliable source available for querying actual system memory bandwidth.
	// Would require platform-specific detection or manual configuration.
	// MaxMemoryBandwidthMBps int64
}

var (
	globalHostConfig *HostConfig
	hostConfigOnce   sync.Once
)

// GetHostConfig returns the singleton HostConfig instance.
func GetHostConfig() (*HostConfig, error) {
	var err error
	hostConfigOnce.Do(func() {
		globalHostConfig, err = initializeHostConfig()
	})
	return globalHostConfig, err
}

func initializeHostConfig() (*HostConfig, error) {
	logger := logging.GetLogger()
	logger.Info("Initializing host configuration")

	config := &HostConfig{
		logger: logger,
	}

	if err := config.initSystemInfo(); err != nil {
		return nil, fmt.Errorf("failed to initialize system info: %v", err)
	}

	if err := config.initCPUInfo(); err != nil {
		return nil, fmt.Errorf("failed to initialize CPU info: %v", err)
	}

	// Initialize CPU topology
	if err := config.initCPUTopology(); err != nil {
		logger.WithError(err).Warn("Failed to initialize CPU topology, using basic info")
		config.setBasicTopology()
	}

	if err := config.initCacheInfo(); err != nil {
		logger.WithError(err).Warn("Failed to initialize cache info")
		return nil, fmt.Errorf("failed to initialize cache info: %v", err)
	}

	if err := config.initRDTInfo(); err != nil {
		logger.WithError(err).Warn("Failed to initialize RDT info, RDT features disabled")
		config.RDT.Supported = false
	}

	logger.WithFields(logrus.Fields{
		"cpu_vendor":     config.CPUVendor,
		"cpu_model":      config.CPUModel,
		"physical_cores": config.Topology.PhysicalCores,
		"logical_cores":  config.Topology.LogicalCores,
		"sockets":        config.Topology.Sockets,
		"l3_cache_mb":    config.L3Cache.SizeMB,
		"cache_ways":     config.L3Cache.WaysPerCache,
		"rdt_supported":  config.RDT.Supported,
	}).Info("Host configuration initialized")

	// Print detailed configuration at debug level
	config.logDetailedConfiguration()

	return config, nil
}

// initSystemInfo initializes system information using sysinfo package
func (hc *HostConfig) initSystemInfo() error {
	var si sysinfo.SysInfo
	si.GetSysInfo()

	hc.Hostname = si.Node.Hostname
	hc.OSInfo = fmt.Sprintf("%s %s", si.OS.Name, si.OS.Architecture)
	hc.KernelVersion = si.Kernel.Release

	return nil
}

// initCPUInfo initializes CPU information using cpuid package
func (hc *HostConfig) initCPUInfo() error {
	// Get CPU information from cpuid
	cpu := cpuid.CPU

	hc.CPUVendor = cpu.VendorString
	hc.CPUModel = cpu.BrandName

	hc.logger.WithFields(logrus.Fields{
		"vendor": hc.CPUVendor,
		"model":  hc.CPUModel,
	}).Debug("CPU info initialized from cpuid")

	return nil
}

// initCPUTopology initializes detailed CPU topology from sysfs
func (hc *HostConfig) initCPUTopology() error {
	cpu := cpuid.CPU

	hc.Topology.PhysicalCores = cpu.PhysicalCores
	hc.Topology.LogicalCores = cpu.LogicalCores
	hc.Topology.ThreadsPerCore = cpu.ThreadsPerCore

	// Parse topology from sysfs for detailed core mapping
	coreMap := make(map[int]CoreInfo)
	socketSet := make(map[int]bool)

	for i := 0; i < hc.Topology.LogicalCores; i++ {
		cpuPath := fmt.Sprintf("/sys/devices/system/cpu/cpu%d/topology", i)

		// Read physical package ID (socket)
		physicalID, err := readIntFromFile(filepath.Join(cpuPath, "physical_package_id"))
		if err != nil {
			continue
		}
		socketSet[physicalID] = true

		// Read core ID
		coreID, err := readIntFromFile(filepath.Join(cpuPath, "core_id"))
		if err != nil {
			continue
		}

		// Read thread siblings
		siblingsStr, err := readStringFromFile(filepath.Join(cpuPath, "thread_siblings_list"))
		if err != nil {
			continue
		}
		siblings := parseCPUList(siblingsStr)

		coreMap[i] = CoreInfo{
			LogicalID:  i,
			PhysicalID: physicalID,
			CoreID:     coreID,
			Siblings:   siblings,
		}
	}

	hc.Topology.CoreMap = coreMap
	hc.Topology.Sockets = len(socketSet)

	if hc.Topology.Sockets > 0 && hc.Topology.PhysicalCores > 0 {
		hc.Topology.CoresPerSocket = hc.Topology.PhysicalCores / hc.Topology.Sockets
	}

	hc.logger.WithFields(logrus.Fields{
		"physical_cores":   hc.Topology.PhysicalCores,
		"logical_cores":    hc.Topology.LogicalCores,
		"threads_per_core": hc.Topology.ThreadsPerCore,
		"sockets":          hc.Topology.Sockets,
		"cores_per_socket": hc.Topology.CoresPerSocket,
		"core_mappings":    len(hc.Topology.CoreMap),
	}).Debug("CPU topology initialized")

	return nil
}

// setBasicTopology sets basic topology when detailed topology cannot be determined
func (hc *HostConfig) setBasicTopology() {
	cpu := cpuid.CPU

	hc.Topology.PhysicalCores = cpu.PhysicalCores
	hc.Topology.LogicalCores = cpu.LogicalCores
	hc.Topology.ThreadsPerCore = cpu.ThreadsPerCore
	hc.Topology.Sockets = 1
	hc.Topology.CoresPerSocket = cpu.PhysicalCores
	hc.Topology.CoreMap = make(map[int]CoreInfo)
}

// initCacheInfo initializes cache hierarchy using cpuid
func (hc *HostConfig) initCacheInfo() error {
	cpu := cpuid.CPU

	if cpu.Cache.L1D > 0 {
		hc.L1Cache.SizeBytes = int64(cpu.Cache.L1D)
		hc.L1Cache.SizeKB = float64(cpu.Cache.L1D) / 1024.0
		hc.L1Cache.SizeMB = float64(cpu.Cache.L1D) / (1024.0 * 1024.0)
	}
	hc.L1Cache.LineSize = cpu.CacheLine

	if cpu.Cache.L2 > 0 {
		hc.L2Cache.SizeBytes = int64(cpu.Cache.L2)
		hc.L2Cache.SizeKB = float64(cpu.Cache.L2) / 1024.0
		hc.L2Cache.SizeMB = float64(cpu.Cache.L2) / (1024.0 * 1024.0)
	}
	hc.L2Cache.LineSize = cpu.CacheLine

	if cpu.Cache.L3 > 0 {
		hc.L3Cache.SizeBytes = int64(cpu.Cache.L3)
		hc.L3Cache.SizeKB = float64(cpu.Cache.L3) / 1024.0
		hc.L3Cache.SizeMB = float64(cpu.Cache.L3) / (1024.0 * 1024.0)
	} else {
		return fmt.Errorf("L3 cache size not available from cpuid")
	}
	hc.L3Cache.LineSize = cpu.CacheLine

	if rdt.MonSupported() {
		if err := hc.initRDTCacheInfo(); err != nil {
			hc.logger.WithError(err).Warn("Failed to initialize RDT cache info from resctrl, RDT-specific fields will be unset")
			// Leave RDT-specific fields at their zero values
		}
	} else {
		hc.logger.Debug("RDT not supported, RDT-specific cache fields will be unset")
		// Leave RDT-specific fields (WaysPerCache, CacheIDs, MaxBitmask, bytesPerWay) unset
	}

	hc.logger.WithFields(logrus.Fields{
		"l1_cache_kb":     hc.L1Cache.SizeKB,
		"l2_cache_kb":     hc.L2Cache.SizeKB,
		"l3_cache_mb":     hc.L3Cache.SizeMB,
		"cache_line_size": hc.L3Cache.LineSize,
		"l3_cache_ways":   hc.L3Cache.WaysPerCache,
	}).Debug("Cache hierarchy initialized")

	return nil
}

// initRDTCacheInfo initializes RDT-specific cache information from resctrl filesystem
func (hc *HostConfig) initRDTCacheInfo() error {
	// Read CBM (Cache Bit Mask) from resctrl to determine cache ways
	cbmMaskPath := "/sys/fs/resctrl/info/L3/cbm_mask"
	cbmStr, err := readStringFromFile(cbmMaskPath)
	if err != nil {
		return fmt.Errorf("failed to read CBM mask: %v", err)
	}

	// Parse hex mask to determine number of ways
	cbmValue, err := strconv.ParseUint(strings.TrimSpace(cbmStr), 16, 64)
	if err != nil {
		return fmt.Errorf("failed to parse CBM mask: %v", err)
	}

	// Count the number of bits set in the mask
	hc.L3Cache.WaysPerCache = bits.OnesCount64(cbmValue)
	hc.L3Cache.MaxBitmask = cbmValue

	if hc.L3Cache.WaysPerCache > 0 {
		hc.L3Cache.BytesPerWay = hc.L3Cache.SizeBytes / int64(hc.L3Cache.WaysPerCache)
	}

	// Read cache IDs typically one per socket
	// For simplicity, create cache IDs based on socket count
	hc.L3Cache.CacheIDs = make([]uint64, 0, hc.Topology.Sockets)
	for i := 0; i < hc.Topology.Sockets; i++ {
		hc.L3Cache.CacheIDs = append(hc.L3Cache.CacheIDs, uint64(i))
	}

	hc.logger.WithFields(logrus.Fields{
		"cbm_mask_hex":  fmt.Sprintf("0x%x", cbmValue),
		"cache_ways":    hc.L3Cache.WaysPerCache,
		"bytes_per_way": hc.L3Cache.BytesPerWay,
		"cache_ids":     hc.L3Cache.CacheIDs,
	}).Debug("RDT cache info initialized from resctrl")

	return nil
}

// initRDTInfo initializes Intel RDT information
func (hc *HostConfig) initRDTInfo() error {
	hc.RDT.Supported = rdt.MonSupported()
	hc.RDT.MonitoringSupported = rdt.MonSupported()

	if !hc.RDT.Supported {
		return nil
	}

	// Get available RDT classes
	classes := rdt.GetClasses()
	for _, class := range classes {
		hc.RDT.AvailableClasses = append(hc.RDT.AvailableClasses, class.Name())
	}

	// Get monitoring features
	monFeatures := rdt.GetMonFeatures()
	hc.RDT.MonitoringFeatures = make(map[string][]string)
	for resource, features := range monFeatures {
		hc.RDT.MonitoringFeatures[string(resource)] = features
	}

	// Check if allocation is supported
	hc.RDT.AllocationSupported = len(hc.RDT.AvailableClasses) > 0

	// Read maximum number of CLOSIDs from resctrl
	numClosidsPath := "/sys/fs/resctrl/info/L3/num_closids"
	if numClosids, err := readIntFromFile(numClosidsPath); err == nil {
		hc.RDT.MaxCLOSIDs = numClosids
	}

	hc.logger.WithFields(logrus.Fields{
		"monitoring_supported": hc.RDT.MonitoringSupported,
		"allocation_supported": hc.RDT.AllocationSupported,
		"available_classes":    len(hc.RDT.AvailableClasses),
		"monitoring_features":  len(hc.RDT.MonitoringFeatures),
		"max_closids":          hc.RDT.MaxCLOSIDs,
	}).Debug("RDT info initialized")

	return nil
}

// calculates L3 cache utilization percentage
func (hc *HostConfig) GetL3CacheUtilizationPercent(occupancyBytes uint64) float64 {
	if hc.L3Cache.SizeBytes == 0 {
		return 0.0
	}
	return float64(occupancyBytes) / float64(hc.L3Cache.SizeBytes) * 100.0
}

// calculates memory bandwidth utilization percentage
// NYI: Memory bandwidth calculation not implemented - no reliable source for max bandwidth
func (hc *HostConfig) GetMemoryBandwidthUtilizationPercent(bandwidthMBps float64) float64 {
	// NYI: No reliable source for maximum memory bandwidth
	// Would require platform-specific detection or manual configuration
	return 0.0
}

// GetFairL3Allocation calculates fair L3 cache allocation for a given number of containers
func (hc *HostConfig) GetFairL3Allocation(totalContainers int) (waysPerContainer int, bitmaskPerContainer uint64) {
	if totalContainers == 0 || hc.L3Cache.WaysPerCache == 0 {
		return 0, 0
	}

	waysPerContainer = hc.L3Cache.WaysPerCache / totalContainers
	if waysPerContainer == 0 {
		waysPerContainer = 1 // Minimum 1 way per container
	}

	// Create bitmask for the allocated ways
	bitmaskPerContainer = (1 << waysPerContainer) - 1

	return waysPerContainer, bitmaskPerContainer
}

// Helper functions for reading system information

// readIntFromFile reads an integer from a file
func readIntFromFile(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	value, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("failed to parse integer from %s: %v", path, err)
	}

	return value, nil
}

// readStringFromFile reads a string from a file
func readStringFromFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// parseCPUList parses a CPU list string (e.g., "0,28" or "0-3,8-11") into a slice of integers
func parseCPUList(cpuList string) []int {
	var result []int

	if cpuList == "" {
		return result
	}

	parts := strings.Split(cpuList, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)

		if strings.Contains(part, "-") {
			// Range like "0-3"
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) == 2 {
				start, err1 := strconv.Atoi(rangeParts[0])
				end, err2 := strconv.Atoi(rangeParts[1])
				if err1 == nil && err2 == nil {
					for i := start; i <= end; i++ {
						result = append(result, i)
					}
				}
			}
		} else {
			// Single value
			if val, err := strconv.Atoi(part); err == nil {
				result = append(result, val)
			}
		}
	}

	return result
}

// logDetailedConfiguration logs comprehensive host configuration at debug level
func (hc *HostConfig) logDetailedConfiguration() {
	if !hc.logger.IsLevelEnabled(logrus.DebugLevel) {
		return
	}

	// System Information
	hc.logger.WithFields(logrus.Fields{
		"hostname": hc.Hostname,
		"os":       hc.OSInfo,
		"kernel":   hc.KernelVersion,
	}).Debug("System Information")

	// CPU Information
	hc.logger.WithFields(logrus.Fields{
		"vendor": hc.CPUVendor,
		"model":  hc.CPUModel,
	}).Debug("CPU Information")

	// CPU Topology
	hc.logger.WithFields(logrus.Fields{
		"physical_cores":   hc.Topology.PhysicalCores,
		"logical_cores":    hc.Topology.LogicalCores,
		"threads_per_core": hc.Topology.ThreadsPerCore,
		"sockets":          hc.Topology.Sockets,
		"cores_per_socket": hc.Topology.CoresPerSocket,
		"core_mappings":    len(hc.Topology.CoreMap),
	}).Debug("CPU Topology")

	// Log all core mappings at debug level
	if len(hc.Topology.CoreMap) > 0 {
		hc.logger.Debug("Core Mappings (Logical CPU -> Socket, Core, Siblings):")
		// Sort logical IDs for consistent output
		logicalIDs := make([]int, 0, len(hc.Topology.CoreMap))
		for id := range hc.Topology.CoreMap {
			logicalIDs = append(logicalIDs, id)
		}

		// Simple sort
		for i := 0; i < len(logicalIDs); i++ {
			for j := i + 1; j < len(logicalIDs); j++ {
				if logicalIDs[i] > logicalIDs[j] {
					logicalIDs[i], logicalIDs[j] = logicalIDs[j], logicalIDs[i]
				}
			}
		}

		for _, logicalID := range logicalIDs {
			coreInfo := hc.Topology.CoreMap[logicalID]
			hc.logger.WithFields(logrus.Fields{
				"cpu":      logicalID,
				"socket":   coreInfo.PhysicalID,
				"core":     coreInfo.CoreID,
				"siblings": fmt.Sprintf("%v", coreInfo.Siblings),
			}).Debugf("  CPU %d -> Socket %d, Core %d, Siblings: %v",
				logicalID, coreInfo.PhysicalID, coreInfo.CoreID, coreInfo.Siblings)
		}
	}

	// Cache Hierarchy
	hc.logger.WithFields(logrus.Fields{
		"l1_kb":        hc.L1Cache.SizeKB,
		"l1_line_size": hc.L1Cache.LineSize,
		"l2_kb":        hc.L2Cache.SizeKB,
		"l2_line_size": hc.L2Cache.LineSize,
		"l3_mb":        hc.L3Cache.SizeMB,
		"l3_line_size": hc.L3Cache.LineSize,
	}).Debug("Cache Hierarchy")

	// L3 (LLC) Details
	l3Fields := logrus.Fields{
		"size_bytes": hc.L3Cache.SizeBytes,
		"size_mb":    hc.L3Cache.SizeMB,
	}

	// Only add RDT-specific fields if they were successfully determined
	if hc.L3Cache.WaysPerCache > 0 {
		l3Fields["cache_ways"] = hc.L3Cache.WaysPerCache
		l3Fields["bytes_per_way"] = hc.L3Cache.BytesPerWay
		l3Fields["mb_per_way"] = float64(hc.L3Cache.BytesPerWay) / (1024 * 1024)
		l3Fields["max_bitmask"] = fmt.Sprintf("0x%x", hc.L3Cache.MaxBitmask)
		l3Fields["cache_ids"] = fmt.Sprintf("%v", hc.L3Cache.CacheIDs)
	} else {
		l3Fields["rdt_info"] = "not available (RDT not supported or resctrl unavailable)"
	}

	hc.logger.WithFields(l3Fields).Debug("L3 (LLC) Details")

	// Intel RDT
	if hc.RDT.Supported {
		fields := logrus.Fields{
			"supported":   hc.RDT.Supported,
			"monitoring":  hc.RDT.MonitoringSupported,
			"allocation":  hc.RDT.AllocationSupported,
			"max_closids": hc.RDT.MaxCLOSIDs,
			"classes":     len(hc.RDT.AvailableClasses),
		}

		if len(hc.RDT.AvailableClasses) > 0 {
			fields["class_list"] = fmt.Sprintf("%v", hc.RDT.AvailableClasses)
		}

		if len(hc.RDT.MonitoringFeatures) > 0 {
			for resource, features := range hc.RDT.MonitoringFeatures {
				fields[fmt.Sprintf("mon_%s", resource)] = fmt.Sprintf("%v", features)
			}
		}

		hc.logger.WithFields(fields).Debug("Intel RDT")
	} else {
		hc.logger.Debug("Intel RDT: Not supported")
	}

	// Utility function examples
	hc.logger.Debug("Utility Functions Example:")

	if hc.L3Cache.WaysPerCache > 0 {
		ways, bitmask := hc.GetFairL3Allocation(4)
		hc.logger.WithFields(logrus.Fields{
			"fair_allocation_4_containers_ways":    ways,
			"fair_allocation_4_containers_bitmask": fmt.Sprintf("0x%x", bitmask),
		}).Debug("  Fair L3 allocation for 4 containers")
	} else {
		hc.logger.Debug("  Fair L3 allocation: not available (cache ways unknown)")
	}

	if hc.L3Cache.SizeBytes > 0 {
		testOccupancy := uint64(10 * 1024 * 1024) // 10 MB
		utilization := hc.GetL3CacheUtilizationPercent(testOccupancy)
		hc.logger.WithFields(logrus.Fields{
			"l3_utilization_10mb_percent": fmt.Sprintf("%.2f%%", utilization),
		}).Debug("  L3 cache utilization for 10 MB")
	}

}
