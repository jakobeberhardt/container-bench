package host

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"container-bench/internal/logging"
	"github.com/intel/goresctrl/pkg/rdt"
	"github.com/sirupsen/logrus"
)

// HostConfig contains host system configuration information
// This is initialized once at startup and used throughout the application
type HostConfig struct {
	// CPU Information
	CPUVendor       string
	CPUModel        string
	TotalCores      int
	TotalThreads    int
	NumSockets      int
	
	// Cache Information
	L3Cache         L3CacheConfig
	
	// RDT Information
	RDT             RDTConfig
	
	// System Information
	Hostname        string
	OSInfo          string
	KernelVersion   string
	
	logger          *logrus.Logger
}

// L3CacheConfig contains L3 cache configuration
type L3CacheConfig struct {
	TotalSizeBytes     int64            // Total L3 cache size in bytes
	TotalSizeKB        float64          // Total L3 cache size in KB
	TotalSizeMB        float64          // Total L3 cache size in MB
	CacheIDs           []uint64         // Available cache IDs
	WaysPerCache       int              // Number of ways per cache
	BytesPerWay        int64            // Bytes per cache way
	MaxBitmask         uint64           // Maximum allocation bitmask
}

// RDTConfig contains RDT system configuration
type RDTConfig struct {
	Supported              bool
	MonitoringSupported    bool
	AllocationSupported    bool
	AvailableClasses       []string
	MonitoringFeatures     map[string][]string  // MonResource -> features
	MaxMemoryBandwidthMBps int64               // System max memory bandwidth
}

var (
	globalHostConfig *HostConfig
	hostConfigOnce   sync.Once
)

// GetHostConfig returns the global host configuration
// It initializes the configuration on first call
func GetHostConfig() (*HostConfig, error) {
	var err error
	hostConfigOnce.Do(func() {
		globalHostConfig, err = initializeHostConfig()
	})
	return globalHostConfig, err
}

// initializeHostConfig performs the actual initialization
func initializeHostConfig() (*HostConfig, error) {
	logger := logging.GetLogger()
	logger.Info("Initializing host configuration")
	
	config := &HostConfig{
		logger: logger,
	}
	
	// Initialize basic system info
	if err := config.initSystemInfo(); err != nil {
		return nil, fmt.Errorf("failed to initialize system info: %v", err)
	}
	
	// Initialize CPU info
	if err := config.initCPUInfo(); err != nil {
		return nil, fmt.Errorf("failed to initialize CPU info: %v", err)
	}
	
	// Initialize L3 cache info
	if err := config.initL3CacheInfo(); err != nil {
		logger.WithError(err).Warn("Failed to initialize L3 cache info, using defaults")
		config.setDefaultL3CacheInfo()
	}
	
	// Initialize RDT info
	if err := config.initRDTInfo(); err != nil {
		logger.WithError(err).Warn("Failed to initialize RDT info, RDT features disabled")
		config.RDT.Supported = false
	}
	
	logger.WithFields(logrus.Fields{
		"cpu_model":       config.CPUModel,
		"total_cores":     config.TotalCores,
		"l3_cache_mb":     config.L3Cache.TotalSizeMB,
		"rdt_supported":   config.RDT.Supported,
	}).Info("Host configuration initialized")
	
	return config, nil
}

func (hc *HostConfig) initSystemInfo() error {
	// Get hostname
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed to get hostname: %v", err)
	}
	hc.Hostname = hostname
	
	// Get OS info
	hc.OSInfo = runtime.GOOS + "/" + runtime.GOARCH
	
	// Get kernel version from /proc/version
	if data, err := os.ReadFile("/proc/version"); err == nil {
		version := strings.Fields(string(data))
		if len(version) >= 3 {
			hc.KernelVersion = version[2]
		}
	}
	
	if hc.KernelVersion == "" {
		hc.KernelVersion = "unknown"
	}
	
	return nil
}

func (hc *HostConfig) initCPUInfo() error {
	hc.TotalCores = runtime.NumCPU()
	hc.TotalThreads = runtime.NumCPU()
	
	// Read detailed CPU info from /proc/cpuinfo
	file, err := os.Open("/proc/cpuinfo")
	if err != nil {
		// Set defaults if we can't read cpuinfo
		hc.CPUVendor = "unknown"
		hc.CPUModel = "unknown"
		hc.NumSockets = 1
		return nil
	}
	defer file.Close()
	
	var physicalIDs []string
	scanner := bufio.NewScanner(file)
	
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		
		if strings.HasPrefix(line, "vendor_id") {
			if hc.CPUVendor == "" {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					hc.CPUVendor = strings.TrimSpace(parts[1])
				}
			}
		} else if strings.HasPrefix(line, "model name") {
			if hc.CPUModel == "" {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					hc.CPUModel = strings.TrimSpace(parts[1])
				}
			}
		} else if strings.HasPrefix(line, "physical id") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				physicalID := strings.TrimSpace(parts[1])
				// Check if this physical ID is already seen
				found := false
				for _, id := range physicalIDs {
					if id == physicalID {
						found = true
						break
					}
				}
				if !found {
					physicalIDs = append(physicalIDs, physicalID)
				}
			}
		}
	}
	
	// Set defaults if not found
	if hc.CPUVendor == "" {
		hc.CPUVendor = "unknown"
	}
	if hc.CPUModel == "" {
		hc.CPUModel = "unknown"
	}
	
	// Number of sockets is the number of unique physical IDs
	hc.NumSockets = len(physicalIDs)
	if hc.NumSockets == 0 {
		hc.NumSockets = 1
	}
	
	return nil
}

func (hc *HostConfig) initL3CacheInfo() error {
	// Try to get L3 cache size from sysfs
	cacheSize, err := hc.getL3CacheSizeFromSysfs()
	if err != nil {
		return fmt.Errorf("failed to get L3 cache size: %v", err)
	}
	
	hc.L3Cache.TotalSizeBytes = cacheSize
	hc.L3Cache.TotalSizeKB = float64(cacheSize) / 1024.0
	hc.L3Cache.TotalSizeMB = float64(cacheSize) / (1024.0 * 1024.0)
	
	// Get cache IDs and ways information from RDT if available
	if rdt.MonSupported() {
		hc.initRDTCacheInfo()
	} else {
		// Set default values
		hc.L3Cache.CacheIDs = []uint64{0}
		hc.L3Cache.WaysPerCache = 20  // Common default for modern CPUs
		hc.L3Cache.BytesPerWay = cacheSize / int64(hc.L3Cache.WaysPerCache)
		hc.L3Cache.MaxBitmask = (1 << 20) - 1  // 20 ways default
	}
	
	return nil
}

func (hc *HostConfig) getL3CacheSizeFromSysfs() (int64, error) {
	// Try multiple possible locations for L3 cache size
	cachePaths := []string{
		"/sys/devices/system/cpu/cpu0/cache/index3/size",
		"/sys/devices/system/cpu/cpu0/cache/index2/size", // Some systems use index2 for L3
	}
	
	for _, path := range cachePaths {
		if data, err := os.ReadFile(path); err == nil {
			sizeStr := strings.TrimSpace(string(data))
			
			// Parse size (format like "8192K" or "8388608")
			if strings.HasSuffix(sizeStr, "K") {
				if sizeKB, err := strconv.ParseInt(sizeStr[:len(sizeStr)-1], 10, 64); err == nil {
					return sizeKB * 1024, nil
				}
			} else if strings.HasSuffix(sizeStr, "M") {
				if sizeMB, err := strconv.ParseInt(sizeStr[:len(sizeStr)-1], 10, 64); err == nil {
					return sizeMB * 1024 * 1024, nil
				}
			} else {
				if sizeBytes, err := strconv.ParseInt(sizeStr, 10, 64); err == nil {
					return sizeBytes, nil
				}
			}
		}
	}
	
	return 0, fmt.Errorf("could not determine L3 cache size")
}

func (hc *HostConfig) initRDTCacheInfo() {
	// This would require parsing resctrl filesystem or using RDT APIs
	// For now, set reasonable defaults based on cache size
	hc.L3Cache.CacheIDs = []uint64{0}
	
	// Estimate ways based on cache size (typical Intel configuration)
	cacheMB := hc.L3Cache.TotalSizeMB
	if cacheMB >= 32 {
		hc.L3Cache.WaysPerCache = 20
	} else if cacheMB >= 16 {
		hc.L3Cache.WaysPerCache = 16
	} else {
		hc.L3Cache.WaysPerCache = 12
	}
	
	hc.L3Cache.BytesPerWay = hc.L3Cache.TotalSizeBytes / int64(hc.L3Cache.WaysPerCache)
	hc.L3Cache.MaxBitmask = (1 << hc.L3Cache.WaysPerCache) - 1
}

func (hc *HostConfig) setDefaultL3CacheInfo() {
	// Set reasonable defaults based on CPU type
	defaultSizeMB := int64(8) // 8MB default
	
	if strings.Contains(strings.ToLower(hc.CPUModel), "xeon") {
		defaultSizeMB = 32 // 32MB for Xeon
	} else if strings.Contains(strings.ToLower(hc.CPUModel), "i7") {
		defaultSizeMB = 12 // 12MB for i7
	}
	
	hc.L3Cache.TotalSizeBytes = defaultSizeMB * 1024 * 1024
	hc.L3Cache.TotalSizeKB = float64(defaultSizeMB * 1024)
	hc.L3Cache.TotalSizeMB = float64(defaultSizeMB)
	hc.L3Cache.CacheIDs = []uint64{0}
	hc.L3Cache.WaysPerCache = 16
	hc.L3Cache.BytesPerWay = hc.L3Cache.TotalSizeBytes / 16
	hc.L3Cache.MaxBitmask = (1 << 16) - 1
}

func (hc *HostConfig) initRDTInfo() error {
	hc.RDT.Supported = rdt.MonSupported()
	hc.RDT.MonitoringSupported = rdt.MonSupported()
	
	if !hc.RDT.Supported {
		return nil
	}
	
	// Get available classes
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
	
	// Check if allocation is supported (approximate)
	hc.RDT.AllocationSupported = len(hc.RDT.AvailableClasses) > 0
	
	// Set default memory bandwidth based on CPU type
	if strings.Contains(strings.ToLower(hc.CPUModel), "xeon") {
		hc.RDT.MaxMemoryBandwidthMBps = 100000 // 100 GB/s
	} else {
		hc.RDT.MaxMemoryBandwidthMBps = 50000  // 50 GB/s
	}
	
	return nil
}

// GetL3CacheUtilizationPercent calculates L3 cache utilization percentage
func (hc *HostConfig) GetL3CacheUtilizationPercent(occupancyBytes uint64) float64 {
	if hc.L3Cache.TotalSizeBytes == 0 {
		return 0.0
	}
	return float64(occupancyBytes) / float64(hc.L3Cache.TotalSizeBytes) * 100.0
}

// GetMemoryBandwidthUtilizationPercent calculates memory bandwidth utilization percentage
func (hc *HostConfig) GetMemoryBandwidthUtilizationPercent(bandwidthMBps float64) float64 {
	if hc.RDT.MaxMemoryBandwidthMBps == 0 {
		return 0.0
	}
	return bandwidthMBps / float64(hc.RDT.MaxMemoryBandwidthMBps) * 100.0
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
