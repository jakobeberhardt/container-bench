package scheduler

import (
	"fmt"
	"os"
	"testing"
)

// TestResourceAllocatorInitialization tests basic allocator setup
func TestResourceAllocatorInitialization(t *testing.T) {
	// Skip if not running as root or if RDT not available
	if os.Geteuid() != 0 {
		t.Skip("Test requires root privileges")
	}

	allocator := NewDefaultResourceAllocator()
	if err := allocator.Initialize(); err != nil {
		t.Fatalf("Failed to initialize allocator: %v", err)
	}
	defer allocator.Close()

	// Check resources were initialized
	resources := allocator.GetAvailableResources()
	if len(resources) == 0 {
		t.Fatal("No socket resources initialized")
	}

	for socket, res := range resources {
		t.Logf("Socket %d: %d cache ways, %d%% bandwidth",
			socket, res.TotalCacheWays, res.TotalMBW)

		if res.TotalCacheWays == 0 {
			t.Errorf("Socket %d has 0 cache ways", socket)
		}

		if res.AvailableCacheWays != res.TotalCacheWays {
			t.Errorf("Socket %d: expected all ways available, got %d/%d",
				socket, res.AvailableCacheWays, res.TotalCacheWays)
		}
	}
}

// TestGroupCreationAndDeletion tests creating and deleting RDT groups
func TestGroupCreationAndDeletion(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("Test requires root privileges")
	}

	allocator := NewDefaultResourceAllocator()
	if err := allocator.Initialize(); err != nil {
		t.Fatalf("Failed to initialize allocator: %v", err)
	}
	defer allocator.Close()
	defer allocator.ResetToDefault()

	// Get initial resources
	resourcesBefore := allocator.GetAvailableResources()
	socket0Before := resourcesBefore[0]

	// Create a test group
	config := GroupConfig{
		L3Allocation: map[int]L3Config{
			0: {Ways: 3},
		},
		MBAllocation: map[int]MBConfig{
			0: {Percentage: 30},
		},
	}

	if err := allocator.CreateGroup("test_group", config); err != nil {
		t.Fatalf("Failed to create group: %v", err)
	}

	// Check resources were allocated
	resourcesAfter := allocator.GetAvailableResources()
	socket0After := resourcesAfter[0]

	expectedAvailable := socket0Before.AvailableCacheWays - 3
	if socket0After.AvailableCacheWays != expectedAvailable {
		t.Errorf("Expected %d available cache ways, got %d",
			expectedAvailable, socket0After.AvailableCacheWays)
	}

	expectedMBW := socket0Before.AvailableMBW - 30
	if socket0After.AvailableMBW != expectedMBW {
		t.Errorf("Expected %d%% available bandwidth, got %d%%",
			expectedMBW, socket0After.AvailableMBW)
	}

	// Delete the group
	if err := allocator.DeleteGroup("test_group"); err != nil {
		t.Fatalf("Failed to delete group: %v", err)
	}

	// Check resources were released
	resourcesFinal := allocator.GetAvailableResources()
	socket0Final := resourcesFinal[0]

	if socket0Final.AvailableCacheWays != socket0Before.AvailableCacheWays {
		t.Errorf("Cache ways not fully released: expected %d, got %d",
			socket0Before.AvailableCacheWays, socket0Final.AvailableCacheWays)
	}

	if socket0Final.AvailableMBW != socket0Before.AvailableMBW {
		t.Errorf("Bandwidth not fully released: expected %d%%, got %d%%",
			socket0Before.AvailableMBW, socket0Final.AvailableMBW)
	}
}

// TestProbeIsolation tests the probe isolation helper
func TestProbeIsolation(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("Test requires root privileges")
	}

	allocator := NewDefaultResourceAllocator()
	if err := allocator.Initialize(); err != nil {
		t.Fatalf("Failed to initialize allocator: %v", err)
	}
	defer allocator.Close()
	defer allocator.ResetToDefault()

	// Create probe isolation
	config := ProbeIsolationConfig{
		GroupPrefix: "test",
		ProbeL3Ways: map[int]int{0: 3},
		ProbeMBW:    map[int]int{0: 30},
	}

	if err := allocator.CreateProbeIsolation(config); err != nil {
		t.Fatalf("Failed to create probe isolation: %v", err)
	}

	// Check both groups were created
	allocator.mu.RLock()
	_, probeExists := allocator.groups["test_probe"]
	_, othersExists := allocator.groups["test_others"]
	allocator.mu.RUnlock()

	if !probeExists {
		t.Error("Probe group was not created")
	}

	if !othersExists {
		t.Error("Others group was not created")
	}

	// Check resources are fully allocated
	resources := allocator.GetAvailableResources()
	socket0 := resources[0]

	if socket0.AvailableCacheWays != 0 {
		t.Errorf("Expected 0 available cache ways, got %d", socket0.AvailableCacheWays)
	}

	if socket0.AvailableMBW != 0 {
		t.Errorf("Expected 0%% available bandwidth, got %d%%", socket0.AvailableMBW)
	}

	// Delete probe isolation
	if err := allocator.DeleteProbeIsolation("test"); err != nil {
		t.Fatalf("Failed to delete probe isolation: %v", err)
	}

	// Check groups were deleted
	allocator.mu.RLock()
	_, probeStillExists := allocator.groups["test_probe"]
	_, othersStillExists := allocator.groups["test_others"]
	allocator.mu.RUnlock()

	if probeStillExists {
		t.Error("Probe group still exists after deletion")
	}

	if othersStillExists {
		t.Error("Others group still exists after deletion")
	}
}

// TestContiguousMaskValidation tests bitmask contiguity checking
func TestContiguousMaskValidation(t *testing.T) {
	allocator := NewDefaultResourceAllocator()
	allocator.cacheWaysTotal = 12

	tests := []struct {
		mask     string
		expected bool
	}{
		{"0x7", true},       // 0111 - contiguous
		{"0x38", true},      // 0111000 - contiguous
		{"0xfff", true},     // all bits - contiguous
		{"0x1", true},       // single bit - contiguous
		{"0x105", false},    // 100000101 - non-contiguous
		{"0x99", false},     // 10011001 - non-contiguous
		{"0x0", false},      // no bits - invalid
		{"0xe00", true},     // 111000000000 - contiguous
		{"0x303", false},    // 1100000011 - non-contiguous
	}

	for _, tt := range tests {
		result := allocator.isContiguousMask(tt.mask)
		if result != tt.expected {
			t.Errorf("isContiguousMask(%s) = %v, expected %v", tt.mask, result, tt.expected)
		}
	}
}

// TestCacheWayRangeParsing tests parsing of cache way ranges
func TestCacheWayRangeParsing(t *testing.T) {
	allocator := NewDefaultResourceAllocator()
	allocator.cacheWaysTotal = 12

	tests := []struct {
		input       string
		expected    string
		shouldError bool
	}{
		{"0-2", "0x7", false},      // ways 0, 1, 2
		{"3-5", "0x38", false},     // ways 3, 4, 5
		{"0-11", "0xfff", false},   // all ways
		{"10-11", "0xc00", false},  // ways 10, 11
		{"5-3", "", true},          // invalid: start > end
		{"0-15", "", true},         // invalid: exceeds total ways
		{"invalid", "", true},      // invalid format
	}

	for _, tt := range tests {
		result, err := allocator.parseCacheWayRange(tt.input)
		if tt.shouldError {
			if err == nil {
				t.Errorf("parseCacheWayRange(%s) should have errored", tt.input)
			}
		} else {
			if err != nil {
				t.Errorf("parseCacheWayRange(%s) unexpected error: %v", tt.input, err)
			}
			if result != tt.expected {
				t.Errorf("parseCacheWayRange(%s) = %s, expected %s", tt.input, result, tt.expected)
			}
		}
	}
}

// ExampleDefaultResourceAllocator demonstrates basic allocator usage
func ExampleDefaultResourceAllocator() {
	allocator := NewDefaultResourceAllocator()
	if err := allocator.Initialize(); err != nil {
		fmt.Printf("Failed to initialize: %v\n", err)
		return
	}
	defer allocator.Close()

	// Check available resources
	resources := allocator.GetAvailableResources()
	for socket, res := range resources {
		fmt.Printf("Socket %d: %d cache ways, %d%% bandwidth\n",
			socket, res.AvailableCacheWays, res.AvailableMBW)
	}

	// Create a group
	config := GroupConfig{
		L3Allocation: map[int]L3Config{
			0: {Ways: 3},
		},
		MBAllocation: map[int]MBConfig{
			0: {Percentage: 30},
		},
	}

	if err := allocator.CreateGroup("my_group", config); err != nil {
		fmt.Printf("Failed to create group: %v\n", err)
		return
	}

	fmt.Println("Group created successfully")

	// Clean up
	if err := allocator.DeleteGroup("my_group"); err != nil {
		fmt.Printf("Failed to delete group: %v\n", err)
		return
	}

	fmt.Println("Group deleted successfully")
}

// ExampleDefaultResourceAllocator_CreateProbeIsolation demonstrates probe isolation pattern
func ExampleDefaultResourceAllocator_CreateProbeIsolation() {
	allocator := NewDefaultResourceAllocator()
	if err := allocator.Initialize(); err != nil {
		fmt.Printf("Failed to initialize: %v\n", err)
		return
	}
	defer allocator.Close()

	// Create probe isolation groups
	config := ProbeIsolationConfig{
		GroupPrefix: "benchmark_1",
		ProbeL3Ways: map[int]int{0: 3},
		ProbeMBW:    map[int]int{0: 30},
		ProbePID:    1234,
		OtherPIDs:   []int{5678, 9012},
	}

	if err := allocator.CreateProbeIsolation(config); err != nil {
		fmt.Printf("Failed to create probe isolation: %v\n", err)
		return
	}

	fmt.Println("Probe isolation created")
	fmt.Println("- benchmark_1_probe: 3 ways, 30% bandwidth")
	fmt.Println("- benchmark_1_others: 9 ways, 70% bandwidth")

	// ... run probe benchmark ...

	// Clean up
	if err := allocator.DeleteProbeIsolation("benchmark_1"); err != nil {
		fmt.Printf("Failed to delete probe isolation: %v\n", err)
		return
	}

	fmt.Println("Probe isolation deleted")
}
