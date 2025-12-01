package accounting

import (
	"container-bench/internal/allocation"
	"container-bench/internal/host"
	"fmt"
	"sync"
	"testing"
)

// MockRDTAllocator is a mock implementation of allocation.RDTAllocator for testing
type MockRDTAllocator struct {
	mu               sync.RWMutex
	initialized      bool
	classes          map[string]*mockClassConfig
	containerClasses map[int]string
	initializeError  error
	createClassError error
	updateClassError error
	assignError      error
	deleteClassError error
}

type mockClassConfig struct {
	socket0 *allocation.SocketAllocation
	socket1 *allocation.SocketAllocation
}

func NewMockRDTAllocator() *MockRDTAllocator {
	return &MockRDTAllocator{
		classes:          make(map[string]*mockClassConfig),
		containerClasses: make(map[int]string),
	}
}

func (m *MockRDTAllocator) Initialize() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.initializeError != nil {
		return m.initializeError
	}
	m.initialized = true
	return nil
}

func (m *MockRDTAllocator) CreateRDTClass(name string, socket0, socket1 *allocation.SocketAllocation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createClassError != nil {
		return m.createClassError
	}
	if _, exists := m.classes[name]; exists {
		return fmt.Errorf("class %s already exists", name)
	}
	m.classes[name] = &mockClassConfig{
		socket0: socket0,
		socket1: socket1,
	}
	return nil
}

func (m *MockRDTAllocator) UpdateRDTClass(name string, socket0, socket1 *allocation.SocketAllocation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.updateClassError != nil {
		return m.updateClassError
	}
	if _, exists := m.classes[name]; !exists {
		return fmt.Errorf("class %s does not exist", name)
	}
	m.classes[name] = &mockClassConfig{
		socket0: socket0,
		socket1: socket1,
	}
	return nil
}

func (m *MockRDTAllocator) CreateAllRDTClasses(classes map[string]struct {
	Socket0 *allocation.SocketAllocation
	Socket1 *allocation.SocketAllocation
}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createClassError != nil {
		return m.createClassError
	}
	for name, config := range classes {
		if _, exists := m.classes[name]; exists {
			return fmt.Errorf("class %s already exists", name)
		}
		m.classes[name] = &mockClassConfig{
			socket0: config.Socket0,
			socket1: config.Socket1,
		}
	}
	return nil
}

func (m *MockRDTAllocator) DeleteRDTClass(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteClassError != nil {
		return m.deleteClassError
	}
	if _, exists := m.classes[name]; !exists {
		return fmt.Errorf("class %s does not exist", name)
	}
	delete(m.classes, name)
	return nil
}

func (m *MockRDTAllocator) ListAvailableClasses() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	classes := make([]string, 0, len(m.classes))
	for name := range m.classes {
		classes = append(classes, name)
	}
	return classes
}

func (m *MockRDTAllocator) AssignContainerToClass(pid int, className string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.assignError != nil {
		return m.assignError
	}
	if _, exists := m.classes[className]; !exists {
		return fmt.Errorf("class %s does not exist", className)
	}
	m.containerClasses[pid] = className
	return nil
}

func (m *MockRDTAllocator) RemoveContainerFromClass(pid int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.containerClasses, pid)
	return nil
}

func (m *MockRDTAllocator) GetContainerClass(pid int) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if class, exists := m.containerClasses[pid]; exists {
		return class, nil
	}
	return "", fmt.Errorf("container %d not assigned to any class", pid)
}

func (m *MockRDTAllocator) Cleanup() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.classes = make(map[string]*mockClassConfig)
	m.containerClasses = make(map[int]string)
	m.initialized = false
	return nil
}

func (m *MockRDTAllocator) GetNumCLOSIDs() (int, error) {
	return 16, nil
}

// Helper function to create a test host config with specified cache parameters
func createTestHostConfig(totalCacheBytes int64, totalWays int) *host.HostConfig {
	bytesPerWay := totalCacheBytes / int64(totalWays)
	return &host.HostConfig{
		L3Cache: host.L3CacheConfig{
			SizeBytes:    totalCacheBytes,
			SizeKB:       float64(totalCacheBytes) / 1024.0,
			SizeMB:       float64(totalCacheBytes) / (1024.0 * 1024.0),
			WaysPerCache: totalWays,
			BytesPerWay:  bytesPerWay,
		},
	}
}

// Helper function to count set bits in a bitmask
func countBits(mask uint64) int {
	count := 0
	for mask > 0 {
		count += int(mask & 1)
		mask >>= 1
	}
	return count
}

// Test creating a class with bitmask allocation
func TestCreateClass_Bitmask(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, err := NewRDTAccountant(mockAlloc, hostCfg)
	if err != nil {
		t.Fatalf("Failed to create accountant: %v", err)
	}

	err = accountant.Initialize()
	if err != nil {
		t.Fatalf("Failed to initialize: %v", err)
	}

	req := &AllocationRequest{
		L3Bitmask:    "0x0f",
		MemBandwidth: 50.0,
	}

	err = accountant.CreateClass("test-class", req, nil)
	if err != nil {
		t.Fatalf("Failed to create class: %v", err)
	}

	state, _ := accountant.GetSocketState(0)
	if state.AllocatedBitmask != 0x0f {
		t.Errorf("Expected allocated bitmask 0x0f, got 0x%x", state.AllocatedBitmask)
	}
	if state.MemBandwidthUsed != 50.0 {
		t.Errorf("Expected 50%% bandwidth used, got %.2f%%", state.MemBandwidthUsed)
	}
}

// Test creating a class with ways allocation
func TestCreateClass_Ways(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req := &AllocationRequest{
		L3Ways:       "4-7",
		MemBandwidth: 25.0,
	}

	err := accountant.CreateClass("test-ways", req, nil)
	if err != nil {
		t.Fatalf("Failed to create class: %v", err)
	}

	state, _ := accountant.GetSocketState(0)
	expectedMask := uint64(0xf0)
	if state.AllocatedBitmask != expectedMask {
		t.Errorf("Expected allocated bitmask 0x%x, got 0x%x", expectedMask, state.AllocatedBitmask)
	}
}

// Test creating a class with percentage allocation (with ceil rounding)
func TestCreateClass_Percentage_CeilRounding(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req := &AllocationRequest{
		L3Percent:    30.0,
		MemBandwidth: 0,
	}

	err := accountant.CreateClass("test-percent", req, nil)
	if err != nil {
		t.Fatalf("Failed to create class: %v", err)
	}

	state, _ := accountant.GetSocketState(0)
	allocatedWays := countBits(state.AllocatedBitmask)
	if allocatedWays != 4 {
		t.Errorf("Expected 4 ways allocated (ceil(12*0.30)), got %d", allocatedWays)
	}
}

// Test creating a class with byte allocation
func TestCreateClass_Bytes(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req := &AllocationRequest{
		L3Bytes:      uint64(2.5 * 1024 * 1024),
		MemBandwidth: 10.0,
	}

	err := accountant.CreateClass("test-bytes", req, nil)
	if err != nil {
		t.Fatalf("Failed to create class: %v", err)
	}

	state, _ := accountant.GetSocketState(0)
	allocatedWays := countBits(state.AllocatedBitmask)
	if allocatedWays != 3 {
		t.Errorf("Expected 3 ways allocated (ceil(2.5MB/1MB)), got %d", allocatedWays)
	}
}

// Test byte allocation with different cache configurations
func TestCreateClass_Bytes_VariousConfigurations(t *testing.T) {
	tests := []struct {
		name         string
		totalBytes   int64
		totalWays    int
		requestBytes uint64
		expectedWays int
	}{
		{"20-way 20MB, request 3MB", 20 * 1024 * 1024, 20, 3 * 1024 * 1024, 3},
		{"16-way 32MB, request 5MB", 32 * 1024 * 1024, 16, 5 * 1024 * 1024, 3},
		{"8-way 16MB, request 1 byte", 16 * 1024 * 1024, 8, 1, 1},
		{"12-way 24MB, request 3.5MB", 24 * 1024 * 1024, 12, uint64(3.5 * 1024 * 1024), 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockAlloc := NewMockRDTAllocator()
			hostCfg := createTestHostConfig(tt.totalBytes, tt.totalWays)
			accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
			accountant.Initialize()

			req := &AllocationRequest{
				L3Bytes: tt.requestBytes,
			}

			err := accountant.CreateClass("test", req, nil)
			if err != nil {
				t.Fatalf("Failed to create class: %v", err)
			}

			state, _ := accountant.GetSocketState(0)
			allocatedWays := countBits(state.AllocatedBitmask)
			if allocatedWays != tt.expectedWays {
				t.Errorf("Expected %d ways, got %d", tt.expectedWays, allocatedWays)
			}
		})
	}
}

// Test overlapping bitmask detection
func TestCreateClass_OverlappingBitmask_Fails(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req1 := &AllocationRequest{L3Bitmask: "0x0f"}
	err := accountant.CreateClass("class1", req1, nil)
	if err != nil {
		t.Fatalf("Failed to create first class: %v", err)
	}

	req2 := &AllocationRequest{L3Bitmask: "0x3c"}
	err = accountant.CreateClass("class2", req2, nil)
	if err == nil {
		t.Fatal("Expected error for overlapping bitmask, got nil")
	}
}

// Test memory bandwidth limit enforcement
func TestCreateClass_BandwidthExceeds100_Fails(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req1 := &AllocationRequest{
		L3Bitmask:    "0x0f",
		MemBandwidth: 60.0,
	}
	err := accountant.CreateClass("class1", req1, nil)
	if err != nil {
		t.Fatalf("Failed to create first class: %v", err)
	}

	req2 := &AllocationRequest{
		L3Bitmask:    "0xf0",
		MemBandwidth: 50.0,
	}
	err = accountant.CreateClass("class2", req2, nil)
	if err == nil {
		t.Fatal("Expected error for exceeding 100% bandwidth, got nil")
	}
}

// Test updating a class allocation
func TestUpdateClass(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req1 := &AllocationRequest{L3Bitmask: "0x0f", MemBandwidth: 30.0}
	err := accountant.CreateClass("test", req1, nil)
	if err != nil {
		t.Fatalf("Failed to create class: %v", err)
	}

	req2 := &AllocationRequest{L3Bitmask: "0xf0", MemBandwidth: 40.0}
	err = accountant.UpdateClass("test", req2, nil)
	if err != nil {
		t.Fatalf("Failed to update class: %v", err)
	}

	state, _ := accountant.GetSocketState(0)
	if state.AllocatedBitmask != 0xf0 {
		t.Errorf("Expected bitmask 0xf0 after update, got 0x%x", state.AllocatedBitmask)
	}
	if state.MemBandwidthUsed != 40.0 {
		t.Errorf("Expected 40%% bandwidth after update, got %.2f%%", state.MemBandwidthUsed)
	}
}

// Test deleting a class
func TestDeleteClass(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req := &AllocationRequest{L3Bitmask: "0x0f", MemBandwidth: 25.0}
	accountant.CreateClass("test", req, nil)

	err := accountant.DeleteClass("test")
	if err != nil {
		t.Fatalf("Failed to delete class: %v", err)
	}

	state, _ := accountant.GetSocketState(0)
	if state.AllocatedBitmask != 0 {
		t.Errorf("Expected bitmask 0 after delete, got 0x%x", state.AllocatedBitmask)
	}
	if state.MemBandwidthUsed != 0 {
		t.Errorf("Expected 0%% bandwidth after delete, got %.2f%%", state.MemBandwidthUsed)
	}
}

// Test moving container between classes
func TestMoveContainer(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req1 := &AllocationRequest{L3Bitmask: "0x0f"}
	req2 := &AllocationRequest{L3Bitmask: "0xf0"}
	accountant.CreateClass("class1", req1, nil)
	accountant.CreateClass("class2", req2, nil)

	err := accountant.MoveContainer(12345, "class1")
	if err != nil {
		t.Fatalf("Failed to move container: %v", err)
	}

	className, err := accountant.GetContainerClass(12345)
	if err != nil {
		t.Fatalf("Failed to get container class: %v", err)
	}
	if className != "class1" {
		t.Errorf("Expected container in class1, got %s", className)
	}

	err = accountant.MoveContainer(12345, "class2")
	if err != nil {
		t.Fatalf("Failed to move container to class2: %v", err)
	}

	className, _ = accountant.GetContainerClass(12345)
	if className != "class2" {
		t.Errorf("Expected container in class2, got %s", className)
	}
}

// Test edge case: requesting 100% of cache
func TestCreateClass_100Percent(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req := &AllocationRequest{L3Percent: 100.0}
	err := accountant.CreateClass("full-cache", req, nil)
	if err != nil {
		t.Fatalf("Failed to create class with 100%% cache: %v", err)
	}

	state, _ := accountant.GetSocketState(0)
	allocatedWays := countBits(state.AllocatedBitmask)
	if allocatedWays != 12 {
		t.Errorf("Expected all 12 ways allocated, got %d", allocatedWays)
	}
}

// Test edge case: very small percentage
func TestCreateClass_SmallPercentage(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req := &AllocationRequest{L3Percent: 1.0}
	err := accountant.CreateClass("tiny", req, nil)
	if err != nil {
		t.Fatalf("Failed to create class with 1%% cache: %v", err)
	}

	state, _ := accountant.GetSocketState(0)
	allocatedWays := countBits(state.AllocatedBitmask)
	if allocatedWays != 1 {
		t.Errorf("Expected 1 way allocated (minimum), got %d", allocatedWays)
	}
}

// Test fragmented allocation scenario
func TestCreateClass_Fragmented(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req1 := &AllocationRequest{L3Bitmask: "0x00f"}
	req2 := &AllocationRequest{L3Bitmask: "0xf00"}
	accountant.CreateClass("class1", req1, nil)
	accountant.CreateClass("class2", req2, nil)

	req3 := &AllocationRequest{L3Percent: 33.33}
	err := accountant.CreateClass("class3", req3, nil)
	if err != nil {
		t.Fatalf("Failed to allocate in fragmented space: %v", err)
	}

	state, _ := accountant.GetSocketState(0)
	expectedMask := uint64(0xfff)
	if state.AllocatedBitmask != expectedMask {
		t.Errorf("Expected bitmask 0x%x, got 0x%x", expectedMask, state.AllocatedBitmask)
	}
}

// Test negative case: deleting non-existent class
func TestDeleteClass_NotExists_Fails(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	err := accountant.DeleteClass("non-existent")
	if err == nil {
		t.Fatal("Expected error when deleting non-existent class, got nil")
	}
}

// Test negative case: moving container to non-existent class
func TestMoveContainer_ClassNotExists_Fails(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	err := accountant.MoveContainer(12345, "non-existent")
	if err == nil {
		t.Fatal("Expected error when moving container to non-existent class, got nil")
	}
}

// Test negative case: invalid percentage
func TestCreateClass_InvalidPercentage_Fails(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	tests := []float64{-10.0, 150.0, -1.0}
	for _, percent := range tests {
		req := &AllocationRequest{L3Percent: percent}
		err := accountant.CreateClass("test", req, nil)
		if err == nil {
			t.Errorf("Expected error for invalid percentage %.2f, got nil", percent)
		}
	}
}

// Test negative case: invalid memory bandwidth
func TestCreateClass_InvalidBandwidth_Fails(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	tests := []float64{-5.0, 105.0, -0.1}
	for _, bw := range tests {
		req := &AllocationRequest{
			L3Bitmask:    "0x0f",
			MemBandwidth: bw,
		}
		err := accountant.CreateClass("test", req, nil)
		if err == nil {
			t.Errorf("Expected error for invalid bandwidth %.2f, got nil", bw)
		}
	}
}

// Test negative case: requesting more bytes than available
func TestCreateClass_BytesExceedTotal_Fails(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req := &AllocationRequest{
		L3Bytes: 20 * 1024 * 1024,
	}
	err := accountant.CreateClass("too-big", req, nil)
	if err == nil {
		t.Fatal("Expected error when requesting more bytes than available, got nil")
	}
}

// Test cleanup
func TestCleanup(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req1 := &AllocationRequest{L3Bitmask: "0x0f", MemBandwidth: 30.0}
	req2 := &AllocationRequest{L3Bitmask: "0xf0", MemBandwidth: 40.0}
	accountant.CreateClass("class1", req1, nil)
	accountant.CreateClass("class2", req2, nil)
	accountant.MoveContainer(12345, "class1")

	err := accountant.Cleanup()
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	state0, _ := accountant.GetSocketState(0)
	if state0.AllocatedBitmask != 0 || state0.MemBandwidthUsed != 0 {
		t.Error("Socket state not cleared after cleanup")
	}

	_, err = accountant.GetContainerClass(12345)
	if err == nil {
		t.Error("Expected error for container class after cleanup, got nil")
	}
}

// Test priority order: Bitmask > Ways > Bytes > Percentage
func TestAllocationPriority(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req := &AllocationRequest{
		L3Bitmask: "0x0f",
		L3Ways:    "8-11",
		L3Bytes:   5 * 1024 * 1024,
		L3Percent: 50.0,
	}
	accountant.CreateClass("test", req, nil)

	state, _ := accountant.GetSocketState(0)
	if state.AllocatedBitmask != 0x0f {
		t.Errorf("Expected bitmask 0x0f (from L3Bitmask), got 0x%x", state.AllocatedBitmask)
	}
}

// Test concurrent operations
func TestConcurrentOperations(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(20*1024*1024, 20)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := &AllocationRequest{
				L3Percent: 5.0,
			}
			err := accountant.CreateClass(fmt.Sprintf("class-%d", idx), req, nil)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent operation failed: %v", err)
	}

	state, _ := accountant.GetSocketState(0)
	allocatedWays := countBits(state.AllocatedBitmask)
	if allocatedWays != 10 {
		t.Errorf("Expected 10 ways allocated from concurrent operations, got %d", allocatedWays)
	}
}

// Test multiple sockets
func TestMultiSocket(t *testing.T) {
	mockAlloc := NewMockRDTAllocator()
	hostCfg := createTestHostConfig(12*1024*1024, 12)
	accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
	accountant.Initialize()

	req0 := &AllocationRequest{L3Bitmask: "0x0f", MemBandwidth: 30.0}
	req1 := &AllocationRequest{L3Bitmask: "0xf0", MemBandwidth: 40.0}

	err := accountant.CreateClass("dual-socket", req0, req1)
	if err != nil {
		t.Fatalf("Failed to create dual-socket class: %v", err)
	}

	state0, _ := accountant.GetSocketState(0)
	if state0.AllocatedBitmask != 0x0f {
		t.Errorf("Socket 0: expected bitmask 0x0f, got 0x%x", state0.AllocatedBitmask)
	}
	if state0.MemBandwidthUsed != 30.0 {
		t.Errorf("Socket 0: expected 30%% bandwidth, got %.2f%%", state0.MemBandwidthUsed)
	}

	state1, _ := accountant.GetSocketState(1)
	if state1.AllocatedBitmask != 0xf0 {
		t.Errorf("Socket 1: expected bitmask 0xf0, got 0x%x", state1.AllocatedBitmask)
	}
	if state1.MemBandwidthUsed != 40.0 {
		t.Errorf("Socket 1: expected 40%% bandwidth, got %.2f%%", state1.MemBandwidthUsed)
	}
}

// Test allocation with different way counts
func TestVariousWayCounts(t *testing.T) {
	wayConfigs := []struct {
		ways    int
		percent float64
		expect  int
	}{
		{8, 25.0, 2},
		{12, 30.0, 4},
		{16, 33.0, 6},
		{20, 15.0, 3},
		{11, 50.0, 6},
	}

	for _, tc := range wayConfigs {
		t.Run(fmt.Sprintf("%d-ways-%.1fpercent", tc.ways, tc.percent), func(t *testing.T) {
			mockAlloc := NewMockRDTAllocator()
			totalBytes := int64(tc.ways * 1024 * 1024)
			hostCfg := createTestHostConfig(totalBytes, tc.ways)
			accountant, _ := NewRDTAccountant(mockAlloc, hostCfg)
			accountant.Initialize()

			req := &AllocationRequest{L3Percent: tc.percent}
			err := accountant.CreateClass("test", req, nil)
			if err != nil {
				t.Fatalf("Failed to create class: %v", err)
			}

			state, _ := accountant.GetSocketState(0)
			allocatedWays := countBits(state.AllocatedBitmask)
			if allocatedWays != tc.expect {
				t.Errorf("Expected %d ways, got %d", tc.expect, allocatedWays)
			}
		})
	}
}
