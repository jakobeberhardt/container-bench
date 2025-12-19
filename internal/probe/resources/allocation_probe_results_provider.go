package resources

// AllocationProbeResultsProvider can be implemented by schedulers that run
// allocation-style probes and want their results exported to the database.
type AllocationProbeResultsProvider interface {
	GetAllocationProbeResults() []*AllocationProbeResult
	HasAllocationProbeResults() bool
}
