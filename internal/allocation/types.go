package allocation

// Only acceps bitmasks
type SocketAllocation struct {
	L3Bitmask    string
	MemBandwidth float64
}

type RDTAllocator interface {
	Initialize() error

	CreateRDTClass(className string, socket0, socket1 *SocketAllocation) error

	UpdateRDTClass(className string, socket0, socket1 *SocketAllocation) error

	CreateAllRDTClasses(classes map[string]struct {
		Socket0 *SocketAllocation
		Socket1 *SocketAllocation
	}) error

	AssignContainerToClass(pid int, className string) error

	RemoveContainerFromClass(pid int) error

	GetContainerClass(pid int) (string, error)

	ListAvailableClasses() []string

	DeleteRDTClass(className string) error

	Cleanup() error
}
