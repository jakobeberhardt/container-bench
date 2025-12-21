package rdtguard

import "sync"

// goresctrl's rdt control is not safe for concurrent use.
// Serialize all interactions with github.com/intel/goresctrl/pkg/rdt across the process.
var mu sync.Mutex

func Lock()   { mu.Lock() }
func Unlock() { mu.Unlock() }
func WithLock(fn func()) {
	mu.Lock()
	defer mu.Unlock()
	fn()
}
