package registry

import (
	"fmt"
	"sync"

	"github.com/yadukrishnan2004/antOrch-server/domain"
)


type Registry struct {
	mu         sync.RWMutex
	activities map[string]domain.ActivityFunc
}

func New() *Registry {
	return &Registry{
		activities: make(map[string]domain.ActivityFunc),
	}
}
 

func (r *Registry) Register(name string, fn domain.ActivityFunc) error {
	r.mu.Lock()
	defer r.mu.Unlock()
 
	if _, exists := r.activities[name]; exists {
		return fmt.Errorf("activity %q is already registered", name)
	}
	r.activities[name] = fn
	fmt.Printf("[registry] registered activity: %q\n", name)
	return nil
}


func (r *Registry) Lookup(name string) (domain.ActivityFunc, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
 
	fn, ok := r.activities[name]
	if !ok {
		return nil, fmt.Errorf("activity %q not found in registry", name)
	}
	return fn, nil
}


func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
 
	names := make([]string, 0, len(r.activities))
	for name := range r.activities {
		names = append(names, name)
	}
	return names
}
 
 