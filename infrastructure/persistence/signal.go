package persistence

import (
	"fmt"
	"sync"

	"github.com/yadukrishnan2004/antOrch-server/domain"
)

// InMemorySignalStore implements domain.SignalRepository.
type InMemorySignalStore struct {
	mu      sync.RWMutex
	signals map[string]*domain.Signal
}

// NewInMemorySignalStore creates an empty signal store.
func NewInMemorySignalStore() *InMemorySignalStore {
	return &InMemorySignalStore{signals: make(map[string]*domain.Signal)}
}

// Save implements domain.SignalRepository.
func (s *InMemorySignalStore) Save(sig *domain.Signal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *sig
	s.signals[sig.ID] = &copy
	return nil
}

// FindByWorkflow implements domain.SignalRepository.
func (s *InMemorySignalStore) FindByWorkflow(workflowID string) ([]*domain.Signal, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*domain.Signal
	for _, sig := range s.signals {
		if sig.WorkflowID == workflowID {
			copy := *sig
			result = append(result, &copy)
		}
	}
	return result, nil
}

// Delete implements domain.SignalRepository.
func (s *InMemorySignalStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.signals[id]; !ok {
		return fmt.Errorf("signal %q not found", id)
	}
	delete(s.signals, id)
	return nil
}