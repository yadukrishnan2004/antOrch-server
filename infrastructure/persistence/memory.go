package persistence

import (
	"fmt"
	"sync"
	"github.com/yadukrishnan2004/antOrch-server/domain"
)


type InMemoryStore struct {
	mu        sync.RWMutex
	workflows map[string]*domain.Workflow
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		workflows: make(map[string]*domain.Workflow),
	}
}

func (s *InMemoryStore) Save(wf *domain.Workflow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
 
	// Store a shallow copy to prevent external mutation of stored state.
	copy := *wf
	s.workflows[wf.ID] = &copy
	return nil
}

// FindByID implements domain.WorkflowRepository.
func (s *InMemoryStore) FindByID(id string) (*domain.Workflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
 
	wf, ok := s.workflows[id]
	if !ok {
		return nil, fmt.Errorf("workflow %q not found", id)
	}
	copy := *wf
	return &copy, nil
}

// Exists implements domain.WorkflowRepository.
func (s *InMemoryStore) Exists(id string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
 
	_, ok := s.workflows[id]
	return ok, nil
}