package persistence

import (
	"fmt"
	"sync"

	"github.com/yadukrishnan2004/antOrch-server/domain"
)

// InMemoryChildStore implements domain.ChildWorkflowRepository.
type InMemoryChildStore struct {
	mu      sync.RWMutex
	records map[string]*domain.ChildWorkflowRecord // keyed by ChildWorkflowID
}

// NewInMemoryChildStore creates an empty child workflow store.
func NewInMemoryChildStore() *InMemoryChildStore {
	return &InMemoryChildStore{records: make(map[string]*domain.ChildWorkflowRecord)}
}

// Save implements domain.ChildWorkflowRepository.
func (s *InMemoryChildStore) Save(r *domain.ChildWorkflowRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *r
	s.records[r.ChildWorkflowID] = &copy
	return nil
}

// FindByParent implements domain.ChildWorkflowRepository.
func (s *InMemoryChildStore) FindByParent(parentID string) ([]*domain.ChildWorkflowRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*domain.ChildWorkflowRecord
	for _, r := range s.records {
		if r.ParentWorkflowID == parentID {
			copy := *r
			result = append(result, &copy)
		}
	}
	return result, nil
}

// FindByChild implements domain.ChildWorkflowRepository.
func (s *InMemoryChildStore) FindByChild(childID string) (*domain.ChildWorkflowRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.records[childID]
	if !ok {
		return nil, fmt.Errorf("child workflow record for %q not found", childID)
	}
	copy := *r
	return &copy, nil
}

// UpdateStatus implements domain.ChildWorkflowRepository.
func (s *InMemoryChildStore) UpdateStatus(childID string, status domain.ChildWorkflowStatus, output interface{}, errStr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[childID]
	if !ok {
		return fmt.Errorf("child workflow record for %q not found", childID)
	}
	r.Status = status
	r.Output = output
	r.Error = errStr
	return nil
}