package persistence

import (
	"fmt"
	"sync"
	"time"

	"github.com/yadukrishnan2004/antOrch-server/domain"
)

// InMemoryTimerStore implements domain.TimerRepository.
type InMemoryTimerStore struct {
	mu     sync.RWMutex
	timers map[string]*domain.Timer
}

// NewInMemoryTimerStore creates an empty timer store.
func NewInMemoryTimerStore() *InMemoryTimerStore {
	return &InMemoryTimerStore{timers: make(map[string]*domain.Timer)}
}

// Save implements domain.TimerRepository.
func (s *InMemoryTimerStore) Save(t *domain.Timer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *t
	s.timers[t.ID] = &copy
	return nil
}

// FindDue implements domain.TimerRepository — returns all timers whose FireAt <= now.
func (s *InMemoryTimerStore) FindDue(now time.Time) ([]*domain.Timer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var due []*domain.Timer
	for _, t := range s.timers {
		if t.State == domain.TimerPending && !t.FireAt.After(now) {
			copy := *t
			due = append(due, &copy)
		}
	}
	return due, nil
}

// MarkFired implements domain.TimerRepository.
func (s *InMemoryTimerStore) MarkFired(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.timers[id]
	if !ok {
		return fmt.Errorf("timer %q not found", id)
	}
	t.State = domain.TimerFired
	return nil
}