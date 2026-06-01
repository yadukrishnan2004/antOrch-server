package persistence

import (
	"fmt"
	"sync"

	"github.com/yadukrishnan2004/antOrch-server/domain"
)

// InMemoryMembershipStore implements domain.MembershipRepository.
// In Phase 5+ this gets replaced with etcd or Redis.
type InMemoryMembershipStore struct {
	mu     sync.RWMutex
	nodes  map[string]*domain.Node
	shards map[int]*domain.Shard
	state  *domain.ClusterState
}

// NewInMemoryMembershipStore creates an empty membership store.
func NewInMemoryMembershipStore(totalShards int) *InMemoryMembershipStore {
	return &InMemoryMembershipStore{
		nodes:  make(map[string]*domain.Node),
		shards: make(map[int]*domain.Shard),
		state: &domain.ClusterState{
			TotalShards: totalShards,
			Version:     0,
		},
	}
}

func (s *InMemoryMembershipStore) SaveNode(n *domain.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *n
	s.nodes[n.ID] = &copy
	return nil
}

func (s *InMemoryMembershipStore) FindNode(id string) (*domain.Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodes[id]
	if !ok {
		return nil, fmt.Errorf("node %q not found", id)
	}
	copy := *n
	return &copy, nil
}

func (s *InMemoryMembershipStore) ListNodes() ([]*domain.Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	nodes := make([]*domain.Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		copy := *n
		nodes = append(nodes, &copy)
	}
	return nodes, nil
}

func (s *InMemoryMembershipStore) SaveShard(sh *domain.Shard) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *sh
	s.shards[sh.ID] = &copy
	return nil
}

func (s *InMemoryMembershipStore) ListShards() ([]*domain.Shard, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	shards := make([]*domain.Shard, 0, len(s.shards))
	for _, sh := range s.shards {
		copy := *sh
		shards = append(shards, &copy)
	}
	return shards, nil
}

func (s *InMemoryMembershipStore) GetState() (*domain.ClusterState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copy := *s.state
	return &copy, nil
}

func (s *InMemoryMembershipStore) SaveState(state *domain.ClusterState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *state
	s.state = &copy
	return nil
}