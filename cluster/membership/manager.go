package membership

import (
	"fmt"
	"sync"
	"time"

	"github.com/yadukrishnan2004/antOrch-server/domain"
)

const (
	heartbeatInterval = 2 * time.Second
	suspectTimeout    = 6 * time.Second  
	deadTimeout       = 12 * time.Second 
)

type Manager struct {
	mu       sync.RWMutex
	localID  string
	repo     domain.MembershipRepository
	onChange func(*domain.ClusterState) // called when membership changes
}

func New(localID string, repo domain.MembershipRepository, onChange func(*domain.ClusterState)) *Manager {
	return &Manager{localID: localID, repo: repo, onChange: onChange}
}

func (m *Manager) Join(address string) error {
	node := &domain.Node{
		ID:       m.localID,
		Address:  address,
		Status:   domain.NodeAlive,
		JoinedAt: time.Now(),
		LastSeen: time.Now(),
	}
	if err := m.repo.SaveNode(node); err != nil {
		return fmt.Errorf("joining cluster: %w", err)
	}
	fmt.Printf("[membership] node joined: id=%s address=%s\n", m.localID, address)
	return m.notifyChange()
}

// Heartbeat updates the local node's LastSeen timestamp.
func (m *Manager) Heartbeat() error {
	node, err := m.repo.FindNode(m.localID)
	if err != nil {
		return fmt.Errorf("finding local node: %w", err)
	}
	node.LastSeen = time.Now()
	node.Status = domain.NodeAlive
	return m.repo.SaveNode(node)
}

// CheckHealth marks nodes as suspect or dead based on missed heartbeats.
// Call this on a tick loop.
func (m *Manager) CheckHealth() error {
	nodes, err := m.repo.ListNodes()
	if err != nil {
		return err
	}
 
	changed := false
	now := time.Now()
 
	for _, node := range nodes {
		if node.ID == m.localID {
			continue // don't mark ourselves dead
		}
 
		age := now.Sub(node.LastSeen)
		var newStatus domain.NodeStatus
 
		switch {
		case age >= deadTimeout:
			newStatus = domain.NodeDead
		case age >= suspectTimeout:
			newStatus = domain.NodeSuspect
		default:
			continue // healthy, no change
		}
 
		if node.Status != newStatus {
			node.Status = newStatus
			_ = m.repo.SaveNode(node)
			fmt.Printf("[membership] node %s: %s (last seen %s ago)\n",
				node.ID, newStatus, age.Round(time.Millisecond))
			changed = true
		}
	}
 
	if changed {
		return m.notifyChange()
	}
	return nil
}
 

// StartHeartbeat begins sending heartbeats in the background.
func (m *Manager) StartHeartbeat(stop <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = m.Heartbeat()
			case <-stop:
				return
			}
		}
	}()
}
 

// StartHealthCheck begins checking peer health in the background.
func (m *Manager) StartHealthCheck(stop <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = m.CheckHealth()
			case <-stop:
				return
			}
		}
	}()
}
 
// LiveNodes returns all nodes currently marked ALIVE.
func (m *Manager) LiveNodes() ([]*domain.Node, error) {
	all, err := m.repo.ListNodes()
	if err != nil {
		return nil, err
	}
	var live []*domain.Node
	for _, n := range all {
		if n.Status == domain.NodeAlive {
			live = append(live, n)
		}
	}
	return live, nil
}

func (m *Manager) notifyChange() error {
	state, err := m.repo.GetState()
	if err != nil {
		return err
	}
	if m.onChange != nil {
		m.onChange(state)
	}
	return nil
}