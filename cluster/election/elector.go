package election

import (
	"fmt"
	"sort"
	"sync"

	"github.com/yadukrishnan2004/antOrch-server/domain"
)

type Elector struct {
	mu          sync.RWMutex
	localID     string
	totalShards int
	repo        domain.MembershipRepository
	onLeader    func()    // called when this node becomes leader
	onFollower  func()    // called when this node becomes follower
}


func New(
	localID string,
	totalShards int,
	repo domain.MembershipRepository,
	onLeader func(),
	onFollower func(),
) *Elector {
	return &Elector{
		localID:     localID,
		totalShards: totalShards,
		repo:        repo,
		onLeader:    onLeader,
		onFollower:  onFollower,
	}
}


func (e *Elector) Elect() error {
	e.mu.Lock()
	defer e.mu.Unlock()
 
	nodes, err := e.repo.ListNodes()
	if err != nil {
		return fmt.Errorf("listing nodes for election: %w", err)
	}
 
	// only alive nodes can be elected
	var alive []*domain.Node
	for _, n := range nodes {
		if n.Status == domain.NodeAlive {
			alive = append(alive, n)
		}
	}
	if len(alive) == 0 {
		return fmt.Errorf("no alive nodes — cannot elect a leader")
	}
 
	// sort by ID — lowest wins
	sort.Slice(alive, func(i, j int) bool {
		return alive[i].ID < alive[j].ID
	})
	leaderID := alive[0].ID
 
	// update leader flag on all nodes
	for _, n := range nodes {
		wasLeader := n.IsLeader
		n.IsLeader = (n.ID == leaderID)
		if n.IsLeader != wasLeader {
			_ = e.repo.SaveNode(n)
		}
	}
 
	fmt.Printf("[election] leader elected: %s (from %d alive nodes)\n", leaderID, len(alive))
 
	// if we are the leader, assign shards
	if leaderID == e.localID {
		if err := e.assignShards(alive); err != nil {
			return err
		}
		if e.onLeader != nil {
			e.onLeader()
		}
	} else {
		if e.onFollower != nil {
			e.onFollower()
		}
	}
 
	return nil
}

func (e *Elector) assignShards(alive []*domain.Node) error {
	fmt.Printf("[election] assigning %d shards across %d nodes\n", e.totalShards, len(alive))
 
	state, err := e.repo.GetState()
	if err != nil {
		return fmt.Errorf("getting cluster state: %w", err)
	}
 
	shards := make([]*domain.Shard, e.totalShards)
	for i := 0; i < e.totalShards; i++ {
		// round-robin assignment
		ownerNode := alive[i%len(alive)]
		shards[i] = &domain.Shard{ID: i, NodeID: ownerNode.ID}
		_ = e.repo.SaveShard(shards[i])
	}
 
	state.Shards = shards
	state.LeaderID = e.localID
	state.Version++
	_ = e.repo.SaveState(state)
 
	// print distribution
	dist := make(map[string]int)
	for _, s := range shards {
		dist[s.NodeID]++
	}
	for nodeID, count := range dist {
		fmt.Printf("[election] node=%s owns %d shards\n", nodeID, count)
	}
 
	return nil
}


func (e *Elector) IsLeader() bool {
	node, err := e.repo.FindNode(e.localID)
	if err != nil {
		return false
	}
	return node.IsLeader
}