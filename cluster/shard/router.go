package shard

import (
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/yadukrishnan2004/antOrch-server/domain"
)

// Router maps a workflow ID to the node that owns it.
// Uses FNV hash for fast, consistent distribution.
type Router struct {
	mu          sync.RWMutex
	totalShards int
	shardToNode map[int]string // shard ID → node ID
	nodeToAddr  map[string]string // node ID → address
}

// NewRouter creates a Router with the given number of shards.
func NewRouter(totalShards int) *Router {
	return &Router{
		totalShards: totalShards,
		shardToNode: make(map[int]string),
		nodeToAddr:  make(map[string]string),
	}
}

// ShardFor returns the shard ID for a given workflow ID.
// This is deterministic — same workflowID always maps to same shard.
func (r *Router) ShardFor(workflowID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(workflowID))
	return int(h.Sum32()) % r.totalShards
}

// NodeFor returns the node ID responsible for a workflow.
func (r *Router) NodeFor(workflowID string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	shardID := r.ShardFor(workflowID)
	nodeID, ok := r.shardToNode[shardID]
	if !ok {
		return "", fmt.Errorf("no node assigned to shard %d", shardID)
	}
	return nodeID, nil
}

// AddressFor returns the HTTP address of the node owning a workflow.
func (r *Router) AddressFor(workflowID string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	shardID := r.ShardFor(workflowID)
	nodeID, ok := r.shardToNode[shardID]
	if !ok {
		return "", fmt.Errorf("shard %d has no assigned node", shardID)
	}
	addr, ok := r.nodeToAddr[nodeID]
	if !ok {
		return "", fmt.Errorf("node %q has no known address", nodeID)
	}
	return addr, nil
}

// IsLocal returns true if the given workflow belongs to the local node.
func (r *Router) IsLocal(workflowID, localNodeID string) bool {
	nodeID, err := r.NodeFor(workflowID)
	if err != nil {
		return false
	}
	return nodeID == localNodeID
}

// UpdateFromState rebuilds routing tables from current cluster state.
// Called whenever the cluster state changes.
func (r *Router) UpdateFromState(state *domain.ClusterState) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.shardToNode = make(map[int]string, len(state.Shards))
	for _, s := range state.Shards {
		r.shardToNode[s.ID] = s.NodeID
	}

	r.nodeToAddr = make(map[string]string, len(state.Nodes))
	for _, n := range state.Nodes {
		if n.Status == domain.NodeAlive {
			r.nodeToAddr[n.ID] = n.Address
		}
	}
}

// Distribution returns how many shards each node owns (for debugging).
func (r *Router) Distribution() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	dist := make(map[string]int)
	for _, nodeID := range r.shardToNode {
		dist[nodeID]++
	}
	return dist
}