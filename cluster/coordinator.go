package cluster

import (
	"sync"
	"time"

	"github.com/yadukrishnan2004/antOrch-server/cluster/election"
	"github.com/yadukrishnan2004/antOrch-server/cluster/membership"
	"github.com/yadukrishnan2004/antOrch-server/cluster/shard"
	"github.com/yadukrishnan2004/antOrch-server/domain"
)

const DefaultShardCount = 16

type Coordinator struct {
	mu         sync.RWMutex
	nodeID     string
	address    string
	membership *membership.Manager
	elector    *election.Elector
	router     *shard.Router
	stop       chan struct{}
}

// New creates a new Coordinator for the node.
func New(nodeID, address string, repo domain.MembershipRepository) *Coordinator {
	c := &Coordinator{
		nodeID:  nodeID,
		address: address,
		router:  shard.NewRouter(DefaultShardCount),
		stop:    make(chan struct{}),
	}
	
	c.membership = membership.New(nodeID, repo, func(state *domain.ClusterState) {
		c.router.UpdateFromState(state)
	})
	
	c.elector = election.New(nodeID, DefaultShardCount, repo, nil, nil)
	return c
}

// Start joins the cluster and starts background loops.
func (c *Coordinator) Start() error {
	if err := c.membership.Join(c.address); err != nil {
		return err
	}
	
	c.membership.StartHeartbeat(c.stop)
	c.membership.StartHealthCheck(c.stop)
	
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = c.elector.Elect()
			case <-c.stop:
				return
			}
		}
	}()
	
	return nil
}

// Stop leaves the cluster and stops background loops.
func (c *Coordinator) Stop() {
	close(c.stop)
}

// NodeID returns the ID of the local node.
func (c *Coordinator) NodeID() string {
	return c.nodeID
}

// IsLeader returns true if the local node is the cluster leader.
func (c *Coordinator) IsLeader() bool {
	return c.elector.IsLeader()
}

// ShardFor returns the shard ID for a given workflow.
func (c *Coordinator) ShardFor(workflowID string) int {
	return c.router.ShardFor(workflowID)
}

// RouteAddress returns the address of the node that owns the workflow and whether it is local.
func (c *Coordinator) RouteAddress(workflowID string) (string, bool) {
	isLocal := c.router.IsLocal(workflowID, c.nodeID)
	addr, err := c.router.AddressFor(workflowID)
	if err != nil {
		// fallback to local if we don't know the address yet
		return c.address, true
	}
	return addr, isLocal
}

// IsLocal returns true if the workflow belongs to this node.
func (c *Coordinator) IsLocal(workflowID string) bool {
	return c.router.IsLocal(workflowID, c.nodeID)
}

// Distribution returns the current distribution of shards.
func (c *Coordinator) Distribution() map[string]int {
	return c.router.Distribution()
}
