package domain
 
import "time"
 
type NodeStatus string
 
const (
	NodeAlive   NodeStatus = "ALIVE"
	NodeSuspect NodeStatus = "SUSPECT" 
	NodeDead    NodeStatus = "DEAD"
)

type Node struct {
	ID        string
	Address   string 
	Status    NodeStatus
	IsLeader  bool
	JoinedAt  time.Time
	LastSeen  time.Time
}


type Shard struct {
	ID     int
	NodeID string 
}

type ClusterState struct {
	Nodes       []*Node
	Shards      []*Shard
	TotalShards int
	LeaderID    string
	Version     int64 // increments on every membership change
}

type MembershipRepository interface{
	SaveNode(n *Node) error
	FindNode(id string) (*Node, error)
	ListNodes() ([]*Node, error)
	SaveShard(s *Shard) error
	ListShards() ([]*Shard, error)
	GetState() (*ClusterState, error)
	SaveState(state *ClusterState) error
}