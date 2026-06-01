package domain

import "time"

// ChildWorkflowStatus tracks the relationship between parent and child.
type ChildWorkflowStatus string

const (
	ChildPending   ChildWorkflowStatus = "PENDING"
	ChildRunning   ChildWorkflowStatus = "RUNNING"
	ChildCompleted ChildWorkflowStatus = "COMPLETED"
	ChildFailed    ChildWorkflowStatus = "FAILED"
)

// ChildWorkflowRecord links a parent workflow to a child it spawned.
type ChildWorkflowRecord struct {
	ID               string
	ParentWorkflowID string
	ChildWorkflowID  string
	Status           ChildWorkflowStatus
	Output           interface{}
	Error            string
	SpawnedAt        time.Time
	CompletedAt      time.Time
}

// ChildWorkflowRepository defines how child workflow relationships are stored.
type ChildWorkflowRepository interface {
	Save(record *ChildWorkflowRecord) error
	FindByParent(parentID string) ([]*ChildWorkflowRecord, error)
	FindByChild(childID string) (*ChildWorkflowRecord, error)
	UpdateStatus(childWorkflowID string, status ChildWorkflowStatus, output interface{}, errStr string) error
}