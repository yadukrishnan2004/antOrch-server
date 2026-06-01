package domain

import "time"

// Signal is an external event sent into a running workflow.
type Signal struct {
	ID         string
	WorkflowID string
	Name       string
	Payload    interface{}
	ReceivedAt time.Time
}
 
// SignalRepository defines how signals are stored and consumed.
type SignalRepository interface {
	Save(s *Signal) error
	FindByWorkflow(workflowID string) ([]*Signal, error)
	Delete(id string) error
}