package domain

import "time"

type TimerState string

const (
	TimerPending  TimerState = "PENDING"
	TimerFired    TimerState = "FIRED"
	TimerCancelled TimerState = "CANCELLED"
)

// Timer represents a scheduled delay attached to a workflow.
type Timer struct {
	ID         string
	WorkflowID string
	FireAt     time.Time
	State      TimerState
	CreatedAt  time.Time
}

// TimerRepository defines how timers are stored and queried.
type TimerRepository interface {
	Save(t *Timer) error
	FindDue(now time.Time) ([]*Timer, error)
	MarkFired(id string) error
}