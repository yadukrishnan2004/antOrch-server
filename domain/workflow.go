package domain

import "time"

//representing one workflow execution.
type Workflow struct{
	ID   string
	Name string
	State State
	History []HistoryEvent
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Activity is a single unit of work within a workflow.
type Activity struct {
	ID         string
	WorkflowID string
	Name       string
	Input      interface{}
	Output     interface{}
	State      State
	Error      string
	ScheduledAt time.Time
	CompletedAt time.Time
}