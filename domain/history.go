package domain

import "time"

type EventType string

const (
	EventWorkflowStarted   EventType = "WORKFLOW_STARTED"
	EventActivityScheduled EventType = "ACTIVITY_SCHEDULED"
	EventActivityCompleted EventType = "ACTIVITY_COMPLETED"
	EventActivityFailed    EventType = "ACTIVITY_FAILED"
	EventActivityRetrying  EventType = "ACTIVITY_RETRYING"   
	EventTimerStarted      EventType = "TIMER_STARTED"       
	EventTimerFired        EventType = "TIMER_FIRED"        
	EventSignalReceived    EventType = "SIGNAL_RECEIVED"    
	EventWorkflowCompleted EventType = "WORKFLOW_COMPLETED"
	EventWorkflowFailed    EventType = "WORKFLOW_FAILED"
)

type HistoryEvent struct {
	ID         int
	Type       EventType
	ActivityID string
	Data       interface{}
	OccurredAt time.Time
}