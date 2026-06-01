package usecase

import (
	"fmt"
	"time"

	"github.com/yadukrishnan2004/antOrch-server/domain"
)

// WorkflowService contains all use cases for workflow orchestration.
type WorkflowService struct {
	repo    domain.WorkflowRepository
	queue   domain.TaskQueue
	timers  domain.TimerRepository
	signals domain.SignalRepository
}

// NewWorkflowService constructs the service with its required ports.
func NewWorkflowService(
	repo domain.WorkflowRepository,
	queue domain.TaskQueue,
	timers domain.TimerRepository,
	signals domain.SignalRepository,
) *WorkflowService {
	return &WorkflowService{
		repo:    repo,
		queue:   queue,
		timers:  timers,
		signals: signals,
	}
}

// StartWorkflow creates a new workflow execution and persists it.
func (s *WorkflowService) StartWorkflow(id, name string) (*domain.Workflow, error) {
	exists, err := s.repo.Exists(id)
	if err != nil {
		return nil, fmt.Errorf("checking workflow existence: %w", err)
	}
	if exists {
		return nil, fmt.Errorf("workflow %q already exists", id)
	}

	wf := &domain.Workflow{
		ID:        id,
		Name:      name,
		State:     domain.StateCreated,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := domain.ValidateTransition(domain.StateCreated, domain.StateRunning); err != nil {
		return nil, err
	}
	wf.State = domain.StateRunning
	wf.History = append(wf.History, domain.HistoryEvent{
		ID:         1,
		Type:       domain.EventWorkflowStarted,
		Data:       name,
		OccurredAt: time.Now(),
	})

	if err := s.repo.Save(wf); err != nil {
		return nil, fmt.Errorf("saving workflow: %w", err)
	}

	fmt.Printf("[usecase] workflow started: id=%s name=%s\n", id, name)
	return wf, nil
}

// ScheduleActivity enqueues an activity with no retry policy.
func (s *WorkflowService) ScheduleActivity(workflowID, activityID, activityName string, input interface{}) error {
	return s.ScheduleActivityWithRetry(workflowID, activityID, activityName, input, domain.NoRetry)
}

// ScheduleActivityWithRetry enqueues an activity with a retry policy attached.
func (s *WorkflowService) ScheduleActivityWithRetry(
	workflowID, activityID, activityName string,
	input interface{},
	policy domain.RetryPolicy,
) error {
	wf, err := s.repo.FindByID(workflowID)
	if err != nil {
		return fmt.Errorf("finding workflow: %w", err)
	}
	if wf.State != domain.StateRunning {
		return fmt.Errorf("workflow %q is not running (state=%s)", workflowID, wf.State)
	}

	wf.History = append(wf.History, domain.HistoryEvent{
		ID:         len(wf.History) + 1,
		Type:       domain.EventActivityScheduled,
		ActivityID: activityID,
		Data:       activityName,
		OccurredAt: time.Now(),
	})
	wf.UpdatedAt = time.Now()

	if err := s.repo.Save(wf); err != nil {
		return fmt.Errorf("saving workflow: %w", err)
	}

	if err := s.queue.Enqueue(domain.Task{
		WorkflowID:     workflowID,
		ActivityID:     activityID,
		Name:           activityName,
		Input:          input,
		RetryPolicy:    policy,
		CurrentAttempt: 1, // always starts at attempt 1
	}); err != nil {
		return fmt.Errorf("enqueuing task: %w", err)
	}

	fmt.Printf("[usecase] activity scheduled: workflow=%s activity=%s name=%s maxAttempts=%d\n",
		workflowID, activityID, activityName, policy.MaxAttempts)
	return nil
}

// RecordActivityRetry logs a retry event into the workflow history.
func (s *WorkflowService) RecordActivityRetry(workflowID, activityID string, attempt int, err error) error {
	wf, err2 := s.repo.FindByID(workflowID)
	if err2 != nil {
		return fmt.Errorf("finding workflow: %w", err2)
	}

	wf.History = append(wf.History, domain.HistoryEvent{
		ID:         len(wf.History) + 1,
		Type:       domain.EventActivityRetrying,
		ActivityID: activityID,
		Data:       fmt.Sprintf("attempt=%d err=%v", attempt, err),
		OccurredAt: time.Now(),
	})
	wf.UpdatedAt = time.Now()
	return s.repo.Save(wf)
}

// RecordActivityResult updates the workflow history with a completed or failed activity.
func (s *WorkflowService) RecordActivityResult(result domain.ActivityResult) error {
	wf, err := s.repo.FindByID(result.WorkflowID)
	if err != nil {
		return fmt.Errorf("finding workflow: %w", err)
	}

	var event domain.HistoryEvent
	if result.Err != nil {
		event = domain.HistoryEvent{
			ID:         len(wf.History) + 1,
			Type:       domain.EventActivityFailed,
			ActivityID: result.ActivityID,
			Data:       result.Err.Error(),
			OccurredAt: time.Now(),
		}
		fmt.Printf("[usecase] activity FAILED permanently: workflow=%s activity=%s err=%v\n",
			result.WorkflowID, result.ActivityID, result.Err)
	} else {
		event = domain.HistoryEvent{
			ID:         len(wf.History) + 1,
			Type:       domain.EventActivityCompleted,
			ActivityID: result.ActivityID,
			Data:       result.Output,
			OccurredAt: time.Now(),
		}
		fmt.Printf("[usecase] activity COMPLETED: workflow=%s activity=%s output=%v\n",
			result.WorkflowID, result.ActivityID, result.Output)
	}

	wf.History = append(wf.History, event)
	wf.UpdatedAt = time.Now()
	return s.repo.Save(wf)
}

// StartTimer schedules a timer that fires after the given duration.
func (s *WorkflowService) StartTimer(workflowID, timerID string, delay time.Duration) error {
	wf, err := s.repo.FindByID(workflowID)
	if err != nil {
		return fmt.Errorf("finding workflow: %w", err)
	}

	fireAt := time.Now().Add(delay)
	timer := &domain.Timer{
		ID:         timerID,
		WorkflowID: workflowID,
		FireAt:     fireAt,
		State:      domain.TimerPending,
		CreatedAt:  time.Now(),
	}

	if err := s.timers.Save(timer); err != nil {
		return fmt.Errorf("saving timer: %w", err)
	}

	wf.History = append(wf.History, domain.HistoryEvent{
		ID:         len(wf.History) + 1,
		Type:       domain.EventTimerStarted,
		Data:       fmt.Sprintf("timerID=%s fireAt=%s", timerID, fireAt.Format(time.RFC3339)),
		OccurredAt: time.Now(),
	})
	wf.UpdatedAt = time.Now()

	if err := s.repo.Save(wf); err != nil {
		return fmt.Errorf("saving workflow: %w", err)
	}

	fmt.Printf("[usecase] timer started: workflow=%s timer=%s delay=%s\n", workflowID, timerID, delay)
	return nil
}

// FireDueTimers checks for timers that have passed their fire time and fires them.
// Call this on a tick loop (e.g. every 100ms).
func (s *WorkflowService) FireDueTimers() error {
	due, err := s.timers.FindDue(time.Now())
	if err != nil {
		return fmt.Errorf("finding due timers: %w", err)
	}

	for _, timer := range due {
		wf, err := s.repo.FindByID(timer.WorkflowID)
		if err != nil {
			continue
		}

		wf.History = append(wf.History, domain.HistoryEvent{
			ID:         len(wf.History) + 1,
			Type:       domain.EventTimerFired,
			Data:       fmt.Sprintf("timerID=%s", timer.ID),
			OccurredAt: time.Now(),
		})
		wf.UpdatedAt = time.Now()
		_ = s.repo.Save(wf)
		_ = s.timers.MarkFired(timer.ID)

		fmt.Printf("[usecase] timer FIRED: workflow=%s timer=%s\n", timer.WorkflowID, timer.ID)
	}
	return nil
}

// SendSignal delivers an external signal into a running workflow.
func (s *WorkflowService) SendSignal(workflowID, signalID, name string, payload interface{}) error {
	wf, err := s.repo.FindByID(workflowID)
	if err != nil {
		return fmt.Errorf("finding workflow: %w", err)
	}
	if wf.State != domain.StateRunning {
		return fmt.Errorf("workflow %q is not running", workflowID)
	}

	signal := &domain.Signal{
		ID:         signalID,
		WorkflowID: workflowID,
		Name:       name,
		Payload:    payload,
		ReceivedAt: time.Now(),
	}

	if err := s.signals.Save(signal); err != nil {
		return fmt.Errorf("saving signal: %w", err)
	}

	wf.History = append(wf.History, domain.HistoryEvent{
		ID:         len(wf.History) + 1,
		Type:       domain.EventSignalReceived,
		Data:       fmt.Sprintf("signal=%s payload=%v", name, payload),
		OccurredAt: time.Now(),
	})
	wf.UpdatedAt = time.Now()

	if err := s.repo.Save(wf); err != nil {
		return fmt.Errorf("saving workflow: %w", err)
	}

	fmt.Printf("[usecase] signal received: workflow=%s signal=%s payload=%v\n", workflowID, name, payload)
	return nil
}

// GetSignals returns all pending signals for a workflow.
func (s *WorkflowService) GetSignals(workflowID string) ([]*domain.Signal, error) {
	return s.signals.FindByWorkflow(workflowID)
}

// CompleteWorkflow transitions a workflow to the COMPLETED state.
func (s *WorkflowService) CompleteWorkflow(id string) error {
	wf, err := s.repo.FindByID(id)
	if err != nil {
		return fmt.Errorf("finding workflow: %w", err)
	}

	if err := domain.ValidateTransition(wf.State, domain.StateCompleted); err != nil {
		return err
	}
	wf.State = domain.StateCompleted
	wf.History = append(wf.History, domain.HistoryEvent{
		ID:         len(wf.History) + 1,
		Type:       domain.EventWorkflowCompleted,
		OccurredAt: time.Now(),
	})
	wf.UpdatedAt = time.Now()

	if err := s.repo.Save(wf); err != nil {
		return fmt.Errorf("saving completed workflow: %w", err)
	}

	fmt.Printf("[usecase] workflow COMPLETED: id=%s\n", id)
	return nil
}

// GetWorkflow retrieves a workflow by ID.
func (s *WorkflowService) GetWorkflow(id string) (*domain.Workflow, error) {
	return s.repo.FindByID(id)
}