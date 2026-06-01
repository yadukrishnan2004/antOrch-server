package worker

import (
	"fmt"
	"time"

	"github.com/yadukrishnan2004/antOrch-server/domain"
	"github.com/yadukrishnan2004/antOrch-server/interface/queue"
	"github.com/yadukrishnan2004/antOrch-server/usecase"
)

// Worker polls the task queue, executes activities, and handles retries.
type Worker struct {
	id      string
	queue   *queue.ChannelQueue
	reg     domain.ActivityRegistry
	service *usecase.WorkflowService
}

// New creates a Worker.
func New(id string, q *queue.ChannelQueue, reg domain.ActivityRegistry, svc *usecase.WorkflowService) *Worker {
	return &Worker{id: id, queue: q, reg: reg, service: svc}
}

// Run starts polling. Call in a goroutine.
func (w *Worker) Run() {
	fmt.Printf("[worker:%s] started, polling for tasks...\n", w.id)
	for task := range w.queue.Poll() {
		w.execute(task)
	}
	fmt.Printf("[worker:%s] stopped\n", w.id)
}

func (w *Worker) execute(task domain.Task) {
	fmt.Printf("[worker:%s] attempt %d/%d: activity=%s name=%s\n",
		w.id, task.CurrentAttempt, task.RetryPolicy.MaxAttempts, task.ActivityID, task.Name)

	fn, err := w.reg.Lookup(task.Name)
	if err != nil {
		// unregistered activity — no point retrying
		_ = w.service.RecordActivityResult(domain.ActivityResult{
			WorkflowID: task.WorkflowID,
			ActivityID: task.ActivityID,
			Err:        fmt.Errorf("registry lookup failed: %w", err),
		})
		return
	}

	output, err := fn(task.Input)

	if err != nil {
		// check if we should retry
		if task.RetryPolicy.ShouldRetry(task.CurrentAttempt) {
			backoff := task.RetryPolicy.IntervalFor(task.CurrentAttempt)

			fmt.Printf("[worker:%s] activity failed, retrying in %s: activity=%s err=%v\n",
				w.id, backoff, task.ActivityID, err)

			// record the retry event in history
			_ = w.service.RecordActivityRetry(task.WorkflowID, task.ActivityID, task.CurrentAttempt, err)

			// wait for backoff duration
			time.Sleep(backoff)

			// re-enqueue with attempt incremented
			task.CurrentAttempt++
			_ = w.queue.Enqueue(task)
			return
		}

		// exhausted all attempts — record permanent failure
		_ = w.service.RecordActivityResult(domain.ActivityResult{
			WorkflowID: task.WorkflowID,
			ActivityID: task.ActivityID,
			Err:        fmt.Errorf("failed after %d attempts: %w", task.CurrentAttempt, err),
		})
		return
	}

	// success
	_ = w.service.RecordActivityResult(domain.ActivityResult{
		WorkflowID: task.WorkflowID,
		ActivityID: task.ActivityID,
		Output:     output,
	})
}