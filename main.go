package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yadukrishnan2004/antOrch-server/domain"
	ifacequeue "github.com/yadukrishnan2004/antOrch-server/interface/queue"
	"github.com/yadukrishnan2004/antOrch-server/interface/registry"
	"github.com/yadukrishnan2004/antOrch-server/interface/worker"
	"github.com/yadukrishnan2004/antOrch-server/infrastructure/persistence"
	"github.com/yadukrishnan2004/antOrch-server/usecase"
)

func main() {
	fmt.Println("=== Phase 2: Retries + Timers + Signals ===")
	fmt.Println()

	// ── Wire all layers ───────────────────────────────────────────
	store       := persistence.NewInMemoryStore()
	timerStore  := persistence.NewInMemoryTimerStore()
	signalStore := persistence.NewInMemorySignalStore()
	queue       := ifacequeue.New(100)
	reg         := registry.New()
	svc         := usecase.NewWorkflowService(store, queue, timerStore, signalStore)
	w           := worker.New("w1", queue, reg, svc)

	go w.Run()

	// ── Register activities ───────────────────────────────────────

	// normal activity
	must(reg.Register("greet", func(input interface{}) (interface{}, error) {
		name, _ := input.(string)
		return fmt.Sprintf("Hello, %s!", name), nil
	}))

	// activity that fails twice then succeeds on third attempt
	attempts := 0
	must(reg.Register("flaky_task", func(input interface{}) (interface{}, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("service temporarily unavailable")
		}
		s, _ := input.(string)
		return strings.ToUpper(s), nil
	}))

	// activity that always fails (to show exhausted retries)
	must(reg.Register("always_fails", func(input interface{}) (interface{}, error) {
		return nil, errors.New("this service is permanently broken")
	}))

	// ── DEMO 1: Retries ───────────────────────────────────────────
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("DEMO 1: Retries")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	wf1, err := svc.StartWorkflow("wf-001", "retry-demo")
	must(err)

	// flaky_task: will fail twice, succeed on attempt 3
	must(svc.ScheduleActivityWithRetry(wf1.ID, "act-1", "flaky_task", "hello",
		domain.RetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    200 * time.Millisecond,
			BackoffCoefficient: 2.0,
		},
	))

	// always_fails: will exhaust all 3 attempts
	must(svc.ScheduleActivityWithRetry(wf1.ID, "act-2", "always_fails", nil,
		domain.RetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    100 * time.Millisecond,
			BackoffCoefficient: 2.0,
		},
	))

	// wait long enough for all retries to finish
	// flaky_task:   attempt1 fail→wait200ms, attempt2 fail→wait400ms, attempt3 success
	// always_fails: attempt1 fail→wait100ms, attempt2 fail→wait200ms, attempt3 fail→permanent
	time.Sleep(2 * time.Second)

	must(svc.CompleteWorkflow(wf1.ID))
	printHistory(svc, wf1.ID)

	// ── DEMO 2: Timers ────────────────────────────────────────────
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("DEMO 2: Timers")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	wf2, err := svc.StartWorkflow("wf-002", "timer-demo")
	must(err)

	// schedule an activity first
	must(svc.ScheduleActivity(wf2.ID, "act-1", "greet", "Timer"))

	// start a 500ms timer
	must(svc.StartTimer(wf2.ID, "timer-1", 500*time.Millisecond))

	// tick loop fires due timers every 100ms
	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(100 * time.Millisecond)
			_ = svc.FireDueTimers()
		}
	}()

	time.Sleep(800 * time.Millisecond)
	must(svc.CompleteWorkflow(wf2.ID))
	printHistory(svc, wf2.ID)

	// ── DEMO 3: Signals ───────────────────────────────────────────
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("DEMO 3: Signals")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	wf3, err := svc.StartWorkflow("wf-003", "signal-demo")
	must(err)

	// send signals into the running workflow from an external goroutine
	go func() {
		time.Sleep(200 * time.Millisecond)
		must(svc.SendSignal(wf3.ID, "sig-1", "payment_received", map[string]interface{}{
			"amount":   99.99,
			"currency": "USD",
		}))

		time.Sleep(200 * time.Millisecond)
		must(svc.SendSignal(wf3.ID, "sig-2", "user_approved", "admin@example.com"))
	}()

	time.Sleep(600 * time.Millisecond)

	// read pending signals (workflow would react to these)
	signals, err := svc.GetSignals(wf3.ID)
	must(err)
	fmt.Printf("[main] workflow has %d pending signals:\n", len(signals))
	for _, s := range signals {
		fmt.Printf("  → signal=%s payload=%v\n", s.Name, s.Payload)
	}

	must(svc.CompleteWorkflow(wf3.ID))
	printHistory(svc, wf3.ID)

	fmt.Println("\n=== Phase 2 complete ✓ ===")
}

func printHistory(svc *usecase.WorkflowService, id string) {
	wf, err := svc.GetWorkflow(id)
	must(err)
	fmt.Printf("\n--- Event history: %s ---\n", id)
	for _, ev := range wf.History {
		fmt.Printf("  [%d] %-28s activity=%-6s data=%v\n",
			ev.ID, ev.Type, ev.ActivityID, ev.Data)
	}
	fmt.Printf("Final state: %s\n", wf.State)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}