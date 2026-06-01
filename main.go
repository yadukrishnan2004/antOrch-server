package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	httpapi "github.com/yadukrishnan2004/antOrch-server/api/http"
	ifacequeue "github.com/yadukrishnan2004/antOrch-server/interface/queue"
	"github.com/yadukrishnan2004/antOrch-server/interface/registry"
	"github.com/yadukrishnan2004/antOrch-server/interface/worker"
	"github.com/yadukrishnan2004/antOrch-server/infrastructure/persistence"
	"github.com/yadukrishnan2004/antOrch-server/usecase"
)

func main() {
	port := "7233" 
	fmt.Printf("=== Temporal Server starting on :%s ===\n", port)

	// ── Wire all layers ───────────────────────────────────────────
	store       := persistence.NewInMemoryStore()
	timerStore  := persistence.NewInMemoryTimerStore()
	signalStore := persistence.NewInMemorySignalStore()
	childStore  := persistence.NewInMemoryChildStore()
	queue       := ifacequeue.New(100)
	reg         := registry.New()
	svc         := usecase.NewWorkflowService(store, queue, timerStore, signalStore, childStore)
	w           := worker.New("w1", queue, reg, svc)

	// ── Start background workers ──────────────────────────────────
	go w.Run()

	// tick loop fires due timers every 100ms
	go func() {
		for range time.Tick(100 * time.Millisecond) {
			_ = svc.FireDueTimers()
		}
	}()

	// ── Start HTTP server ─────────────────────────────────────────
	handler := httpapi.New(svc)
	server  := &http.Server{
		Addr:    ":" + port,
		Handler: handler,
	}

	go func() {
		fmt.Printf("[server] listening on http://localhost:%s\n", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[server] error: %v\n", err)
			os.Exit(1)
		}
	}()

	// ── Graceful shutdown on Ctrl+C ───────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("\n[server] shutting down...")
}