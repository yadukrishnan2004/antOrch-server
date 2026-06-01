package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	httpapi "github.com/yadukrishnan2004/antOrch-server/api/http"
	"github.com/yadukrishnan2004/antOrch-server/infrastructure/persistence"
	ifacequeue "github.com/yadukrishnan2004/antOrch-server/interface/queue"
	"github.com/yadukrishnan2004/antOrch-server/interface/registry"
	"github.com/yadukrishnan2004/antOrch-server/interface/worker"
	"github.com/yadukrishnan2004/antOrch-server/usecase"
)

func main() {
	// ── Config from env (or defaults for local dev) ───────────────
	port := getEnv("PORT", "7233")

	fmt.Printf("=== Temporal Server — Single Node MVP ===\n")
	fmt.Printf("Starting on port %s\n\n", port)

	// ── Wire workflow engine layers ───────────────────────────────
	store := persistence.NewInMemoryStore()
	timerStore := persistence.NewInMemoryTimerStore()
	signalStore := persistence.NewInMemorySignalStore()
	childStore := persistence.NewInMemoryChildStore()
	queue := ifacequeue.New(100)
	
	reg := registry.New()
	// Register a dummy activity for testing
	reg.Register("EchoActivity", func(input interface{}) (interface{}, error) {
		fmt.Printf(">>> Executing EchoActivity with input: %v\n", input)
		return fmt.Sprintf("Echo: %v", input), nil
	})

	svc := usecase.NewWorkflowService(store, queue, timerStore, signalStore, childStore)
	w := worker.New("w1", queue, reg, svc)

	// ── Start background services ─────────────────────────────────
	go w.Run()

	go func() {
		for range time.Tick(100 * time.Millisecond) {
			_ = svc.FireDueTimers()
		}
	}()

	// ── Start HTTP server ─────────────────────────────────────────
	handler := httpapi.New(svc)
	server := &http.Server{
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

	// ── Graceful shutdown ─────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("\n[server] shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
