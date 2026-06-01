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
	"github.com/yadukrishnan2004/antOrch-server/cluster"
	cluster_persistence "github.com/yadukrishnan2004/antOrch-server/cluster/infra/persistence"
	"github.com/yadukrishnan2004/antOrch-server/infrastructure/persistence"
	ifacequeue "github.com/yadukrishnan2004/antOrch-server/interface/queue"
	"github.com/yadukrishnan2004/antOrch-server/interface/registry"
	"github.com/yadukrishnan2004/antOrch-server/interface/worker"
	"github.com/yadukrishnan2004/antOrch-server/usecase"
)

func main() {
	// ── Config from env (or defaults for local dev) ───────────────
	nodeID := getEnv("NODE_ID", "node-1")
	address := getEnv("NODE_ADDR", "localhost:7233")
	port := getEnv("PORT", "7233")

	fmt.Printf("=== Temporal Server — Phase 4 (Clustering) ===\n")
	fmt.Printf("Node: %s  Address: %s\n\n", nodeID, address)

	// ── Wire workflow engine layers ───────────────────────────────
	store := persistence.NewInMemoryStore()
	timerStore := persistence.NewInMemoryTimerStore()
	signalStore := persistence.NewInMemorySignalStore()
	childStore := persistence.NewInMemoryChildStore()
	memberStore := cluster_persistence.NewInMemoryMembershipStore(cluster.DefaultShardCount)
	queue := ifacequeue.New(100)
	reg := registry.New()
	svc := usecase.NewWorkflowService(store, queue, timerStore, signalStore, childStore)
	w := worker.New("w1", queue, reg, svc)

	// ── Wire cluster layer ────────────────────────────────────────
	coordinator := cluster.New(nodeID, address, memberStore)

	// ── Start background services ─────────────────────────────────
	go w.Run()

	go func() {
		for range time.Tick(100 * time.Millisecond) {
			_ = svc.FireDueTimers()
		}
	}()

	// ── Join cluster ──────────────────────────────────────────────
	if err := coordinator.Start(); err != nil {
		fmt.Printf("[main] failed to start cluster: %v\n", err)
		os.Exit(1)
	}
	defer coordinator.Stop()

	// ── Start HTTP server ─────────────────────────────────────────
	handler := httpapi.New(svc, coordinator)
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

	// ── Demo: show cluster status ─────────────────────────────────
	time.Sleep(500 * time.Millisecond)
	showClusterStatus(coordinator)

	// ── Graceful shutdown ─────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("\n[server] shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func showClusterStatus(c *cluster.Coordinator) {
	fmt.Println("\n─── Cluster status ──────────────────────────")
	fmt.Printf("  Node:      %s\n", c.NodeID())
	fmt.Printf("  Leader:    %v\n", c.IsLeader())
	fmt.Printf("  Shards:    %d total\n", cluster.DefaultShardCount)
	fmt.Println("  Shard distribution:")
	for nodeID, count := range c.Distribution() {
		fmt.Printf("    %s → %d shards\n", nodeID, count)
	}
	fmt.Println("─────────────────────────────────────────────")

	// show where a few sample workflows would land
	fmt.Println("\n  Sample workflow routing:")
	samples := []string{"wf-001", "wf-002", "wf-003", "wf-abc", "wf-xyz"}
	for _, id := range samples {
		shard := c.ShardFor(id)
		local := c.IsLocal(id)
		fmt.Printf("    %s → shard=%d local=%v\n", id, shard, local)
	}
	fmt.Println()
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
