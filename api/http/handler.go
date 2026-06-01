package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/yadukrishnan2004/antOrch-server/cluster"
	"github.com/yadukrishnan2004/antOrch-server/domain"
	"github.com/yadukrishnan2004/antOrch-server/usecase"
)

type Handler struct {
	svc         *usecase.WorkflowService
	coordinator *cluster.Coordinator
	mux         *http.ServeMux
}

// New creates the HTTP handler and registers all routes.
func New(svc *usecase.WorkflowService, coordinator *cluster.Coordinator) *Handler {
	h := &Handler{svc: svc, coordinator: coordinator, mux: http.NewServeMux()}
	h.mux.HandleFunc("POST /workflows", h.startWorkflow)
	h.mux.HandleFunc("GET /workflows/{id}", h.getWorkflow)
	h.mux.HandleFunc("POST /workflows/{id}/complete", h.completeWorkflow)
	h.mux.HandleFunc("POST /workflows/{id}/activities", h.scheduleActivity)
	h.mux.HandleFunc("POST /workflows/{id}/signals", h.sendSignal)
	h.mux.HandleFunc("POST /workflows/{id}/timers", h.startTimer)
	h.mux.HandleFunc("POST /activities/result", h.recordResult)
	h.mux.HandleFunc("GET /cluster/status", h.clusterStatus)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// ── Shard routing middleware ───────────────────────────────────────────────────

// routeOrHandle checks if this node owns the workflow.
// If not, it proxies the request to the correct node.
func (h *Handler) routeOrHandle(workflowID string, w http.ResponseWriter, r *http.Request, handle func()) {
	addr, isLocal := h.coordinator.RouteAddress(workflowID)
	if isLocal {
		handle()
		return
	}

	// proxy to the correct node
	fmt.Printf("[handler] routing workflow=%s to node at %s\n", workflowID, addr)
	h.proxy(addr, w, r)
}

func (h *Handler) proxy(targetAddr string, w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	url := "http://" + targetAddr + r.URL.Path
	req, err := http.NewRequest(r.Method, url, bytes.NewReader(body))
	if err != nil {
		writeError(w, 500, fmt.Errorf("building proxy request: %w", err))
		return
	}
	req.Header = r.Header

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		writeError(w, 502, fmt.Errorf("proxying to %s: %w", targetAddr, err))
		return
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// ── Request/Response types ────────────────────────────────────────────────────

type startWorkflowReq struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
type scheduleActivityReq struct {
	ActivityID   string             `json:"activity_id"`
	ActivityName string             `json:"activity_name"`
	Input        interface{}        `json:"input"`
	RetryPolicy  domain.RetryPolicy `json:"retry_policy"`
}
type completeActivityReq struct {
	WorkflowID string      `json:"workflow_id"`
	ActivityID string      `json:"activity_id"`
	Output     interface{} `json:"output"`
	Error      string      `json:"error"`
}
type sendSignalReq struct {
	SignalID string      `json:"signal_id"`
	Name     string      `json:"name"`
	Payload  interface{} `json:"payload"`
}
type startTimerReq struct {
	TimerID string  `json:"timer_id"`
	Seconds float64 `json:"seconds"`
}

// ── Handlers ──────────────────────────────────────────────────────────────────

func (h *Handler) startWorkflow(w http.ResponseWriter, r *http.Request) {
	var req startWorkflowReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, err)
		return
	}
	h.routeOrHandle(req.ID, w, r, func() {
		shard := h.coordinator.ShardFor(req.ID)
		fmt.Printf("[handler] starting workflow id=%s shard=%d node=%s\n",
			req.ID, shard, h.coordinator.NodeID())
		wf, err := h.svc.StartWorkflow(req.ID, req.Name)
		if err != nil {
			writeError(w, 500, err)
			return
		}
		writeJSON(w, 201, wf)
	})
}

func (h *Handler) getWorkflow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	h.routeOrHandle(id, w, r, func() {
		wf, err := h.svc.GetWorkflow(id)
		if err != nil {
			writeError(w, 404, err)
			return
		}
		writeJSON(w, 200, wf)
	})
}

func (h *Handler) completeWorkflow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	h.routeOrHandle(id, w, r, func() {
		if err := h.svc.CompleteWorkflow(id); err != nil {
			writeError(w, 500, err)
			return
		}
		writeJSON(w, 200, map[string]string{"status": "completed"})
	})
}

func (h *Handler) scheduleActivity(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req scheduleActivityReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, err)
		return
	}
	h.routeOrHandle(id, w, r, func() {
		err := h.svc.ScheduleActivityWithRetry(id, req.ActivityID, req.ActivityName, req.Input, req.RetryPolicy)
		if err != nil {
			writeError(w, 500, err)
			return
		}
		writeJSON(w, 201, map[string]string{"status": "scheduled"})
	})
}

func (h *Handler) recordResult(w http.ResponseWriter, r *http.Request) {
	var req completeActivityReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, err)
		return
	}
	h.routeOrHandle(req.WorkflowID, w, r, func() {
		var execErr error
		if req.Error != "" {
			execErr = fmt.Errorf("%s", req.Error)
		}
		err := h.svc.RecordActivityResult(domain.ActivityResult{
			WorkflowID: req.WorkflowID,
			ActivityID: req.ActivityID,
			Output:     req.Output,
			Err:        execErr,
		})
		if err != nil {
			writeError(w, 500, err)
			return
		}
		writeJSON(w, 200, map[string]string{"status": "recorded"})
	})
}

func (h *Handler) sendSignal(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req sendSignalReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, err)
		return
	}
	h.routeOrHandle(id, w, r, func() {
		if err := h.svc.SendSignal(id, req.SignalID, req.Name, req.Payload); err != nil {
			writeError(w, 500, err)
			return
		}
		writeJSON(w, 201, map[string]string{"status": "signalled"})
	})
}

func (h *Handler) startTimer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req startTimerReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, err)
		return
	}
	h.routeOrHandle(id, w, r, func() {
		delay := time.Duration(req.Seconds * float64(time.Second))
		if err := h.svc.StartTimer(id, req.TimerID, delay); err != nil {
			writeError(w, 500, err)
			return
		}
		writeJSON(w, 201, map[string]string{"status": "timer_started"})
	})
}

func (h *Handler) clusterStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"node_id":      h.coordinator.NodeID(),
		"is_leader":    h.coordinator.IsLeader(),
		"shard_count":  cluster.DefaultShardCount,
		"distribution": h.coordinator.Distribution(),
	})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}
func writeError(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}