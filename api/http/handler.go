package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/yadukrishnan2004/antOrch-server/domain"
	"github.com/yadukrishnan2004/antOrch-server/usecase"
)

// Handler exposes the WorkflowService over HTTP REST.
type Handler struct {
	svc *usecase.WorkflowService
	mux *http.ServeMux
}

// New creates the HTTP handler and registers all routes.
func New(svc *usecase.WorkflowService) *Handler {
	h := &Handler{svc: svc, mux: http.NewServeMux()}
	h.mux.HandleFunc("POST /workflows", h.startWorkflow)
	h.mux.HandleFunc("GET /workflows/{id}", h.getWorkflow)
	h.mux.HandleFunc("POST /workflows/{id}/complete", h.completeWorkflow)
	h.mux.HandleFunc("POST /workflows/{id}/activities", h.scheduleActivity)
	h.mux.HandleFunc("POST /workflows/{id}/signals", h.sendSignal)
	h.mux.HandleFunc("POST /workflows/{id}/timers", h.startTimer)
	h.mux.HandleFunc("POST /activities/result", h.recordResult)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// ── Request/Response types ────────────────────────────────────────────────────

type startWorkflowReq  struct { ID   string `json:"id"`;   Name string `json:"name"` }
type completeActivityReq struct {
	WorkflowID string      `json:"workflow_id"`
	ActivityID string      `json:"activity_id"`
	Output     interface{} `json:"output"`
	Error      string      `json:"error"`
}
type scheduleActivityReq struct {
	ActivityID   string             `json:"activity_id"`
	ActivityName string             `json:"activity_name"`
	Input        interface{}        `json:"input"`
	RetryPolicy  domain.RetryPolicy `json:"retry_policy"`
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil { writeError(w, 400, err); return }
	wf, err := h.svc.StartWorkflow(req.ID, req.Name)
	if err != nil { writeError(w, 500, err); return }
	writeJSON(w, 201, wf)
}

func (h *Handler) getWorkflow(w http.ResponseWriter, r *http.Request) {
	wf, err := h.svc.GetWorkflow(r.PathValue("id"))
	if err != nil { writeError(w, 404, err); return }
	writeJSON(w, 200, wf)
}

func (h *Handler) completeWorkflow(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.CompleteWorkflow(r.PathValue("id")); err != nil { writeError(w, 500, err); return }
	writeJSON(w, 200, map[string]string{"status": "completed"})
}

func (h *Handler) scheduleActivity(w http.ResponseWriter, r *http.Request) {
	var req scheduleActivityReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil { writeError(w, 400, err); return }
	err := h.svc.ScheduleActivityWithRetry(r.PathValue("id"), req.ActivityID, req.ActivityName, req.Input, req.RetryPolicy)
	if err != nil { writeError(w, 500, err); return }
	writeJSON(w, 201, map[string]string{"status": "scheduled"})
}

func (h *Handler) recordResult(w http.ResponseWriter, r *http.Request) {
	var req completeActivityReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil { writeError(w, 400, err); return }
	var execErr error
	if req.Error != "" { execErr = fmt.Errorf("%s", req.Error) }
	err := h.svc.RecordActivityResult(domain.ActivityResult{
		WorkflowID: req.WorkflowID,
		ActivityID: req.ActivityID,
		Output:     req.Output,
		Err:        execErr,
	})
	if err != nil { writeError(w, 500, err); return }
	writeJSON(w, 200, map[string]string{"status": "recorded"})
}

func (h *Handler) sendSignal(w http.ResponseWriter, r *http.Request) {
	var req sendSignalReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil { writeError(w, 400, err); return }
	if err := h.svc.SendSignal(r.PathValue("id"), req.SignalID, req.Name, req.Payload); err != nil { writeError(w, 500, err); return }
	writeJSON(w, 201, map[string]string{"status": "signalled"})
}

func (h *Handler) startTimer(w http.ResponseWriter, r *http.Request) {
	var req startTimerReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil { writeError(w, 400, err); return }
	delay := time.Duration(req.Seconds * float64(time.Second))
	if err := h.svc.StartTimer(r.PathValue("id"), req.TimerID, delay); err != nil { writeError(w, 500, err); return }
	writeJSON(w, 201, map[string]string{"status": "timer_started"})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if v != nil { _ = json.NewEncoder(w).Encode(v) }
}
func writeError(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}