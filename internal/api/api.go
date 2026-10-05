// Package api wires the event bus, participants, and failure injector to HTTP.
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/siamosystems/siamo-poc-event-driven/internal/bus"
	"github.com/siamosystems/siamo-poc-event-driven/internal/participants"
)

// Server holds the POC's wiring: the bus, the participants, and the injector.
type Server struct {
	Bus   *bus.InProcessBus
	Inj   *participants.Injector
	order *participants.Order
	mux   *http.ServeMux
}

func New() *Server {
	b := bus.NewInProcessBus()
	inj := participants.NewInjector()
	s := &Server{Bus: b, Inj: inj, order: participants.NewOrder(b), mux: http.NewServeMux()}
	participants.NewPayment(b, inj)
	participants.NewInventory(b, inj)

	s.mux.HandleFunc("POST /saga/start", s.handleStart)
	s.mux.HandleFunc("POST /saga/inject-failure", s.handleInjectFailure)
	s.mux.HandleFunc("GET /events", s.handleEvents)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

type startRequest struct {
	Item string `json:"item"`
	Qty  int    `json:"qty"`
	// Fail optionally injects a failure into this very saga ("payment" or
	// "inventory"); "" means happy path.
	Fail string `json:"fail"`
}

// handleStart kicks off one saga run. Because the bus delivers synchronously,
// the whole choreography (including any compensation) has already completed
// in the event trail by the time this returns.
func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	var req startRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		req = startRequest{}
	}
	if req.Item == "" {
		req.Item = "demo-widget"
	}
	if req.Qty == 0 {
		req.Qty = 1
	}
	switch participants.FailMode(req.Fail) {
	case participants.FailPayment, participants.FailInventory:
		s.Inj.Set(participants.FailMode(req.Fail))
	case "":
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": `fail must be "", "payment", or "inventory"`,
		})
		return
	}

	sagaID := fmt.Sprintf("saga-%d", time.Now().UnixNano())
	s.order.Start(sagaID, req.Item, req.Qty)

	writeJSON(w, http.StatusOK, map[string]any{
		"saga_id": sagaID,
		"fail":    req.Fail,
		"events":  s.Bus.Trail(),
	})
}

type injectRequest struct {
	// Fail is "payment" or "inventory"; "" clears a pending injection.
	Fail string `json:"fail"`
}

// handleInjectFailure arms the failure injector for the NEXT saga run.
// Handy when you want the failure armed ahead of time rather than inline.
func (s *Server) handleInjectFailure(w http.ResponseWriter, r *http.Request) {
	var req injectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	switch participants.FailMode(req.Fail) {
	case participants.FailPayment, participants.FailInventory:
		s.Inj.Set(participants.FailMode(req.Fail))
		writeJSON(w, http.StatusOK, map[string]string{
			"armed": fmt.Sprintf("next saga will fail at: %s", req.Fail),
		})
	case "":
		s.Inj.Set(participants.FailNone)
		writeJSON(w, http.StatusOK, map[string]string{"armed": "cleared — next saga takes the happy path"})
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": `fail must be "", "payment", or "inventory"`,
		})
	}
}

// handleEvents returns the full append-only event trail (oldest first).
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"events": s.Bus.Trail()})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
