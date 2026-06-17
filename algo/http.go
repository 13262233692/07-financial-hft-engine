package algo

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/hft-engine/model"
)

type HTTPServer struct {
	engine *AlgoEngine
	server *http.Server
	addr   string
	mu     sync.Mutex
}

type SubmitParentRequest struct {
	ClientID      string  `json:"client_id"`
	Symbol        string  `json:"symbol"`
	Side          int     `json:"side"`
	TotalQty      int64   `json:"total_qty"`
	StartTime     int64   `json:"start_time,omitempty"`
	EndTime       int64   `json:"end_time,omitempty"`
	DurationSec   int64   `json:"duration_sec,omitempty"`
	MaxPrice      int64   `json:"max_price"`
	MinIntervalMs int64   `json:"min_interval_ms,omitempty"`
	SliceCount    int     `json:"slice_count,omitempty"`
	PerturbPct    float64 `json:"perturb_pct,omitempty"`
	CreatedBy     string  `json:"created_by,omitempty"`
}

type ParentSummary struct {
	ID         uint64  `json:"id"`
	ClientID   string  `json:"client_id"`
	Symbol     string  `json:"symbol"`
	Side       int     `json:"side"`
	TotalQty   int64   `json:"total_qty"`
	FilledQty  int64   `json:"filled_qty"`
	Progress   float64 `json:"progress"`
	StartTime  int64   `json:"start_time"`
	EndTime    int64   `json:"end_time"`
	Status     int32   `json:"status"`
	StatusStr  string  `json:"status_str"`
	AlgoType   int     `json:"algo_type"`
	MaxPrice   int64   `json:"max_price"`
	SliceCount int     `json:"slice_count"`
	CreatedAt  int64   `json:"created_at"`
}

type ChildSummary struct {
	ID          uint64 `json:"id"`
	ParentID    uint64 `json:"parent_id"`
	Qty         int64  `json:"qty"`
	FilledQty   int64  `json:"filled_qty"`
	Price       int64  `json:"price"`
	ScheduledAt int64  `json:"scheduled_at"`
	SentAt      int64  `json:"sent_at"`
	Status      int32  `json:"status"`
	ExternalID  uint64 `json:"external_id"`
}

func NewHTTPServer(addr string, engine *AlgoEngine) *HTTPServer {
	return &HTTPServer{
		engine: engine,
		addr:   addr,
	}
}

func (s *HTTPServer) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v1/algo/parents", s.handleSubmitParent)
	mux.HandleFunc("GET /api/v1/algo/parents", s.handleListParents)
	mux.HandleFunc("GET /api/v1/algo/parents/", s.handleGetParent)
	mux.HandleFunc("DELETE /api/v1/algo/parents/", s.handleCancelParent)
	mux.HandleFunc("GET /api/v1/health", s.handleHealth)

	s.mu.Lock()
	s.server = &http.Server{
		Addr:         s.addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	s.mu.Unlock()

	log.Printf("[ALGO-API] Listening on %s", s.addr)

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[ALGO-API] Server error: %v", err)
		}
	}()

	return nil
}

func (s *HTTPServer) Stop(ctx context.Context) error {
	s.mu.Lock()
	srv := s.server
	s.mu.Unlock()

	if srv != nil {
		return srv.Shutdown(ctx)
	}
	return nil
}

func (s *HTTPServer) handleSubmitParent(w http.ResponseWriter, r *http.Request) {
	var req SubmitParentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	if req.Symbol == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "symbol required"})
		return
	}
	if req.Side != 1 && req.Side != 2 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "side must be 1 (buy) or 2 (sell)"})
		return
	}
	if req.TotalQty <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "total_qty must be positive"})
		return
	}

	var start, end int64
	now := time.Now().UnixNano()

	if req.StartTime > 0 {
		start = req.StartTime
	} else {
		start = now
	}

	if req.EndTime > 0 {
		end = req.EndTime
	} else if req.DurationSec > 0 {
		end = now + req.DurationSec*int64(time.Second)
	} else {
		end = now + int64(time.Hour*2)
	}

	parent := &ParentOrder{
		ClientID:      req.ClientID,
		Symbol:        req.Symbol,
		Side:          model.Side(req.Side),
		TotalQty:      req.TotalQty,
		StartTime:     start,
		EndTime:       end,
		AlgoType:      AlgoTWAP,
		MaxPrice:      req.MaxPrice,
		MinIntervalMs: req.MinIntervalMs,
		SliceCount:    req.SliceCount,
		PerturbPct:    req.PerturbPct,
		CreatedBy:     req.CreatedBy,
	}

	result, err := s.engine.SubmitParent(parent)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, s.parentToSummary(result))
}

func (s *HTTPServer) handleListParents(w http.ResponseWriter, r *http.Request) {
	parents := s.engine.ListParents()
	resp := make([]ParentSummary, 0, len(parents))
	for _, p := range parents {
		resp = append(resp, s.parentToSummary(p))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *HTTPServer) handleGetParent(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Path[len("/api/v1/algo/parents/"):]
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}

	parent, children, err := s.engine.GetParent(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	childSummaries := make([]ChildSummary, 0, len(children))
	var sent, filled, cancelled int
	for _, c := range children {
		childSummaries = append(childSummaries, ChildSummary{
			ID:          c.ID,
			ParentID:    c.ParentID,
			Qty:         c.Qty,
			FilledQty:   c.FilledQty.Load(),
			Price:       c.Price,
			ScheduledAt: c.ScheduledAt,
			SentAt:      c.SentAt,
			Status:      c.Status.Load(),
			ExternalID:  c.ExternalID.Load(),
		})
		switch ExecutionStatus(c.Status.Load()) {
		case ChildSent:
			sent++
		case ChildFilled:
			filled++
		case ChildCancelled:
			cancelled++
		}
	}

	resp := map[string]interface{}{
		"parent": s.parentToSummary(parent),
		"children": childSummaries,
		"stats": map[string]int{
			"total_children": len(children),
			"pending":        len(children) - sent - filled - cancelled,
			"sent":           sent,
			"filled":         filled,
			"cancelled":      cancelled,
		},
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *HTTPServer) handleCancelParent(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Path[len("/api/v1/algo/parents/"):]
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}

	if err := s.engine.CancelParent(id); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled", "id": fmt.Sprintf("%d", id)})
}

func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "algo-executor"})
}

func (s *HTTPServer) parentToSummary(p *ParentOrder) ParentSummary {
	filled := p.FilledQty.Load()
	progress := 0.0
	if p.TotalQty > 0 {
		progress = float64(filled) / float64(p.TotalQty) * 100
	}
	return ParentSummary{
		ID:         p.ID,
		ClientID:   p.ClientID,
		Symbol:     p.Symbol,
		Side:       int(p.Side),
		TotalQty:   p.TotalQty,
		FilledQty:  filled,
		Progress:   progress,
		StartTime:  p.StartTime,
		EndTime:    p.EndTime,
		Status:     p.Status.Load(),
		StatusStr:  statusStr(ParentOrderStatus(p.Status.Load())),
		AlgoType:   int(p.AlgoType),
		MaxPrice:   p.MaxPrice,
		SliceCount: p.SliceCount,
		CreatedAt:  p.CreatedAt,
	}
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
