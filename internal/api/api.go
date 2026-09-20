// Package api is the HTTP surface: the ingest API sources post to, the admin
// API humans and automations read, and the health endpoint.
//
// Two tokens, deliberately: the ingest token ends up in scripts and n8n
// workflows, so it can post a notification and nothing else. Rewriting rooms,
// reading stored notification bodies or reloading config needs the admin one.
package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/notify"
	"github.com/SelfRef/beeper-intercom/internal/service"
)

// Server is the HTTP API.
type Server struct {
	svc         *service.Service
	log         zerolog.Logger
	ingestToken string
	adminToken  string
	extra       map[string]http.Handler
}

// New builds the API. extra holds handlers mounted behind the admin token —
// the MCP endpoint, in practice.
func New(svc *service.Service, log zerolog.Logger, extra map[string]http.Handler) *Server {
	cfg := svc.Config()
	return &Server{
		svc:         svc,
		log:         log,
		ingestToken: cfg.IngestToken(),
		adminToken:  cfg.AdminToken(),
		extra:       extra,
	}
}

// Handler builds the mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", s.health)

	// Ingest: what a source is allowed to do.
	mux.Handle("POST /v1/notify", s.ingest(s.notifyHandler))
	mux.Handle("POST /v1/reply", s.ingest(s.replyHandler))
	mux.Handle("POST /v1/poll", s.ingest(s.pollHandler))

	// Admin: everything that reads state or changes it.
	mux.Handle("GET /v1/rooms", s.admin(s.roomsHandler))
	mux.Handle("GET /v1/ghosts", s.admin(s.ghostsHandler))
	mux.Handle("GET /v1/sessions", s.admin(s.sessionsHandler))
	mux.Handle("GET /v1/events", s.admin(s.eventsHandler))
	mux.Handle("GET /v1/deliveries", s.admin(s.deliveriesHandler))
	mux.Handle("POST /v1/deliveries/{id}/retry", s.admin(s.retryHandler))
	mux.Handle("POST /v1/reload", s.admin(s.reloadHandler))
	mux.Handle("GET /v1/status", s.admin(s.statusHandler))

	for path, handler := range s.extra {
		mux.Handle(path, s.adminHandler(handler))
	}
	return logging(s.log, mux)
}

// --- auth ------------------------------------------------------------------

func (s *Server) ingest(next http.HandlerFunc) http.Handler {
	return s.authorised(next, s.ingestToken, s.adminToken)
}

func (s *Server) admin(next http.HandlerFunc) http.Handler {
	return s.authorised(next, s.adminToken)
}

func (s *Server) adminHandler(next http.Handler) http.Handler {
	return s.authorised(next.ServeHTTP, s.adminToken)
}

func (s *Server) authorised(next http.HandlerFunc, accepted ...string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		for _, token := range accepted {
			// An empty configured token would accept everything, which is a
			// misconfiguration rather than an open door; the process warns
			// about it on startup.
			if token != "" && presented == token {
				next(w, r)
				return
			}
		}
		writeError(w, http.StatusUnauthorized, "unauthorized")
	})
}

// --- ingest ----------------------------------------------------------------

func (s *Server) notifyHandler(w http.ResponseWriter, r *http.Request) {
	var n notify.Notification
	if err := decode(r, &n); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	eventID, duplicate, err := s.svc.Notify(r.Context(), &n)
	if err != nil {
		writeError(w, statusForError(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id":  eventID.String(),
		"duplicate": duplicate,
	})
}

type replyRequest struct {
	Room   string `json:"room"`
	Ghost  string `json:"ghost"`
	Text   string `json:"text"`
	Thread string `json:"thread"`
}

func (s *Server) replyHandler(w http.ResponseWriter, r *http.Request) {
	var req replyRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	eventID, err := s.svc.Reply(r.Context(), req.Room, req.Ghost, req.Text, req.Thread)
	if err != nil {
		writeError(w, statusForError(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"event_id": eventID.String()})
}

type pollRequest struct {
	Room     string          `json:"room"`
	Ghost    string          `json:"ghost"`
	Question string          `json:"question"`
	Answers  []string        `json:"answers"`
	Thread   string          `json:"thread"`
	Source   json.RawMessage `json:"source"`
	WaitSecs int             `json:"wait_seconds"`
}

func (s *Server) pollHandler(w http.ResponseWriter, r *http.Request) {
	var req pollRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Question == "" {
		writeError(w, http.StatusBadRequest, "question is required")
		return
	}
	wait := time.Duration(req.WaitSecs) * time.Second
	eventID, answer, err := s.svc.AskPoll(r.Context(), req.Room, req.Ghost, req.Question, req.Answers, req.Thread, req.Source, wait)
	if err != nil && answer == "" && eventID == "" {
		writeError(w, statusForError(err), err.Error())
		return
	}
	out := map[string]any{"event_id": eventID.String(), "answer": answer}
	if err != nil {
		out["timeout"] = true
	}
	writeJSON(w, http.StatusOK, out)
}

// --- admin -----------------------------------------------------------------

func (s *Server) roomsHandler(w http.ResponseWriter, r *http.Request) {
	cfg := s.svc.Config()
	ids := s.svc.Bridge().RoomIDs()
	out := make([]map[string]any, 0, len(cfg.Rooms))
	for _, key := range config.SortedKeys(cfg.Rooms) {
		room := cfg.Rooms[key]
		out = append(out, map[string]any{
			"key":     key,
			"name":    room.Name,
			"kind":    room.Kind,
			"ghosts":  room.Ghosts,
			"agent":   room.Agent,
			"tags":    room.Tags,
			"muted":   room.Muted,
			"room_id": ids[key].String(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) ghostsHandler(w http.ResponseWriter, r *http.Request) {
	cfg := s.svc.Config()
	bridge := s.svc.Bridge()
	out := make([]map[string]any, 0, len(cfg.Ghosts))
	for _, key := range config.SortedKeys(cfg.Ghosts) {
		out = append(out, map[string]any{
			"key":  key,
			"name": cfg.Ghosts[key].Name,
			"mxid": bridge.GhostMXID(key).String(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) sessionsHandler(w http.ResponseWriter, r *http.Request) {
	includeClosed := r.URL.Query().Get("all") == "true"
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	sessions, err := s.svc.Store().ListSessions(r.Context(), includeClosed, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, sessions)
}

func (s *Server) eventsHandler(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	notifications, err := s.svc.Store().RecentNotifications(r.Context(), r.URL.Query().Get("room"), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, notifications)
}

func (s *Server) deliveriesHandler(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	deliveries, err := s.svc.Store().ListDeliveries(r.Context(), r.URL.Query().Get("status"), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, deliveries)
}

func (s *Server) retryHandler(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad delivery id")
		return
	}
	if err := s.svc.Store().RetryDelivery(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queued": true})
}

func (s *Server) reloadHandler(w http.ResponseWriter, r *http.Request) {
	if err := s.svc.Reload(r.Context()); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"reloaded": true})
}

func (s *Server) statusHandler(w http.ResponseWriter, r *http.Request) {
	cfg := s.svc.Config()
	bridge := s.svc.Bridge()
	writeJSON(w, http.StatusOK, map[string]any{
		"network":   cfg.Network.Name,
		"bridge":    cfg.Network.Bridge,
		"protocol":  cfg.Network.ID,
		"connected": bridge.Connected(),
		"user":      bridge.UserID().String(),
		"bot":       bridge.BotMXID().String(),
		"rooms":     len(cfg.Rooms),
		"ghosts":    len(cfg.Ghosts),
		"agents":    config.SortedKeys(cfg.Agents),
		"uptime":    time.Since(s.svc.StartedAt()).Round(time.Second).String(),
	})
}

// health is intentionally shallow: it reports that the process is up and
// whether the transport is connected, without touching Beeper. A healthcheck
// that fails because the upstream is having a bad minute restarts a container
// that would have recovered on its own.
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"ready":     s.svc.Ready(),
		"connected": s.svc.Bridge().Connected(),
		"uptime":    time.Since(s.svc.StartedAt()).Round(time.Second).String(),
	})
}

// --- helpers ---------------------------------------------------------------

func decode(r *http.Request, out any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 4<<20))
	dec.DisallowUnknownFields()
	return dec.Decode(out)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}

// statusForError keeps "you asked for something that does not exist" out of
// the 500 bucket, so a caller can tell a typo from an outage.
func statusForError(err error) int {
	text := err.Error()
	switch {
	case strings.Contains(text, "unknown room"), strings.Contains(text, "not found"),
		strings.Contains(text, "is not a member"), strings.Contains(text, "required"),
		strings.Contains(text, "unknown priority"), strings.Contains(text, "is empty"):
		return http.StatusBadRequest
	case strings.Contains(text, "queue is full"):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func logging(log zerolog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		if r.URL.Path == "/health" {
			return
		}
		log.Debug().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", recorder.status).
			Dur("took", time.Since(start)).
			Msg("HTTP")
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// Flush keeps streaming handlers (the MCP endpoint) working through the
// logging wrapper.
func (r *statusRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
