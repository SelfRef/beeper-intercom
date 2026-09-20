// Package service is the layer that knows what the bridge is for: it turns
// inbound notifications into room messages, inbound messages into agent
// turns, and inbound reactions and poll answers into webhook calls.
//
// Everything above it (HTTP, MCP, the ntfy mirror) calls into this; everything
// below it (Matrix, agents, the store) is called by it.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/agent"
	"github.com/SelfRef/beeper-intercom/internal/bridge"
	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/notify"
	"github.com/SelfRef/beeper-intercom/internal/store"
)

// Service is the orchestrator.
type Service struct {
	// cfg is swapped on reload; readers take it through conf().
	cfg    atomic.Pointer[config.Config]
	store  *store.Store
	bridge *bridge.Bridge
	log    zerolog.Logger
	// configPath is where a reload re-reads from.
	configPath string

	agentsMu sync.RWMutex
	agents   map[string]agent.Agent

	// queue serialises outbound announcements. A burst from the ntfy mirror
	// after downtime must not become 200 parallel sends against hungryserv.
	queue chan *sendJob

	// ready closes once the rooms exist.
	ready chan struct{}

	// Version is reported in the status room on startup; main sets it.
	Version string

	turnMu sync.Mutex
	turns  map[string]*runningTurn // session key -> in-flight turn

	// toolsetTimers are the idle countdowns of switched-on toolsets, so an
	// escalation closes itself and says so rather than waiting to be noticed.
	toolsetMu     sync.Mutex
	toolsetTimers map[string]*time.Timer

	// pendingAsks are the questions waiting for an answer, by conversation
	// slot. A Matrix poll cannot take a typed answer, so a message sent while
	// one is open is delivered here instead of starting a new turn.
	askMu       sync.Mutex
	pendingAsks map[string]*openQuestion

	// noticeAvatar caches the uploaded mxc for the notice profile's avatar, so
	// a notice does not read and hash a file every time.
	noticeMu         sync.Mutex
	noticeAvatar     string
	noticeAvatarPath string

	waiters sync.Map // poll event ID -> chan string

	startedAt time.Time
}

type sendJob struct {
	notification *notify.Notification
	result       chan sendResult
}

type sendResult struct {
	EventID   id.EventID
	Duplicate bool
	Err       error
}

type runningTurn struct {
	cancel  context.CancelFunc
	eventID id.EventID
}

// New wires the service. The bridge is created here too, because its inbound
// handlers are service methods.
func New(cfg *config.Config, configPath string, st *store.Store, log zerolog.Logger) (*Service, error) {
	s := &Service{
		configPath:    configPath,
		store:         st,
		log:           log,
		agents:        map[string]agent.Agent{},
		queue:         make(chan *sendJob, cfg.Limits.QueueSize),
		turns:         map[string]*runningTurn{},
		toolsetTimers: map[string]*time.Timer{},
		pendingAsks:   map[string]*openQuestion{},
		ready:         make(chan struct{}),
		startedAt:     time.Now(),
	}
	s.cfg.Store(cfg)
	if err := s.buildAgents(cfg); err != nil {
		return nil, err
	}
	s.bridge = bridge.New(cfg, st, log, bridge.Handlers{
		OnMessage:      s.onMessage,
		OnReaction:     s.onReaction,
		OnRedaction:    s.onRedaction,
		OnPollResponse: s.onPollResponse,
	})
	return s, nil
}

// conf is the current config.
func (s *Service) conf() *config.Config { return s.cfg.Load() }

// buildAgents (re)creates the adapters from a config.
func (s *Service) buildAgents(cfg *config.Config) error {
	agents := make(map[string]agent.Agent, len(cfg.Agents))
	for _, name := range config.SortedKeys(cfg.Agents) {
		built, err := agent.New(name, cfg.Agents[name], s.store, s.log)
		if err != nil {
			return err
		}
		agents[name] = built
	}
	s.agentsMu.Lock()
	s.agents = agents
	s.agentsMu.Unlock()
	return nil
}

// agentFor looks up a built adapter.
func (s *Service) agentFor(name string) (agent.Agent, bool) {
	s.agentsMu.RLock()
	defer s.agentsMu.RUnlock()
	built, ok := s.agents[name]
	return built, ok
}

// Reload re-reads the config file, rebuilds the agents and reconciles the
// rooms. Sessions survive: they are keyed by room, not by config object.
//
// The registration name cannot change this way — it decides which appservice
// this process IS, and swapping it would mean a different set of ghosts, a
// different namespace and orphaned rooms.
func (s *Service) Reload(ctx context.Context) error {
	if s.configPath == "" {
		return fmt.Errorf("no config file to reload")
	}
	next, err := config.Load(s.configPath)
	if err != nil {
		return err
	}
	current := s.conf()
	if next.Network.Bridge != current.Network.Bridge {
		return fmt.Errorf("network.bridge cannot change on reload (%s -> %s); restart instead",
			current.Network.Bridge, next.Network.Bridge)
	}
	if err := s.buildAgents(next); err != nil {
		return err
	}
	s.cfg.Store(next)
	s.bridge.SetConfig(next)
	if err := s.bridge.Reconcile(ctx); err != nil {
		return fmt.Errorf("reconcile after reload: %w", err)
	}
	s.log.Info().Msg("Reloaded configuration")
	s.bridge.Status(ctx, fmt.Sprintf("Configuration reloaded — %d rooms, %d ghosts, %d agents.",
		len(next.Rooms), len(next.Ghosts), len(next.Agents)))
	return nil
}

// Bridge exposes the Matrix side for the health endpoint and admin API.
func (s *Service) Bridge() *bridge.Bridge { return s.bridge }

// Config exposes the loaded config for read-only surfaces.
func (s *Service) Config() *config.Config { return s.conf() }

// Store exposes the database for read-only surfaces.
func (s *Service) Store() *store.Store { return s.store }

// StartedAt is the process start, for /health.
func (s *Service) StartedAt() time.Time { return s.startedAt }

// Start starts the workers and begins connecting. It returns an error only
// for something no amount of retrying will fix.
//
// Connecting happens in the background on purpose: a Beeper outage, or a
// hungryserv that is slow to come back, should not crash-loop the container
// and take the HTTP API with it. The API answers throughout, /health reports
// the transport as disconnected, and sources are held back until the rooms
// exist (Ready).
func (s *Service) Start(ctx context.Context) error {
	cfg := s.conf()
	if cfg.AccountToken() == "" {
		return fmt.Errorf("no Beeper account token in $%s", cfg.Matrix.TokenEnv)
	}
	go s.sendWorker(ctx)
	go s.deliveryWorker(ctx)
	go s.cleanupWorker(ctx)
	go s.connectLoop(ctx)
	return nil
}

// connectLoop registers, connects and reconciles, retrying until it works.
func (s *Service) connectLoop(ctx context.Context) {
	backoff := 5 * time.Second
	for ctx.Err() == nil {
		err := s.bridge.Start(ctx)
		if err == nil {
			err = s.bridge.Reconcile(ctx)
		}
		if err == nil {
			close(s.ready)
			s.log.Info().Msg("Bridge is ready")
			cfg := s.conf()
			s.bridge.Status(ctx, fmt.Sprintf("Bridge up — %s, %d rooms, %d ghosts, %d agents.",
				s.Version, len(cfg.Rooms), len(cfg.Ghosts), len(cfg.Agents)))
			return
		}
		s.log.Error().Err(err).Dur("retry_in", backoff).Msg("Could not bring the bridge up")
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 5*time.Minute {
			backoff *= 2
		}
	}
}

// Ready reports whether the rooms exist and announcements can be delivered.
func (s *Service) Ready() bool {
	select {
	case <-s.ready:
		return true
	default:
		return false
	}
}

// WaitReady blocks until the rooms exist. Sources call it so a mirrored
// message is never dropped for want of a room.
func (s *Service) WaitReady(ctx context.Context) error {
	select {
	case <-s.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// --- announcements ---------------------------------------------------------

// Notify delivers one notification. It blocks until the message is sent so
// the caller gets a real event ID back, but the work happens on one worker,
// so a burst queues instead of stampeding.
func (s *Service) Notify(ctx context.Context, n *notify.Notification) (id.EventID, bool, error) {
	if err := n.Validate(); err != nil {
		return "", false, err
	}
	if _, ok := s.conf().Rooms[n.Room]; !ok && !strings.HasPrefix(n.Room, "!") {
		return "", false, fmt.Errorf("unknown room %q", n.Room)
	}
	job := &sendJob{notification: n, result: make(chan sendResult, 1)}
	select {
	case s.queue <- job:
	case <-ctx.Done():
		return "", false, ctx.Err()
	default:
		return "", false, fmt.Errorf("send queue is full (%d): the bridge is behind", cap(s.queue))
	}
	select {
	case res := <-job.result:
		return res.EventID, res.Duplicate, res.Err
	case <-ctx.Done():
		return "", false, ctx.Err()
	}
}

func (s *Service) sendWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-s.queue:
			eventID, duplicate, err := s.deliver(ctx, job.notification)
			job.result <- sendResult{EventID: eventID, Duplicate: duplicate, Err: err}
		}
	}
}

func (s *Service) deliver(ctx context.Context, n *notify.Notification) (id.EventID, bool, error) {
	if n.Dedupe != "" {
		existing, err := s.store.NotificationByDedupe(ctx, n.Dedupe)
		if err != nil {
			return "", false, err
		}
		if existing != nil {
			return id.EventID(existing.EventID), true, nil
		}
	}

	roomKey := n.Room
	ghostKey := n.Ghost
	if strings.HasPrefix(roomKey, "!") {
		// A raw room ID is allowed so a caller can post into a room the
		// bridge created earlier but no longer declares.
		key, ok := s.bridge.RoomKey(id.RoomID(roomKey))
		if !ok {
			return "", false, fmt.Errorf("room %s is not one of this bridge's rooms", roomKey)
		}
		roomKey = key
	}
	ghostKey, err := s.conf().GhostForRoom(roomKey, ghostKey)
	if err != nil {
		return "", false, err
	}

	threadRoot, err := s.resolveThread(ctx, n.Thread)
	if err != nil {
		return "", false, err
	}

	eventID, err := s.bridge.SendNotification(ctx, roomKey, ghostKey, n, threadRoot)
	if err != nil {
		return "", false, err
	}

	roomID, _ := s.bridge.RoomID(roomKey)
	record := &store.Notification{
		EventID:    eventID.String(),
		RoomKey:    roomKey,
		RoomID:     roomID.String(),
		Ghost:      ghostKey,
		Title:      n.Title,
		Body:       n.Text,
		Priority:   n.Priority,
		Tags:       n.Tags,
		Actions:    n.Actions,
		ThreadRoot: threadRoot.String(),
		Dedupe:     n.Dedupe,
	}
	if n.Source != nil {
		record.SourceKind = n.Source.Kind
		record.SourceID = n.Source.ID
		record.SourcePayload = n.Source.Payload
	}
	if err := s.store.PutNotification(ctx, record); err != nil {
		// The message is already in the room; losing the record only costs
		// actions and threads on it, so log rather than fail the caller.
		s.log.Error().Err(err).Str("event_id", eventID.String()).Msg("Failed to record notification")
	}
	return eventID, false, nil
}

// resolveThread accepts an event ID, a "kind:id" source reference or a dedupe
// key, so a publisher can thread onto its own earlier message without having
// kept the event ID.
func (s *Service) resolveThread(ctx context.Context, thread string) (id.EventID, error) {
	if thread == "" {
		return "", nil
	}
	if strings.HasPrefix(thread, "$") {
		return id.EventID(thread), nil
	}
	found, err := s.store.NotificationByDedupe(ctx, thread)
	if err != nil {
		return "", err
	}
	if found != nil {
		return threadRootOf(found), nil
	}
	if kind, sourceID, ok := strings.Cut(thread, ":"); ok {
		found, err := s.store.NotificationBySource(ctx, kind, sourceID)
		if err != nil {
			return "", err
		}
		if found != nil {
			return threadRootOf(found), nil
		}
		// "Thread with earlier notifications from this source, if any": the
		// first one from a source has nothing to hang under, so it becomes the
		// root the later ones will find. That is how one thread per chat, per
		// camera or per job works without the publisher tracking event IDs.
		return "", nil
	}
	return "", fmt.Errorf("thread target %q not found", thread)
}

// threadRootOf keeps threads one level deep: replying to something that is
// already in a thread joins that thread rather than nesting.
func threadRootOf(n *store.Notification) id.EventID {
	if n.ThreadRoot != "" {
		return id.EventID(n.ThreadRoot)
	}
	return id.EventID(n.EventID)
}

// Reply posts into an existing thread, which is what an automation's answer
// (an Open WebUI notification relayed by n8n) needs.
func (s *Service) Reply(ctx context.Context, roomKey, ghostKey, text, thread string) (id.EventID, error) {
	n := &notify.Notification{Room: roomKey, Ghost: ghostKey, Text: text, Thread: thread}
	eventID, _, err := s.Notify(ctx, n)
	return eventID, err
}

// --- polls -----------------------------------------------------------------

// AskPoll posts a question and, if wait is non-zero, blocks until it is
// answered. This is the confirmation primitive for anything irreversible.
func (s *Service) AskPoll(ctx context.Context, roomKey, ghostKey, question string, answers []string, thread string, source json.RawMessage, wait time.Duration) (id.EventID, string, error) {
	room, ok := s.conf().Rooms[roomKey]
	if !ok {
		return "", "", fmt.Errorf("unknown room %q", roomKey)
	}
	ghostKey, err := s.conf().GhostForRoom(roomKey, ghostKey)
	if err != nil {
		return "", "", err
	}
	roomID, ok := s.bridge.RoomID(roomKey)
	if !ok {
		return "", "", fmt.Errorf("room %q has no Matrix room yet", roomKey)
	}
	threadRoot, err := s.resolveThread(ctx, thread)
	if err != nil {
		return "", "", err
	}

	eventID, err := s.bridge.SendPoll(ctx, roomID, ghostKey, question, answers, bridge.SendOptions{ThreadRoot: threadRoot})
	if err != nil {
		return "", "", err
	}
	if err := s.store.PutPoll(ctx, &store.Poll{
		EventID:  eventID.String(),
		RoomKey:  roomKey,
		RoomID:   roomID.String(),
		Question: question,
		Answers:  answers,
		Webhook:  s.actionWebhook(room),
		Source:   string(source),
	}); err != nil {
		return "", "", err
	}
	if wait <= 0 {
		return eventID, "", nil
	}

	ch := make(chan string, 1)
	s.waiters.Store(eventID.String(), ch)
	defer s.waiters.Delete(eventID.String())
	select {
	case answer := <-ch:
		return eventID, answer, nil
	case <-time.After(wait):
		return eventID, "", fmt.Errorf("no answer within %s", wait)
	case <-ctx.Done():
		return eventID, "", ctx.Err()
	}
}

func (s *Service) onPollResponse(ctx context.Context, evt *bridge.PollResponse) {
	poll, err := s.store.Poll(ctx, evt.PollID.String())
	if err != nil || poll == nil {
		return
	}
	answer := ""
	if len(evt.Answers) > 0 {
		answer = evt.Answers[0]
	}
	if err := s.store.AnswerPoll(ctx, poll.EventID, answer); err != nil {
		s.log.Error().Err(err).Msg("Failed to record poll answer")
	}
	if ch, ok := s.waiters.Load(poll.EventID); ok {
		select {
		case ch.(chan string) <- answer:
		default:
		}
	}
	if poll.Webhook != "" {
		s.enqueueAction(ctx, poll.Webhook, map[string]any{
			"schema":   1,
			"kind":     "poll_response",
			"room":     evt.RoomKey,
			"event_id": poll.EventID,
			"question": poll.Question,
			"answer":   answer,
			"answers":  evt.Answers,
			"source":   rawOrNil(poll.Source),
			"actor":    evt.Sender.String(),
			"at":       time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// --- reactions as commands -------------------------------------------------

func (s *Service) onReaction(ctx context.Context, evt *bridge.Reaction) {
	room, ok := s.conf().Rooms[evt.RoomKey]
	if !ok {
		return
	}
	// Reacting to one of my own command messages is how I throw it away
	// together with whatever the bridge answered — a bridge message cannot be
	// reacted to, so the command is the only handle there is. It is never an
	// action: my own messages declare none.
	if evt.Sender == s.bridge.UserID() && s.clearForCommand(ctx, evt, room) {
		return
	}

	webhook := s.actionWebhook(room)
	if webhook == "" {
		return
	}

	notification, err := s.store.NotificationByEvent(ctx, evt.Target.String())
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to look up reacted event")
		return
	}

	var action any
	var source any
	if notification != nil {
		if name, ok := notification.Actions[evt.Key]; ok {
			action = name
		}
		source = map[string]any{
			"kind":    notification.SourceKind,
			"id":      notification.SourceID,
			"payload": rawOrNil(string(notification.SourcePayload)),
		}
	}
	if action == nil && !s.conf().Actions.ReportUndeclared {
		return
	}

	s.enqueueAction(ctx, webhook, map[string]any{
		"schema":   1,
		"kind":     "reaction",
		"action":   action, // null when the reaction declared no action
		"key":      evt.Key,
		"room":     evt.RoomKey,
		"event_id": evt.Target.String(),
		"source":   source,
		"actor":    evt.Sender.String(),
		"at":       time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Service) actionWebhook(room config.Room) string {
	if room.ActionsWebhook != "" {
		return room.ActionsWebhook
	}
	return s.conf().Actions.Webhook
}

func (s *Service) enqueueAction(ctx context.Context, url string, payload map[string]any) {
	if _, err := s.store.EnqueueDelivery(ctx, "action", url, payload); err != nil {
		s.log.Error().Err(err).Msg("Failed to enqueue action delivery")
	}
}

func rawOrNil(raw string) any {
	if raw == "" {
		return nil
	}
	var out any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return raw
	}
	return out
}

// SetRoomMuted mutes or unmutes one of the bridge's rooms.
func (s *Service) SetRoomMuted(ctx context.Context, roomKey string, muted bool) error {
	roomID, ok := s.bridge.RoomID(roomKey)
	if !ok {
		return fmt.Errorf("unknown room %q", roomKey)
	}
	return s.bridge.SetMuted(ctx, roomID, muted)
}
