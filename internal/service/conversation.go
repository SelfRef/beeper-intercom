package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/agent"
	"github.com/SelfRef/beeper-intercom/internal/bridge"
	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/store"
)

const (
	kvAgentOverride = "agent_override:"
	kvModelOverride = "model_override:"
	typingRefresh   = 20 * time.Second
	workingReaction = "⚙️"
)

// onMessage is the entry point for everything the account owner types.
func (s *Service) onMessage(ctx context.Context, msg *bridge.Message) {
	room, ok := s.conf().Rooms[msg.RoomKey]
	if !ok {
		return
	}
	if strings.HasPrefix(strings.TrimSpace(msg.Body), "/") {
		// Commands are answered even in a room with no agent, because /status
		// is how you find out why a room has no agent.
		go s.handleCommand(detach(ctx), msg, room)
		return
	}
	if room.Agent == "" {
		return
	}
	go s.handleTurn(detach(ctx), msg, room)
}

// detach keeps a turn running after the transaction that delivered the
// message has been acknowledged. The homeserver's context dies in seconds; a
// model answer takes longer than that.
func detach(ctx context.Context) context.Context {
	return context.WithoutCancel(ctx)
}

func sessionKey(roomKey string, threadRoot id.EventID) string {
	return roomKey + "\x00" + threadRoot.String()
}

func (s *Service) handleTurn(ctx context.Context, msg *bridge.Message, room config.Room) {
	key := sessionKey(msg.RoomKey, msg.ThreadRoot)

	// One turn at a time per conversation: two questions in a row should be
	// two turns in order, not a race over the same parent id.
	turnCtx, cancel := context.WithCancel(ctx)
	s.turnMu.Lock()
	if existing, running := s.turns[key]; running {
		existing.cancel()
	}
	s.turns[key] = &runningTurn{cancel: cancel, eventID: msg.EventID}
	s.turnMu.Unlock()
	defer func() {
		cancel()
		s.turnMu.Lock()
		if current, ok := s.turns[key]; ok && current.eventID == msg.EventID {
			delete(s.turns, key)
		}
		s.turnMu.Unlock()
	}()

	sess, backend, err := s.sessionFor(turnCtx, msg, room)
	if err != nil {
		s.reportFailure(ctx, msg, err)
		return
	}

	// An edit re-runs the turn from the same point in the conversation, which
	// is what "regenerate" means to the backend.
	parent := sess.ParentID
	if msg.Edits != "" {
		if turn, err := s.store.Turn(ctx, msg.Edits.String()); err == nil && turn != nil {
			parent = turn.ParentID
		}
	}

	roomID := msg.RoomID
	ghostKey := room.Ghosts[0]

	stopTyping := s.keepTyping(turnCtx, roomID, ghostKey)
	defer stopTyping()

	// A reaction on my own message is cheaper than a "working on it" bubble
	// and disappears when the answer lands.
	marker, err := s.bridge.React(ctx, roomID, ghostKey, msg.EventID, workingReaction)
	if err != nil {
		s.log.Debug().Err(err).Msg("Could not set the working marker")
	}
	defer func() {
		if marker != "" {
			if err := s.bridge.Redact(ctx, roomID, ghostKey, marker); err != nil {
				s.log.Debug().Err(err).Msg("Could not clear the working marker")
			}
		}
	}()

	// Streaming: the first delta opens a live bubble (com.beeper.stream) and
	// every later one feeds it; the finished answer is committed as an edit.
	// If the anchor cannot be sent, deltas are dropped and the answer arrives
	// the old way — streaming is comfort, not correctness.
	answerOpts := bridge.SendOptions{ThreadRoot: msg.ThreadRoot}
	if answerOpts.ThreadRoot != "" {
		answerOpts.ReplyTo = msg.EventID
	}
	var stream *bridge.Stream
	var streamMu sync.Mutex
	streamFailed := false
	sink := agent.Sink{}
	if backend.Caps().Streaming {
		sink.Delta = func(text string) {
			streamMu.Lock()
			defer streamMu.Unlock()
			if stream == nil {
				if streamFailed {
					return
				}
				opened, err := s.bridge.StartStream(ctx, roomID, ghostKey, answerOpts)
				if err != nil {
					streamFailed = true
					s.log.Warn().Err(err).Msg("Could not open a stream; falling back to one message")
					return
				}
				stream = opened
				stopTyping()
			}
			stream.Push(text)
		}
	}

	conv := agent.Conversation{ID: sess.ConvID, Parent: parent, Model: sess.Model, Turns: sess.Turns}
	reply, err := backend.Send(turnCtx, conv, agent.Turn{Text: msg.Body}, sink)
	streamMu.Lock()
	opened := stream
	streamMu.Unlock()
	if err != nil {
		if opened != nil {
			opened.Abort(ctx)
		}
		if turnCtx.Err() != nil {
			// Cancelled on purpose (a redaction, or a newer message): say
			// nothing, the user already knows.
			return
		}
		s.reportFailure(ctx, msg, err)
		return
	}
	stopTyping()

	replyEvent, err := s.postAnswer(ctx, msg, ghostKey, reply.Text, opened, answerOpts)
	if err != nil {
		s.reportFailure(ctx, msg, err)
		return
	}

	if reply.Parent != "" {
		parent = reply.Parent
	}
	if err := s.store.AdvanceSession(ctx, sess.ID, parent); err != nil {
		s.log.Error().Err(err).Msg("Failed to advance session")
	}
	if err := s.store.PutTurn(ctx, &store.Turn{
		EventID:    msg.EventID.String(),
		SessionID:  sess.ID,
		ParentID:   conv.Parent,
		ReplyEvent: replyEvent.String(),
	}); err != nil {
		s.log.Error().Err(err).Msg("Failed to record turn")
	}
}

// postAnswer renders and sends the answer, splitting it at paragraph
// boundaries and attaching anything that is too long to read in a bubble.
// With a live stream, the first part becomes the stream's final edit — the
// durable copy of what was streamed — and only the overflow is new messages.
func (s *Service) postAnswer(ctx context.Context, msg *bridge.Message, ghostKey, text string, stream *bridge.Stream, opts bridge.SendOptions) (id.EventID, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		text = "_(the agent returned an empty answer)_"
	}

	limit := s.conf().Limits.MessageSplitBytes
	parts := bridge.SplitMessage(text, limit)

	var first id.EventID
	for i, part := range parts {
		plain, formatted := bridge.Markdown(part)
		if i == 0 && stream != nil {
			if err := stream.Finish(ctx, plain, formatted); err != nil {
				return "", err
			}
			first = stream.EventID
			continue
		}
		sent, err := s.bridge.SendText(ctx, msg.RoomID, ghostKey, plain, formatted, opts)
		if err != nil {
			return first, err
		}
		if first == "" {
			first = sent
		}
	}

	// Past the limit the answer is also a file, so it can be read, searched
	// and kept rather than scrolled.
	if len(text) > limit {
		uri, err := s.bridge.UploadBytes(ctx, ghostKey, "answer.md", "text/markdown", []byte(text))
		if err != nil {
			s.log.Warn().Err(err).Msg("Failed to upload the long answer")
		} else if _, err := s.bridge.SendFile(ctx, msg.RoomID, ghostKey, "answer.md", "text/markdown", uri, len(text), opts); err != nil {
			s.log.Warn().Err(err).Msg("Failed to attach the long answer")
		}
	}
	return first, nil
}

// sessionFor returns the live session for this message, rotating it when the
// rules say this is a new conversation.
func (s *Service) sessionFor(ctx context.Context, msg *bridge.Message, room config.Room) (*store.Session, agent.Agent, error) {
	agentName := room.Agent
	if override, _ := s.store.GetKV(ctx, kvAgentOverride+msg.RoomKey); override != "" {
		if _, ok := s.conf().Agents[override]; ok {
			agentName = override
		}
	}
	agentCfg, agentOK := s.conf().Agents[agentName]
	if !agentOK {
		return nil, nil, fmt.Errorf("room %q has no usable agent", msg.RoomKey)
	}
	backend, ok := s.agentFor(agentName)
	if !ok {
		return nil, nil, fmt.Errorf("agent %q is configured but was not built", agentName)
	}

	model := agentCfg.Model
	if override, _ := s.store.GetKV(ctx, kvModelOverride+msg.RoomKey); override != "" {
		model = override
	}

	threadRoot := msg.ThreadRoot.String()
	live, err := s.store.LiveSession(ctx, msg.RoomKey, threadRoot)
	if err != nil {
		return nil, nil, err
	}

	if live != nil && live.Agent == agentName {
		reason := rotationReason(live, agentCfg.Session)
		if reason == "" {
			return live, backend, nil
		}
		s.rotate(ctx, msg, room, live, backend, reason)
	} else if live != nil {
		// The agent changed under it; the old conversation cannot continue.
		_ = s.store.CloseSession(ctx, live.ID)
	}

	seed := s.seedFor(ctx, msg, live, agentCfg)
	convID, err := backend.NewConversation(ctx, seed)
	if err != nil {
		return nil, nil, fmt.Errorf("start conversation: %w", err)
	}
	created, err := s.store.CreateSession(ctx, &store.Session{
		RoomKey:    msg.RoomKey,
		ThreadRoot: threadRoot,
		Agent:      agentName,
		ConvID:     convID,
		Model:      model,
	})
	if err != nil {
		return nil, nil, err
	}
	return created, backend, nil
}

// rotationReason implements the table from the design: explicit reset, idle,
// or a hard turn ceiling. Compaction has been handling growth inside the
// conversation long before the ceiling is reached.
func rotationReason(sess *store.Session, policy config.Session) string {
	if policy.MaxTurns > 0 && sess.Turns >= policy.MaxTurns {
		return fmt.Sprintf("%d turns", sess.Turns)
	}
	if policy.IdleMinutes > 0 {
		idle := time.Since(time.UnixMilli(sess.LastActive))
		if idle > time.Duration(policy.IdleMinutes)*time.Minute {
			return fmt.Sprintf("idle for %s", idle.Round(time.Minute))
		}
	}
	return ""
}

// rotate closes a conversation and tells the room where it went, so the
// history stays one tap away in the backend's own UI.
func (s *Service) rotate(ctx context.Context, msg *bridge.Message, room config.Room, sess *store.Session, backend agent.Agent, reason string) {
	if err := s.store.CloseSession(ctx, sess.ID); err != nil {
		s.log.Error().Err(err).Msg("Failed to close session")
	}
	text := fmt.Sprintf("New conversation (%s).", reason)
	if link := backend.Link(sess.ConvID); link != "" {
		text += fmt.Sprintf(" Previous: %s", link)
	}
	plain, formatted := bridge.Markdown(text)
	if _, err := s.bridge.SendText(ctx, msg.RoomID, room.Ghosts[0], plain, formatted, bridge.SendOptions{
		Notice:     true,
		ThreadRoot: msg.ThreadRoot,
	}); err != nil {
		s.log.Debug().Err(err).Msg("Failed to announce conversation rotation")
	}
}

// seedFor decides what a new conversation starts from: the notification a
// thread hangs under, or a summary carried over from the conversation this
// one replaces.
func (s *Service) seedFor(ctx context.Context, msg *bridge.Message, previous *store.Session, agentCfg config.Agent) *agent.Seed {
	if msg.ThreadRoot != "" {
		if n, err := s.store.NotificationByEvent(ctx, msg.ThreadRoot.String()); err == nil && n != nil {
			text := strings.TrimSpace(n.Title + "\n" + n.Body)
			if len(n.SourcePayload) > 0 {
				text += "\n\nRaw payload:\n" + string(n.SourcePayload)
			}
			return &agent.Seed{Room: msg.RoomKey, Kind: "notification", Text: text}
		}
	}
	if previous != nil && agentCfg.Session.CarrySummary {
		if summary := s.summarise(ctx, previous, agentCfg); summary != "" {
			return &agent.Seed{Room: msg.RoomKey, Kind: "summary", Text: summary}
		}
	}
	return nil
}

// summarise asks the outgoing conversation to summarise itself. Doing it as a
// normal turn rather than through a backend-specific compaction API means
// every adapter gets continuity across a rotation, not just Open WebUI.
func (s *Service) summarise(ctx context.Context, sess *store.Session, agentCfg config.Agent) string {
	backend, ok := s.agentFor(sess.Agent)
	if !ok {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	reply, err := backend.Send(ctx, agent.Conversation{ID: sess.ConvID, Parent: sess.ParentID, Model: sess.Model},
		agent.Turn{Text: agent.SummaryRequest, Internal: true}, agent.Sink{})
	if err != nil {
		s.log.Debug().Err(err).Msg("Could not summarise the previous conversation")
		return ""
	}
	return strings.TrimSpace(reply.Text)
}

// keepTyping shows the ghost as typing and refreshes it, because the
// indicator expires server-side well before a long answer arrives.
func (s *Service) keepTyping(ctx context.Context, roomID id.RoomID, ghostKey string) func() {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(typingRefresh)
		defer ticker.Stop()
		for {
			if err := s.bridge.Typing(ctx, roomID, ghostKey, true, typingRefresh+10*time.Second); err != nil {
				s.log.Debug().Err(err).Msg("Failed to send typing")
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			stopCtx, stopCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer stopCancel()
			if err := s.bridge.Typing(stopCtx, roomID, ghostKey, false, 0); err != nil {
				s.log.Debug().Err(err).Msg("Failed to clear typing")
			}
		})
	}
}

// reportFailure marks MY message as failed, which is the real error UI in the
// client rather than another chat line nobody can act on. A timeout is
// retriable, everything else is permanent — the difference is whether the
// client offers to send it again.
func (s *Service) reportFailure(ctx context.Context, msg *bridge.Message, cause error) {
	s.log.Error().Err(cause).Str("room", msg.RoomKey).Msg("Turn failed")
	status := event.MessageStatusFail
	if isTimeout(cause) {
		status = event.MessageStatusRetriable
	}
	if err := s.bridge.SendStatus(ctx, msg.RoomID, msg.EventID, status, event.MessageStatusNetworkError,
		shortError(cause), cause.Error()); err != nil {
		s.log.Debug().Err(err).Msg("Failed to report message status")
	}
}

func shortError(err error) string {
	text := err.Error()
	if idx := strings.Index(text, ":"); idx > 0 && idx < 40 {
		text = text[:idx] + ":" + text[idx+1:]
	}
	if len(text) > 200 {
		text = text[:200] + "…"
	}
	return text
}

func isTimeout(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "timeout") || strings.Contains(text, "deadline exceeded") ||
		strings.Contains(text, "connection refused")
}

// onRedaction cancels a running turn when I delete the message that started
// it — the one way to stop a model that is answering the wrong question.
func (s *Service) onRedaction(ctx context.Context, roomKey string, roomID id.RoomID, target id.EventID) {
	s.turnMu.Lock()
	var found *runningTurn
	for key, turn := range s.turns {
		if turn.eventID == target {
			found = turn
			delete(s.turns, key)
			break
		}
	}
	s.turnMu.Unlock()
	if found == nil {
		return
	}
	found.cancel()

	if turn, err := s.store.Turn(ctx, target.String()); err == nil && turn != nil {
		if sess, err := s.store.SessionByID(ctx, turn.SessionID); err == nil && sess != nil {
			if backend, ok := s.agentFor(sess.Agent); ok && backend.Caps().Cancel {
				if err := backend.Cancel(ctx, sess.ConvID); err != nil {
					s.log.Debug().Err(err).Msg("Backend cancel failed")
				}
			}
		}
	}
	s.log.Info().Str("room", roomKey).Msg("Cancelled a turn after its message was deleted")
}
