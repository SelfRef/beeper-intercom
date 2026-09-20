package service

import (
	"context"
	"encoding/json"
	"errors"
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
	// kvCompactRequest marks a conversation that was closed by /compact, so
	// the next one is seeded with its summary even when the agent does not
	// carry summaries on an ordinary rotation.
	kvCompactRequest = "compact:"
	kvModelOverride  = "model_override:"
	typingRefresh    = 20 * time.Second
)

// progressMarker is the reaction on the question, tracking which phase the
// turn is in. Matrix has no editable reaction, so a phase change is a new
// reaction and a redaction of the old one; phases that map to the same emoji
// (or to none) cost nothing, and a backend that reports nothing leaves the
// first one in place for the whole turn.
type progressMarker struct {
	s        *Service
	roomID   id.RoomID
	ghostKey string
	target   id.EventID

	mu      sync.Mutex
	phase   string
	emoji   string
	eventID id.EventID
}

func (s *Service) newProgress(roomID id.RoomID, ghostKey string, target id.EventID) *progressMarker {
	return &progressMarker{s: s, roomID: roomID, ghostKey: ghostKey, target: target}
}

// set moves the marker to a phase. Unknown phases and phases the operator left
// empty are ignored, so the previous one stays rather than the marker
// flickering off.
func (m *progressMarker) set(ctx context.Context, phase string) {
	if m == nil {
		return
	}
	emoji := m.s.conf().Progress.Reactions.Emoji(phase)
	if emoji == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if phase == m.phase || emoji == m.emoji {
		m.phase = phase
		return
	}
	old := m.eventID
	// New first, old second: a moment with two reactions reads better than a
	// moment with none.
	added, err := m.s.bridge.React(ctx, m.roomID, m.ghostKey, m.target, emoji)
	if err != nil {
		m.s.log.Debug().Err(err).Str("phase", phase).Msg("Could not set the progress marker")
		return
	}
	m.phase, m.emoji, m.eventID = phase, emoji, added
	if old != "" {
		if err := m.s.bridge.Redact(ctx, m.roomID, m.ghostKey, old); err != nil {
			m.s.log.Debug().Err(err).Msg("Could not clear the previous progress marker")
		}
	}
}

// clear removes the marker: the answer is there, the phase no longer matters.
func (m *progressMarker) clear(ctx context.Context) {
	if m == nil {
		return
	}
	m.mu.Lock()
	old := m.eventID
	m.eventID, m.emoji, m.phase = "", "", ""
	m.mu.Unlock()
	if old == "" {
		return
	}
	if err := m.s.bridge.Redact(ctx, m.roomID, m.ghostKey, old); err != nil {
		m.s.log.Debug().Err(err).Msg("Could not clear the progress marker")
	}
}

// onMessage is the entry point for everything the account owner types.
func (s *Service) onMessage(ctx context.Context, msg *bridge.Message) {
	room, ok := s.conf().Rooms[msg.RoomKey]
	if !ok {
		return
	}
	body := strings.TrimSpace(msg.Body)
	switch {
	case strings.HasPrefix(body, "//"):
		// The escape hatch: everything else starting with a slash is a
		// command, so a message that really begins with one needs a way in.
		msg.Body = strings.TrimPrefix(body, "/")
	case strings.HasPrefix(body, "/"):
		// Commands are answered even in a room with no agent, because /status
		// is how you find out why a room has no agent.
		go s.handleCommand(detach(ctx), msg, room)
		return
	}
	if room.Agent == "" {
		return
	}
	// A question of the model's may be open, and a poll cannot take a typed
	// answer. One that accepts free text takes this message as the answer; one
	// that does not is dropped, and the message carries on as a new turn.
	if s.offerMessage(ctx, sessionKey(msg.RoomKey, msg.ThreadRoot), strings.TrimSpace(msg.Body)) == messageIsTheAnswer {
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

	// A reaction on the question is cheaper than a "working on it" bubble and
	// disappears when the answer lands. It also says which phase the turn is
	// in, so a long wait is legible: queued, thinking, searching, writing.
	marker := s.newProgress(roomID, ghostKey, msg.EventID)
	marker.set(ctx, "queued")
	defer marker.clear(ctx)

	// Progress: with progress.mode off nothing is shown until the answer is
	// finished — the ghost simply types. Otherwise the first chunk opens the
	// answer bubble and every later one grows it, either as edits of that
	// bubble or as com.beeper.stream deltas (see internal/bridge/stream.go for
	// why edits are the default of the two). If the bubble cannot be opened,
	// the chunks are dropped and the answer arrives the old way — progress is
	// comfort, not correctness.
	// The answer is sent as a reply to the question, which is how the web UI
	// pairs them: in a room where several questions can be in flight at once,
	// the quote is what says which answer belongs to which. Inside a thread
	// the same field doubles as the thread's reply fallback.
	answerOpts := bridge.SendOptions{ThreadRoot: msg.ThreadRoot, ReplyTo: msg.EventID}
	progress := s.conf().Progress
	streamOpts := bridge.StreamOptions{
		Edits:    progress.Mode == config.ProgressEdits,
		Deltas:   progress.Mode == config.ProgressStream,
		Interval: progress.Interval.Or(1500 * time.Millisecond),
		Suffix:   progress.Suffix,
	}
	var stream *bridge.Stream
	var streamMu sync.Mutex
	streamFailed := false
	sink := agent.Sink{Status: func(state string) { marker.set(ctx, state) }}
	if backend.Caps().Streaming && progress.Mode != config.ProgressOff {
		sink.Delta = func(text string) {
			streamMu.Lock()
			defer streamMu.Unlock()
			if stream == nil {
				if streamFailed {
					return
				}
				// The first chunk becomes the anchor's body, so the bubble
				// starts with words instead of a placeholder.
				opened, err := s.bridge.StartStream(ctx, roomID, ghostKey, answerOpts, text, streamOpts)
				if err != nil {
					streamFailed = true
					s.log.Warn().Err(err).Msg("Could not open a stream; falling back to one message")
					return
				}
				stream = opened
				stopTyping()
				return
			}
			stream.Push(text)
		}
	}

	turn, err := s.buildTurn(turnCtx, msg, room, backend, ghostKey)
	if err != nil {
		s.reportFailure(ctx, msg, err)
		return
	}

	// Toolsets: whatever this conversation has switched on, on top of the
	// agent's fixed tools. A toolset that timed out while nobody was looking
	// is reported here rather than silently missing from the answer.
	if agentCfg, ok := s.conf().Agents[sess.Agent]; ok {
		sessionID := fmt.Sprint(sess.ID)
		s.adoptToolsets(ctx, agentCfg, msg.RoomKey, msg.ThreadRoot, sessionID)
		tools, expired := s.activeTools(ctx, agentCfg, msg.RoomKey, msg.ThreadRoot, sessionID)
		turn.Tools = tools
		for _, name := range expired {
			s.notice(ctx, roomID, ghostKey, msg.ThreadRoot,
				fmt.Sprintf("`%s` had switched off on idle; this answer runs without it.", name))
		}
		s.touchToolsets(ctx, agentCfg, room, msg.RoomKey, roomID, msg.ThreadRoot, sessionID)
	}

	conv := agent.Conversation{ID: sess.ConvID, Parent: parent, Model: sess.Model, Turns: sess.Turns}
	reply, err := backend.Send(turnCtx, conv, turn, sink)
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
	// A turn can stop to ask ME something instead of answering. The questions
	// go out as polls and the answers resume the same turn, so what lands
	// below is the answer to the question I actually meant.
	if reply.Ask != nil {
		// Not typing: the model is waiting for me, not the other way round.
		stopTyping()
		if opened != nil {
			// Whatever was streamed so far is posted by resolveAsks as its own
			// message; the anchor would otherwise sit half-written forever.
			opened.Abort(ctx)
			streamMu.Lock()
			stream, opened = nil, nil
			streamMu.Unlock()
		}
		reply = s.resolveAsks(turnCtx, msg, room, ghostKey, backend, conv, reply, marker, sess.ID)
		if reply == nil || (reply.Ask != nil && strings.TrimSpace(reply.Text) == "") {
			return
		}
		stopTyping = s.keepTyping(ctx, roomID, ghostKey)
	}

	stopTyping()

	replyEvent, answerEvents, err := s.postAnswer(ctx, msg, ghostKey, reply.Text, opened, answerOpts)
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
	usage := ""
	if reply.Usage != nil {
		if raw, err := json.Marshal(reply.Usage); err == nil {
			usage = string(raw)
		}
	}
	if err := s.store.PutTurn(ctx, &store.Turn{
		EventID:      msg.EventID.String(),
		SessionID:    sess.ID,
		UserMsgID:    reply.UserMessage,
		ParentID:     conv.Parent,
		ReplyEvent:   replyEvent.String(),
		AnswerEvents: eventStrings(answerEvents),
		Question:     msg.Body,
		Usage:        usage,
	}); err != nil {
		s.log.Error().Err(err).Msg("Failed to record turn")
	}
}

// buildTurn turns the message into what the backend gets: the text, plus any
// attachment — an image for a vision model, a document for the file path, or
// a voice message transcribed first.
func (s *Service) buildTurn(ctx context.Context, msg *bridge.Message, room config.Room, backend agent.Agent, ghostKey string) (agent.Turn, error) {
	turn := agent.Turn{Text: msg.Body}
	att := msg.Attachment
	if att == nil {
		return turn, nil
	}

	data, err := s.bridge.DownloadMedia(ctx, att.URL, s.conf().Limits.MaxMediaBytes)
	if err != nil {
		return turn, fmt.Errorf("download attachment: %w", err)
	}
	file := agent.Attachment{Name: att.Name, Mime: att.Mime, Data: data}
	if file.Mime == "" {
		file.Mime = "application/octet-stream"
	}
	if file.Name == "" {
		file.Name = "attachment"
	}

	if att.Voice || (att.Kind == event.MsgAudio && msg.Body == "") {
		transcript, err := backend.Transcribe(ctx, file)
		if err != nil {
			if errors.Is(err, agent.ErrUnsupported) {
				return turn, fmt.Errorf("this agent cannot transcribe voice messages")
			}
			return turn, fmt.Errorf("transcribe: %w", err)
		}
		if transcript == "" {
			return turn, fmt.Errorf("the voice message came back empty from transcription")
		}
		// The transcript is shown so a misheard word explains a strange answer.
		s.postNotice(ctx, msg, room, "🎙️ "+transcript)
		if turn.Text != "" {
			turn.Text += "\n\n"
		}
		turn.Text += transcript
		return turn, nil
	}

	turn.Attachments = []agent.Attachment{file}
	if turn.Text == "" {
		// No caption: give the model something to do with what it was sent.
		if strings.HasPrefix(file.Mime, "image/") {
			turn.Text = "What is in this image?"
		} else {
			turn.Text = fmt.Sprintf("I sent you the file %q. Summarise what it contains.", file.Name)
		}
	}
	return turn, nil
}

// postAnswer renders and sends the answer, splitting it at paragraph
// boundaries and attaching anything that is too long to read in a bubble.
// With a live stream, the first part becomes the stream's final edit — the
// durable copy of what was streamed — and only the overflow is new messages.
func (s *Service) postAnswer(ctx context.Context, msg *bridge.Message, ghostKey, text string, stream *bridge.Stream, opts bridge.SendOptions) (id.EventID, []id.EventID, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		text = "_(the agent returned an empty answer)_"
	}

	limit := s.conf().Limits.MessageSplitBytes
	parts := bridge.SplitMessage(text, limit)

	// Every event the answer occupies, so /undo can take all of it back — an
	// answer is not always one event: it can be an anchor plus the edits that
	// grew it, several parts, and a file.
	var first id.EventID
	var events []id.EventID
	for i, part := range parts {
		plain, formatted := bridge.Markdown(part)
		if i == 0 && stream != nil {
			if err := stream.Finish(ctx, plain, formatted); err != nil {
				return "", stream.Events(), err
			}
			first = stream.EventID
			events = append(events, stream.Events()...)
			continue
		}
		sent, err := s.bridge.SendText(ctx, msg.RoomID, ghostKey, plain, formatted, opts)
		if err != nil {
			return first, events, err
		}
		events = append(events, sent)
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
		} else if sent, err := s.bridge.SendFile(ctx, msg.RoomID, ghostKey, "answer.md", "text/markdown", uri, len(text), opts); err != nil {
			s.log.Warn().Err(err).Msg("Failed to attach the long answer")
		} else {
			events = append(events, sent)
		}
	}
	return first, events, nil
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

	model := s.currentModel(ctx, msg, agentCfg)

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
		// /think before the conversation existed was meant for this one.
		Model: s.applyPendingReasoning(ctx, msg.RoomKey, threadRoot, model),
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
	// /compact closed the conversation on purpose and wants its summary
	// carried over, whether or not this agent does that on an ordinary
	// rotation. The marker is consumed here: one compaction, one seed.
	compactKey := kvCompactRequest + msg.RoomKey + "\x00" + msg.ThreadRoot.String()
	compact := false
	if marked, err := s.store.GetKV(ctx, compactKey); err == nil && marked != "" {
		compact = true
		_ = s.store.SetKV(ctx, compactKey, "")
		if previous == nil {
			// The session was closed by the command, so it is no longer the
			// live one — take the newest closed conversation of this slot.
			if recent, err := s.store.RoomSessions(ctx, msg.RoomKey, msg.ThreadRoot.String(), 1); err == nil && len(recent) > 0 {
				previous = recent[0]
			}
		}
	}
	if previous != nil && (compact || agentCfg.Session.CarrySummary) {
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

// onRedaction is me deleting one of my own messages, which the web UI treats
// as deleting the exchange: the question and what it produced go together.
// Three kinds of message can be deleted, and each takes its answer with it —
// a command takes the bridge's reply, a question in flight is cancelled, and a
// question that has been answered takes the answer out of the room and out of
// the backend's conversation.
func (s *Service) onRedaction(ctx context.Context, roomKey string, roomID id.RoomID, target id.EventID) {
	s.clearDeletedCommand(ctx, roomKey, roomID, target)
	if s.cancelDeletedTurn(ctx, roomKey, target) {
		return
	}
	s.undoDeletedTurn(ctx, roomKey, roomID, target)
}

// cancelDeletedTurn stops a model that is answering a question I have taken
// back — the one way to stop one at all. Reports whether there was one: a turn
// that is still running has no answer in the room yet, so there is nothing to
// undo after it.
func (s *Service) cancelDeletedTurn(ctx context.Context, roomKey string, target id.EventID) bool {
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
		return false
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
	return true
}

// undoDeletedTurn is /undo without the command: deleting a question deletes
// what it was answered with, in the room and in the backend, exactly as
// deleting a message in the web UI does. Anything that fails here is logged
// rather than reported — there is no message left to report it under, and the
// room has already lost the question.
func (s *Service) undoDeletedTurn(ctx context.Context, roomKey string, roomID id.RoomID, target id.EventID) {
	turn, err := s.store.Turn(ctx, target.String())
	if err != nil || turn == nil {
		return
	}
	room := s.conf().Rooms[roomKey]
	ghostKey := ""
	if len(room.Ghosts) > 0 {
		ghostKey = room.Ghosts[0]
	}
	// Every event the answer occupies: the anchor, the interim edits that grew
	// it, the later parts of a split answer, the attached file. An edit is its
	// own event and outlives a redaction of the anchor.
	for _, raw := range turn.AnswerEvents {
		if err := s.bridge.Redact(ctx, roomID, ghostKey, id.EventID(raw)); err != nil {
			s.log.Debug().Err(err).Str("event", raw).Msg("Could not redact an answer event")
		}
	}

	sess, err := s.store.SessionByID(ctx, turn.SessionID)
	if err != nil || sess == nil {
		return
	}
	if backend, ok := s.agentFor(sess.Agent); ok {
		undoCtx, cancel := context.WithTimeout(ctx, time.Minute)
		err := backend.Undo(undoCtx, sess.ConvID, turn.UserMsgID)
		cancel()
		switch {
		case errors.Is(err, agent.ErrUnsupported):
			s.log.Debug().Msg("Backend cannot delete an exchange")
		case err != nil:
			s.log.Warn().Err(err).Msg("Could not delete the exchange from the backend")
		}
	}
	// Only the newest turn moves the conversation's parent. Deleting an older
	// one takes it out of the record, but what the next turn continues from is
	// still the tip — and LastTurn only looks at live conversations, so a turn
	// from one that has already been closed never rewinds anything.
	if last, err := s.store.LastTurn(ctx, sess.RoomKey, sess.ThreadRoot); err == nil &&
		last != nil && last.EventID == turn.EventID {
		if err := s.store.RewindSession(ctx, sess.ID, turn.ParentID); err != nil {
			s.log.Warn().Err(err).Msg("Could not rewind the conversation after a deleted question")
		}
	}
	if err := s.store.DeleteTurn(ctx, turn.EventID); err != nil {
		s.log.Warn().Err(err).Msg("Could not forget a deleted turn")
	}
}

// eventStrings is the storable form of an event list.
func eventStrings(events []id.EventID) []string {
	out := make([]string, 0, len(events))
	seen := make(map[id.EventID]bool, len(events))
	for _, e := range events {
		if e == "" || seen[e] {
			continue
		}
		seen[e] = true
		out = append(out, e.String())
	}
	return out
}
