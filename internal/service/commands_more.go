package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/agent"
	"github.com/SelfRef/beeper-intercom/internal/bridge"
	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/store"
)

// The commands that do something to a conversation rather than to the room's
// settings: ask beside it, take back part of it, summarise it, replace it with
// its own summary, list the ones before it, publish it.

// commandBtw answers a question that is not part of the conversation. Nothing
// is stored on the backend and the session's thread is untouched, so asking
// what a flag means does not become context the next answer has to carry.
func (s *Service) commandBtw(ctx context.Context, msg *bridge.Message, room config.Room, question string) string {
	if strings.TrimSpace(question) == "" {
		return "Usage: `/btw <question>` — answered outside this conversation, and forgotten."
	}
	agentName, agentCfg, ok := s.agentConfigFor(ctx, room, msg.RoomKey)
	if !ok {
		return "This room has no agent."
	}
	backend, built := s.agentFor(agentName)
	if !built {
		return "This room's agent is not available."
	}
	model := agentCfg.Model
	if override, _ := s.store.GetKV(ctx, kvModelOverride+msg.RoomKey); override != "" {
		model = override
	}

	go func() {
		ctx := detach(ctx)
		marker := s.newProgress(msg.RoomID, room.Ghosts[0], msg.EventID)
		marker.set(ctx, "queued")
		defer marker.clear(ctx)
		stopTyping := s.keepTyping(ctx, msg.RoomID, room.Ghosts[0])
		defer stopTyping()

		answer, err := backend.Ask(ctx, model, question)
		if errors.Is(err, agent.ErrUnsupported) {
			s.postNotice(ctx, msg, room, "This backend cannot answer outside a conversation.")
			return
		}
		if err != nil {
			s.postNotice(ctx, msg, room, "Could not answer: "+err.Error())
			return
		}
		stopTyping()
		plain, formatted := bridge.Markdown(answer)
		opts := bridge.SendOptions{ThreadRoot: msg.ThreadRoot, ReplyTo: msg.EventID}
		if _, err := s.bridge.SendText(ctx, msg.RoomID, room.Ghosts[0], plain, formatted, opts); err != nil {
			s.log.Warn().Err(err).Msg("Failed to post a /btw answer")
		}
	}()
	return ""
}

// commandUndo takes back the last exchange: it is removed from the backend's
// conversation, both messages are removed from the room, and the session is
// rewound to the point before it. A model that went the wrong way should leave
// nothing behind to read or to condition on.
func (s *Service) commandUndo(ctx context.Context, msg *bridge.Message, room config.Room) string {
	live, err := s.store.LiveSession(ctx, msg.RoomKey, msg.ThreadRoot.String())
	if err != nil {
		return "Could not read the session: " + err.Error()
	}
	if live == nil {
		return "No conversation to undo."
	}
	last, err := s.store.LastTurn(ctx, msg.RoomKey, msg.ThreadRoot.String())
	if err != nil {
		return "Could not read the last turn: " + err.Error()
	}
	if last == nil {
		return "Nothing to undo yet."
	}

	// Three things have to forget this turn, and they fail independently: the
	// backend's own copy, the bridge's session pointer, and the room.
	var problems []string
	if backend, ok := s.agentFor(live.Agent); ok {
		undoCtx, cancel := context.WithTimeout(ctx, time.Minute)
		err := backend.Undo(undoCtx, live.ConvID, last.UserMsgID)
		cancel()
		switch {
		case errors.Is(err, agent.ErrUnsupported):
			// The parent rewind below still keeps it out of the next turn.
			s.log.Debug().Msg("Backend cannot delete an exchange")
		case err != nil:
			problems = append(problems, "the copy in "+backend.Type())
			s.log.Warn().Err(err).Msg("Could not delete the exchange from the backend")
		}
	}

	// The backend keeps the branch; rewinding the parent is what makes the
	// next turn continue from before the undone one.
	if err := s.store.RewindSession(ctx, live.ID, last.ParentID); err != nil {
		return "Could not rewind the conversation: " + err.Error()
	}
	if err := s.store.DeleteTurn(ctx, last.EventID); err != nil {
		s.log.Warn().Err(err).Msg("Rewound the session but could not forget the turn")
	}

	ghostKey := room.Ghosts[0]
	// The answer is every event it produced: the anchor, each interim edit
	// that grew it, the later parts of a split answer, the attached file. An
	// edit is its own event and outlives a redaction of the anchor — redact
	// one and the client keeps rendering the other.
	answerGone := true
	for _, raw := range last.AnswerEvents {
		if err := s.bridge.Redact(ctx, msg.RoomID, ghostKey, id.EventID(raw)); err != nil {
			answerGone = false
			s.log.Debug().Err(err).Str("event", raw).Msg("Could not redact an answer event")
		}
	}
	if !answerGone {
		problems = append(problems, "the answer")
	}
	// My own messages are mine, not the ghost's: removing one needs whatever
	// the room's power levels allow, so redactMine tries the ghost and falls
	// back to the bot.
	s.redactMine(ctx, msg.RoomID, room, id.EventID(last.EventID), "the question")
	// The /undo itself is noise once it has been carried out.
	s.redactMine(ctx, msg.RoomID, room, msg.EventID, "the /undo command")

	if len(problems) > 0 {
		return fmt.Sprintf("Conversation rewound, but I could not remove %s.",
			strings.Join(problems, " or "))
	}
	// Nothing left to reply to: the exchange and the command are both gone.
	return ""
}

// commandSummary asks the conversation to describe itself, as a message in the
// room rather than as context for the next turn.
func (s *Service) commandSummary(ctx context.Context, msg *bridge.Message, room config.Room) string {
	live, backend, err := s.liveBackend(ctx, msg)
	if err != "" {
		return err
	}
	go func() {
		ctx := detach(ctx)
		marker := s.newProgress(msg.RoomID, room.Ghosts[0], msg.EventID)
		marker.set(ctx, "thinking")
		defer marker.clear(ctx)
		reply, sendErr := backend.Send(ctx,
			agent.Conversation{ID: live.ConvID, Parent: live.ParentID, Model: live.Model, Turns: live.Turns},
			agent.Turn{Text: agent.SummaryRequest, Internal: true}, agent.Sink{})
		if sendErr != nil {
			s.postNotice(ctx, msg, room, "Could not summarise: "+sendErr.Error())
			return
		}
		s.postNotice(ctx, msg, room, reply.Text)
	}()
	return ""
}

// commandCompact shrinks the conversation without ending it, when the backend
// can do that itself — Open WebUI summarises the branch in place and keeps the
// chat, its link and its recent turns. Only when it cannot does the bridge do
// the blunt version: close the conversation and seed the next one with a
// summary.
func (s *Service) commandCompact(ctx context.Context, msg *bridge.Message, room config.Room) string {
	live, backend, err := s.liveBackend(ctx, msg)
	if err != "" {
		return err
	}
	// Compaction is a model call on the whole conversation; it can take a
	// while, so it says so on the command it is running for.
	marker := s.newProgress(msg.RoomID, room.Ghosts[0], msg.EventID)
	marker.set(ctx, "compacting")
	defer marker.clear(ctx)

	compactCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	switch summary, compactErr := backend.Compact(compactCtx, live.ConvID); {
	case compactErr == nil:
		return summary
	case !errors.Is(compactErr, agent.ErrUnsupported):
		return "Could not compact: " + compactErr.Error()
	}
	// No native compaction: fall back to rotating the conversation.
	if err := s.store.CloseSession(ctx, live.ID); err != nil {
		return "Could not close the conversation: " + err.Error()
	}
	// The seed is built from the closed session when the next message opens a
	// new one, exactly as an idle rotation does — see seedFor.
	if err := s.store.SetKV(ctx, kvCompactRequest+msg.RoomKey+"\x00"+msg.ThreadRoot.String(), "1"); err != nil {
		s.log.Warn().Err(err).Msg("Could not mark the conversation for compaction")
	}
	return "Compacting. The next message continues in a fresh conversation, seeded with a summary of this one."
}

// commandHistory lists the recent conversations of this room.
func (s *Service) commandHistory(ctx context.Context, msg *bridge.Message, room config.Room) string {
	sessions, err := s.store.RoomSessions(ctx, msg.RoomKey, msg.ThreadRoot.String(), 10)
	if err != nil {
		return "Could not read the history: " + err.Error()
	}
	if len(sessions) == 0 {
		return "No conversations in this room yet."
	}
	rows := make([][]string, 0, len(sessions))
	for _, sess := range sessions {
		when := time.UnixMilli(sess.LastActive).Local().Format("Mon 15:04")
		turns := fmt.Sprintf("%d turns", sess.Turns)
		if sess.Live {
			when, turns = yes(when), yes(turns)
		}
		link := ""
		if backend, ok := s.agentFor(sess.Agent); ok {
			if url := backend.Link(sess.ConvID); url != "" {
				link = "[open](" + url + ")"
			}
		}
		rows = append(rows, []string{when, turns, code(sess.Model), link})
	}
	return table([]string{"Recent conversations", "", "Model", ""}, rows) +
		"\nThe current one is marked."
}

// commandShare publishes the conversation and returns a link that does not ask
// the reader to log in.
func (s *Service) commandShare(ctx context.Context, msg *bridge.Message) string {
	live, backend, err := s.liveBackend(ctx, msg)
	if err != "" {
		return err
	}
	link, shareErr := backend.Share(ctx, live.ConvID)
	switch {
	case errors.Is(shareErr, agent.ErrUnsupported):
		return "This backend cannot publish a conversation."
	case shareErr != nil:
		return "Could not share: " + shareErr.Error()
	}
	return "Anyone with this link can read the conversation as it is now: " + link +
		"\nRe-run `/share` after more messages to update the snapshot."
}

// liveBackend is the pair every conversation command needs, with the excuses
// already turned into something worth posting.
func (s *Service) liveBackend(ctx context.Context, msg *bridge.Message) (*store.Session, agent.Agent, string) {
	live, err := s.store.LiveSession(ctx, msg.RoomKey, msg.ThreadRoot.String())
	if err != nil {
		return nil, nil, "Could not read the session: " + err.Error()
	}
	if live == nil {
		return nil, nil, "No conversation yet — the next message starts one."
	}
	backend, ok := s.agentFor(live.Agent)
	if !ok {
		return nil, nil, "This conversation's agent is no longer configured."
	}
	return live, backend, ""
}

// usageLine renders what the backend reported about the last turn.
func usageLine(raw string) string {
	if raw == "" {
		return ""
	}
	var u agent.Usage
	if json.Unmarshal([]byte(raw), &u) != nil {
		return ""
	}
	var parts []string
	if u.PromptTokens > 0 {
		in := fmt.Sprintf("%d in", u.PromptTokens)
		if u.CachedTokens > 0 {
			in += fmt.Sprintf(" (%d cached)", u.CachedTokens)
		}
		parts = append(parts, in)
	}
	if u.CompletionTokens > 0 {
		parts = append(parts, fmt.Sprintf("%d out", u.CompletionTokens))
	}
	if u.PromptPerSecond > 0 {
		parts = append(parts, fmt.Sprintf("%.0f tok/s prefill", u.PromptPerSecond))
	}
	if u.TokensPerSecond > 0 {
		parts = append(parts, fmt.Sprintf("%.1f tok/s decode", u.TokensPerSecond))
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ", ")
}
