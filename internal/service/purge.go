package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/SelfRef/beeper-intercom/internal/bridge"
	"github.com/SelfRef/beeper-intercom/internal/config"
)

// Emptying the room and starting again.
//
// /clear tidies up after the bridge — its own messages, in this conversation,
// and it is safe to fire blind. /purge is the other thing: the room itself,
// every message in it whoever sent it, and the conversations behind them. It
// is how a room that has drifted is put back to the state it was created in,
// which is why it also starts a new conversation — an empty room still
// answering from a history nobody can see would be worse than either.
//
// Hence two steps. `/purge` counts what it would remove and says so; `/purge
// yes` carries it out. There is no undo, and the count is the thing worth
// seeing before committing to that.
//
// Scope, deliberately: the room and the bridge's memory of it. State events
// stay, so the room keeps its name, avatar, members and power levels — it ends
// up empty, not broken. The backend's own copy of the conversations stays too:
// the chats are still in the web UI, and /purge is about the room.
//
// `/purge new-room` is the third step, for what redaction cannot reach at all:
// tombstones from before the bridge started hiding them, and polls a client has
// cached. It leaves the room instead of emptying it, and builds another.

func (s *Service) commandPurge(ctx context.Context, msg *bridge.Message, room config.Room, args string) string {
	switch strings.ToLower(strings.TrimSpace(args)) {
	case "":
		return s.purgePreview(ctx, msg, room)
	case "yes":
		return s.purgeRoom(ctx, msg, room)
	case "new-room", "newroom":
		return s.purgeNewRoom(ctx, msg, room)
	default:
		return fmt.Sprintf("`/purge` counts what it would remove, %s carries it out, and %s starts a new room instead.",
			code("/purge yes"), code("/purge new-room"))
	}
}

// purgeRefusal is why this room cannot be emptied right now, or "".
func (s *Service) purgeRefusal(room config.Room, roomKey string) string {
	// An announcements room is a record somebody else acts on, not a
	// conversation to start again — and it has no conversation to start.
	if room.Kind == config.KindBroadcast {
		return "This room is announcements only: what is in it is a record, not a conversation to start again."
	}
	if s.roomBusy(roomKey) {
		return fmt.Sprintf("Something is still being answered here — %s it first.", code("/stop"))
	}
	return ""
}

// roomBusy reports whether a turn is in flight in any of the room's slots, the
// main timeline or a thread. Emptying the room under a running answer would
// redact the events it is still growing.
func (s *Service) roomBusy(roomKey string) bool {
	prefix := roomKey + "\x00"
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	for key := range s.turns {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// purgePreview is `/purge` on its own: what it would do, and how to say yes.
func (s *Service) purgePreview(ctx context.Context, msg *bridge.Message, room config.Room) string {
	if why := s.purgeRefusal(room, msg.RoomKey); why != "" {
		return why
	}
	events, err := s.bridge.RoomEvents(ctx, msg.RoomID)
	if err != nil {
		return "Could not read the room's history: " + err.Error()
	}
	if len(events) == 0 {
		return "Nothing here to purge."
	}
	// Walked newest first, so the last one is the oldest.
	oldest := events[len(events)-1].Time
	warning := ""
	if room.DeletePlaceholder {
		warning = " This room keeps its delete markers, so it would be left with one per message."
	}
	return fmt.Sprintf("%d messages here, back to %s. %s removes every one of them and starts a new conversation.%s It cannot be undone.",
		len(events), oldest.Format("2 Jan 2006"), code("/purge yes"), warning)
}

// purgeRoom is `/purge yes`: the room, and then the bridge's memory of it.
func (s *Service) purgeRoom(ctx context.Context, msg *bridge.Message, room config.Room) string {
	if why := s.purgeRefusal(room, msg.RoomKey); why != "" {
		return why
	}
	events, err := s.bridge.RoomEvents(ctx, msg.RoomID)
	if err != nil && len(events) == 0 {
		return "Could not read the room's history: " + err.Error()
	}
	if err != nil {
		s.log.Warn().Err(err).Int("found", len(events)).Msg("Read only part of the room's history")
	}
	if len(events) == 0 {
		return "Nothing here to purge."
	}

	// Whoever sent it, the bot can take it: it created the room and outranks
	// everything in it. The ghost is the fallback for a room whose power
	// levels have been changed from under the bridge.
	senders := []string{bridge.BotKey}
	if len(room.Ghosts) > 0 {
		senders = append(senders, room.Ghosts[0])
	}
	gone := 0
	for _, evt := range events {
		var err error
		for _, sender := range senders {
			if err = s.bridge.Redact(ctx, msg.RoomID, sender, evt.ID); err == nil {
				gone++
				break
			}
		}
		if err != nil {
			s.log.Debug().Err(err).Str("event", evt.ID.String()).Msg("Could not redact an event")
		}
	}

	if err := s.forgetRoom(ctx, msg.RoomKey); err != nil {
		s.log.Warn().Err(err).Msg("Emptied the room but could not forget its conversations")
	}

	// Posted rather than returned, for two reasons. A returned reply would be
	// a reply to the /purge that has just been redacted, and it would land in
	// the thread the command was typed in — which went with everything else.
	ghost := ""
	if len(room.Ghosts) > 0 {
		ghost = room.Ghosts[0]
	}
	text := fmt.Sprintf("%s — %d messages gone, new conversation.", yes("Purged"), gone)
	if gone < len(events) {
		text = fmt.Sprintf("%s — %d of %d gone, %d would not go; new conversation.",
			colour(colourWarn, "Purged"), gone, len(events), len(events)-gone)
	}
	s.notice(ctx, msg.RoomID, ghost, "", text)
	return ""
}

// purgeNewRoom is `/purge new-room`: leave this room behind and build another.
//
// It is the escape hatch from what redaction cannot reach. A message deleted
// before the bridge started stamping the hide key keeps its tombstone for ever
// — re-redacting it has the content stripped, measured — and a client that has
// cached a poll goes on drawing the card after the poll event is gone. Neither
// survives a room that never had them.
//
// The old room is left whole rather than emptied first: it is about to stop
// being anything the bridge writes to, and a room full of tombstones is no
// better to be left with than a room full of messages. Deleting the chat is one
// action in the client, and it is the owner's to take.
func (s *Service) purgeNewRoom(ctx context.Context, msg *bridge.Message, room config.Room) string {
	if why := s.purgeRefusal(room, msg.RoomKey); why != "" {
		return why
	}
	ghost := ""
	if len(room.Ghosts) > 0 {
		ghost = room.Ghosts[0]
	}
	// Said while this is still the portal, so it is the last thing in it.
	s.notice(ctx, msg.RoomID, ghost, "",
		"Replaced by a new room. Nothing more will be sent here — delete the chat when you have finished with it.")

	old, created, err := s.bridge.RecreateRoom(ctx, msg.RoomKey)
	if err != nil {
		return "Could not create the new room: " + err.Error()
	}
	// The conversations belonged to the room that has just been left behind.
	if err := s.forgetRoom(ctx, msg.RoomKey); err != nil {
		s.log.Warn().Err(err).Msg("Created the new room but could not forget the old one's conversations")
	}
	s.log.Info().Str("room", msg.RoomKey).Str("old", old.String()).Str("new", created.String()).
		Msg("Replaced the room")

	s.notice(ctx, created, ghost, "", yes("New room")+" — nothing carried over, new conversation.")
	return ""
}

// forgetRoom drops everything the store keeps for one room's conversations.
func (s *Service) forgetRoom(ctx context.Context, roomKey string) error {
	if err := s.store.PurgeRoom(ctx, roomKey); err != nil {
		return err
	}
	// The KV state that belongs to a conversation rather than to the room.
	// Toolset rows are the one that would be noticed: they name a session,
	// which is enough for the next turn to ignore them, but not enough to stop
	// an idle timer announcing a toolset switching off in a room that no
	// longer has the conversation it was on in.
	for _, prefix := range []string{kvToolsetPrefix, kvCompactRequest, kvThinkPending} {
		if err := s.store.DeleteKVPrefix(ctx, prefix+roomKey+"\x00"); err != nil {
			return err
		}
	}
	s.stopToolsetTimers(roomKey)
	// Kept: the agent and model overrides. Those say what this room talks to,
	// which is a setting somebody chose, not something that was said here.
	return nil
}

// stopToolsetTimers cancels the idle countdowns of every conversation in a
// room, for the announcement they would otherwise post into it.
func (s *Service) stopToolsetTimers(roomKey string) {
	prefix := roomKey + "\x00"
	s.toolsetMu.Lock()
	defer s.toolsetMu.Unlock()
	for key, timer := range s.toolsetTimers {
		if strings.HasPrefix(key, prefix) {
			timer.Stop()
			delete(s.toolsetTimers, key)
		}
	}
}
