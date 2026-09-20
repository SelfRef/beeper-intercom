package service

import (
	"context"
	"fmt"
	"html"
	"strings"

	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/bridge"
	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/store"
)

// Taking the bridge's own messages back out of the room.
//
// A command reply is useful for about as long as it takes to read it, and then
// it is clutter in a conversation that is supposed to be a conversation. Two
// ways to remove it, because a bridge message cannot be reacted to — only my
// own messages can:
//
//   - /clear (or its older name /clean), which takes the last one — or all of
//     them, in this conversation;
//   - the delete button: a reaction the bot puts on the command I typed.
//     Tapping it sends the same reaction from me, and that is the press;
//   - deleting the command message myself, which takes its answer with it.
//
// All three rely on the redaction path hiding the tombstone (bridge/send.go),
// so a cleared message leaves nothing at all behind.

func (s *Service) commandClear(ctx context.Context, msg *bridge.Message, room config.Room, args string) string {
	all := strings.EqualFold(strings.TrimSpace(args), "all")
	thread := msg.ThreadRoot.String()

	// "All" means this conversation: a /new is a line under everything before
	// it, and sweeping across it would remove the record of a conversation I
	// may still be reading.
	since := int64(0)
	if all {
		if live, err := s.store.LiveSession(ctx, msg.RoomKey, thread); err == nil && live != nil {
			since = live.CreatedAt
		}
	}

	var notices []*store.Notice
	var err error
	if all {
		notices, err = s.store.NoticesSince(ctx, msg.RoomKey, thread, since)
	} else {
		notices, err = s.store.Notices(ctx, msg.RoomKey, thread, 1)
	}
	if err != nil {
		return "Could not read what I have said here: " + err.Error()
	}

	// A command and its answer are one exchange: clearing the answer and
	// leaving "/help" behind would tidy up half of it.
	commands, err := s.commandsToClear(ctx, msg, notices, all, since)
	if err != nil {
		return "Could not read the commands here: " + err.Error()
	}
	if len(notices) == 0 && len(commands) == 0 {
		return "Nothing of mine left to clear."
	}

	removed := s.removeNotices(ctx, msg.RoomID, notices)
	gone := make([]string, 0, len(commands))
	for _, command := range commands {
		if command == msg.EventID.String() {
			continue // this /clear is redacted below, whatever else happens
		}
		s.dropButton(ctx, msg.RoomID, room, command)
		s.redactMine(ctx, msg.RoomID, room, id.EventID(command), "a command")
		gone = append(gone, command)
	}
	if err := s.store.DeleteCommands(ctx, append(gone, msg.EventID.String())); err != nil {
		s.log.Debug().Err(err).Msg("Removed commands but could not forget them")
	}

	// The /clear command itself is the last piece of clutter.
	s.redactMine(ctx, msg.RoomID, room, msg.EventID, "the cleanup command")
	if removed < len(notices) {
		return fmt.Sprintf("Removed %d of %d — the rest would not go.", removed, len(notices))
	}
	// Everything asked for is gone, including this command: saying so would
	// put back what was just taken away.
	return ""
}

// commandsToClear is the set of my own command messages that go with the
// bridge messages being removed: the ones that caused them, plus — when
// clearing the whole conversation — every command in it, including the ones
// whose answer left no trace.
func (s *Service) commandsToClear(ctx context.Context, msg *bridge.Message, notices []*store.Notice, all bool, since int64) ([]string, error) {
	seen := make(map[string]bool, len(notices))
	var out []string
	add := func(eventID string) {
		if eventID == "" || seen[eventID] {
			return
		}
		seen[eventID] = true
		out = append(out, eventID)
	}
	for _, notice := range notices {
		add(notice.CommandEvent)
	}
	if !all {
		return out, nil
	}
	commands, err := s.store.Commands(ctx, msg.RoomKey, msg.ThreadRoot.String(), since)
	if err != nil {
		return out, err
	}
	for _, command := range commands {
		add(command.EventID)
	}
	return out, nil
}

// addClearButton puts the delete button on a command I have just typed.
//
// A bridge message cannot be reacted to at all, so there is no way to offer an
// action ON the answer. A reaction the bot has already placed on MY message
// can be tapped though, and tapping it sends the same reaction from me — which
// is exactly the signal clearForCommand is waiting for. The bot places the
// button; my tap is the press.
func (s *Service) addClearButton(ctx context.Context, msg *bridge.Message, room config.Room) {
	notices := s.conf().Notices
	if !notices.HasClearButton() {
		return
	}
	// A command that took itself away (/clear) has nothing left to hang a
	// button on: its row went with it.
	if command, err := s.store.Command(ctx, msg.EventID.String()); err != nil || command == nil {
		return
	}
	sender := bridge.BotKey
	if notices.Sender != config.NoticeSenderBot && len(room.Ghosts) > 0 {
		sender = room.Ghosts[0]
	}
	button, err := s.bridge.React(ctx, msg.RoomID, sender, msg.EventID, notices.ClearEmojiOr())
	if err != nil {
		s.log.Debug().Err(err).Msg("Could not put the delete button on a command")
		return
	}
	if err := s.store.SetCommandButton(ctx, msg.EventID.String(), button.String()); err != nil {
		s.log.Debug().Err(err).Msg("Could not remember a delete button")
	}
}

// clearForCommand is the button being pressed: sweep away one of my commands
// and everything the bridge said in answer to it. Reports whether it did
// anything, because a reaction that clears nothing may still be an action.
func (s *Service) clearForCommand(ctx context.Context, evt *bridge.Reaction, room config.Room) bool {
	if !s.conf().Notices.ClearTriggeredBy(evt.Key) {
		return false
	}
	notices, err := s.store.NoticesForCommand(ctx, evt.Target.String())
	if err != nil || len(notices) == 0 {
		return false
	}
	roomID := id.RoomID(notices[0].RoomID)
	s.removeNotices(ctx, roomID, notices)
	// The button, the command, and then the reaction that asked for this —
	// otherwise both reactions are left pointing at an event that no longer
	// exists.
	s.dropButton(ctx, roomID, room, evt.Target.String())
	s.redactMine(ctx, roomID, room, evt.Target, "the reacted command")
	if err := s.store.DeleteCommands(ctx, []string{evt.Target.String()}); err != nil {
		s.log.Debug().Err(err).Msg("Could not forget the cleared command")
	}
	s.redactMine(ctx, roomID, room, evt.EventID, "the button press")
	return true
}

// clearDeletedCommand is the other half of that: a command I delete myself
// takes the bridge's answer with it. A command and its answer are one
// exchange, and half of one left in the room is litter — so deleting the
// question is the second way to delete the answer, and needs no button.
func (s *Service) clearDeletedCommand(ctx context.Context, roomKey string, roomID id.RoomID, target id.EventID) {
	command, err := s.store.Command(ctx, target.String())
	if err != nil || command == nil {
		return // not a command of mine; nothing here to answer for
	}
	room := s.conf().Rooms[roomKey]
	s.dropButton(ctx, roomID, room, target.String())
	if notices, err := s.store.NoticesForCommand(ctx, target.String()); err != nil {
		s.log.Debug().Err(err).Msg("Could not read what a deleted command caused")
	} else if len(notices) > 0 {
		s.removeNotices(ctx, roomID, notices)
	}
	if err := s.store.DeleteCommands(ctx, []string{target.String()}); err != nil {
		s.log.Debug().Err(err).Msg("Could not forget a deleted command")
	}
}

// dropButton takes the bot's delete button off a command that is about to
// disappear, so it is not left pointing at an event that no longer exists.
func (s *Service) dropButton(ctx context.Context, roomID id.RoomID, room config.Room, commandEvent string) {
	command, err := s.store.Command(ctx, commandEvent)
	if err != nil || command == nil || command.ButtonEvent == "" {
		return
	}
	s.redactMine(ctx, roomID, room, id.EventID(command.ButtonEvent), "a delete button")
}

// removeNotices redacts bridge messages and forgets them. Each is redacted by
// whoever sent it: the bot cannot redact a ghost's message in a room where it
// has no power to, and vice versa.
func (s *Service) removeNotices(ctx context.Context, roomID id.RoomID, notices []*store.Notice) int {
	gone := make([]string, 0, len(notices))
	for _, notice := range notices {
		target := roomID
		if notice.RoomID != "" {
			target = id.RoomID(notice.RoomID)
		}
		if err := s.bridge.Redact(ctx, target, notice.Ghost, id.EventID(notice.EventID)); err != nil {
			s.log.Debug().Err(err).Str("event", notice.EventID).Msg("Could not remove a bridge message")
			continue
		}
		gone = append(gone, notice.EventID)
	}
	if err := s.store.DeleteNotices(ctx, gone); err != nil {
		s.log.Debug().Err(err).Msg("Removed bridge messages but could not forget them")
	}
	return len(gone)
}

// redactMine removes one of MY events. Which of the bridge's identities may do
// that depends on the room's power levels, so it tries the room's ghost and
// falls back to the bot, which created the room and outranks everything in it.
func (s *Service) redactMine(ctx context.Context, roomID id.RoomID, room config.Room, target id.EventID, what string) {
	senders := make([]string, 0, 2)
	if len(room.Ghosts) > 0 {
		senders = append(senders, room.Ghosts[0])
	}
	senders = append(senders, bridge.BotKey)
	var err error
	for _, sender := range senders {
		if err = s.bridge.Redact(ctx, roomID, sender, target); err == nil {
			return
		}
	}
	s.log.Debug().Err(err).Str("event", target.String()).Msgf("Could not redact %s", what)
}

// Editing a command I already sent.
//
// Matrix has no way to forbid an edit — the client sends it and the server
// takes it — so the bridge decides what an edit MEANS and puts the room back
// the way it says.
//
// Editing the newest command of the running conversation is a correction:
// whatever it said before is swept away and the new text runs in its place, as
// if it had been typed that way. Editing anything older is refused, because
// its answer has already been read: a /model list from four commands ago
// cannot be quietly turned into a /new. The refusal is not a complaint — the
// message is edited back to what it said, so the room matches what happened.
//
// rerunEdited reports whether the command should now run.
func (s *Service) rerunEdited(ctx context.Context, msg *bridge.Message, room config.Room, body string) bool {
	target := msg.Edits.String()
	original, err := s.store.Command(ctx, target)
	if err != nil {
		s.log.Debug().Err(err).Msg("Could not read the edited command")
	}
	last, err := s.store.LastCommand(ctx, msg.RoomKey, msg.ThreadRoot.String())
	if err != nil {
		s.log.Debug().Err(err).Msg("Could not read the last command")
	}

	sessionID := int64(0)
	if live, err := s.store.LiveSession(ctx, msg.RoomKey, msg.ThreadRoot.String()); err == nil && live != nil {
		sessionID = live.ID
	}

	switch {
	case original == nil:
		// Either something that was not a command has been edited into one, or
		// a command from before the bridge started keeping track.
		question := s.originalQuestion(ctx, target)
		why := "I have no record of that message, so I cannot tell what editing it should undo — send the command as a new message."
		if question != "" {
			why = "That was a question, not a command; turning it into one would leave its answer standing. Send the command as a new message."
		}
		s.refuseEdit(ctx, msg, room, target, question, why)
		return false
	case last != nil && last.EventID != original.EventID:
		s.refuseEdit(ctx, msg, room, target, original.Body,
			"That is not the last command — its answer has already been read, so editing it would change history. Send the new one as a message.")
		return false
	case original.SessionID != sessionID:
		s.refuseEdit(ctx, msg, room, target, original.Body,
			"That command belongs to a conversation that has ended. Send the new one as a message.")
		return false
	}

	// A correction to the newest command: take back what it produced, and let
	// the caller run the new text under the same message.
	notices, err := s.store.NoticesForCommand(ctx, target)
	if err != nil {
		s.log.Debug().Err(err).Msg("Could not read what the edited command produced")
	}
	s.removeNotices(ctx, msg.RoomID, notices)
	// The command message itself is the one being edited, so it stays; only
	// its event id is reused, which is what keeps the new answer attached to
	// it.
	msg.EventID = id.EventID(target)
	return true
}

// refuseEdit puts the message back the way it was and says why. Restoring the
// text is the part that matters: an edit that is not acted on but is left
// standing would show a command that never ran.
func (s *Service) refuseEdit(ctx context.Context, msg *bridge.Message, room config.Room, target, restore, why string) {
	if restore != "" {
		formatted := html.EscapeString(restore)
		if s.conf().Notices.FormatCommands && strings.HasPrefix(restore, "/") {
			formatted = "<code>" + formatted + "</code>"
		}
		if err := s.bridge.EditMine(ctx, msg.RoomID, id.EventID(target), restore, formatted); err != nil {
			s.log.Debug().Err(err).Msg("Could not restore an edited command")
		}
	}
	s.postNoticeReply(ctx, msg.RoomID, room.Ghosts[0], msg.ThreadRoot, id.EventID(target), why)
}

// originalQuestion is what a message said before it was edited, for a message
// that was a turn rather than a command.
func (s *Service) originalQuestion(ctx context.Context, eventID string) string {
	turn, err := s.store.Turn(ctx, eventID)
	if err != nil || turn == nil {
		return ""
	}
	return turn.Question
}
