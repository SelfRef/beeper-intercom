package bridge

import (
	"context"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// Reading a room back from the homeserver.
//
// The bridge normally knows a room only through what it has put into it: a
// notice it can take back, a turn it can undo, a command it recorded. Emptying
// a room is the one job that needs the other direction — the messages I typed,
// the answers from before the database existed, everything nothing ever kept a
// row for — so it asks the homeserver for the timeline instead of the store.

// TimelineEvent is one event of a room's history, reduced to what emptying a
// room needs to know about it.
type TimelineEvent struct {
	ID     id.EventID
	Sender id.UserID
	Type   event.Type
	Time   time.Time
}

const (
	// historyPage is how many events one /messages call asks for.
	historyPage = 100
	// historyMaxPages bounds the walk. A homeserver that keeps handing back a
	// token would otherwise page forever; 200 pages is far more history than
	// any room here has, and stopping early only leaves messages behind.
	historyMaxPages = 200
)

// RoomEvents walks a room's timeline backwards from its newest event and
// returns everything in it that can be redacted — messages, attachments,
// reactions, polls, notices — whoever sent them, newest first.
//
// State is left out deliberately: the room's name, avatar, membership and
// power levels are what make it a room, and redacting those would not empty it
// but break it. Events that are already redacted are left out too — they are
// gone, and asking again only costs a request.
func (b *Bridge) RoomEvents(ctx context.Context, roomID id.RoomID) ([]TimelineEvent, error) {
	// The bot reads, because the bot created the room and is in every one of
	// them; a ghost may not be.
	intent := b.BotIntent()
	filter := &mautrix.FilterPart{
		Limit:           historyPage,
		LazyLoadMembers: true,
		// A redaction cannot be redacted, so asking for them back only makes
		// the pages longer. Hungryserv ignores this — measured 2026-09-20: a
		// page of 100 came back with 32 of them in it — so the walk drops them
		// again below. The filter stays because it is free and correct.
		NotTypes: []event.Type{event.EventRedaction},
	}

	var out []TimelineEvent
	from := ""
	for page := 0; page < historyMaxPages; page++ {
		resp, err := intent.Messages(ctx, roomID, from, "", mautrix.DirectionBackward, filter, historyPage)
		if err != nil {
			// What has been collected so far is still worth redacting, so the
			// error travels with it rather than instead of it.
			return out, err
		}
		for _, evt := range resp.Chunk {
			if evt.StateKey != nil || evt.Type == event.EventRedaction || evt.Unsigned.RedactedBecause != nil {
				continue
			}
			out = append(out, TimelineEvent{
				ID:     evt.ID,
				Sender: evt.Sender,
				Type:   evt.Type,
				Time:   time.UnixMilli(evt.Timestamp),
			})
		}
		// The start of the room: no token to continue from, the same token
		// handed back, or a page that brought nothing.
		if resp.End == "" || resp.End == from || len(resp.Chunk) == 0 {
			break
		}
		from = resp.End
	}
	return out, nil
}
