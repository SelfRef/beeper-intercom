package bridge

import (
	"context"
	"fmt"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// The status room is where the bridge talks about itself. It is a bridge-bot
// room (`com.beeper.is_bridge_bot_room`), which is the one place Beeper draws
// an m.notice from the bridge bot as dim centred text rather than a bubble —
// the style used for bridge login prompts. Both conditions are needed: the
// room flag and the bot as sender, so nothing else in this bridge can use it.
const (
	kvStatusRoom     = "status_room_id"
	kvStatusRoomHash = "status_room_hash"
)

// reconcileStatusRoom creates or patches the status room when one is named.
func (b *Bridge) reconcileStatusRoom(ctx context.Context) error {
	name := b.conf().Network.StatusRoom
	if name == "" {
		return nil
	}
	roomID, _ := b.store.GetKV(ctx, kvStatusRoom)
	if roomID == "" {
		created, err := b.createStatusRoom(ctx, name)
		if err != nil {
			return fmt.Errorf("create status room: %w", err)
		}
		roomID = created.String()
		if err := b.store.SetKV(ctx, kvStatusRoom, roomID); err != nil {
			return err
		}
		b.log.Info().Str("room_id", roomID).Msg("Created status room")
	}
	rid := id.RoomID(roomID)

	hash := hashOf(name, b.conf().Network.Name)
	if stored, _ := b.store.GetKV(ctx, kvStatusRoomHash); stored != hash {
		bot := b.BotIntent()
		if _, err := bot.SendStateEvent(ctx, rid, event.StateRoomName, "", map[string]any{
			"name": name, "com.beeper.exclude_from_timeline": true,
		}); err != nil {
			return err
		}
		if _, err := bot.SendStateEvent(ctx, rid, event.StateBridge, "", b.statusBridgeContent(name)); err != nil {
			return err
		}
		if err := b.store.SetKV(ctx, kvStatusRoomHash, hash); err != nil {
			return err
		}
	}

	b.mu.Lock()
	b.statusRoom = rid
	b.mu.Unlock()
	return nil
}

func (b *Bridge) createStatusRoom(ctx context.Context, name string) (id.RoomID, error) {
	resp, err := b.BotIntent().CreateRoom(ctx, &mautrix.ReqCreateRoom{
		Name:            name,
		Topic:           "The bridge reporting on itself: connection, reloads, deliveries.",
		Preset:          "private_chat",
		Visibility:      "private",
		RoomVersion:     "11",
		IsDirect:        true,
		Invite:          []id.UserID{b.userID},
		CreationContent: map[string]any{"m.federate": false},
		InitialState: []*event.Event{
			// Management rooms carry m.bridge with an EMPTY state key and the
			// appservice id as protocol id — the shape real bridge bot rooms use.
			stateEvent(event.StateBridge, "", b.statusBridgeContent(name)),
		},
		PowerLevelOverride: &event.PowerLevelsEventContent{
			Users:         map[id.UserID]int{b.BotMXID(): plBot, b.userID: plUser},
			EventsDefault: 0,
		},
		BeeperAutoJoinInvites: true,
	})
	if err != nil {
		return "", err
	}
	return resp.RoomID, nil
}

func (b *Bridge) statusBridgeContent(name string) map[string]any {
	cfg := b.conf()
	return map[string]any{
		"bridgebot":                     b.BotMXID().String(),
		"creator":                       b.BotMXID().String(),
		"com.beeper.bridge_name":        cfg.Network.Bridge,
		"com.beeper.self_hosted":        true,
		"com.beeper.is_bridge_bot_room": true,
		"protocol":                      map[string]any{"id": b.reg.ID, "displayname": cfg.Network.Name},
		"channel":                       map[string]any{"id": "unknown", "displayname": name},
	}
}

// Status posts a dim notice in the status room. It never fails the caller:
// a bridge that cannot report a problem should still fix it.
func (b *Bridge) Status(ctx context.Context, text string) {
	b.mu.RLock()
	roomID := b.statusRoom
	b.mu.RUnlock()
	if roomID == "" || b.as == nil {
		return
	}
	plain, formatted := Markdown(text)
	content := &event.MessageEventContent{MsgType: event.MsgNotice, Body: plain}
	if formatted != "" && formatted != plain {
		content.Format = event.FormatHTML
		content.FormattedBody = formatted
	}
	if _, err := b.BotIntent().SendMessageEvent(ctx, roomID, event.EventMessage, content); err != nil {
		b.log.Debug().Err(err).Msg("Could not post to the status room")
	}
}

// HasStatusRoom reports whether status notices go anywhere.
func (b *Bridge) HasStatusRoom() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.statusRoom != ""
}
