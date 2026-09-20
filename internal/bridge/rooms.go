package bridge

import (
	"context"
	"encoding/json"
	"fmt"

	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/config"
)

// Power levels. The numbers matter more than they look:
//
//   - a broadcast room locks the composer only if events_default is strictly
//     above my own level (equal still passes the PL >= required check), so 60
//     against my 50;
//   - m.reaction is pinned at 0 in a broadcast room, because reacting is the
//     acknowledgement channel for an alert I cannot reply to;
//   - ghosts sit at 75, which is above the room-mention requirement (50), so
//     an urgent notification can legitimately @room;
//   - the bot sits far above everything so state can always be patched.
const (
	plBot        = 9001
	plGhost      = 75
	plUser       = 50
	plBroadcast  = 60
	plNotifyRoom = 50
)

// Reconcile creates what is missing and patches what has drifted. It never
// deletes: a room that disappears from config keeps existing, with its
// history, and simply stops being written to.
func (b *Bridge) Reconcile(ctx context.Context) error {
	if err := b.reconcileGhosts(ctx); err != nil {
		return err
	}
	if err := b.reconcileRooms(ctx); err != nil {
		return err
	}
	return b.reconcileStatusRoom(ctx)
}

func (b *Bridge) reconcileGhosts(ctx context.Context) error {
	for _, key := range config.SortedKeys(b.conf().Ghosts) {
		ghost := b.conf().Ghosts[key]
		mxid := b.GhostMXID(key)
		intent := b.as.Intent(mxid)
		if err := intent.EnsureRegistered(ctx); err != nil {
			return fmt.Errorf("register ghost %s: %w", key, err)
		}

		avatar, err := b.uploadFile(ctx, ghost.Avatar)
		if err != nil {
			b.log.Warn().Err(err).Str("ghost", key).Msg("Failed to upload ghost avatar")
		}
		hash := hashOf(ghost.Name, avatar.String())
		stored, err := b.store.GhostHash(ctx, key)
		if err != nil {
			return err
		}
		if stored != hash {
			if err := intent.SetDisplayName(ctx, ghost.Name); err != nil {
				return fmt.Errorf("set displayname for %s: %w", key, err)
			}
			if !avatar.IsEmpty() {
				if err := intent.SetAvatarURL(ctx, avatar); err != nil {
					return fmt.Errorf("set avatar for %s: %w", key, err)
				}
			}
			if err := b.store.PutGhost(ctx, key, mxid.String(), hash); err != nil {
				return err
			}
			b.log.Info().Str("ghost", key).Str("mxid", mxid.String()).Msg("Updated ghost profile")
		}

		b.mu.Lock()
		b.ghosts[key] = mxid
		b.mu.Unlock()
	}
	return nil
}

func (b *Bridge) reconcileRooms(ctx context.Context) error {
	networkAvatar, err := b.uploadFile(ctx, b.conf().Network.Avatar)
	if err != nil {
		b.log.Warn().Err(err).Msg("Failed to upload network avatar")
	} else if !networkAvatar.IsEmpty() {
		_ = b.store.SetKV(ctx, kvNetworkAvatar, networkAvatar.String())
	}

	for _, key := range config.SortedKeys(b.conf().Rooms) {
		room := b.conf().Rooms[key]
		roomID, storedHash, err := b.store.RoomID(ctx, key)
		if err != nil {
			return err
		}

		avatar := networkAvatar
		if room.Avatar != "" {
			if uploaded, err := b.uploadFile(ctx, room.Avatar); err != nil {
				b.log.Warn().Err(err).Str("room", key).Msg("Failed to upload room avatar")
			} else {
				avatar = uploaded
			}
		}

		if roomID == "" {
			created, err := b.createRoom(ctx, key, room, avatar, networkAvatar)
			if err != nil {
				return fmt.Errorf("create room %s: %w", key, err)
			}
			roomID = created.String()
			storedHash = ""
			b.log.Info().Str("room", key).Str("room_id", roomID).Msg("Created room")
		}

		rid := id.RoomID(roomID)
		hash := hashOf(room.Name, room.Topic, avatar.String(), room.Kind, room.Ghosts, room.Tags,
			room.Muted, room.DeletePlaceholder, b.conf().Network.Name)
		if storedHash != hash {
			if err := b.patchRoom(ctx, key, room, rid, avatar, networkAvatar); err != nil {
				return fmt.Errorf("patch room %s: %w", key, err)
			}
			if err := b.store.PutRoom(ctx, key, roomID, hash); err != nil {
				return err
			}
			b.log.Info().Str("room", key).Str("room_id", roomID).Msg("Reconciled room state")
		}

		b.mu.Lock()
		b.roomIDs[key] = rid
		b.roomKey[rid] = key
		b.mu.Unlock()
	}
	return nil
}

func (b *Bridge) createRoom(ctx context.Context, key string, room config.Room, avatar, networkAvatar id.ContentURI) (id.RoomID, error) {
	invite := []id.UserID{b.userID}
	for _, ghost := range room.Ghosts {
		invite = append(invite, b.GhostMXID(ghost))
	}
	for _, extra := range room.Invite {
		invite = append(invite, id.UserID(extra))
	}

	initialState := []*event.Event{
		stateEvent(event.StateBridge, b.bridgeStateKey(), b.bridgeContent(key, room, networkAvatar)),
		stateEvent(event.StateHalfShotBridge, b.bridgeStateKey(), b.halfShotBridgeContent(key, room, networkAvatar)),
		stateEvent(event.StateElementFunctionalMembers, "", map[string]any{
			"service_members": []string{b.BotMXID().String()},
		}),
		stateEventJSON(event.StateBeeperRoomFeatures, b.bridgeStateKey(), b.roomFeatures(room)),
	}
	if !avatar.IsEmpty() {
		initialState = append(initialState, stateEvent(event.StateRoomAvatar, "", map[string]any{
			"url":                              avatar.String(),
			"com.beeper.exclude_from_timeline": true,
		}))
	}

	req := &mautrix.ReqCreateRoom{
		Name:               room.Name,
		Topic:              room.Topic,
		Preset:             "private_chat",
		Visibility:         "private",
		RoomVersion:        "11", // v12 fails: hungryserv does no version negotiation
		IsDirect:           room.Kind == config.KindDM,
		Invite:             invite,
		CreationContent:    map[string]any{"m.federate": false},
		InitialState:       initialState,
		PowerLevelOverride: b.powerLevels(room),
		// Without this the account owner's membership sticks at "invite" and
		// the room shows up empty and broken.
		BeeperAutoJoinInvites: true,
		// NOT com.beeper.bridge_name: hungryserv rejects the create outright
		// ("no beeper bridge details should be provided for unknown room IDs")
		// unless a local room ID comes with it. The m.bridge state below is
		// what actually groups the room under its network.
	}
	resp, err := b.BotIntent().CreateRoom(ctx, req)
	if err != nil {
		return "", err
	}
	if err := b.store.PutRoom(ctx, key, resp.RoomID.String(), ""); err != nil {
		return "", err
	}
	return resp.RoomID, nil
}

// patchRoom brings an existing room back in line with config. Every state
// event carries com.beeper.exclude_from_timeline so a rename does not draw a
// line in the timeline of every device.
func (b *Bridge) patchRoom(ctx context.Context, key string, room config.Room, roomID id.RoomID, avatar, networkAvatar id.ContentURI) error {
	bot := b.BotIntent()

	if _, err := bot.SendStateEvent(ctx, roomID, event.StateRoomName, "", map[string]any{
		"name":                             room.Name,
		"com.beeper.exclude_from_timeline": true,
	}); err != nil {
		return err
	}
	if room.Topic != "" {
		if _, err := bot.SendStateEvent(ctx, roomID, event.StateTopic, "", map[string]any{
			"topic":                            room.Topic,
			"com.beeper.exclude_from_timeline": true,
		}); err != nil {
			return err
		}
	}
	if !avatar.IsEmpty() {
		if _, err := bot.SendStateEvent(ctx, roomID, event.StateRoomAvatar, "", map[string]any{
			"url":                              avatar.String(),
			"com.beeper.exclude_from_timeline": true,
		}); err != nil {
			return err
		}
	}
	if _, err := bot.SendStateEvent(ctx, roomID, event.StateBridge, b.bridgeStateKey(),
		b.bridgeContent(key, room, networkAvatar)); err != nil {
		return err
	}
	if _, err := bot.SendStateEvent(ctx, roomID, event.StateHalfShotBridge, b.bridgeStateKey(),
		b.halfShotBridgeContent(key, room, networkAvatar)); err != nil {
		return err
	}
	if _, err := bot.SendStateEvent(ctx, roomID, event.StateElementFunctionalMembers, "", map[string]any{
		"service_members": []string{b.BotMXID().String()},
	}); err != nil {
		return err
	}
	// What the client may do in this room — and, the reason it is here at all,
	// whether a redaction leaves a tombstone behind (see features.go).
	if _, err := bot.SendStateEvent(ctx, roomID, event.StateBeeperRoomFeatures, b.bridgeStateKey(),
		b.roomFeatures(room)); err != nil {
		return err
	}
	if _, err := bot.SetPowerLevels(ctx, roomID, b.powerLevels(room)); err != nil {
		return err
	}

	// Ghosts must be joined before they speak, or the client renders a raw
	// MXID instead of the display name.
	for _, ghost := range room.Ghosts {
		mxid := b.GhostMXID(ghost)
		if err := bot.EnsureInvited(ctx, roomID, mxid); err != nil {
			b.log.Debug().Err(err).Str("ghost", ghost).Msg("Invite failed (already joined?)")
		}
		if err := b.as.Intent(mxid).EnsureJoined(ctx, roomID); err != nil {
			return fmt.Errorf("join ghost %s: %w", ghost, err)
		}
	}
	if err := bot.EnsureInvited(ctx, roomID, b.userID); err != nil {
		b.log.Debug().Err(err).Msg("Owner invite failed (already joined?)")
	}

	return b.applyAccountData(ctx, room, roomID)
}

// applyAccountData writes the per-user state: tags and mute. These belong to
// the account, not the appservice, so they go through the account token.
func (b *Bridge) applyAccountData(ctx context.Context, room config.Room, roomID id.RoomID) error {
	for _, tag := range room.Tags {
		if err := b.user.AddTag(ctx, roomID, event.RoomTag("m."+tag), 0.5); err != nil {
			b.log.Warn().Err(err).Str("tag", tag).Msg("Failed to set room tag")
		}
	}
	if room.Muted {
		if err := b.user.SetRoomAccountData(ctx, roomID, "com.beeper.mute", map[string]any{"muted_until": -1}); err != nil {
			b.log.Warn().Err(err).Msg("Failed to mute room")
		}
	}
	return nil
}

// bridgeStateKey is <domain>/<appservice id>, the shape real portals use.
func (b *Bridge) bridgeStateKey() string {
	return fmt.Sprintf("%s/%s", b.conf().Matrix.HomeserverDomain, b.reg.ID)
}

// bridgeContent is what makes these rooms one network in the client instead
// of a handful of loose "beeper (matrix)" chats.
func (b *Bridge) bridgeContent(key string, room config.Room, avatar id.ContentURI) map[string]any {
	protocol := map[string]any{
		"id":          b.conf().Network.ID,
		"displayname": b.conf().Network.Name,
	}
	if !avatar.IsEmpty() {
		protocol["avatar_url"] = avatar.String()
	}
	if b.conf().Network.ExternalURL != "" {
		protocol["external_url"] = b.conf().Network.ExternalURL
	}
	return map[string]any{
		"bridgebot":              b.BotMXID().String(),
		"creator":                b.BotMXID().String(),
		"com.beeper.bridge_name": b.conf().Network.Bridge,
		"com.beeper.self_hosted": true,
		"protocol":               protocol,
		"channel": map[string]any{
			"id":          "room:" + key,
			"displayname": room.Name,
		},
	}
}

// halfShotBridgeContent is the same thing without the Beeper keys, for the
// MSC2346 event every real portal also sets.
func (b *Bridge) halfShotBridgeContent(key string, room config.Room, avatar id.ContentURI) map[string]any {
	content := b.bridgeContent(key, room, avatar)
	delete(content, "com.beeper.bridge_name")
	delete(content, "com.beeper.self_hosted")
	return content
}

func (b *Bridge) powerLevels(room config.Room) *event.PowerLevelsEventContent {
	users := map[id.UserID]int{
		b.BotMXID(): plBot,
		b.userID:    plUser,
	}
	for _, ghost := range room.Ghosts {
		users[b.GhostMXID(ghost)] = plGhost
	}
	pl := &event.PowerLevelsEventContent{
		Users:           users,
		UsersDefault:    0,
		StateDefaultPtr: ptr.Ptr(plBot),
		InvitePtr:       ptr.Ptr(plBot),
		KickPtr:         ptr.Ptr(plBot),
		BanPtr:          ptr.Ptr(plBot),
		RedactPtr:       ptr.Ptr(plUser),
		Notifications:   &event.NotificationPowerLevels{RoomPtr: ptr.Ptr(plNotifyRoom)},
	}
	if room.Kind == config.KindBroadcast {
		pl.EventsDefault = plBroadcast
		pl.Events = map[string]int{
			// The one thing I can still do in a room I cannot type in.
			event.EventReaction.Type:  0,
			event.EventRedaction.Type: plUser,
		}
	} else {
		pl.EventsDefault = 0
	}
	return pl
}

func stateEvent(evtType event.Type, stateKey string, content map[string]any) *event.Event {
	return &event.Event{
		Type:     evtType,
		StateKey: &stateKey,
		Content:  event.Content{Raw: content},
	}
}

// stateEventJSON is the same for a typed content struct, which has to go
// through JSON to land in the Raw map createRoom sends.
func stateEventJSON(evtType event.Type, stateKey string, content any) *event.Event {
	raw, err := json.Marshal(content)
	if err != nil {
		return stateEvent(evtType, stateKey, map[string]any{})
	}
	var asMap map[string]any
	if err := json.Unmarshal(raw, &asMap); err != nil {
		return stateEvent(evtType, stateKey, map[string]any{})
	}
	return stateEvent(evtType, stateKey, asMap)
}

// SetMuted mutes or unmutes a room for the account owner. Mute is per-user
// state, so it goes through the account token rather than the appservice.
func (b *Bridge) SetMuted(ctx context.Context, roomID id.RoomID, muted bool) error {
	mutedUntil := 0
	if muted {
		mutedUntil = -1 // forever; a positive value would be a ms timestamp
	}
	return b.user.SetRoomAccountData(ctx, roomID, "com.beeper.mute", map[string]any{"muted_until": mutedUntil})
}
