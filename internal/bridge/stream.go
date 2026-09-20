package bridge

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// Live token streaming into a bubble, over Beeper's com.beeper.stream.
//
// The protocol (BEEPER_REF §8): the ghost sends an ordinary message carrying a
// stream descriptor; every client that shows it sends a to-device
// `com.beeper.stream.subscribe` to the descriptor's user AND device; the
// publisher answers with to-device `com.beeper.stream.update` events carrying
// deltas, replaying what was buffered to a late subscriber. To-device traffic
// is ephemeral, so the finished answer is committed as an edit at the end —
// that is the copy every other device and every reload sees.
//
// This is a deliberately small publisher rather than mautrix-go's
// beeperstream helper. The helper is built for /sync clients and encrypted
// rooms; wired into an appservice it received the subscribes and sent
// nothing, with no way to see why. Rooms here are unencrypted, so the whole
// protocol is a subscriber table and one to-device call.
//
// Facts this leans on:
//
//   - a `*` device wildcard is accepted and silently dropped, so the ghost
//     needs a real device id in the descriptor. Hungryserv has no MSC4190
//     device endpoint, but `m.login.application_service` returns one, and the
//     appservice keeps receiving that device's to-device events over its
//     websocket. The token from that login is discarded;
//   - the delta format is the one Beeper's own tests use: stream type
//     `com.beeper.llm`, updates of `{"com.beeper.llm.deltas": [{"delta": …}]}`.
const (
	streamType     = "com.beeper.llm"
	streamDeltaKey = "com.beeper.llm.deltas"
	streamDeviceID = id.DeviceID("INTERCOM")
	kvDevicePrefix = "device:"

	// streamFlush bounds how often deltas go out: tokens arrive faster than a
	// phone wants to repaint, and each update is a to-device event.
	streamFlush = 150 * time.Millisecond
	// streamPlaceholder is what a client without stream support sees until
	// the final edit lands.
	streamPlaceholder = "…"
	// Descriptor and subscription lifetimes, matching beeperstream's defaults.
	descriptorExpiry  = 30 * time.Minute
	subscribeExpiry   = 5 * time.Minute
	maxBufferedDeltas = 1024
	// A stream stays known for a while after it finished so a subscribe that
	// arrives just after the last token is recognised (and ignored) rather
	// than logged as unknown.
	closedStreamGrace = 2 * time.Minute
)

type streamKey struct {
	roomID  id.RoomID
	eventID id.EventID
}

type subscriber struct {
	userID   id.UserID
	deviceID id.DeviceID
}

// streamState is one live (or recently closed) stream.
type streamState struct {
	ghostKey    string
	descriptor  *event.BeeperStreamInfo
	subscribers map[subscriber]time.Time
	buffered    []map[string]any
	closed      bool
}

// streamer is the publisher: which streams exist, who subscribed, and the
// ghost devices that speak for them.
type streamer struct {
	mu      sync.Mutex
	streams map[streamKey]*streamState
	devices map[string]id.DeviceID // ghost key -> device
}

// Stream is one live answer.
type Stream struct {
	b        *Bridge
	key      streamKey
	ghostKey string
	EventID  id.EventID
	opts     SendOptions

	mu      sync.Mutex
	pending strings.Builder
	timer   *time.Timer
	closed  bool
	ctx     context.Context
}

// StartStream sends the anchor message and registers the stream. Deltas go out
// with Push; the answer is committed with Finish.
func (b *Bridge) StartStream(ctx context.Context, roomID id.RoomID, ghostKey string, opts SendOptions) (*Stream, error) {
	deviceID, err := b.streamDevice(ctx, ghostKey)
	if err != nil {
		return nil, err
	}
	descriptor := &event.BeeperStreamInfo{
		UserID:   b.GhostMXID(ghostKey),
		DeviceID: deviceID,
		Type:     streamType,
		ExpiryMS: descriptorExpiry.Milliseconds(),
	}
	content := &event.MessageEventContent{
		MsgType:      event.MsgText,
		Body:         streamPlaceholder,
		BeeperStream: descriptor,
	}
	applyRelations(content, opts)
	resp, err := b.Intent(ghostKey).SendMessageEvent(ctx, roomID, event.EventMessage, content)
	if err != nil {
		return nil, err
	}
	key := streamKey{roomID: roomID, eventID: resp.EventID}
	b.streams.mu.Lock()
	b.streams.streams[key] = &streamState{
		ghostKey:    ghostKey,
		descriptor:  descriptor,
		subscribers: map[subscriber]time.Time{},
	}
	b.streams.mu.Unlock()
	b.log.Debug().Str("event_id", resp.EventID.String()).Msg("Stream opened")
	return &Stream{b: b, key: key, ghostKey: ghostKey, EventID: resp.EventID, opts: opts, ctx: ctx}, nil
}

// Push queues generated text. Deltas are coalesced for streamFlush before they
// go out, so a burst of tokens is one update.
func (s *Stream) Push(text string) {
	if text == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.pending.WriteString(text)
	if s.timer == nil {
		s.timer = time.AfterFunc(streamFlush, s.flush)
	}
}

func (s *Stream) flush() {
	s.mu.Lock()
	s.timer = nil
	if s.closed || s.pending.Len() == 0 {
		s.mu.Unlock()
		return
	}
	delta := s.pending.String()
	s.pending.Reset()
	s.mu.Unlock()
	s.b.publishDelta(s.ctx, s.key, delta)
}

// Finish commits the answer as an edit of the anchor and closes the stream.
// The edit is the durable copy; everything before it was ephemeral.
func (s *Stream) Finish(ctx context.Context, body, formatted string) error {
	s.close()
	if body == "" {
		body = streamPlaceholder
	}
	_, err := s.b.SendEdit(ctx, s.key.roomID, s.ghostKey, s.EventID, body, formatted)
	return err
}

// Abort closes the stream and removes the anchor: nothing to show, so nothing
// should be left behind.
func (s *Stream) Abort(ctx context.Context) {
	s.close()
	if err := s.b.Redact(ctx, s.key.roomID, s.ghostKey, s.EventID); err != nil {
		s.b.log.Debug().Err(err).Msg("Could not remove the stream anchor")
	}
}

func (s *Stream) close() {
	s.mu.Lock()
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	// Anything still queued goes out once, so the last tokens are not lost to
	// the edit racing ahead of them.
	pending := s.pending.String()
	s.pending.Reset()
	alreadyClosed := s.closed
	s.closed = true
	s.mu.Unlock()
	if alreadyClosed {
		return
	}
	if pending != "" {
		s.b.publishDelta(s.ctx, s.key, pending)
	}
	s.b.closeStream(s.key)
}

// publishDelta buffers a delta for late subscribers and sends it to the
// current ones.
func (b *Bridge) publishDelta(ctx context.Context, key streamKey, delta string) {
	update := map[string]any{streamDeltaKey: []map[string]any{{"delta": delta}}}

	b.streams.mu.Lock()
	state := b.streams.streams[key]
	if state == nil || state.closed {
		b.streams.mu.Unlock()
		return
	}
	state.buffered = append(state.buffered, update)
	if len(state.buffered) > maxBufferedDeltas {
		state.buffered = state.buffered[len(state.buffered)-maxBufferedDeltas:]
	}
	subs := b.activeSubscribers(state)
	ghostKey := state.ghostKey
	b.streams.mu.Unlock()

	if len(subs) == 0 {
		return
	}
	b.sendStreamUpdate(ctx, ghostKey, key, []map[string]any{update}, subs)
}

// handleStreamSubscribe is the inbound half: a client asked to follow a
// stream. It gets everything buffered so far in one update, then lives.
func (b *Bridge) handleStreamSubscribe(ctx context.Context, evt *event.Event) {
	sub := evt.Content.AsBeeperStreamSubscribe()
	if sub.RoomID == "" || sub.EventID == "" || sub.DeviceID == "" {
		return
	}
	key := streamKey{roomID: sub.RoomID, eventID: sub.EventID}
	who := subscriber{userID: evt.Sender, deviceID: sub.DeviceID}

	b.streams.mu.Lock()
	state := b.streams.streams[key]
	if state == nil || state.closed {
		b.streams.mu.Unlock()
		b.log.Debug().Str("event_id", sub.EventID.String()).Str("device", sub.DeviceID.String()).
			Bool("known", state != nil).Msg("Stream subscribe for a stream that is not live")
		return
	}
	expiry := subscribeExpiry
	if sub.ExpiryMS > 0 && time.Duration(sub.ExpiryMS)*time.Millisecond < expiry {
		expiry = time.Duration(sub.ExpiryMS) * time.Millisecond
	}
	state.subscribers[who] = time.Now().Add(expiry)
	replay := append([]map[string]any(nil), state.buffered...)
	ghostKey := state.ghostKey
	b.streams.mu.Unlock()

	b.log.Debug().Str("event_id", sub.EventID.String()).Str("subscriber", evt.Sender.String()).
		Str("device", sub.DeviceID.String()).Int("replay", len(replay)).Msg("Stream subscriber added")
	if len(replay) > 0 {
		b.sendStreamUpdate(ctx, ghostKey, key, replay, []subscriber{who})
	}
}

// sendStreamUpdate is the one to-device call of the protocol.
func (b *Bridge) sendStreamUpdate(ctx context.Context, ghostKey string, key streamKey, updates []map[string]any, subs []subscriber) {
	content := &event.Content{Raw: map[string]any{
		"room_id":  key.roomID.String(),
		"event_id": key.eventID.String(),
		"updates":  updates,
	}}
	req := &mautrix.ReqSendToDevice{Messages: map[id.UserID]map[id.DeviceID]*event.Content{}}
	for _, sub := range subs {
		if req.Messages[sub.userID] == nil {
			req.Messages[sub.userID] = map[id.DeviceID]*event.Content{}
		}
		req.Messages[sub.userID][sub.deviceID] = content
	}
	if _, err := b.Intent(ghostKey).SendToDevice(ctx, event.ToDeviceBeeperStreamUpdate, req); err != nil {
		b.log.Debug().Err(err).Msg("Stream update failed")
		return
	}
	b.log.Trace().Int("subscribers", len(subs)).Int("updates", len(updates)).Msg("Stream update sent")
}

func (b *Bridge) activeSubscribers(state *streamState) []subscriber {
	now := time.Now()
	out := make([]subscriber, 0, len(state.subscribers))
	for sub, expiry := range state.subscribers {
		if now.After(expiry) {
			delete(state.subscribers, sub)
			continue
		}
		out = append(out, sub)
	}
	return out
}

// closeStream marks a stream finished and forgets it after a grace period.
func (b *Bridge) closeStream(key streamKey) {
	b.streams.mu.Lock()
	if state := b.streams.streams[key]; state != nil {
		state.closed = true
		state.subscribers = nil
		state.buffered = nil
	}
	b.streams.mu.Unlock()
	time.AfterFunc(closedStreamGrace, func() {
		b.streams.mu.Lock()
		delete(b.streams.streams, key)
		b.streams.mu.Unlock()
	})
}

// streamDevice returns the ghost's device id, logging one in the first time.
// The login's access token is thrown away on purpose: the appservice token
// already speaks for the ghost, and a second credential is a second thing to
// leak.
func (b *Bridge) streamDevice(ctx context.Context, ghostKey string) (id.DeviceID, error) {
	b.streams.mu.Lock()
	if dev, ok := b.streams.devices[ghostKey]; ok {
		b.streams.mu.Unlock()
		return dev, nil
	}
	b.streams.mu.Unlock()

	if stored, err := b.store.GetKV(ctx, kvDevicePrefix+ghostKey); err == nil && stored != "" {
		b.streams.mu.Lock()
		b.streams.devices[ghostKey] = id.DeviceID(stored)
		b.streams.mu.Unlock()
		return id.DeviceID(stored), nil
	}
	login, err := mautrix.NewClient(b.hsURL, "", b.reg.AppToken)
	if err != nil {
		return "", err
	}
	localpart, _, _ := b.GhostMXID(ghostKey).Parse()
	resp, err := login.Login(ctx, &mautrix.ReqLogin{
		Type:                     mautrix.AuthTypeAppservice,
		Identifier:               mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: localpart},
		DeviceID:                 streamDeviceID,
		InitialDeviceDisplayName: "beeper-intercom",
		StoreCredentials:         false,
	})
	if err != nil {
		return "", fmt.Errorf("create device for %s: %w", ghostKey, err)
	}
	if resp.DeviceID == "" {
		return "", fmt.Errorf("login for %s returned no device id", ghostKey)
	}
	if err := b.store.SetKV(ctx, kvDevicePrefix+ghostKey, resp.DeviceID.String()); err != nil {
		return "", err
	}
	b.streams.mu.Lock()
	b.streams.devices[ghostKey] = resp.DeviceID
	b.streams.mu.Unlock()
	b.log.Info().Str("ghost", ghostKey).Str("device", resp.DeviceID.String()).Msg("Created stream device")
	return resp.DeviceID, nil
}
