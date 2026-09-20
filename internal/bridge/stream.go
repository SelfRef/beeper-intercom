package bridge

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/beeperstream"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// Live token streaming into a bubble, over Beeper's com.beeper.stream.
//
// The mechanism (BEEPER_REF §8, mautrix-go beeperstream): the ghost sends an
// ordinary message carrying a stream descriptor, the client that shows it
// subscribes with a to-device event addressed to the descriptor's user AND
// device, and the publisher answers with to-device updates. To-device traffic
// is ephemeral, so the finished answer is committed as an edit at the end —
// that is the copy every other device and every reload sees.
//
// Two facts shaped this file:
//
//   - a `*` device wildcard is accepted and silently dropped, so the ghost
//     needs a real device id in the descriptor. Hungryserv has no MSC4190
//     device endpoint, but `m.login.application_service` returns one, and the
//     appservice keeps receiving that device's to-device events over its
//     websocket. The token from that login is discarded; the as_token does
//     everything else;
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
)

// streamer holds one beeperstream helper per ghost. The helper is bound to a
// client (user + device), so a ghost that streams needs its own.
type streamer struct {
	mu      sync.Mutex
	helpers map[string]*beeperstream.Helper
}

// Stream is one live answer.
type Stream struct {
	b        *Bridge
	helper   *beeperstream.Helper
	roomID   id.RoomID
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
	helper, err := b.streamHelper(ctx, ghostKey)
	if err != nil {
		return nil, err
	}
	descriptor, err := helper.NewDescriptor(ctx, roomID, streamType)
	if err != nil {
		return nil, err
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
	if err := helper.Register(ctx, roomID, resp.EventID, descriptor); err != nil {
		return nil, err
	}
	return &Stream{
		b: b, helper: helper, roomID: roomID, ghostKey: ghostKey,
		EventID: resp.EventID, opts: opts, ctx: ctx,
	}, nil
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

	err := s.helper.Publish(s.ctx, s.roomID, s.EventID, map[string]any{
		streamDeltaKey: []map[string]any{{"delta": delta}},
	})
	if err != nil {
		s.b.log.Debug().Err(err).Msg("Stream update failed")
	}
}

// Finish commits the answer as an edit of the anchor and closes the stream.
// The edit is the durable copy; everything before it was ephemeral.
func (s *Stream) Finish(ctx context.Context, body, formatted string) error {
	s.close()
	if body == "" {
		body = streamPlaceholder
	}
	_, err := s.b.SendEdit(ctx, s.roomID, s.ghostKey, s.EventID, body, formatted)
	return err
}

// Abort closes the stream and removes the anchor: nothing to show, so nothing
// should be left behind.
func (s *Stream) Abort(ctx context.Context) {
	s.close()
	if err := s.b.Redact(ctx, s.roomID, s.ghostKey, s.EventID); err != nil {
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
		_ = s.helper.Publish(s.ctx, s.roomID, s.EventID, map[string]any{
			streamDeltaKey: []map[string]any{{"delta": pending}},
		})
	}
	s.helper.Unregister(s.roomID, s.EventID)
}

// streamHelper returns the helper for a ghost, creating it — and the ghost's
// device — on first use.
func (b *Bridge) streamHelper(ctx context.Context, ghostKey string) (*beeperstream.Helper, error) {
	b.streams.mu.Lock()
	defer b.streams.mu.Unlock()
	if helper, ok := b.streams.helpers[ghostKey]; ok {
		return helper, nil
	}

	mxid := b.GhostMXID(ghostKey)
	deviceID, err := b.ghostDevice(ctx, ghostKey, mxid)
	if err != nil {
		return nil, err
	}
	client := b.as.NewMautrixClient(mxid)
	client.DeviceID = deviceID
	helper, err := beeperstream.New(client)
	if err != nil {
		return nil, err
	}
	if err := helper.InitAppservice(b.ep); err != nil {
		return nil, err
	}
	b.streams.helpers[ghostKey] = helper
	return helper, nil
}

// ghostDevice returns the ghost's device id, logging one in the first time.
// The login's access token is thrown away on purpose: the appservice token
// already speaks for the ghost, and a second credential is a second thing to
// leak.
func (b *Bridge) ghostDevice(ctx context.Context, ghostKey string, mxid id.UserID) (id.DeviceID, error) {
	if stored, err := b.store.GetKV(ctx, kvDevicePrefix+ghostKey); err == nil && stored != "" {
		return id.DeviceID(stored), nil
	}
	login, err := mautrix.NewClient(b.hsURL, "", b.reg.AppToken)
	if err != nil {
		return "", err
	}
	localpart, _, _ := mxid.Parse()
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
	b.log.Info().Str("ghost", ghostKey).Str("device", resp.DeviceID.String()).Msg("Created stream device")
	return resp.DeviceID, nil
}
