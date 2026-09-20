// Package bridge is the Matrix side and nothing else: registration, the
// appservice websocket, ghosts, rooms, and sending. It knows what a
// notification looks like, but nothing about agents, sessions or webhooks —
// those live above it, in the service layer.
package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beeper/bridge-manager/api/beeperapi"
	"github.com/beeper/bridge-manager/api/hungryapi"
	"github.com/rs/zerolog"
	"go.mau.fi/util/jsontime"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/store"
)

const (
	kvRegistration  = "registration"
	kvHomeserverURL = "homeserver_url"
	kvUserID        = "user_id"
	kvUsername      = "username"
	kvAvatarPrefix  = "avatar:"
	kvNetworkAvatar = "network_avatar"
)

// Handlers are what the service layer wants to hear about. Everything else
// the homeserver sends is dropped here.
type Handlers struct {
	// Message is a message from the account owner in a known room.
	OnMessage func(ctx context.Context, evt *Message)
	// Reaction is the account owner reacting to anything in a known room.
	OnReaction func(ctx context.Context, evt *Reaction)
	// Redaction is the account owner deleting one of their own messages.
	OnRedaction func(ctx context.Context, roomKey string, roomID id.RoomID, target id.EventID)
	// PollResponse is an answer to a poll the bridge asked.
	OnPollResponse func(ctx context.Context, evt *PollResponse)
}

// Message is one inbound message from the account owner.
type Message struct {
	RoomKey    string
	RoomID     id.RoomID
	EventID    id.EventID
	Sender     id.UserID
	Body       string
	ThreadRoot id.EventID
	ReplyTo    id.EventID
	// Edits names the event this message replaces, when it is an edit.
	Edits   id.EventID
	Content *event.MessageEventContent
	Time    time.Time
	// Attachment is set for image, file, audio and video messages. Body then
	// holds the caption, if there is one, rather than the file name.
	Attachment *Attachment
}

// Attachment is a file the account owner sent.
type Attachment struct {
	URL   id.ContentURI
	Name  string
	Mime  string
	Size  int
	Kind  event.MessageType
	Voice bool
}

// Reaction is one inbound reaction.
type Reaction struct {
	RoomKey string
	RoomID  id.RoomID
	EventID id.EventID
	Sender  id.UserID
	Key     string
	Target  id.EventID
}

// PollResponse is one inbound poll answer.
type PollResponse struct {
	RoomKey string
	RoomID  id.RoomID
	Sender  id.UserID
	PollID  id.EventID
	Answers []string
}

// Bridge is the Matrix connection.
type Bridge struct {
	// cfg is swapped wholesale on reload; every reader takes it through
	// conf(), so a reload never tears a half-applied config.
	cfg      atomic.Pointer[config.Config]
	store    *store.Store
	log      zerolog.Logger
	handlers Handlers

	as     *appservice.AppService
	ep     *appservice.EventProcessor
	reg    *appservice.Registration
	userID id.UserID
	// user is a client authenticated as the account owner, needed for the
	// per-user state an appservice cannot write: tags and mute.
	user *mautrix.Client

	hsURL    string
	username string

	mu      sync.RWMutex
	roomIDs map[string]id.RoomID // config key -> room
	roomKey map[id.RoomID]string // room -> config key
	ghosts  map[string]id.UserID // config key -> mxid

	connected     bool
	everConnected bool
	lastConnet    time.Time
	disconnectAt  time.Time

	streams    streamer
	statusRoom id.RoomID
}

// New prepares a bridge. Nothing talks to the network until Start.
func New(cfg *config.Config, st *store.Store, log zerolog.Logger, handlers Handlers) *Bridge {
	b := &Bridge{
		store:    st,
		log:      log,
		handlers: handlers,
		roomIDs:  map[string]id.RoomID{},
		roomKey:  map[id.RoomID]string{},
		ghosts:   map[string]id.UserID{},
		streams:  streamer{streams: map[streamKey]*streamState{}, devices: map[string]id.DeviceID{}},
	}
	b.cfg.Store(cfg)
	return b
}

// conf is the current config.
func (b *Bridge) conf() *config.Config { return b.cfg.Load() }

// SetConfig swaps the config. Only fields that reconciliation re-applies may
// change; the registration name is checked by the caller.
func (b *Bridge) SetConfig(cfg *config.Config) { b.cfg.Store(cfg) }

// Start registers the appservice if needed, opens the websocket and keeps it
// open. It returns once the first connection is established; the reconnect
// loop runs until ctx is cancelled.
//
// It is safe to call again after a failure: the expensive, failure-prone part
// is registration, and everything after it only happens once.
func (b *Bridge) Start(ctx context.Context) error {
	if b.as != nil {
		return nil
	}
	if err := b.loadOrRegister(ctx); err != nil {
		return err
	}

	as, err := appservice.CreateFull(appservice.CreateOpts{
		Registration:     b.reg,
		HomeserverDomain: b.conf().Matrix.HomeserverDomain,
		HomeserverURL:    b.hsURL,
	})
	if err != nil {
		return fmt.Errorf("create appservice: %w", err)
	}
	as.Log = b.log.With().Str("component", "appservice").Logger()
	b.as = as

	b.user, err = mautrix.NewClient(b.hsURL, b.userID, b.conf().AccountToken())
	if err != nil {
		return fmt.Errorf("create user client: %w", err)
	}

	ep := appservice.NewEventProcessor(as)
	b.ep = ep
	ep.On(event.EventMessage, b.handleMessage)
	ep.On(event.EventReaction, b.handleReaction)
	ep.On(event.EventRedaction, b.handleRedaction)
	ep.On(eventPollResponse, b.handlePollResponse)
	// Stream subscriptions are handled by the beeperstream helpers; this only
	// makes their arrival visible at debug level, because a subscribe that is
	// delivered but not acted on is otherwise invisible.
	ep.On(event.ToDeviceBeeperStreamSubscribe, b.handleStreamSubscribe)
	ep.Start(ctx)

	ready := make(chan struct{})
	var once sync.Once
	go b.websocketLoop(ctx, func() { once.Do(func() { close(ready) }) })
	go b.loginStateLoop(ctx)
	go b.pingLoop(ctx, pingInterval, pingTimeout)

	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(60 * time.Second):
		// Not fatal: the loop keeps retrying, and announcements queue up in
		// the meantime. Report it so the operator sees it in the logs.
		b.log.Warn().Msg("appservice websocket not connected after 60s, continuing to retry")
		return nil
	}
}

// websocketLoop keeps the transport up. Beeper closes idle sockets and
// restarts hungryserv on deploys; a bridge that does not reconnect looks
// exactly like a bridge that works until you need it.
func (b *Bridge) websocketLoop(ctx context.Context, onConnect func()) {
	backoff := time.Second
	for ctx.Err() == nil {
		b.log.Debug().Msg("Connecting appservice websocket")
		err := b.as.StartWebsocket(ctx, "", func() {
			b.mu.Lock()
			reconnect := b.everConnected
			gap := time.Since(b.disconnectAt).Round(time.Second)
			b.connected = true
			b.everConnected = true
			b.lastConnet = time.Now()
			b.mu.Unlock()
			backoff = time.Second
			b.log.Info().Msg("Appservice websocket connected")
			// RUNNING, not CONNECTED: Beeper's bridge list treats CONNECTED as a
			// per-login state and left this bridge at STARTING (from the
			// registration call) until RUNNING was posted — verified 2026-09-20
			// against the ten bbctl bridges, which all sit at RUNNING.
			b.postBridgeState(ctx, status.StateRunning, "")
			// Only report outages worth reporting. Beeper recycles sockets
			// routinely and the reconnect takes about a second; a notice for
			// every one of those is noise in the bot room, not status.
			if reconnect && gap >= outageNoticeAfter {
				b.Status(ctx, fmt.Sprintf("Reconnected to Beeper after %s.", gap))
			} else if reconnect {
				b.log.Debug().Dur("gap", gap).Msg("Reconnected after a short gap, not reporting")
			}
			onConnect()
		})
		b.mu.Lock()
		b.connected = false
		b.disconnectAt = time.Now()
		b.mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			b.log.Warn().Err(err).Dur("retry_in", backoff).Msg("Appservice websocket closed")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 2*time.Minute {
			backoff *= 2
		}
	}
}

// loadOrRegister gets the appservice registration. It lives on Beeper's
// servers, not on disk, so the local copy is a cache; the registration call
// is idempotent and returns the existing one.
func (b *Bridge) loadOrRegister(ctx context.Context) error {
	token := b.conf().AccountToken()
	if token == "" {
		return fmt.Errorf("no Beeper account token in $%s", b.conf().Matrix.TokenEnv)
	}

	var reg appservice.Registration
	found, err := b.store.GetJSON(ctx, kvRegistration, &reg)
	if err != nil {
		return err
	}
	hsURL, _ := b.store.GetKV(ctx, kvHomeserverURL)
	userID, _ := b.store.GetKV(ctx, kvUserID)
	username, _ := b.store.GetKV(ctx, kvUsername)

	if found && !b.conf().Matrix.ReRegister && hsURL != "" && userID != "" {
		b.reg, b.hsURL, b.userID, b.username = &reg, hsURL, id.UserID(userID), username
		b.log.Info().Str("bridge", b.conf().Network.Bridge).Str("user", userID).Msg("Using stored registration")
		return nil
	}

	whoami, err := beeperapi.Whoami(b.conf().Matrix.BaseDomain, token)
	if err != nil {
		return fmt.Errorf("whoami: %w", err)
	}
	b.username = whoami.UserInfo.Username
	hungry := hungryapi.NewClient(b.conf().Matrix.BaseDomain, b.username, token)
	newReg, err := hungry.RegisterAppService(ctx, b.conf().Network.Bridge, hungryapi.ReqRegisterAppService{
		Push:       false, // websocket transport: no public endpoint to push to
		SelfHosted: true,
	})
	if err != nil {
		return fmt.Errorf("register appservice %q: %w", b.conf().Network.Bridge, err)
	}
	// Beeper returns the bot's own namespace as a second entry; bbctl drops it
	// and so do we, because it is the same user as sender_localpart.
	if len(newReg.Namespaces.UserIDs) > 1 {
		newReg.Namespaces.UserIDs = newReg.Namespaces.UserIDs[0:1]
	}

	b.reg = &newReg
	b.hsURL = hungry.HomeserverURL.String()
	b.userID = hungry.UserID

	if err := b.store.SetJSON(ctx, kvRegistration, b.reg); err != nil {
		return err
	}
	if err := b.store.SetKV(ctx, kvHomeserverURL, b.hsURL); err != nil {
		return err
	}
	if err := b.store.SetKV(ctx, kvUserID, b.userID.String()); err != nil {
		return err
	}
	if err := b.store.SetKV(ctx, kvUsername, b.username); err != nil {
		return err
	}
	b.log.Info().Str("bridge", b.conf().Network.Bridge).Str("id", b.reg.ID).
		Str("user", b.userID.String()).Msg("Registered appservice")
	// bbctl posts RUNNING for a register-created (typeless) bridge; STARTING is
	// for typed bridges that still have to log in, and it never clears on its
	// own.
	b.postBridgeState(ctx, status.StateRunning, "SELF_HOST_REGISTERED")
	return nil
}

// postBridgeState tells Beeper the bridge is alive, which is what keeps it
// from being shown as broken in the client's bridge list.
func (b *Bridge) postBridgeState(ctx context.Context, state status.BridgeStateEvent, reason string) {
	if b.reg == nil || b.username == "" {
		return
	}
	err := beeperapi.PostBridgeState(b.conf().Matrix.BaseDomain, b.username, b.conf().Network.Bridge, b.reg.AppToken,
		beeperapi.ReqPostBridgeState{
			StateEvent:   state,
			Reason:       reason,
			IsSelfHosted: true,
		})
	if err != nil {
		b.log.Debug().Err(err).Msg("Failed to post bridge state")
	}
	b.postLoginState(ctx)
}

// loginStateTTL is how long Beeper trusts a login state before it wants to
// hear from the bridge again; mautrix bridges refresh at half the TTL.
const loginStateTTL = time.Hour

const (
	// pingInterval is how often we send a `ping` command over the appservice
	// websocket. hungryserv closes a socket that has been silent for around
	// five minutes, so a bridge that never pings reconnects all day;
	// bbctl-generated bridge configs default to ping_interval_seconds: 180,
	// and that is where this number comes from.
	pingInterval = 180 * time.Second
	// pingTimeout bounds one ping. A server that has not answered in this
	// long is not going to; tearing the socket down is how the reconnect
	// loop finds out.
	pingTimeout = 10 * time.Second
	// outageNoticeAfter is the shortest gap worth a notice in the bot room.
	outageNoticeAfter = 60 * time.Second
)

// wsPing is the payload of the `ping` websocket command, matching what
// mautrix bridges send (bridgev2/matrix/websocket.go).
type wsPing struct {
	Timestamp int64 `json:"timestamp"`
}

// pingLoop keeps the appservice websocket alive. Without it hungryserv drops
// the connection as idle every few minutes: harmless in itself, because the
// websocket loop reconnects, but it means a steady drip of reconnects and
// every to-device subscriber has to re-subscribe after each one.
func (b *Bridge) pingLoop(ctx context.Context, interval, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// b.connected, not as.HasWebsocket(): mautrix keeps that flag in
			// an unsynchronised field that the websocket goroutine writes as
			// it connects, and this loop reads it from another goroutine.
			// The bridge already tracks the same thing under its own lock.
			if b.as == nil || !b.Connected() {
				continue
			}
			pingCtx, cancel := context.WithTimeout(ctx, timeout)
			start := time.Now()
			var resp wsPing
			err := b.as.RequestWebsocket(pingCtx, &appservice.WebsocketRequest{
				Command: "ping",
				Data:    &wsPing{Timestamp: start.UnixMilli()},
			}, &resp)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				b.log.Warn().Err(err).Dur("duration", time.Since(start)).
					Msg("Websocket ping failed, reconnecting")
				b.as.StopWebsocket(fmt.Errorf("websocket ping failed: %w", err))
				continue
			}
			b.log.Debug().Dur("duration", time.Since(start)).Msg("Websocket ping ok")
		}
	}
}

// postLoginState is the second half of "being a network": Beeper's sidebar
// lists a bridge's ACCOUNTS (logins), each described by a bridge state that
// carries remote_id / remote_name / remote_profile. A bridge with only the
// bridge-level RUNNING is connected but has no account to show, so the
// network stays out of the list. This bridge has exactly one "login": the
// network itself.
func (b *Bridge) postLoginState(ctx context.Context) {
	if b.reg == nil || b.username == "" {
		return
	}
	cfg := b.conf()
	state := &status.BridgeState{
		StateEvent: status.StateConnected,
		Timestamp:  jsontime.UnixNow(),
		TTL:        int(loginStateTTL.Seconds()),
		Source:     "bridge",
		UserID:     b.userID,
		RemoteID:   networkid.UserLoginID(cfg.Network.ID),
		RemoteName: cfg.Network.Name,
		RemoteProfile: status.RemoteProfile{
			Name:     cfg.Network.Name,
			Username: cfg.Network.ID,
		},
		Info: map[string]any{"is_self_hosted": true},
	}
	if avatar, err := b.store.GetKV(ctx, kvNetworkAvatar); err == nil && avatar != "" {
		state.RemoteProfile.Avatar = id.ContentURIString(avatar)
	}
	// Over the appservice websocket, the way every mautrix bridge on Beeper
	// does it: the REST endpoint bbctl uses only knows bridge-level states
	// and rejects CONNECTED ("Unknown state CONNECTED"); per-login states are
	// `bridge_status` websocket commands, and they are what fills the
	// account list.
	if b.as == nil || !b.Connected() {
		return
	}
	if err := b.as.SendWebsocket(ctx, &appservice.WebsocketRequest{Command: "bridge_status", Data: state}); err != nil {
		b.log.Debug().Err(err).Msg("Failed to send login state")
	}
}

// loginStateLoop refreshes the login state while connected, so the account
// never ages out of the sidebar.
func (b *Bridge) loginStateLoop(ctx context.Context) {
	ticker := time.NewTicker(loginStateTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if b.Connected() {
				b.postLoginState(ctx)
			}
		}
	}
}

// --- identity helpers ------------------------------------------------------

// GhostMXID is the MXID of a config ghost key. The namespace is exclusive, so
// any localpart with the bridge prefix is ours.
func (b *Bridge) GhostMXID(key string) id.UserID {
	return id.NewUserID(fmt.Sprintf("%s_%s", b.conf().Network.Bridge, key), b.conf().Matrix.HomeserverDomain)
}

// BotMXID is the bridge bot: the sender of room state, and the only sender
// whose m.notice renders as dim centred text with no bubble. That style works
// in ANY room, not only a bridge-bot room (measured 2026-09-20: the same
// notice from a ghost is an ordinary bubble, from the bot it is centred — in a
// plain dm portal with no com.beeper.is_bridge_bot_room flag). It is empty
// until registration has happened, which is observable through the admin API
// while the bridge is still trying to connect.
func (b *Bridge) BotMXID() id.UserID {
	if b.reg == nil {
		return ""
	}
	return id.NewUserID(b.reg.SenderLocalpart, b.conf().Matrix.HomeserverDomain)
}

// UserID is the account owner.
func (b *Bridge) UserID() id.UserID { return b.userID }

// Intent returns an appservice intent for a ghost key.
func (b *Bridge) Intent(key string) *appservice.IntentAPI {
	if key == BotKey {
		return b.BotIntent()
	}
	return b.as.Intent(b.GhostMXID(key))
}

// BotKey is the ghost key that means the bridge itself rather than one of its
// ghosts. Every send path takes a ghost key, and this is how a caller says
// "this is the bridge speaking, not the network".
const BotKey = ""

// BotIntent returns the bridge bot's intent.
func (b *Bridge) BotIntent() *appservice.IntentAPI { return b.as.BotIntent() }

// IsOwnGhost reports whether an MXID belongs to this bridge.
func (b *Bridge) IsOwnGhost(user id.UserID) bool {
	return strings.HasPrefix(user.String(), "@"+b.conf().Network.Bridge+"_") ||
		user == b.BotMXID()
}

// RoomID looks up a configured room.
func (b *Bridge) RoomID(key string) (id.RoomID, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	roomID, ok := b.roomIDs[key]
	return roomID, ok
}

// RoomKey is the reverse: which configured room an event arrived in.
func (b *Bridge) RoomKey(roomID id.RoomID) (string, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	key, ok := b.roomKey[roomID]
	return key, ok
}

// RoomIDs returns a copy of the whole map, for status output.
func (b *Bridge) RoomIDs() map[string]id.RoomID {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[string]id.RoomID, len(b.roomIDs))
	for k, v := range b.roomIDs {
		out[k] = v
	}
	return out
}

// Connected reports websocket state for the health endpoint.
func (b *Bridge) Connected() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.connected
}

// Registration exposes the registration for status output. The tokens are not
// included in anything that leaves the process.
func (b *Bridge) Registration() *appservice.Registration { return b.reg }

// --- media -----------------------------------------------------------------

// UploadAvatar uploads an image from the config directory and returns its mxc
// URI, cached so a restart does not re-upload it. The notice profile needs the
// same thing a ghost does.
func (b *Bridge) UploadAvatar(ctx context.Context, path string) (string, error) {
	uri, err := b.uploadFile(ctx, path)
	if err != nil || uri.IsEmpty() {
		return "", err
	}
	return uri.String(), nil
}

// uploadFile uploads a local file once and caches the mxc URI: avatars are
// re-uploaded on every start otherwise, and each upload is a new mxc, which
// makes the client re-download them.
func (b *Bridge) uploadFile(ctx context.Context, path string) (id.ContentURI, error) {
	if path == "" {
		return id.ContentURI{}, nil
	}
	full := path
	if !filepath.IsAbs(full) {
		full = filepath.Join(b.conf().Dir, path)
	}
	data, err := os.ReadFile(full)
	if err != nil {
		return id.ContentURI{}, fmt.Errorf("read %s: %w", full, err)
	}
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])

	if cached, err := b.store.GetKV(ctx, kvAvatarPrefix+hash); err == nil && cached != "" {
		return id.ParseContentURI(cached)
	}
	mime := http.DetectContentType(data)
	resp, err := b.BotIntent().UploadMedia(ctx, mautrix.ReqUploadMedia{
		ContentBytes:  data,
		ContentType:   mime,
		FileName:      filepath.Base(full),
		ContentLength: int64(len(data)),
	})
	if err != nil {
		return id.ContentURI{}, fmt.Errorf("upload %s: %w", full, err)
	}
	_ = b.store.SetKV(ctx, kvAvatarPrefix+hash, resp.ContentURI.String())
	return resp.ContentURI, nil
}

// hashOf is the drift detector for room and ghost state: everything that
// would have to be patched, hashed into one string stored next to the room.
func hashOf(parts ...any) string {
	raw, _ := json.Marshal(parts)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:8])
}
