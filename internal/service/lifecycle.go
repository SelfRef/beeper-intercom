package service

import (
	"context"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/bridge"
	"github.com/SelfRef/beeper-intercom/internal/config"
)

// /bridge: operating the bridge from the room it bridges.
//
// It exists because the alternative is a shell on the host, and the whole
// point of this thing is that the room is the interface. Its two options are
// the two halves of one question — `reload` re-reads the config file into the
// running process, `restart` replaces the process, which is the only way to
// pick up a new binary, a changed registration, or the config keys a reload
// refuses to touch.

// kvRestartRequest remembers where a /bridge restart was asked for, so the process
// that comes back can say so in the room that asked rather than only in the
// log. It is a single key: a restart takes the whole process with it, so there
// can only ever be one outstanding.
const kvRestartRequest = "restart_request"

// restartRequest is what survives the process.
type restartRequest struct {
	RoomKey    string `json:"room"`
	ThreadRoot string `json:"thread,omitempty"`
	Command    string `json:"command,omitempty"`
	Ghost      string `json:"ghost,omitempty"`
	At         int64  `json:"at"`
}

// commandBridge is the one command; the option says which half.
func (s *Service) commandBridge(ctx context.Context, msg *bridge.Message, room config.Room, args string) string {
	switch strings.ToLower(strings.TrimSpace(args)) {
	case "reload":
		return s.commandReload(ctx, msg, room)
	case "restart":
		return s.commandRestart(ctx, msg, room)
	case "":
		return table([]string{"`/bridge`", "What it does"}, [][]string{
			{"`reload`", "re-read the config file; rooms reconcile, conversations carry on"},
			{"`restart`", "replace the process — for a new binary, or what a reload cannot touch"},
		})
	default:
		return fmt.Sprintf("`/bridge` takes %s or %s.", code("reload"), code("restart"))
	}
}

// commandReload re-reads the config file. Rooms reconcile, ghosts refresh,
// conversations carry on: a reload is not meant to be felt.
func (s *Service) commandReload(ctx context.Context, msg *bridge.Message, room config.Room) string {
	if err := s.Reload(ctx); err != nil {
		// Reload is all-or-nothing — a config that does not load leaves the
		// running one in place, so this is a report, not a warning.
		return colour(colourBad, "Reload failed") + ", keeping the configuration that is running: " + err.Error()
	}
	cfg := s.conf()
	return table([]string{"Reloaded", ""}, [][]string{
		{"Rooms", fmt.Sprintf("%d", len(cfg.Rooms))},
		{"Ghosts", fmt.Sprintf("%d", len(cfg.Ghosts))},
		{"Agents", fmt.Sprintf("%d", len(cfg.Agents))},
	})
}

// commandRestart replaces the process.
//
// There is nothing to restart the bridge from inside the bridge, so it stops
// itself the way anything else would: the same signal the container runtime
// sends, handled by the same graceful shutdown. What brings it back is the
// restart policy of whatever is supervising it (`restart: unless-stopped` in
// the compose file) — run it bare and it simply stops.
func (s *Service) commandRestart(ctx context.Context, msg *bridge.Message, room config.Room) string {
	// The notice goes out before the signal does: afterwards there is nothing
	// left to send it with. Posting it here rather than returning it is what
	// makes that ordering certain.
	s.postNotice(ctx, msg, room, colour(colourWarn, "Restarting")+" — back in a moment.")
	s.addClearButton(ctx, msg, room)

	ghostKey := ""
	if len(room.Ghosts) > 0 {
		ghostKey = room.Ghosts[0]
	}
	if err := s.store.SetJSON(ctx, kvRestartRequest, restartRequest{
		RoomKey:    msg.RoomKey,
		ThreadRoot: msg.ThreadRoot.String(),
		Command:    msg.EventID.String(),
		Ghost:      ghostKey,
		At:         time.Now().UnixMilli(),
	}); err != nil {
		s.log.Warn().Err(err).Msg("Could not record the restart request")
	}

	s.log.Info().Str("room", msg.RoomKey).Msg("Restarting on request")
	go func() {
		// A moment for this handler to return and for the homeserver to have
		// the notice; the shutdown itself is graceful either way.
		time.Sleep(250 * time.Millisecond)
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			s.log.Error().Err(err).Msg("Could not signal myself to restart")
		}
	}()
	return ""
}

// announceRestart closes the loop: the process that comes back says so in the
// room that asked for it. Called once the bridge is ready, because a notice
// sent before the rooms are reconciled has nowhere to go.
func (s *Service) announceRestart(ctx context.Context) {
	var request restartRequest
	found, err := s.store.GetJSON(ctx, kvRestartRequest, &request)
	if err != nil || !found || request.RoomKey == "" {
		return
	}
	// Cleared first: a request that cannot be announced is still spent, and
	// announcing it on every start afterwards would be worse than silence.
	if err := s.store.SetKV(ctx, kvRestartRequest, ""); err != nil {
		s.log.Debug().Err(err).Msg("Could not clear the restart request")
	}
	roomID, ok := s.bridge.RoomID(request.RoomKey)
	if !ok {
		return
	}
	took := time.Since(time.UnixMilli(request.At)).Round(100 * time.Millisecond)
	text := fmt.Sprintf("%s — %s, back in %s.", yes("Restarted"), code(s.Version), took)
	// Sent as an answer to the /bridge restart that asked for it, so the two are one
	// exchange: the delete button on that command takes both notices away.
	s.postNoticeReply(ctx, roomID, request.Ghost, id.EventID(request.ThreadRoot), id.EventID(request.Command), text)
}
