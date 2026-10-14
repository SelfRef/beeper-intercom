package service

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/config"
)

// Rooms with `watch: true` report their events to an event hook in the
// format beeper-watch takes from its patched bridges: one JSON object per
// message, edit, reaction or deletion, in both directions. A rule written for
// a bridged chat can then be tried on a room this bridge owns.
//
// Two sources, because an appservice is never sent its own ghosts' events:
// what I (or anything acting as me) do in the room arrives through the bridge
// as raw events, and what a ghost says is reported where the bridge posts it.

// watchEvent is one Matrix event as the hook wants it, or nil for an event
// type the hook has no kind for.
func watchEvent(network config.Network, owner id.UserID, evt *event.Event) map[string]any {
	out := map[string]any{
		"schema":    1,
		"bridge":    network.Bridge,
		"network":   network.ID,
		"direction": "in",
		"room_id":   evt.RoomID.String(),
		"event_id":  evt.ID.String(),
		"sender":    evt.Sender.String(),
		"is_self":   evt.Sender == owner,
		"timestamp": evt.Timestamp,
		"type":      evt.Type.Type,
	}
	if evt.Sender == owner {
		out["direction"] = "out"
	}
	switch evt.Type {
	case event.EventMessage:
		content, ok := evt.Content.Parsed.(*event.MessageEventContent)
		if !ok || content == nil {
			return nil
		}
		out["kind"] = "message"
		out["msgtype"] = string(content.MsgType)
		out["body"] = content.Body
		if rel := content.RelatesTo; rel != nil {
			switch rel.Type {
			case event.RelReplace:
				out["kind"] = "edit"
				out["edits"] = rel.EventID.String()
				if content.NewContent != nil {
					out["body"] = content.NewContent.Body
				}
			case event.RelThread:
				out["thread_root"] = rel.EventID.String()
			}
			if rel.InReplyTo != nil {
				out["reply_to"] = rel.InReplyTo.EventID.String()
			}
		}
	case event.EventReaction:
		content, ok := evt.Content.Parsed.(*event.ReactionEventContent)
		if !ok || content == nil {
			return nil
		}
		out["kind"] = "reaction"
		out["reaction_key"] = content.RelatesTo.Key
	case event.EventRedaction:
		out["kind"] = "deletion"
		redacts := evt.Redacts
		if content, ok := evt.Content.Parsed.(*event.RedactionEventContent); ok && content != nil && content.Redacts != "" {
			redacts = content.Redacts
		}
		out["redacts"] = redacts.String()
	default:
		return nil
	}
	return out
}

// onRoomEvent reports an event that arrived in a watched room.
func (s *Service) onRoomEvent(_ context.Context, roomKey string, evt *event.Event) {
	cfg := s.conf()
	if !cfg.Rooms[roomKey].Watch {
		return
	}
	if payload := watchEvent(cfg.Network, s.bridge.UserID(), evt); payload != nil {
		s.sendWatch(payload)
	}
}

// reportPosted reports a message a ghost just posted into a watched room —
// the other party's side of the conversation.
func (s *Service) reportPosted(roomKey string, roomID id.RoomID, eventID id.EventID, ghostKey, body string) {
	cfg := s.conf()
	if !cfg.Rooms[roomKey].Watch || eventID == "" {
		return
	}
	s.sendWatch(map[string]any{
		"schema":           1,
		"bridge":           cfg.Network.Bridge,
		"network":          cfg.Network.ID,
		"direction":        "in",
		"kind":             "message",
		"room_id":          roomID.String(),
		"event_id":         eventID.String(),
		"sender":           s.bridge.GhostMXID(ghostKey).String(),
		"sender_remote_id": ghostKey,
		"is_self":          false,
		"timestamp":        time.Now().UnixMilli(),
		"type":             event.EventMessage.Type,
		"msgtype":          string(event.MsgText),
		"body":             body,
	})
}

// sendWatch posts one event in the background. Like the bridges' own hook it
// never holds up the room and does not retry: the receiver dedupes by event
// ID, and a missed test event is a re-run, not lost data.
func (s *Service) sendWatch(payload map[string]any) {
	cfg := s.conf().Watch
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(raw))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if token := cfg.Token(); token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			s.log.Warn().Err(err).Msg("Watch hook unreachable")
			return
		}
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			s.log.Warn().Int("status", resp.StatusCode).Msg("Watch hook refused an event")
		}
	}()
}
