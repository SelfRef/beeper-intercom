// Package mcpserver exposes the bridge to agents over MCP.
//
// This is the loop closing: an agent that was reached through a room can also
// speak into one. "Remind me at 18:00" becomes a wait plus one tool call;
// "restart X?" becomes a poll the agent waits on rather than a decision it
// takes alone.
//
// Tools are named so a hub can split them into read-only and read-write sets:
// everything that only reads is list_*/get_*, everything that writes says so.
package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/SelfRef/beeper-intercom/internal/bridge"
	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/notify"
	"github.com/SelfRef/beeper-intercom/internal/service"
	"github.com/SelfRef/beeper-intercom/internal/store"
)

// Handler builds the streamable-HTTP MCP endpoint.
func Handler(svc *service.Service, version string) http.Handler {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "beeper-intercom",
		Title:   "Intercom",
		Version: version,
	}, nil)
	register(server, svc)
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{
		// One process, one set of rooms, no per-client state worth keeping:
		// a stateless endpoint is what a hub in front of this wants anyway.
		Stateless:    true,
		JSONResponse: true,
	})
}

// --- reads -----------------------------------------------------------------

type emptyInput struct{}

type roomInfo struct {
	Key    string   `json:"key"`
	Name   string   `json:"name"`
	Kind   string   `json:"kind"`
	Ghosts []string `json:"ghosts"`
	Agent  string   `json:"agent,omitempty"`
	RoomID string   `json:"room_id,omitempty"`
}

type ghostInfo struct {
	Key  string `json:"key"`
	Name string `json:"name"`
	MXID string `json:"mxid"`
}

// The MCP spec requires a tool's outputSchema to be an object schema, and the
// Go SDK derives it from the result type, so a bare slice produces
// "type": ["null", "array"] and a strict client (MCPHub) rejects the WHOLE
// tools/list response. Every list tool therefore returns a one-field wrapper.
type roomList struct {
	Rooms []roomInfo `json:"rooms"`
}

type ghostList struct {
	Ghosts []ghostInfo `json:"ghosts"`
}

type sessionList struct {
	Sessions []*store.Session `json:"sessions"`
}

type notificationList struct {
	Notifications []*store.Notification `json:"notifications"`
}

type listSessionsInput struct {
	IncludeClosed bool `json:"include_closed,omitempty" jsonschema:"include conversations that have already been closed"`
	Limit         int  `json:"limit,omitempty" jsonschema:"maximum number of sessions to return"`
}

type getSessionInput struct {
	Room   string `json:"room" jsonschema:"room key"`
	Thread string `json:"thread,omitempty" jsonschema:"thread root event ID; omit for the main timeline"`
}

type listNotificationsInput struct {
	Room  string `json:"room,omitempty" jsonschema:"only this room"`
	Limit int    `json:"limit,omitempty" jsonschema:"maximum number to return"`
}

type testRenderInput struct {
	Text  string   `json:"text" jsonschema:"Markdown to render"`
	Title string   `json:"title,omitempty" jsonschema:"optional bold first line"`
	URL   string   `json:"url,omitempty" jsonschema:"optional trailing link"`
	Tags  []string `json:"tags,omitempty" jsonschema:"ntfy-style tags; known ones become emoji"`
}

type renderedOutput struct {
	Body      string `json:"body"`
	Formatted string `json:"formatted_body"`
}

// --- writes ----------------------------------------------------------------

type sendNotificationInput struct {
	Room     string            `json:"room" jsonschema:"room key"`
	Ghost    string            `json:"ghost,omitempty" jsonschema:"ghost key; defaults to the room's first ghost"`
	Title    string            `json:"title,omitempty"`
	Text     string            `json:"text" jsonschema:"message body, Markdown"`
	Priority string            `json:"priority,omitempty" jsonschema:"min|low|default|high|urgent"`
	URL      string            `json:"url,omitempty" jsonschema:"click-through link"`
	Tags     []string          `json:"tags,omitempty"`
	Thread   string            `json:"thread,omitempty" jsonschema:"event ID or kind:id of an earlier notification"`
	Dedupe   string            `json:"dedupe,omitempty" jsonschema:"idempotency key"`
	Actions  map[string]string `json:"actions,omitempty" jsonschema:"reaction key to action name"`
}

type sentOutput struct {
	EventID   string `json:"event_id"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

type askConfirmationInput struct {
	Room        string   `json:"room" jsonschema:"room key"`
	Ghost       string   `json:"ghost,omitempty"`
	Question    string   `json:"question" jsonschema:"what to confirm"`
	Answers     []string `json:"answers,omitempty" jsonschema:"choices; defaults to Yes and No"`
	Thread      string   `json:"thread,omitempty"`
	WaitSeconds int      `json:"wait_seconds,omitempty" jsonschema:"how long to wait for an answer; 0 returns immediately"`
}

type confirmationOutput struct {
	EventID string `json:"event_id"`
	Answer  string `json:"answer,omitempty"`
	Timeout bool   `json:"timeout,omitempty"`
}

type newConversationInput struct {
	Room string `json:"room" jsonschema:"room key"`
}

type setRoomMutedInput struct {
	Room  string `json:"room" jsonschema:"room key"`
	Muted bool   `json:"muted"`
}

type okOutput struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

func register(server *mcp.Server, svc *service.Service) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_rooms",
		Description: "List the rooms this bridge owns, with their kind, ghosts and agent.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, roomList, error) {
		cfg := svc.Config()
		ids := svc.Bridge().RoomIDs()
		out := make([]roomInfo, 0, len(cfg.Rooms))
		for _, key := range config.SortedKeys(cfg.Rooms) {
			room := cfg.Rooms[key]
			out = append(out, roomInfo{
				Key: key, Name: room.Name, Kind: room.Kind,
				Ghosts: room.Ghosts, Agent: room.Agent, RoomID: ids[key].String(),
			})
		}
		return nil, roomList{Rooms: out}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_ghosts",
		Description: "List the identities that can speak in this bridge's rooms.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, ghostList, error) {
		cfg := svc.Config()
		out := make([]ghostInfo, 0, len(cfg.Ghosts))
		for _, key := range config.SortedKeys(cfg.Ghosts) {
			out = append(out, ghostInfo{Key: key, Name: cfg.Ghosts[key].Name, MXID: svc.Bridge().GhostMXID(key).String()})
		}
		return nil, ghostList{Ghosts: out}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_sessions",
		Description: "List conversations, newest activity first.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in listSessionsInput) (*mcp.CallToolResult, sessionList, error) {
		sessions, err := svc.Store().ListSessions(ctx, in.IncludeClosed, in.Limit)
		return nil, sessionList{Sessions: sessions}, err
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_session",
		Description: "The live conversation in a room or thread, if there is one.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in getSessionInput) (*mcp.CallToolResult, *store.Session, error) {
		sess, err := liveSession(ctx, svc, in)
		return nil, sess, err
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_recent_notifications",
		Description: "Recent announcements, with their source payloads and declared actions.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in listNotificationsInput) (*mcp.CallToolResult, notificationList, error) {
		notifications, err := svc.Store().RecentNotifications(ctx, in.Room, in.Limit)
		return nil, notificationList{Notifications: notifications}, err
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "test_render",
		Description: "Dry run: show exactly what a notification would look like in a room, " +
			"without sending it. Useful before sending something long or formatted.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in testRenderInput) (*mcp.CallToolResult, renderedOutput, error) {
		n := &notify.Notification{Title: in.Title, Text: in.Text, URL: in.URL, Tags: in.Tags}
		rendered := n.Render(bridge.Markdown)
		return nil, renderedOutput{Body: rendered.Body, Formatted: rendered.Formatted}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "send_notification",
		Description: "Post an announcement into one of the bridge's rooms, as one of its ghosts. " +
			"Use actions to declare which reactions on it should fire an action.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in sendNotificationInput) (*mcp.CallToolResult, sentOutput, error) {
		n := &notify.Notification{
			Room: in.Room, Ghost: in.Ghost, Title: in.Title, Text: in.Text,
			Priority: in.Priority, URL: in.URL, Tags: in.Tags, Thread: in.Thread,
			Dedupe: in.Dedupe, Actions: in.Actions,
			Source: &notify.Source{Kind: "mcp"},
		}
		eventID, duplicate, err := svc.Notify(ctx, n)
		if err != nil {
			return nil, sentOutput{}, err
		}
		return nil, sentOutput{EventID: eventID.String(), Duplicate: duplicate}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "send_reply",
		Description: "Post into an existing thread, given the event ID of its root.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in struct {
		Room   string `json:"room"`
		Ghost  string `json:"ghost,omitempty"`
		Text   string `json:"text"`
		Thread string `json:"thread" jsonschema:"event ID or kind:id of the message to reply under"`
	}) (*mcp.CallToolResult, sentOutput, error) {
		eventID, err := svc.Reply(ctx, in.Room, in.Ghost, in.Text, in.Thread)
		if err != nil {
			return nil, sentOutput{}, err
		}
		return nil, sentOutput{EventID: eventID.String()}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "ask_confirmation",
		Description: "Ask a yes/no (or multiple choice) question as a poll and, if wait_seconds is set, " +
			"block until it is answered. This is how an irreversible action gets a human decision.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in askConfirmationInput) (*mcp.CallToolResult, confirmationOutput, error) {
		wait := time.Duration(in.WaitSeconds) * time.Second
		source, _ := json.Marshal(map[string]any{"kind": "mcp", "question": in.Question})
		eventID, answer, err := svc.AskPoll(ctx, in.Room, in.Ghost, in.Question, in.Answers, in.Thread, source, wait)
		if err != nil && eventID == "" {
			return nil, confirmationOutput{}, err
		}
		return nil, confirmationOutput{EventID: eventID.String(), Answer: answer, Timeout: err != nil}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "new_conversation",
		Description: "Close the live conversation in a room so the next message starts a fresh one.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in newConversationInput) (*mcp.CallToolResult, okOutput, error) {
		live, err := svc.Store().LiveSession(ctx, in.Room, "")
		if err != nil {
			return nil, okOutput{}, err
		}
		if live == nil {
			return nil, okOutput{OK: true, Message: "no live conversation; the next message starts one"}, nil
		}
		if err := svc.Store().CloseSession(ctx, live.ID); err != nil {
			return nil, okOutput{}, err
		}
		return nil, okOutput{OK: true, Message: fmt.Sprintf("closed session #%d", live.ID)}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "set_room_muted",
		Description: "Mute or unmute one of the bridge's rooms for the account owner.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in setRoomMutedInput) (*mcp.CallToolResult, okOutput, error) {
		if err := svc.SetRoomMuted(ctx, in.Room, in.Muted); err != nil {
			return nil, okOutput{}, err
		}
		state := "unmuted"
		if in.Muted {
			state = "muted"
		}
		return nil, okOutput{OK: true, Message: in.Room + " is now " + state}, nil
	})
}

func liveSession(ctx context.Context, svc *service.Service, in getSessionInput) (*store.Session, error) {
	sess, err := svc.Store().LiveSession(ctx, in.Room, in.Thread)
	if err != nil {
		return nil, err
	}
	if sess == nil {
		return nil, fmt.Errorf("no live conversation in %q", in.Room)
	}
	return sess, nil
}
