package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// A minimal Engine.IO v4 / Socket.IO client, websocket transport only —
// exactly what Open WebUI serves at /ws/socket.io and nothing more.
//
// Open WebUI emits a running turn's deltas only here: `stream: true` against a
// saved chat returns `null` over HTTP, and the response-stream overlay on
// GET /api/v1/chats/{id} never sees the running task (0.11.3 keys the saves
// by a task id it never puts into the request metadata). So live streaming
// means joining the user's socket room and reading `events`.
//
// Wire format, for the reader who has to debug it:
//
//	server → "0{...}"                 open (sid, pingInterval, pingTimeout)
//	client → "40{"token":"…"}"        connect to "/" with auth
//	server → "40{"sid":"…"}"          connected   |  "44{...}" refused
//	client → "42["user-join",{...}]"  Open WebUI's own join (idempotent)
//	server → "2"   client → "3"       ping / pong
//	server → "42["events",{...}]"     an event
type owuiSocket struct {
	conn *websocket.Conn
}

// dialOWUISocket connects and authenticates. The token must be a session
// JWT: the socket path verifies it with the session secret, so an API key
// is refused.
func dialOWUISocket(ctx context.Context, baseURL, token string) (*owuiSocket, error) {
	parsed, err := url.Parse(strings.TrimSuffix(baseURL, "/"))
	if err != nil {
		return nil, err
	}
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/") + "/ws/socket.io/"
	parsed.RawQuery = "EIO=4&transport=websocket"

	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dialCtx, parsed.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("socket.io dial: %w", err)
	}
	conn.SetReadLimit(16 << 20)
	s := &owuiSocket{conn: conn}

	// open packet
	if _, err := s.expect(dialCtx, "0"); err != nil {
		s.Close()
		return nil, err
	}
	auth, _ := json.Marshal(map[string]string{"token": token})
	if err := s.write(dialCtx, "40"+string(auth)); err != nil {
		s.Close()
		return nil, err
	}
	if msg, err := s.expect(dialCtx, "4"); err != nil {
		s.Close()
		return nil, err
	} else if strings.HasPrefix(msg, "44") {
		s.Close()
		return nil, fmt.Errorf("socket.io connect refused: %s", strings.TrimPrefix(msg, "44"))
	}
	join, _ := json.Marshal([]any{"user-join", map[string]any{"auth": map[string]string{"token": token}}})
	if err := s.write(dialCtx, "42"+string(join)); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// expect reads until a packet with the given prefix arrives, answering pings
// on the way.
func (s *owuiSocket) expect(ctx context.Context, prefix string) (string, error) {
	for {
		msg, err := s.read(ctx)
		if err != nil {
			return "", err
		}
		if msg == "2" {
			_ = s.write(ctx, "3")
			continue
		}
		if strings.HasPrefix(msg, prefix) {
			return msg, nil
		}
	}
}

// events delivers every Socket.IO event to fn until fn returns false, the
// connection drops, or ctx ends. Pings are answered here.
func (s *owuiSocket) events(ctx context.Context, fn func(name string, payload json.RawMessage) bool) error {
	for {
		msg, err := s.read(ctx)
		if err != nil {
			return err
		}
		switch {
		case msg == "2":
			if err := s.write(ctx, "3"); err != nil {
				return err
			}
		case strings.HasPrefix(msg, "42"):
			var packet []json.RawMessage
			if err := json.Unmarshal([]byte(msg[2:]), &packet); err != nil || len(packet) < 1 {
				continue
			}
			var name string
			if err := json.Unmarshal(packet[0], &name); err != nil {
				continue
			}
			var payload json.RawMessage
			if len(packet) > 1 {
				payload = packet[1]
			}
			if !fn(name, payload) {
				return nil
			}
		case strings.HasPrefix(msg, "41"), msg == "1":
			return fmt.Errorf("socket.io disconnected by server")
		}
	}
}

func (s *owuiSocket) read(ctx context.Context) (string, error) {
	_, data, err := s.conn.Read(ctx)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (s *owuiSocket) write(ctx context.Context, msg string) error {
	return s.conn.Write(ctx, websocket.MessageText, []byte(msg))
}

func (s *owuiSocket) Close() {
	_ = s.conn.Close(websocket.StatusNormalClosure, "")
}

// owuiEvent is the envelope Open WebUI emits on "events".
type owuiEvent struct {
	ChatID    string `json:"chat_id"`
	MessageID string `json:"message_id"`
	Data      struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	} `json:"data"`
}

// owuiDelta is what interests us inside a response:completion or
// chat:completion event.
// owuiOutputItem is one entry of a message's output array.
type owuiOutputItem struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	Arguments string `json:"arguments"`
}

type owuiDelta struct {
	Type    string          `json:"type"`  // response.output_text.delta, response.reasoning_text.delta, …
	Delta   string          `json:"delta"` // for *.delta
	Done    bool            `json:"done"`  // chat:completion terminal event
	Content json.RawMessage `json:"content"`
	Error   json.RawMessage `json:"error"`
	// Item carries the tool call on response.output_item.added/done. Measured
	// against 0.11.3 on 2026-09-20: item.type is "function_call" and item.name
	// is the tool, e.g. "mcphub_time-get_current_time".
	Item struct {
		Type string `json:"type"`
		Name string `json:"name"`
	} `json:"item"`
	// Output is the whole message so far, on chat:completion. It is the only
	// place a STAGED tool call shows up — ask_user ends the turn without
	// done:true, so this is what says the turn is over (see askuser.go).
	Output []owuiOutputItem `json:"output"`
	// Usage rides on the terminal chat:completion event, straight from
	// llama.cpp: prompt_per_second is prefill, predicted_per_second decode.
	Usage *struct {
		PromptTokens        int     `json:"prompt_tokens"`
		CompletionTokens    int     `json:"completion_tokens"`
		PromptPerSecond     float64 `json:"prompt_per_second"`
		PredictedPerSecond  float64 `json:"predicted_per_second"`
		PromptTokensDetails struct {
			CachedTokens int `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
}
