package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/rs/zerolog"

	"github.com/SelfRef/beeper-intercom/internal/config"
)

// A fake Open WebUI socket: Engine.IO open packet, connect ack, then the
// events a streaming turn produces. It pins the wire format the client
// speaks, which no other test exercises.
func fakeOWUISocket(t *testing.T, wantToken string, script func(send func(string))) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/ws/socket.io/") || r.URL.Query().Get("EIO") != "4" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		ctx := context.Background()
		send := func(msg string) { _ = conn.Write(ctx, websocket.MessageText, []byte(msg)) }
		read := func() string {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return ""
			}
			return string(data)
		}
		send(`0{"sid":"abc","upgrades":[],"pingInterval":25000,"pingTimeout":20000}`)
		connect := read()
		if !strings.HasPrefix(connect, "40") || !strings.Contains(connect, wantToken) {
			send(`44{"message":"bad token"}`)
			return
		}
		send(`40{"sid":"def"}`)
		if join := read(); !strings.Contains(join, "user-join") {
			t.Errorf("expected user-join, got %q", join)
		}
		// A ping must be answered or the server would drop us.
		send("2")
		if pong := read(); pong != "3" {
			t.Errorf("expected pong, got %q", pong)
		}
		script(send)
		time.Sleep(50 * time.Millisecond)
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
}

func TestOWUISocketHandshakeAndEvents(t *testing.T) {
	server := httptest.NewServer(fakeOWUISocket(t, "jwt-1", func(send func(string)) {
		send(`42["events",{"chat_id":"c","message_id":"m","data":{"type":"response:completion","data":{"type":"response.output_text.delta","delta":"Hel"}}}]`)
		send(`42["events",{"chat_id":"c","message_id":"m","data":{"type":"response:completion","data":{"type":"response.output_text.delta","delta":"lo"}}}]`)
		send(`42["events",{"chat_id":"c","message_id":"m","data":{"type":"chat:completion","data":{"done":true}}}]`)
	}))
	defer server.Close()

	sock, err := dialOWUISocket(context.Background(), server.URL, "jwt-1")
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close()

	var names []string
	var deltas []string
	err = sock.events(context.Background(), func(name string, payload json.RawMessage) bool {
		names = append(names, name)
		var evt owuiEvent
		_ = json.Unmarshal(payload, &evt)
		var inner owuiDelta
		_ = json.Unmarshal(evt.Data.Data, &inner)
		if inner.Delta != "" {
			deltas = append(deltas, inner.Delta)
		}
		return !inner.Done
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(deltas, "") != "Hello" || len(names) != 3 {
		t.Errorf("deltas = %q names = %q", deltas, names)
	}
}

func TestOWUISocketRefusesBadToken(t *testing.T) {
	server := httptest.NewServer(fakeOWUISocket(t, "jwt-1", func(func(string)) {}))
	defer server.Close()
	if _, err := dialOWUISocket(context.Background(), server.URL, "wrong"); err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("expected a refused connect, got %v", err)
	}
}

// The whole adapter path over the socket: the completions request returns
// null (as Open WebUI does), the deltas arrive on the socket, and the stored
// message is read back as the authoritative answer.
func TestOpenWebUIStreamsViaSocket(t *testing.T) {
	// The completions handler, the socket script and the chat read all touch
	// the assistant id from different goroutines, so it needs a lock of its
	// own — the race detector is right about that even in a fake.
	var idMu sync.Mutex
	var assistantID string
	setAssistantID := func(id string) {
		idMu.Lock()
		assistantID = id
		idMu.Unlock()
	}
	getAssistantID := func() string {
		idMu.Lock()
		defer idMu.Unlock()
		return assistantID
	}
	socket := fakeOWUISocket(t, "jwt-1", func(send func(string)) {
		var assistantID string
		for i := 0; i < 20; i++ {
			if assistantID = getAssistantID(); assistantID != "" {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		for _, piece := range []string{"GRA", "NITE"} {
			send(`42["events",{"chat_id":"chat-1","message_id":"` + assistantID + `","data":{"type":"response:completion","data":{"type":"response.output_text.delta","delta":"` + piece + `"}}}]`)
		}
		send(`42["events",{"chat_id":"chat-1","message_id":"` + assistantID + `","data":{"type":"chat:completion","data":{"done":true}}}]`)
	})
	mux := http.NewServeMux()
	mux.Handle("/ws/socket.io/", socket)
	mux.HandleFunc("/api/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = decodeJSON(r, &body)
		id, _ := body["id"].(string)
		setAssistantID(id)
		time.Sleep(150 * time.Millisecond)
		_, _ = w.Write([]byte("null"))
	})
	mux.HandleFunc("/api/v1/chats/chat-1", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"chat":{"history":{"messages":{"` + getAssistantID() + `":{"content":"GRANITE","done":true}}}}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	t.Setenv("OWUI_TEST_SOCKET", "jwt-1")
	adapter := newOpenWebUI(config.Agent{URL: server.URL, Model: "m", SocketTokenEnv: "OWUI_TEST_SOCKET"}, server.Client(), zerolog.Nop())
	var deltas []string
	reply, err := adapter.Send(context.Background(), Conversation{ID: "chat-1"}, Turn{Text: "codeword?"}, Sink{
		Delta: func(text string) { deltas = append(deltas, text) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(deltas, "") != "GRANITE" || reply.Text != "GRANITE" || reply.Parent != assistantID {
		t.Errorf("deltas = %q reply = %+v", deltas, reply)
	}
}
