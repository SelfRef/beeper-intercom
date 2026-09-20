package bridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/appservice"
)

// fakeHungryservWS is the server half of the appservice websocket: it accepts
// the connection and answers commands, recording the ones it was sent.
func fakeHungryservWS(t *testing.T, commands chan<- string, reply bool) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		ctx := context.Background()
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var cmd appservice.WebsocketCommand
			if json.Unmarshal(data, &cmd) != nil {
				continue
			}
			select {
			case commands <- cmd.Command:
			default:
			}
			if !reply || cmd.ReqID == 0 {
				continue
			}
			resp, _ := json.Marshal(appservice.WebsocketRequest{
				ReqID:   cmd.ReqID,
				Command: "response",
				Data:    json.RawMessage(`{"timestamp":1}`),
			})
			_ = conn.Write(ctx, websocket.MessageText, resp)
		}
	})
}

// startPingBridge wires a Bridge to a fake homeserver websocket and returns it
// with the channel of commands the server saw.
func startPingBridge(t *testing.T, reply bool) (*Bridge, chan string) {
	t.Helper()
	commands := make(chan string, 8)
	server := httptest.NewServer(fakeHungryservWS(t, commands, reply))
	t.Cleanup(server.Close)

	as, err := appservice.CreateFull(appservice.CreateOpts{
		Registration:     &appservice.Registration{ID: "test", AppToken: "as-token"},
		HomeserverDomain: "beeper.local",
		HomeserverURL:    server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	as.Log = zerolog.Nop()

	b := &Bridge{as: as, log: zerolog.Nop()}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = as.StartWebsocket(ctx, "", func() {}) }()
	for i := 0; i < 100 && !as.HasWebsocket(); i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if !as.HasWebsocket() {
		t.Fatal("websocket never connected")
	}
	go b.pingLoop(ctx, 20*time.Millisecond, 200*time.Millisecond)
	return b, commands
}

// hungryserv closes a socket that has been silent for a few minutes, so the
// bridge has to speak first. This is the frame that keeps it open; without it
// the bridge reconnected every five minutes all day.
func TestPingLoopPingsTheServer(t *testing.T) {
	_, commands := startPingBridge(t, true)
	deadline := time.After(2 * time.Second)
	for {
		select {
		case cmd := <-commands:
			if cmd == "ping" {
				return
			}
		case <-deadline:
			t.Fatal("no ping within 2s")
		}
	}
}

// A ping that goes unanswered means the socket is dead even though it still
// looks open, so the loop tears it down and lets the reconnect loop rebuild it.
func TestPingLoopDropsTheSocketWhenPingsGoUnanswered(t *testing.T) {
	b, _ := startPingBridge(t, false)
	for i := 0; i < 60; i++ {
		if !b.as.HasWebsocket() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("socket still up after pings went unanswered")
}
