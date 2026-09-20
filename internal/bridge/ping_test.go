package bridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
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
// skipUnderRace skips the tests that drive a real appservice websocket while
// the race detector is on.
//
// The race they find is upstream's, not the bridge's: mautrix keeps the live
// socket in a plain `as.ws` field, written by StartWebsocket (including the
// `as.ws = nil` when it closes) and read by SendWebsocket and HasWebsocket
// from whichever goroutine is sending. Every mautrix bridge uses it exactly
// that way, and a torn pointer read is not a thing on the platforms this runs
// on, so the consequence is theoretical — but the detector is right, and it
// cannot be fixed from this side. The rest of the suite still runs under
// -race, which is what protects this repo's own code.
func skipUnderRace(t *testing.T) {
	t.Helper()
	if raceEnabled {
		t.Skip("mautrix appservice.AppService.ws is unsynchronised upstream; see skipUnderRace")
	}
}

// startPingBridge returns the bridge, the commands the fake server saw, and a
// channel that closes when StartWebsocket returns — which is how a test sees
// the socket go away without reading mautrix's unlocked state.
func startPingBridge(t *testing.T, reply bool) (*Bridge, chan string, chan struct{}) {
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
	// Wait on the connect callback rather than polling as.HasWebsocket():
	// mautrix writes that field from the websocket goroutine without a lock,
	// so polling it from the test goroutine is a data race (and the race
	// detector fails the build for it).
	connected := make(chan struct{})
	dropped := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(dropped)
		_ = as.StartWebsocket(ctx, "", func() { once.Do(func() { close(connected) }) })
	}()
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("websocket never connected")
	}
	b.mu.Lock()
	b.connected = true
	b.mu.Unlock()
	go b.pingLoop(ctx, 20*time.Millisecond, 200*time.Millisecond)
	return b, commands, dropped
}

// hungryserv closes a socket that has been silent for a few minutes, so the
// bridge has to speak first. This is the frame that keeps it open; without it
// the bridge reconnected every five minutes all day.
func TestPingLoopPingsTheServer(t *testing.T) {
	skipUnderRace(t)
	_, commands, _ := startPingBridge(t, true)
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
	skipUnderRace(t)
	_, _, dropped := startPingBridge(t, false)
	select {
	case <-dropped:
	case <-time.After(3 * time.Second):
		t.Fatal("socket still up after pings went unanswered")
	}
}
