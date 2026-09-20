package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/SelfRef/beeper-intercom/internal/config"
)

func TestStripDetails(t *testing.T) {
	cases := map[string]struct{ in, want string }{
		"plain":            {"hello", "hello"},
		"closed reasoning": {"<details type=\"reasoning\" done=\"true\">\n<summary>Thought</summary>\n> hmm\n</details>\nBASALT", "BASALT"},
		"unclosed cut":     {"<details type=\"reasoning\">\n<summary>Thinking…</summary>\n> half", ""},
		"two blocks":       {"<details type=\"tool_calls\">a</details>mid<details type=\"reasoning\">b</details>end", "midend"},
		"text before":      {"start <details>x</details> end", "start  end"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := StripDetails(tc.in); got != tc.want {
				t.Errorf("StripDetails(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestAccumulatorEmitsOnlyNewText(t *testing.T) {
	var acc accumulator
	if d, ok := acc.delta("Hel"); !ok || d != "Hel" {
		t.Fatalf("first = %q %v", d, ok)
	}
	if d, ok := acc.delta("Hello"); !ok || d != "lo" {
		t.Fatalf("second = %q %v", d, ok)
	}
	// Same text twice is not a delta.
	if _, ok := acc.delta("Hello"); ok {
		t.Fatal("unchanged text produced a delta")
	}
	// A rewrite that is not an extension emits nothing and resynchronises,
	// so the next extension is measured from the new text.
	if _, ok := acc.delta("Bye"); ok {
		t.Fatal("a diverged rewrite produced a delta")
	}
	if d, ok := acc.delta("Bye!"); !ok || d != "!" {
		t.Fatalf("after resync = %q %v", d, ok)
	}
}

func TestReadSSEStopsAtDone(t *testing.T) {
	body := "event: x\ndata: {\"a\":1}\n\n: comment\ndata:   {\"a\":2}\n\ndata: [DONE]\ndata: {\"a\":3}\n"
	var seen []string
	err := readSSE(strings.NewReader(body), func(data []byte) error {
		seen = append(seen, string(data))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[0] != `{"a":1}` || seen[1] != `{"a":2}` {
		t.Errorf("seen = %q", seen)
	}
}

func TestOpenAIStreamingDeltas(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, piece := range []string{"Hel", "lo", " world"} {
			_, _ = w.Write([]byte(`data: {"choices":[{"delta":{"content":"` + piece + `"}}]}` + "\n\n"))
		}
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	st := openTestStore(t)
	adapter := newOpenAI(config.Agent{URL: server.URL, Model: "m", MaxHistory: 10}, server.Client(), st, zerolog.Nop())

	var deltas []string
	reply, err := adapter.Send(context.Background(), Conversation{ID: "c"}, Turn{Text: "hi"}, Sink{
		Delta: func(text string) { deltas = append(deltas, text) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Text != "Hello world" {
		t.Errorf("reply = %q", reply.Text)
	}
	if strings.Join(deltas, "") != "Hello world" || len(deltas) != 3 {
		t.Errorf("deltas = %q", deltas)
	}
	// The transcript is what the next turn replays, so it must hold the
	// assembled answer, not the chunks.
	entries, err := st.Transcript(context.Background(), "c", 10)
	if err != nil || len(entries) != 2 || entries[1].Content != "Hello world" {
		t.Errorf("transcript = %+v %v", entries, err)
	}
}

func TestWebhookStreamingAndJSONFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/turn") && r.URL.Query().Get("mode") == "" {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"delta\":\"one \"}\n\ndata: {\"delta\":\"two\"}\n\ndata: {\"link\":\"http://x/1\"}\n\ndata: [DONE]\n"))
			return
		}
		_, _ = w.Write([]byte(`{"text":"plain answer"}`))
	}))
	defer server.Close()

	streaming := newWebhook(config.Agent{URL: server.URL}, server.Client(), zerolog.Nop())
	var deltas []string
	reply, err := streaming.Send(context.Background(), Conversation{ID: "c"}, Turn{Text: "hi"}, Sink{
		Delta: func(text string) { deltas = append(deltas, text) },
	})
	if err != nil {
		t.Fatal(err)
	}
	// No final text: the deltas are the answer, and the link still arrives.
	if reply.Text != "one two" || reply.Link != "http://x/1" || len(deltas) != 2 {
		t.Errorf("reply = %+v deltas = %q", reply, deltas)
	}

	plain := newWebhook(config.Agent{URL: server.URL + "/?mode=json"}, server.Client(), zerolog.Nop())
	reply, err = plain.Send(context.Background(), Conversation{ID: "c"}, Turn{Text: "hi"}, Sink{})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Text != "plain answer" {
		t.Errorf("json reply = %+v", reply)
	}
}

func TestOpenWebUIStreamingPollsUntilDone(t *testing.T) {
	// The server returns null to the streaming request (as Open WebUI does)
	// and serves a growing message on the chat endpoint.
	states := []struct {
		content string
		done    bool
	}{
		{"<details type=\"reasoning\">\n> thinking", false},
		{"<details type=\"reasoning\" done=\"true\">\n> thought\n</details>\nGRA", false},
		{"<details type=\"reasoning\" done=\"true\">\n> thought\n</details>\nGRANITE", true},
	}
	polls := 0
	var assistantID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/chat/completions":
			var body map[string]any
			_ = decodeJSON(r, &body)
			assistantID, _ = body["id"].(string)
			if body["stream"] != true {
				t.Errorf("stream = %v, want true", body["stream"])
			}
			_, _ = w.Write([]byte("null"))
		case strings.HasPrefix(r.URL.Path, "/api/v1/chats/"):
			state := states[min(polls, len(states)-1)]
			polls++
			_, _ = w.Write([]byte(`{"chat":{"history":{"messages":{"` + assistantID + `":{"content":` +
				quoteJSON(state.content) + `,"done":` + boolString(state.done) + `}}}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	adapter := newOpenWebUI(config.Agent{URL: server.URL, Model: "m"}, server.Client(), zerolog.Nop())
	var deltas []string
	reply, err := adapter.Send(context.Background(), Conversation{ID: "chat-1"}, Turn{Text: "codeword?"}, Sink{
		Delta: func(text string) { deltas = append(deltas, text) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Text != "GRANITE" {
		t.Errorf("reply = %q", reply.Text)
	}
	// The reasoning block never reached the bubble; the answer did, in pieces.
	if strings.Join(deltas, "") != "GRANITE" {
		t.Errorf("deltas = %q", deltas)
	}
	if reply.Parent != assistantID || assistantID == "" {
		t.Errorf("parent = %q, assistant id = %q", reply.Parent, assistantID)
	}
}
