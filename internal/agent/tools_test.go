package agent

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/rs/zerolog"

	"github.com/SelfRef/beeper-intercom/internal/config"
)

// TestToolsTurn runs one turn end to end against a fake chat endpoint and a
// real MCP server: the first model fails and hands over, a read runs at once,
// a write to a `confirm` server waits for a yes, and the answer resumes the
// turn. Then the yield lock takes a model out of the order.
func TestToolsTurn(t *testing.T) {
	mcpServer := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	type in struct {
		V string `json:"v"`
	}
	mcp.AddTool(mcpServer, &mcp.Tool{Name: "lookup"}, func(_ context.Context, _ *mcp.CallToolRequest, a in) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "found " + a.V}}}, nil, nil
	})
	mcp.AddTool(mcpServer, &mcp.Tool{Name: "change"}, func(_ context.Context, _ *mcp.CallToolRequest, a in) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "changed " + a.V}}}, nil, nil
	})
	hub := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return mcpServer },
		&mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
	defer hub.Close()

	chat := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model    string `json:"model"`
			Messages []struct {
				Role    string `json:"role"`
				Content any    `json:"content"`
			} `json:"messages"`
		}
		if err := decodeJSON(r, &req); err != nil {
			t.Error(err)
		}
		if req.Model == "down" {
			http.Error(w, "no such model", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		last := req.Messages[len(req.Messages)-1]
		if last.Role == "user" {
			fmt.Fprint(w, `data: {"choices":[{"delta":{"tool_calls":[`+
				`{"index":0,"id":"c1","function":{"name":"ro__lookup","arguments":"{\"v\":\"a\"}"}},`+
				`{"index":1,"id":"c2","function":{"name":"rw__change","arguments":"{\"v\":\"b\"}"}}]}}]}`+"\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		var results []string
		for _, m := range req.Messages {
			if m.Role == "tool" {
				results = append(results, fmt.Sprint(m.Content))
			}
		}
		fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%s}}]}\n\n", quoteJSON("done: "+strings.Join(results, "; ")))
		fmt.Fprint(w, `data: {"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":3}}`+"\n\ndata: [DONE]\n\n")
	}))
	defer chat.Close()

	cfg := config.Agent{
		Type:       config.AgentTools,
		URL:        chat.URL,
		Model:      "down",
		Fallback:   []string{"up"},
		MaxHistory: 20,
		ToolIDs:    []string{"ro", "rw"},
		MCP: map[string]config.MCPServer{
			"ro": {URL: hub.URL},
			"rw": {URL: hub.URL, Confirm: true},
		},
	}
	st := openTestStore(t)
	agent := newTools(cfg, http.DefaultClient, st, zerolog.Nop())
	ctx := context.Background()
	convID, err := agent.NewConversation(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	var notes []string
	sink := Sink{Activity: func(line string) { notes = append(notes, line) }}
	reply, err := agent.Send(ctx, Conversation{ID: convID}, Turn{Text: "go"}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Ask == nil || len(reply.Ask.Questions) != 1 || reply.Ask.Questions[0].ID != "c2" {
		t.Fatalf("want one confirmation for c2, got %+v", reply.Ask)
	}
	joined := strings.Join(notes, "\n")
	if !strings.Contains(joined, "`down` failed") || !strings.Contains(joined, "🔧 ro: lookup") || strings.Contains(joined, "rw: change") {
		t.Fatalf("activity before the answer: %q", joined)
	}

	reply, err = agent.Answer(ctx, Conversation{ID: convID}, reply.Ask, map[string]string{"c2": confirmAllow}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if reply == nil || reply.Text != "done: found a; changed b" {
		t.Fatalf("answer after the confirmation: %+v", reply)
	}
	history, err := st.Transcript(ctx, convID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || !strings.Contains(history[1].Content, "Tools used for this answer: ro: lookup, rw: change") {
		t.Fatalf("transcript: %+v", history)
	}

	// The yield lock: present takes "down" out, stale is ignored.
	lock := filepath.Join(t.TempDir(), "lock")
	agent.cfg.Yield = config.Yield{File: lock, Models: []string{"down"}}
	if err := os.WriteFile(lock, []byte("reason=bench\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	notes = nil
	if got := agent.models("down:m", sink); !slices.Equal(got, []string{"up"}) || !strings.Contains(strings.Join(notes, ""), "bench") {
		t.Fatalf("yield: %v %v", got, notes)
	}
	if err := os.WriteFile(lock, []byte(fmt.Sprintf("until=%d\n", time.Now().Add(-time.Minute).Unix())), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := agent.models("down:m", sink); !slices.Equal(got, []string{"down:m", "up"}) {
		t.Fatalf("stale yield: %v", got)
	}
}
