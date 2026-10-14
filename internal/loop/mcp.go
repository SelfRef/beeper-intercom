package loop

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Server is one MCP endpoint (streamable HTTP). Name prefixes its tools, so
// two endpoints that offer the same tool — two MCPHub $smart groups both have
// search_tools — stay apart: "ro__search_tools", "rw__search_tools".
type Server struct {
	Name    string
	URL     string
	Headers map[string]string
}

// toolSep joins a server name and a tool name. Two underscores, because a
// single one is common inside tool names and a dot is not a legal function
// name for every endpoint.
const toolSep = "__"

// Toolbox is a set of connected MCP servers and the tools they offer, for the
// duration of one turn. Sessions are opened per turn and closed after it:
// a handful of local round trips, against keeping long-lived sessions alive
// across hub restarts.
type Toolbox struct {
	sessions map[string]*mcp.ClientSession
	tools    []Tool
}

// Open connects to every server and lists its tools. A server that cannot be
// reached is skipped and reported, not fatal: one dead endpoint should cost
// its own tools, not the whole answer.
func Open(ctx context.Context, servers []Server, client *http.Client) (*Toolbox, []string) {
	box := &Toolbox{sessions: make(map[string]*mcp.ClientSession, len(servers))}
	type opened struct {
		session *mcp.ClientSession
		tools   []Tool
		err     error
	}
	results := make([]opened, len(servers))
	var wg sync.WaitGroup
	for i, srv := range servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			session, tools, err := connect(ctx, srv, client)
			results[i] = opened{session, tools, err}
		}()
	}
	wg.Wait()

	var problems []string
	for i, srv := range servers {
		if results[i].err != nil {
			problems = append(problems, fmt.Sprintf("`%s` is unreachable (%s); running without it", srv.Name, short(results[i].err.Error(), 120)))
			continue
		}
		box.sessions[srv.Name] = results[i].session
		box.tools = append(box.tools, results[i].tools...)
	}
	return box, problems
}

func connect(ctx context.Context, srv Server, client *http.Client) (*mcp.ClientSession, []Tool, error) {
	httpClient := *client
	httpClient.Transport = headerTransport{base: client.Transport, headers: srv.Headers}
	c := mcp.NewClient(&mcp.Implementation{Name: "beeper-intercom", Version: "1"}, nil)
	session, err := c.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:             srv.URL,
		HTTPClient:           &httpClient,
		DisableStandaloneSSE: true,
		MaxRetries:           -1,
	}, nil)
	if err != nil {
		return nil, nil, err
	}
	var tools []Tool
	for tool, err := range session.Tools(ctx, nil) {
		if err != nil {
			session.Close()
			return nil, nil, err
		}
		schema := tool.InputSchema
		if schema == nil {
			schema = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		tools = append(tools, Tool{Type: "function", Function: ToolFunction{
			Name:        srv.Name + toolSep + tool.Name,
			Description: tool.Description,
			Parameters:  schema,
		}})
	}
	return session, tools, nil
}

type headerTransport struct {
	base    http.RoundTripper
	headers map[string]string
}

func (t headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if len(t.headers) > 0 {
		req = req.Clone(req.Context())
		for k, v := range t.headers {
			req.Header.Set(k, v)
		}
	}
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(req)
}

// Tools is what the model is offered.
func (b *Toolbox) Tools() []Tool {
	if b == nil {
		return nil
	}
	return b.tools
}

// Close ends every session.
func (b *Toolbox) Close() {
	if b == nil {
		return
	}
	for _, s := range b.sessions {
		s.Close()
	}
}

// SplitName returns the server and tool a prefixed name refers to.
func SplitName(name string) (server, tool string) {
	server, tool, ok := strings.Cut(name, toolSep)
	if !ok {
		return "", name
	}
	return server, tool
}

// ResultLimit caps a tool result in characters. One fetch of a web page can
// be a hundred thousand tokens; past this the model gets the start and a note
// that there was more.
const ResultLimit = 16000

// Call runs one tool and returns what goes back to the model. Failures are
// results too — the model reads "the call failed because…" and can react —
// so this never returns an error.
func (b *Toolbox) Call(ctx context.Context, call ToolCall) string {
	server, tool := SplitName(call.Function.Name)
	var session *mcp.ClientSession
	if b != nil {
		session = b.sessions[server]
	}
	if session == nil {
		return fmt.Sprintf("Error: there is no tool called %q.", call.Function.Name)
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err != nil {
		return fmt.Sprintf("Error: the arguments are not a JSON object (%v). Call the tool again with valid JSON.", err)
	}
	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		return "Error: " + err.Error()
	}
	var out strings.Builder
	if res.IsError {
		out.WriteString("Error: ")
	}
	for _, content := range res.Content {
		switch c := content.(type) {
		case *mcp.TextContent:
			out.WriteString(c.Text)
		case *mcp.ImageContent:
			out.WriteString("[image omitted]")
		default:
			if raw, err := json.Marshal(c); err == nil {
				out.Write(raw)
			}
		}
		out.WriteString("\n")
	}
	if out.Len() == 0 && res.StructuredContent != nil {
		if raw, err := json.Marshal(res.StructuredContent); err == nil {
			out.Write(raw)
		}
	}
	text := strings.TrimSpace(out.String())
	if len(text) > ResultLimit {
		text = strings.ToValidUTF8(text[:ResultLimit], "") + fmt.Sprintf("\n\n[truncated: %d more characters not shown]", len(text)-ResultLimit)
	}
	if text == "" {
		text = "(no output)"
	}
	return text
}

func short(s string, n int) string {
	s = strings.TrimSpace(s)
	if len([]rune(s)) <= n {
		return s
	}
	return string([]rune(s)[:n-1]) + "…"
}
