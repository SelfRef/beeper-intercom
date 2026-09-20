package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/SelfRef/beeper-intercom/internal/config"
)

// openWebUI talks to Open WebUI's chat API, which is the reason this adapter
// exists at all: the conversation lives in Open WebUI's database, so the same
// chat is visible and continuable in the web UI, memory and RAG apply, and
// context compaction is somebody else's problem.
//
// The contract, verified against 0.11.3:
//
//   - a chat is created up front with POST /api/v1/chats/new, which returns
//     its id. Letting /api/chat/completions create the chat (parent_id: null,
//     no chat_id) works too, but the generated id is never returned, so the
//     follow-up turn has nothing to point at;
//   - each turn is POST /api/chat/completions with chat_id, a fresh assistant
//     id, and a user_message object. **user_message.parentId is what threads
//     the conversation** — the top-level parent_id only decides whether a new
//     chat is created. With parentId set, the server loads the stored history
//     and the model sees earlier turns; without it, every turn starts cold;
//   - stream: false returns the answer in the HTTP body. A streaming request
//     against a saved chat is turned into a background task and the body is
//     empty, with the deltas going to socket.io instead.
type openWebUI struct {
	cfg    config.Agent
	client *http.Client
	log    zerolog.Logger

	folderOnce sync.Once
	folderID   string
	folderErr  error
}

func newOpenWebUI(cfg config.Agent, client *http.Client, log zerolog.Logger) *openWebUI {
	return &openWebUI{cfg: cfg, client: client, log: log}
}

func (o *openWebUI) Type() string { return config.AgentOpenWebUI }

func (o *openWebUI) Caps() Caps {
	return Caps{ServerHistory: true, Cancel: true, DeepLink: true}
}

func (o *openWebUI) Link(convID string) string {
	if convID == "" {
		return ""
	}
	return strings.TrimSuffix(o.cfg.URL, "/") + "/c/" + convID
}

func (o *openWebUI) NewConversation(ctx context.Context, seed *Seed) (string, error) {
	body := map[string]any{
		"chat": map[string]any{
			"title":    "Intercom",
			"models":   []string{o.cfg.Model},
			"history":  map[string]any{"currentId": nil, "messages": map[string]any{}},
			"messages": []any{},
		},
	}
	if folder, err := o.folder(ctx); err != nil {
		o.log.Warn().Err(err).Msg("Could not resolve folder, chat will land in the root")
	} else if folder != "" {
		body["folder_id"] = folder
	}

	var resp struct {
		ID string `json:"id"`
	}
	if err := o.do(ctx, http.MethodPost, "/api/v1/chats/new", body, &resp); err != nil {
		return "", err
	}
	if resp.ID == "" {
		return "", fmt.Errorf("open webui returned no chat id")
	}

	if prompt := SeedPrompt(seed); prompt != "" {
		// The seed is a real first turn so it is stored, visible in the web UI
		// and part of the history the model is given.
		if _, err := o.Send(ctx, Conversation{ID: resp.ID, Model: o.cfg.Model}, Turn{Text: prompt, Internal: true}, Sink{}); err != nil {
			o.log.Warn().Err(err).Msg("Failed to seed conversation")
		}
	}
	return resp.ID, nil
}

func (o *openWebUI) Send(ctx context.Context, conv Conversation, turn Turn, sink Sink) (*Reply, error) {
	model := conv.Model
	if model == "" {
		model = o.cfg.Model
	}
	userID := uuid.NewString()
	assistantID := uuid.NewString()

	userMessage := map[string]any{
		"id":        userID,
		"role":      "user",
		"content":   turn.Text,
		"timestamp": time.Now().Unix(),
		"models":    []string{model},
	}
	// parentId is the whole contract for history: with it the server loads the
	// stored branch, without it the turn starts a new root.
	if conv.Parent != "" {
		userMessage["parentId"] = conv.Parent
	}

	body := map[string]any{
		"model":        model,
		"chat_id":      conv.ID,
		"id":           assistantID,
		"stream":       false,
		"messages":     []map[string]any{{"role": "user", "content": turn.Text}},
		"user_message": userMessage,
		"background_tasks": map[string]any{
			"title_generation":     !turn.Internal,
			"tags_generation":      !turn.Internal,
			"follow_up_generation": false,
		},
	}
	if conv.Parent == "" {
		// Explicit null: "this is the root of the chat", as opposed to absent,
		// which means "legacy caller, no chat management".
		body["parent_id"] = nil
	} else {
		body["parent_id"] = conv.Parent
	}
	if len(o.cfg.ToolIDs) > 0 {
		body["tool_ids"] = o.cfg.ToolIDs
	}
	if len(o.cfg.ToolServers) > 0 {
		body["tool_servers"] = o.cfg.ToolServers
	}

	if sink.Status != nil {
		sink.Status("thinking")
	}

	var resp struct {
		Choices []struct {
			Message struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"message"`
		} `json:"choices"`
		Error any `json:"error"`
	}
	if err := o.do(ctx, http.MethodPost, "/api/chat/completions", body, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("open webui: %v", resp.Error)
	}
	if len(resp.Choices) == 0 {
		return nil, fmt.Errorf("open webui returned no choices")
	}
	text := resp.Choices[0].Message.Content
	if text == "" {
		text = resp.Choices[0].Message.ReasoningContent
	}
	return &Reply{Text: text, Parent: assistantID, Link: o.Link(conv.ID)}, nil
}

func (o *openWebUI) Cancel(ctx context.Context, convID string) error {
	return o.do(ctx, http.MethodPost, "/api/tasks/chat/"+convID+"/stop", map[string]any{}, nil)
}

// folder resolves the configured sidebar folder once per process, creating it
// if it does not exist: a folder_id on a request must already exist.
func (o *openWebUI) folder(ctx context.Context) (string, error) {
	if o.cfg.Folder == "" {
		return "", nil
	}
	o.folderOnce.Do(func() {
		var folders []struct {
			ID       string  `json:"id"`
			Name     string  `json:"name"`
			ParentID *string `json:"parent_id"`
		}
		if err := o.do(ctx, http.MethodGet, "/api/v1/folders/", nil, &folders); err != nil {
			o.folderErr = err
			return
		}
		// Nested folders are written "Beeper/Ops"; only the leaf name is
		// matched, which is enough to keep two rooms in two folders.
		want := o.cfg.Folder
		if idx := strings.LastIndex(want, "/"); idx >= 0 {
			want = want[idx+1:]
		}
		for _, f := range folders {
			if f.Name == want {
				o.folderID = f.ID
				return
			}
		}
		var created struct {
			ID string `json:"id"`
		}
		if err := o.do(ctx, http.MethodPost, "/api/v1/folders/", map[string]any{"name": want}, &created); err != nil {
			o.folderErr = err
			return
		}
		o.folderID = created.ID
	})
	return o.folderID, o.folderErr
}

func (o *openWebUI) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(o.cfg.URL, "/")+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+o.cfg.Key())
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("open webui %s %s: HTTP %d: %s", method, path, resp.StatusCode, trim(string(data)))
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("open webui %s %s: bad response: %w", method, path, err)
	}
	return nil
}

func trim(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 400 {
		return s[:400] + "…"
	}
	return s
}
