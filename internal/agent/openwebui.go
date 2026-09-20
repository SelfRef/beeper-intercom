package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
//   - stream: false returns the answer in the HTTP body;
//   - stream: true against a saved chat returns `null` and runs the turn as a
//     background task. The deltas go to socket.io, but the server also keeps
//     the partial answer in its response-stream store, and
//     GET /api/v1/chats/{id} overlays it onto the assistant message with
//     done: false. Polling that is how this adapter streams: no socket.io
//     client, no second protocol, and it reads exactly what the web UI would
//     show on reconnect.
type openWebUI struct {
	cfg    config.Agent
	client *http.Client
	log    zerolog.Logger

	folderOnce sync.Once
	folderID   string
	folderErr  error
}

// pollInterval is how often a streaming turn re-reads the chat. Open WebUI
// flushes partial output every few deltas, so anything much faster only
// finds the same text again.
const pollInterval = 400 * time.Millisecond

func newOpenWebUI(cfg config.Agent, client *http.Client, log zerolog.Logger) *openWebUI {
	return &openWebUI{cfg: cfg, client: client, log: log}
}

func (o *openWebUI) Type() string { return config.AgentOpenWebUI }

func (o *openWebUI) Caps() Caps {
	return Caps{ServerHistory: true, Streaming: !o.cfg.NoStream, Cancel: true, DeepLink: true}
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

// turnRequest builds the completions body. Streaming and one-shot differ in
// exactly one field.
func (o *openWebUI) turnRequest(conv Conversation, turn Turn, assistantID string, stream bool) map[string]any {
	model := conv.Model
	if model == "" {
		model = o.cfg.Model
	}
	userMessage := map[string]any{
		"id":        uuid.NewString(),
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
		"stream":       stream,
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
	return body
}

func (o *openWebUI) Send(ctx context.Context, conv Conversation, turn Turn, sink Sink) (*Reply, error) {
	if sink.Delta != nil && !o.cfg.NoStream {
		return o.sendStreaming(ctx, conv, turn, sink)
	}
	return o.sendOneShot(ctx, conv, turn, sink)
}

func (o *openWebUI) sendOneShot(ctx context.Context, conv Conversation, turn Turn, sink Sink) (*Reply, error) {
	assistantID := uuid.NewString()
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
	if err := o.do(ctx, http.MethodPost, "/api/chat/completions", o.turnRequest(conv, turn, assistantID, false), &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("open webui: %v", resp.Error)
	}
	if len(resp.Choices) == 0 {
		return nil, fmt.Errorf("open webui returned no choices")
	}
	// content and reasoning_content are separate fields. A reasoning model
	// that spends its whole budget thinking comes back with an empty answer;
	// showing the reasoning is more useful than an empty bubble, but it has to
	// be labelled as what it is rather than passed off as the answer.
	text := resp.Choices[0].Message.Content
	if text == "" && resp.Choices[0].Message.ReasoningContent != "" {
		text = "*No answer — the model returned only its reasoning:*\n\n" +
			resp.Choices[0].Message.ReasoningContent
	}
	return &Reply{Text: text, Parent: assistantID, Link: o.Link(conv.ID)}, nil
}

// sendStreaming runs the turn as Open WebUI's background task and polls the
// chat for the growing answer. The HTTP request itself blocks until the task
// is done and returns nothing useful, so it runs in the background and only
// its error matters.
func (o *openWebUI) sendStreaming(ctx context.Context, conv Conversation, turn Turn, sink Sink) (*Reply, error) {
	assistantID := uuid.NewString()
	if sink.Status != nil {
		sink.Status("thinking")
	}

	requestDone := make(chan error, 1)
	go func() {
		requestDone <- o.do(ctx, http.MethodPost, "/api/chat/completions", o.turnRequest(conv, turn, assistantID, true), nil)
	}()

	var acc accumulator
	var lastVisible, rawContent string
	// Once the request has returned, the task is over, but the last flush and
	// the done flag can lag behind it by a moment. A few more polls catch
	// them; after that whatever is there is the answer.
	const gracePolls = 10
	graceLeft := -1
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-requestDone:
			if err != nil {
				return nil, err
			}
			requestDone = nil
			graceLeft = gracePolls
		case <-ticker.C:
		}

		msg, err := o.assistantMessage(ctx, conv.ID, assistantID)
		if err != nil {
			// A transient read failure should not kill the turn; the next tick
			// tries again, unless the grace period is over.
			o.log.Debug().Err(err).Msg("Poll failed")
			if graceLeft == 0 {
				return nil, err
			}
		} else if msg != nil {
			if msg.Error != "" {
				return nil, fmt.Errorf("open webui: %s", msg.Error)
			}
			rawContent = msg.Content
			visible := StripDetails(msg.Content)
			if delta, ok := acc.delta(visible); ok {
				if sink.Status != nil && lastVisible == "" {
					sink.Status("generating")
				}
				sink.Delta(delta)
			}
			lastVisible = visible
			if msg.Done {
				break
			}
		}
		if graceLeft == 0 {
			break
		}
		if graceLeft > 0 {
			graceLeft--
		}
	}

	text := lastVisible
	if text == "" && strings.Contains(rawContent, "<details") {
		// The whole answer was a reasoning block. Say so rather than sending
		// an empty bubble.
		text = "*No answer — the model returned only its reasoning.*"
	}
	return &Reply{Text: text, Parent: assistantID, Link: o.Link(conv.ID)}, nil
}

// chatMessage is the slice of a stored message the streaming loop reads.
type chatMessage struct {
	Content string
	Done    bool
	Error   string
}

// assistantMessage reads one message out of the chat, with the live
// response-stream overlay the server applies to GET /api/v1/chats/{id}.
func (o *openWebUI) assistantMessage(ctx context.Context, chatID, messageID string) (*chatMessage, error) {
	var resp struct {
		Chat struct {
			History struct {
				Messages map[string]struct {
					Content string          `json:"content"`
					Done    bool            `json:"done"`
					Error   json.RawMessage `json:"error"`
				} `json:"messages"`
			} `json:"history"`
		} `json:"chat"`
	}
	if err := o.do(ctx, http.MethodGet, "/api/v1/chats/"+chatID, nil, &resp); err != nil {
		return nil, err
	}
	raw, ok := resp.Chat.History.Messages[messageID]
	if !ok {
		return nil, nil
	}
	msg := &chatMessage{Content: raw.Content, Done: raw.Done}
	if len(raw.Error) > 0 && string(raw.Error) != "null" {
		var structured struct {
			Content string `json:"content"`
		}
		if json.Unmarshal(raw.Error, &structured) == nil && structured.Content != "" {
			msg.Error = structured.Content
		} else {
			msg.Error = trim(string(raw.Error))
		}
	}
	return msg, nil
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
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return errors.New("open webui returned null")
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
