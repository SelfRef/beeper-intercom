package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"sort"
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
	return o.publicURL() + "/c/" + convID
}

// publicURL is where a human goes, which is not always where the bridge goes.
func (o *openWebUI) publicURL() string {
	if o.cfg.PublicURL != "" {
		return strings.TrimSuffix(o.cfg.PublicURL, "/")
	}
	return strings.TrimSuffix(o.cfg.URL, "/")
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

// sessionID is the marker that makes Open WebUI treat a request as one of its
// own: it is what unlocks the built-in tools (web search and friends), and it
// also moves the turn onto the background-task path, where the HTTP call
// returns immediately and the answer only arrives over the socket.
func (o *openWebUI) sessionID(assistantID string) string {
	if len(o.cfg.Features) == 0 {
		return ""
	}
	return "intercom-" + assistantID
}

// turnRequest builds the completions body. Streaming and one-shot differ in
// exactly one field.
func (o *openWebUI) turnRequest(conv Conversation, turn Turn, assistantID, userMsgID string, stream bool) map[string]any {
	model := conv.Model
	if model == "" {
		model = o.cfg.Model
	}
	userMessage := map[string]any{
		"id":        userMsgID,
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
	if len(turn.Attachments) > 0 {
		// Images ride along as data URLs on the stored user message: the server
		// rebuilds them into image_url parts on every replay, exactly as the web
		// UI does. Documents were uploaded first (see attachFiles) and are
		// referenced by id, which puts them through the same RAG/full-context
		// path as a file dropped into the web UI.
		var stored, metadata []map[string]any
		for _, att := range turn.Attachments {
			if strings.HasPrefix(att.Mime, "image/") {
				stored = append(stored, map[string]any{"type": "image", "url": dataURL(att), "name": att.Name})
				continue
			}
			if att.uploaded != nil {
				stored = append(stored, att.uploaded)
				metadata = append(metadata, att.uploaded)
			}
		}
		if len(stored) > 0 {
			userMessage["files"] = stored
		}
		if len(metadata) > 0 {
			body["files"] = metadata
		}
	}
	// The agent's own tool ids plus whatever toolsets the conversation has
	// switched on. Open WebUI's backend does NOT apply a model's configured
	// toolIds to API callers (measured 2026-09-20: it reads them nowhere
	// outside automations; the web UI is what turns them into tool_ids), so
	// whatever this bridge does not send, the model does not get.
	if tools := mergeTools(o.cfg.ToolIDs, turn.Tools); len(tools) > 0 {
		body["tool_ids"] = tools
	}
	if len(o.cfg.ToolServers) > 0 {
		body["tool_servers"] = o.cfg.ToolServers
	}
	if len(o.cfg.Features) > 0 {
		features := make(map[string]any, len(o.cfg.Features))
		for _, name := range o.cfg.Features {
			features[name] = true
		}
		body["features"] = features
		// Only on the streaming path: a session id turns the request into a
		// background task whose HTTP response is empty, and the one-shot path
		// has nothing but that response to read.
		if stream {
			body["session_id"] = o.sessionID(assistantID)
		}
	}
	return body
}

func (o *openWebUI) Send(ctx context.Context, conv Conversation, turn Turn, sink Sink) (*Reply, error) {
	if err := o.attachFiles(ctx, &turn); err != nil {
		return nil, err
	}
	if sink.Streams() && !o.cfg.NoStream {
		return o.sendStreaming(ctx, conv, turn, sink)
	}
	return o.sendOneShot(ctx, conv, turn, sink)
}

func (o *openWebUI) sendOneShot(ctx context.Context, conv Conversation, turn Turn, sink Sink) (*Reply, error) {
	assistantID, userMsgID := uuid.NewString(), uuid.NewString()
	// No phase is reported here on purpose: this path sees nothing until the
	// whole answer lands, and "thinking" would be a lie for a model with
	// reasoning switched off. The caller's own "queued" covers the wait.
	var resp struct {
		Choices []struct {
			Message struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"message"`
		} `json:"choices"`
		Error any `json:"error"`
	}
	if err := o.do(ctx, http.MethodPost, "/api/chat/completions", o.turnRequest(conv, turn, assistantID, userMsgID, false), &resp); err != nil {
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
	return &Reply{Text: text, Parent: assistantID, UserMessage: userMsgID, Link: o.Link(conv.ID)}, nil
}

// sendStreaming runs the turn as Open WebUI's background task. The HTTP
// request itself blocks until the task is done and returns nothing useful, so
// it runs in the background and only its error matters; the deltas come from
// the user's socket.io room when a session token is configured, and from
// polling the chat otherwise (which, on 0.11.3, only ever sees the finished
// answer — see socketio.go).
func (o *openWebUI) sendStreaming(ctx context.Context, conv Conversation, turn Turn, sink Sink) (*Reply, error) {
	assistantID, userMsgID := uuid.NewString(), uuid.NewString()
	// The phase stays at the caller's "queued" until the backend says what it
	// is doing. Claiming "thinking" up front marks the prompt-processing wait
	// as reasoning, which is wrong whenever reasoning is off — and that wait
	// is exactly when the model is NOT thinking.

	submit := func(ctx context.Context) error {
		return o.do(ctx, http.MethodPost, "/api/chat/completions",
			o.turnRequest(conv, turn, assistantID, userMsgID, true), nil)
	}
	reply, err := o.awaitAnswer(ctx, conv, assistantID, sink, submit)
	if reply != nil {
		reply.UserMessage = userMsgID
	}
	return reply, err
}

// awaitAnswer submits a turn (a new one, or the resumption of one that stopped
// to ask a question) and collects the answer: over the socket when a session
// token is configured, by polling the chat otherwise.
func (o *openWebUI) awaitAnswer(ctx context.Context, conv Conversation, assistantID string, sink Sink, submit func(context.Context) error) (*Reply, error) {
	if token := o.cfg.SocketToken(); token != "" {
		socket, err := dialOWUISocket(ctx, o.cfg.URL, token)
		if err != nil {
			o.log.Warn().Err(err).Msg("Socket.io unavailable; falling back to polling")
		} else {
			defer socket.Close()
			return o.streamViaSocket(ctx, socket, conv, assistantID, sink, submit)
		}
	}

	requestDone := make(chan error, 1)
	go func() {
		requestDone <- submit(ctx)
	}()

	var acc accumulator
	var lastVisible, rawContent string
	var polledAsk *Ask
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
			if o.sessionID(assistantID) == "" {
				graceLeft = gracePolls
			}
			// With a session id the response means "accepted", not
			// "finished" (see turnRequest), so the poll keeps going until the
			// stored message says done — or the context gives up.
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
				if lastVisible == "" {
					sink.Report("generating")
				}
				sink.Push(delta)
			}
			lastVisible = visible
			if msg.Done {
				break
			}
			// A turn that stopped to ask something never sets done: the
			// questions staged on the message are what says it is over.
			if msg.Ask != nil {
				polledAsk = msg.Ask
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
	return o.finish(ctx, conv, assistantID, text, nil, polledAsk)
}

// streamViaSocket consumes the turn's events from the socket: text deltas as
// they are generated, and the terminal chat:completion. Reasoning deltas are
// skipped — they are the model thinking, not the answer.
func (o *openWebUI) streamViaSocket(ctx context.Context, socket *owuiSocket, conv Conversation, assistantID string, sink Sink, submit func(context.Context) error) (*Reply, error) {
	requestDone := make(chan error, 1)
	go func() {
		requestDone <- submit(ctx)
	}()

	var text strings.Builder
	var acc accumulator
	var phase phaseTracker
	var usage *Usage
	var asked *Ask
	done := make(chan error, 1)
	go func() {
		done <- socket.events(ctx, func(name string, payload json.RawMessage) bool {
			if name != "events" {
				return true
			}
			var evt owuiEvent
			if json.Unmarshal(payload, &evt) != nil || evt.MessageID != assistantID {
				return true
			}
			var inner owuiDelta
			_ = json.Unmarshal(evt.Data.Data, &inner)
			switch evt.Data.Type {
			case "response:completion":
				switch {
				case strings.HasSuffix(inner.Type, "reasoning_text.delta"):
					// Reasoning is not the answer and never reaches the bubble,
					// but it is the difference between "stuck" and "thinking".
					phase.to(sink, "thinking")
				case inner.Type == "response.output_item.added" && inner.Item.Type == "function_call":
					phase.to(sink, toolPhase(inner.Item.Name))
				}
				if strings.HasSuffix(inner.Type, "output_text.delta") && inner.Delta != "" {
					phase.to(sink, "writing")
					text.WriteString(inner.Delta)
					acc.seen = text.String()
					sink.Push(inner.Delta)
				}
			case "chat:completion":
				// A turn that stopped to ask something ends HERE, with the
				// questions in the output and no done flag. Waiting for one
				// would leave the ghost typing until the context gave up.
				if staged, err := askFromOutput(inner.Output, assistantID); err == nil && staged != nil {
					asked = staged
					return false
				}
				if inner.Usage != nil {
					usage = &Usage{
						PromptTokens:     inner.Usage.PromptTokens,
						CompletionTokens: inner.Usage.CompletionTokens,
						CachedTokens:     inner.Usage.PromptTokensDetails.CachedTokens,
						PromptPerSecond:  inner.Usage.PromptPerSecond,
						TokensPerSecond:  inner.Usage.PredictedPerSecond,
					}
				}
				// The non-delta path carries the whole content so far.
				var content string
				if json.Unmarshal(inner.Content, &content) == nil && content != "" {
					if delta, ok := acc.delta(StripDetails(content)); ok {
						text.WriteString(delta)
						sink.Push(delta)
					}
				}
				if inner.Done {
					return false
				}
			case "chat:message:error":
				return false
			}
			return true
		})
	}()

	// Which signal means "the turn is over" depends on how it was submitted.
	// Without a session id the HTTP call blocks for the whole run, so its
	// return is the end and the terminal socket event is a moment behind it.
	// With one, Open WebUI runs the turn as a background task and answers the
	// HTTP call immediately — there the socket is the only signal, and
	// treating the response as the end would cut every answer short.
	background := o.sessionID(assistantID) != "" // streaming path: see turnRequest
	var requestErr error
	requestFinished := false
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case requestErr = <-requestDone:
			if requestErr != nil {
				return nil, requestErr
			}
			requestFinished = true
			if background {
				// Accepted, not finished: keep reading the socket.
				continue
			}
			// The task is over; the terminal event is at most a moment behind.
			select {
			case <-done:
			case <-time.After(3 * time.Second):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		case err := <-done:
			if err != nil && !requestFinished {
				o.log.Debug().Err(err).Msg("Socket ended before the request; waiting for the request")
				if err := <-requestDone; err != nil {
					return nil, err
				}
			}
		}
		break
	}

	// The stored message is authoritative: it is what the web UI shows and
	// what the next turn replays.
	final := text.String()
	if msg, err := o.assistantMessage(ctx, conv.ID, assistantID); err == nil && msg != nil {
		if msg.Error != "" {
			return nil, fmt.Errorf("open webui: %s", msg.Error)
		}
		if visible := StripDetails(msg.Content); visible != "" {
			final = visible
		} else if final == "" && strings.Contains(msg.Content, "<details") {
			final = "*No answer — the model returned only its reasoning.*"
		}
	}
	return o.finish(ctx, conv, assistantID, final, usage, asked)
}

// chatMessage is the slice of a stored message the streaming loop reads.
type chatMessage struct {
	Content string
	Done    bool
	Error   string
	// Ask is a staged question: the turn is over and waiting for me, even
	// though Done is false.
	Ask *Ask
}

// assistantMessage reads one message out of the chat, with the live
// response-stream overlay the server applies to GET /api/v1/chats/{id}.
func (o *openWebUI) assistantMessage(ctx context.Context, chatID, messageID string) (*chatMessage, error) {
	var resp struct {
		Chat struct {
			History struct {
				Messages map[string]struct {
					Content string           `json:"content"`
					Done    bool             `json:"done"`
					Error   json.RawMessage  `json:"error"`
					Output  []owuiOutputItem `json:"output"`
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
	if ask, err := askFromOutput(raw.Output, messageID); err == nil {
		msg.Ask = ask
	}
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

// attachFiles uploads every non-image attachment to Open WebUI's file store
// and records the reference the chat API wants back.
func (o *openWebUI) attachFiles(ctx context.Context, turn *Turn) error {
	for i := range turn.Attachments {
		att := &turn.Attachments[i]
		if strings.HasPrefix(att.Mime, "image/") || att.uploaded != nil {
			continue
		}
		var resp struct {
			ID       string `json:"id"`
			Filename string `json:"filename"`
			Meta     struct {
				Size        int64  `json:"size"`
				ContentType string `json:"content_type"`
			} `json:"meta"`
		}
		if err := o.multipart(ctx, "/api/v1/files/", *att, nil, &resp); err != nil {
			return fmt.Errorf("upload %s: %w", att.Name, err)
		}
		att.uploaded = map[string]any{
			"type":         "file",
			"id":           resp.ID,
			"name":         resp.Filename,
			"url":          "/api/v1/files/" + resp.ID,
			"size":         resp.Meta.Size,
			"content_type": resp.Meta.ContentType,
			"status":       "uploaded",
		}
	}
	return nil
}

// Transcribe sends a voice message through Open WebUI's STT, which converts
// and forwards it to whatever engine the instance is configured with.
// Ask answers one question with no chat behind it: no chat_id, so Open WebUI
// stores nothing, the running conversation is untouched and the model sees
// only what was asked. This is /btw.
func (o *openWebUI) Ask(ctx context.Context, model, question string) (string, error) {
	if model == "" {
		model = o.cfg.Model
	}
	body := map[string]any{
		"model":    model,
		"stream":   false,
		"messages": []map[string]string{{"role": "user", "content": question}},
	}
	if tools := o.cfg.ToolIDs; len(tools) > 0 {
		body["tool_ids"] = tools
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
		return "", err
	}
	if resp.Error != nil {
		return "", fmt.Errorf("open webui: %v", resp.Error)
	}
	if len(resp.Choices) == 0 {
		return "", fmt.Errorf("open webui returned no choices")
	}
	text := StripDetails(resp.Choices[0].Message.Content)
	if text == "" {
		text = "*No answer.*"
	}
	return text, nil
}

// Share publishes the conversation and opens the link to anyone who has it.
// Two calls: the share itself, then the access grant — a fresh share is
// readable by the account that made it until an `anyone` grant says otherwise.
func (o *openWebUI) Share(ctx context.Context, convID string) (string, error) {
	if convID == "" {
		return "", fmt.Errorf("no conversation to share")
	}
	var shared struct {
		ShareID string `json:"share_id"`
	}
	if err := o.do(ctx, http.MethodPost, "/api/v1/chats/"+convID+"/share", map[string]any{}, &shared); err != nil {
		return "", err
	}
	if shared.ShareID == "" {
		return "", fmt.Errorf("open webui returned no share id")
	}
	grants := map[string]any{"access_grants": []map[string]any{
		{"principal_type": "anyone", "principal_id": "*", "permission": "read"},
	}}
	if err := o.do(ctx, http.MethodPost, "/api/v1/chats/shared/"+convID+"/access/update", grants, nil); err != nil {
		return "", fmt.Errorf("shared, but could not open it to everyone: %w", err)
	}
	return o.publicURL() + "/s/" + shared.ShareID, nil
}

// Undo deletes one exchange from the chat. Open WebUI's own endpoint removes
// the message and its direct children — my question and the answers under it —
// re-parents anything below, and repairs the chat's current message id, which
// is exactly what taking back the last turn means. Rewinding the parent
// pointer alone would leave the exchange readable in the web UI.
func (o *openWebUI) Undo(ctx context.Context, convID, userMessageID string) error {
	if convID == "" || userMessageID == "" {
		return ErrUnsupported
	}
	return o.do(ctx, http.MethodDelete, "/api/v1/chats/"+convID+"/messages/"+userMessageID, nil, nil)
}

// Compact asks Open WebUI to summarise the branch in place: the conversation
// keeps its id, its link and its recent turns, and everything older becomes a
// summary the model still sees. Open WebUI does this automatically past
// chat.context_compaction.token_threshold — this is the same thing on demand.
func (o *openWebUI) Compact(ctx context.Context, convID string) (string, error) {
	if convID == "" {
		return "", fmt.Errorf("no conversation to compact")
	}
	var resp struct {
		Compacted    bool   `json:"compacted"`
		Reason       string `json:"reason"`
		Dropped      int    `json:"dropped_messages"`
		Kept         int    `json:"kept_messages"`
		ContextUsage *struct {
			Tokens    int `json:"tokens"`
			Threshold int `json:"threshold"`
			Percent   int `json:"percent"`
		} `json:"context_usage"`
	}
	if err := o.do(ctx, http.MethodPost, "/api/v1/chats/"+convID+"/compact", map[string]any{}, &resp); err != nil {
		return "", err
	}
	if !resp.Compacted {
		switch resp.Reason {
		case "disabled":
			return "", fmt.Errorf("context compaction is switched off in Open WebUI")
		case "too_short", "empty":
			return "Nothing to compact yet — the conversation is still short.", nil
		default:
			return "Nothing was compacted.", nil
		}
	}
	out := fmt.Sprintf("Compacted: %d messages summarised, %d kept.", resp.Dropped, resp.Kept)
	if u := resp.ContextUsage; u != nil && u.Threshold > 0 {
		out += fmt.Sprintf(" Context now %d%% of %s.", u.Percent, formatTokens(u.Threshold))
	}
	return out, nil
}

// ContextUsage reads how full the conversation is. The chat endpoint reports
// it, so this costs one GET and no model call.
func (o *openWebUI) ContextUsage(ctx context.Context, convID string) (string, error) {
	if convID == "" {
		return "", ErrUnsupported
	}
	var resp struct {
		ContextUsage *struct {
			Tokens    int `json:"tokens"`
			Threshold int `json:"threshold"`
			Percent   int `json:"percent"`
		} `json:"context_usage"`
	}
	if err := o.do(ctx, http.MethodGet, "/api/v1/chats/"+convID, nil, &resp); err != nil {
		return "", err
	}
	if resp.ContextUsage == nil || resp.ContextUsage.Threshold == 0 {
		return "", ErrUnsupported
	}
	return fmt.Sprintf("%d%% of %s (compacts automatically past that)",
		resp.ContextUsage.Percent, formatTokens(resp.ContextUsage.Threshold)), nil
}

// formatTokens prints a token budget the way it is usually spoken.
func formatTokens(n int) string {
	if n >= 1000 && n%1000 == 0 {
		return fmt.Sprintf("%dk", n/1000)
	}
	if n >= 1000 {
		return fmt.Sprintf("%.1fk", float64(n)/1000)
	}
	return fmt.Sprint(n)
}

// Models lists the ids Open WebUI will accept, which is every model the key
// can see — base models and workspace presets alike.
func (o *openWebUI) Models(ctx context.Context) ([]string, error) {
	var resp struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := o.do(ctx, http.MethodGet, "/api/models", nil, &resp); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(resp.Data))
	for _, m := range resp.Data {
		if m.ID != "" {
			out = append(out, m.ID)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (o *openWebUI) Transcribe(ctx context.Context, att Attachment) (string, error) {
	var resp struct {
		Text string `json:"text"`
	}
	if err := o.multipart(ctx, "/api/v1/audio/transcriptions", att, nil, &resp); err != nil {
		return "", err
	}
	return strings.TrimSpace(resp.Text), nil
}

// multipart posts one file as form field "file".
func (o *openWebUI) multipart(ctx context.Context, path string, att Attachment, fields map[string]string, out any) error {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	header := textproto.MIMEHeader{}
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, strings.ReplaceAll(att.Name, `"`, "")))
	header.Set("Content-Type", att.Mime)
	part, err := writer.CreatePart(header)
	if err != nil {
		return err
	}
	if _, err := part.Write(att.Data); err != nil {
		return err
	}
	for k, v := range fields {
		_ = writer.WriteField(k, v)
	}
	if err := writer.Close(); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(o.cfg.URL, "/")+path, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+o.cfg.Key())
	req.Header.Set("Content-Type", writer.FormDataContentType())
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
		return fmt.Errorf("open webui POST %s: HTTP %d: %s", path, resp.StatusCode, trim(string(data)))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
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

// phaseTracker reports a phase change to the sink once, not once per token.
type phaseTracker struct {
	last string
}

func (p *phaseTracker) to(sink Sink, phase string) {
	if phase == "" || phase == p.last {
		return
	}
	p.last = phase
	sink.Report(phase)
}

// webToolHints are the substrings that make a tool call somebody else's
// latency: a fetch over the network, not a local lookup. Matching is by name
// because that is all the event carries, and a false "web" is a better guess
// than a false "local tool" for something called "search".
var webToolHints = []string{"web", "search", "fetch", "browse", "crawl", "scrape", "url", "http"}

// toolPhase classifies a tool call for the progress marker.
func toolPhase(name string) string {
	lower := strings.ToLower(name)
	for _, hint := range webToolHints {
		if strings.Contains(lower, hint) {
			return "web"
		}
	}
	return "tools"
}
