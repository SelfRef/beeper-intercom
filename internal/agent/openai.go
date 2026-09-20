package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/store"
)

// openAI is the universal fallback: anything that speaks
// POST /v1/chat/completions — llama.cpp's server, LibreChat, LobeChat, vLLM.
// There is no conversation on the other side, so the bridge keeps the
// transcript itself and replays a rolling window of it on every turn.
type openAI struct {
	cfg    config.Agent
	client *http.Client
	store  *store.Store
	log    zerolog.Logger
}

func newOpenAI(cfg config.Agent, client *http.Client, st *store.Store, log zerolog.Logger) *openAI {
	return &openAI{cfg: cfg, client: client, store: st, log: log}
}

func (o *openAI) Type() string { return config.AgentOpenAI }

func (o *openAI) Caps() Caps { return Caps{Streaming: !o.cfg.NoStream} }

func (o *openAI) Link(string) string { return "" }

func (o *openAI) NewConversation(ctx context.Context, seed *Seed) (string, error) {
	convID := uuid.NewString()
	if prompt := SeedPrompt(seed); prompt != "" {
		// Stored as a user turn rather than a system one: it is data, and a
		// system prompt is the one place data must never end up.
		if err := o.store.AppendTranscript(ctx, convID, "user", prompt); err != nil {
			return "", err
		}
	}
	return convID, nil
}

func (o *openAI) Send(ctx context.Context, conv Conversation, turn Turn, sink Sink) (*Reply, error) {
	history, err := o.store.Transcript(ctx, conv.ID, o.cfg.MaxHistory)
	if err != nil {
		return nil, err
	}

	messages := make([]map[string]string, 0, len(history)+2)
	if o.cfg.SystemPrompt != "" {
		messages = append(messages, map[string]string{"role": "system", "content": o.cfg.SystemPrompt})
	}
	for _, entry := range history {
		messages = append(messages, map[string]string{"role": entry.Role, "content": entry.Content})
	}
	messages = append(messages, map[string]string{"role": "user", "content": turn.Text})
	// Images go in as OpenAI vision parts; anything else is named so the model
	// knows something was sent that it cannot see.
	var userContent any = turn.Text
	if len(turn.Attachments) > 0 {
		parts := []map[string]any{{"type": "text", "text": turn.Text}}
		for _, att := range turn.Attachments {
			if strings.HasPrefix(att.Mime, "image/") {
				parts = append(parts, map[string]any{"type": "image_url", "image_url": map[string]any{"url": dataURL(att)}})
			} else {
				parts[0]["text"] = parts[0]["text"].(string) + fmt.Sprintf("\n\n(attached file %q, %s, %d bytes — not readable by this backend)", att.Name, att.Mime, len(att.Data))
			}
		}
		userContent = parts
	}

	model := conv.Model
	if model == "" {
		model = o.cfg.Model
	}
	// Not "thinking": until a delta arrives this is the queue and the prompt,
	// and a model with reasoning off never thinks at all.
	sink.Report("queued")

	stream := sink.Streams() && !o.cfg.NoStream
	payload := make([]any, 0, len(messages))
	for i, m := range messages {
		if i == len(messages)-1 {
			payload = append(payload, map[string]any{"role": "user", "content": userContent})
		} else {
			payload = append(payload, m)
		}
	}
	body := map[string]any{"model": model, "messages": payload, "stream": stream}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimSuffix(o.cfg.URL, "/")+"/chat/completions", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if key := o.cfg.Key(); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	for k, v := range o.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := o.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		return nil, fmt.Errorf("openai endpoint: HTTP %d: %s", resp.StatusCode, trim(string(data)))
	}

	var answer string
	if stream {
		answer, err = o.readStream(resp.Body, sink)
	} else {
		answer, err = o.readOneShot(resp.Body)
	}
	if err != nil {
		return nil, err
	}

	// No conversation id means no conversation: Ask uses that to get an answer
	// that leaves nothing behind.
	if conv.ID != "" {
		if err := o.store.AppendTranscript(ctx, conv.ID, "user", turn.Text); err != nil {
			return nil, err
		}
		if err := o.store.AppendTranscript(ctx, conv.ID, "assistant", answer); err != nil {
			return nil, err
		}
	}
	return &Reply{Text: answer}, nil
}

func (o *openAI) readOneShot(body io.Reader) (string, error) {
	data, err := io.ReadAll(io.LimitReader(body, 8<<20))
	if err != nil {
		return "", err
	}
	var parsed struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return "", fmt.Errorf("openai endpoint: bad response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return "", fmt.Errorf("openai endpoint returned no choices")
	}
	return parsed.Choices[0].Message.Content, nil
}

// readStream consumes chat.completion.chunk events, handing each content
// delta to the sink and returning the whole answer at the end.
func (o *openAI) readStream(body io.Reader, sink Sink) (string, error) {
	var answer strings.Builder
	var phase phaseTracker
	err := readSSE(body, func(data []byte) error {
		chunk, err := parseOpenAIChunk(data)
		if err != nil {
			return fmt.Errorf("openai endpoint: bad chunk: %w", err)
		}
		if chunk.Error != nil {
			return fmt.Errorf("openai endpoint: %v", chunk.Error)
		}
		for _, choice := range chunk.Choices {
			if choice.Delta.Content != "" {
				phase.to(sink, "writing")
				answer.WriteString(choice.Delta.Content)
				sink.Push(choice.Delta.Content)
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return answer.String(), nil
}

// Cancel is a no-op: a plain completions endpoint has nothing to cancel
// beyond dropping the request, which the caller's context already does.
func (o *openAI) Cancel(context.Context, string) error { return nil }

// Transcribe uses the same endpoint family's /audio/transcriptions, which
// llama.cpp's server and most OpenAI-compatible stacks provide.
// Ask answers one question with no transcript around it. The bridge keeps the
// history for this adapter, so "outside the conversation" is simply a request
// that does not include it.
func (o *openAI) Ask(ctx context.Context, model, question string) (string, error) {
	if model == "" {
		model = o.cfg.Model
	}
	// Send with no conversation: this adapter's history comes from the bridge,
	// so a turn handed an empty transcript is a question on its own.
	reply, err := o.Send(ctx, Conversation{Model: model}, Turn{Text: question, Internal: true}, Sink{})
	if err != nil {
		return "", err
	}
	return reply.Text, nil
}

// Share: a bare OpenAI endpoint has no conversation to publish.
// Undo drops the last exchange from the transcript the bridge keeps for this
// backend — for an adapter with no server-side history, that IS the history.
func (o *openAI) Answer(ctx context.Context, conv Conversation, ask *Ask, answers map[string]string, sink Sink) (*Reply, error) {
	return nil, ErrUnsupported
}

func (o *openAI) Undo(ctx context.Context, convID, _ string) error {
	if o.store == nil {
		return ErrUnsupported
	}
	return o.store.TrimTranscript(ctx, convID, 2)
}

func (o *openAI) Share(ctx context.Context, convID string) (string, error) {
	return "", ErrUnsupported
}

// Compact: the bridge keeps this adapter's history, so compaction is its own
// job — closing the conversation and seeding the next one with a summary.
func (o *openAI) Compact(ctx context.Context, convID string) (string, error) {
	return "", ErrUnsupported
}

// ContextUsage: nothing here counts tokens.
func (o *openAI) ContextUsage(ctx context.Context, convID string) (string, error) {
	return "", ErrUnsupported
}

// Models asks the endpoint what it serves. Anything OpenAI-shaped answers
// /v1/models; one that does not simply has no list to offer.
func (o *openAI) Models(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(o.cfg.URL, "/")+"/models", nil)
	if err != nil {
		return nil, err
	}
	if key := o.cfg.Key(); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := o.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("model list: %s", resp.Status)
	}
	var out struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(out.Data))
	for _, m := range out.Data {
		if m.ID != "" {
			ids = append(ids, m.ID)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func (o *openAI) Transcribe(ctx context.Context, att Attachment) (string, error) {
	return postTranscription(ctx, o.client, strings.TrimSuffix(o.cfg.URL, "/")+"/audio/transcriptions", o.cfg.Key(), o.cfg.Headers, att, o.cfg.Model)
}
