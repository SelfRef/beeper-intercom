package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

	model := conv.Model
	if model == "" {
		model = o.cfg.Model
	}
	if sink.Status != nil {
		sink.Status("thinking")
	}

	stream := sink.Delta != nil && !o.cfg.NoStream
	body := map[string]any{"model": model, "messages": messages, "stream": stream}
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

	if err := o.store.AppendTranscript(ctx, conv.ID, "user", turn.Text); err != nil {
		return nil, err
	}
	if err := o.store.AppendTranscript(ctx, conv.ID, "assistant", answer); err != nil {
		return nil, err
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
				answer.WriteString(choice.Delta.Content)
				sink.Delta(choice.Delta.Content)
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
