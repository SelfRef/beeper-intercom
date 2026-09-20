package agent

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/SelfRef/beeper-intercom/internal/config"
)

// webhook is the external adapter protocol: anything that can answer three
// HTTP calls can be the agent, in any language.
//
//	POST <url>/new     {"seed": {"kind": "...", "text": "..."}}
//	                   -> {"conv_id": "..."}        (optional; a missing id
//	                      means the sidecar is stateless and the bridge
//	                      generates one)
//	POST <url>/turn    {"conv_id": "...", "text": "...", "internal": false}
//	                   -> {"text": "...", "link": "..."}
//	                   or, to stream, a text/event-stream response of
//	                      data: {"delta": "..."}   (any number)
//	                      data: {"text": "...", "link": "..."}  (optional final)
//	                      data: [DONE]
//	POST <url>/cancel  {"conv_id": "..."} -> anything
//
// A 404 on /new or /cancel is not an error: those are optional.
type webhook struct {
	cfg    config.Agent
	client *http.Client
	log    zerolog.Logger
}

func newWebhook(cfg config.Agent, client *http.Client, log zerolog.Logger) *webhook {
	return &webhook{cfg: cfg, client: client, log: log}
}

func (w *webhook) Type() string { return config.AgentWebhook }

func (w *webhook) Caps() Caps { return Caps{Streaming: !w.cfg.NoStream, Cancel: true} }

func (w *webhook) Link(string) string { return "" }

func (w *webhook) NewConversation(ctx context.Context, seed *Seed) (string, error) {
	body := map[string]any{}
	if seed != nil {
		body["seed"] = map[string]any{"kind": seed.Kind, "text": seed.Text, "room": seed.Room}
	}
	var resp struct {
		ConvID string `json:"conv_id"`
	}
	if err := w.do(ctx, "/new", body, &resp); err != nil {
		if isNotFound(err) {
			return uuid.NewString(), nil
		}
		return "", err
	}
	if resp.ConvID == "" {
		return uuid.NewString(), nil
	}
	return resp.ConvID, nil
}

// turnReply is the JSON shape of a finished turn, whether it arrived as one
// body or as the last event of a stream.
type turnReply struct {
	Text  string `json:"text"`
	Link  string `json:"link"`
	Delta string `json:"delta"`
}

func (w *webhook) Send(ctx context.Context, conv Conversation, turn Turn, sink Sink) (*Reply, error) {
	if sink.Status != nil {
		sink.Status("thinking")
	}
	body := map[string]any{
		"conv_id":  conv.ID,
		"text":     turn.Text,
		"internal": turn.Internal,
		"turns":    conv.Turns,
		// Tells the sidecar it may stream; one that cannot just answers JSON.
		"stream": sink.Delta != nil && !w.cfg.NoStream,
	}
	if len(turn.Attachments) > 0 {
		atts := make([]map[string]any, 0, len(turn.Attachments))
		for _, att := range turn.Attachments {
			atts = append(atts, map[string]any{
				"name": att.Name, "mime": att.Mime, "size": len(att.Data),
				"data_base64": base64.StdEncoding.EncodeToString(att.Data),
			})
		}
		body["attachments"] = atts
	}
	resp, err := w.post(ctx, "/turn", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		return w.readStream(resp.Body, sink)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	var reply turnReply
	if err := json.Unmarshal(data, &reply); err != nil {
		return nil, fmt.Errorf("webhook agent /turn: bad response: %w", err)
	}
	if reply.Text == "" {
		return nil, fmt.Errorf("webhook agent returned an empty answer")
	}
	return &Reply{Text: reply.Text, Link: reply.Link}, nil
}

// readStream accepts deltas and an optional final message. If the sidecar
// sends no final text, the deltas concatenated are the answer.
func (w *webhook) readStream(body io.Reader, sink Sink) (*Reply, error) {
	var answer strings.Builder
	var final turnReply
	err := readSSE(body, func(data []byte) error {
		var evt turnReply
		if err := json.Unmarshal(data, &evt); err != nil {
			return fmt.Errorf("webhook agent stream: bad event: %w", err)
		}
		if evt.Delta != "" {
			answer.WriteString(evt.Delta)
			if sink.Delta != nil {
				sink.Delta(evt.Delta)
			}
		}
		if evt.Text != "" || evt.Link != "" {
			final = evt
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	text := final.Text
	if text == "" {
		text = answer.String()
	}
	if text == "" {
		return nil, fmt.Errorf("webhook agent returned an empty answer")
	}
	return &Reply{Text: text, Link: final.Link}, nil
}

// Transcribe asks the sidecar: POST /transcribe with the audio as base64,
// expecting {"text": "..."}. A 404 means it does not do voice.
func (w *webhook) Transcribe(ctx context.Context, att Attachment) (string, error) {
	var resp struct {
		Text string `json:"text"`
	}
	err := w.do(ctx, "/transcribe", map[string]any{
		"name": att.Name, "mime": att.Mime, "data_base64": base64.StdEncoding.EncodeToString(att.Data),
	}, &resp)
	if isNotFound(err) {
		return "", ErrUnsupported
	}
	if err != nil {
		return "", err
	}
	return resp.Text, nil
}

func (w *webhook) Cancel(ctx context.Context, convID string) error {
	err := w.do(ctx, "/cancel", map[string]any{"conv_id": convID}, nil)
	if isNotFound(err) {
		return nil
	}
	return err
}

func (w *webhook) post(ctx context.Context, path string, body any) (*http.Response, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(w.cfg.URL, "/")+path, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if key := w.cfg.Key(); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	for k, v := range w.cfg.Headers {
		req.Header.Set(k, v)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, errNotFound
	}
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		return nil, fmt.Errorf("webhook agent %s: HTTP %d: %s", path, resp.StatusCode, trim(string(data)))
	}
	return resp, nil
}

func (w *webhook) do(ctx context.Context, path string, body, out any) error {
	resp, err := w.post(ctx, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

type notFoundError struct{}

func (notFoundError) Error() string { return "endpoint not implemented" }

var errNotFound = notFoundError{}

func isNotFound(err error) bool {
	_, ok := err.(notFoundError)
	return ok
}
