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

func (w *webhook) Caps() Caps { return Caps{Cancel: true} }

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

func (w *webhook) Send(ctx context.Context, conv Conversation, turn Turn, sink Sink) (*Reply, error) {
	if sink.Status != nil {
		sink.Status("thinking")
	}
	var resp struct {
		Text string `json:"text"`
		Link string `json:"link"`
	}
	body := map[string]any{
		"conv_id":  conv.ID,
		"text":     turn.Text,
		"internal": turn.Internal,
		"turns":    conv.Turns,
	}
	if err := w.do(ctx, "/turn", body, &resp); err != nil {
		return nil, err
	}
	if resp.Text == "" {
		return nil, fmt.Errorf("webhook agent returned an empty answer")
	}
	return &Reply{Text: resp.Text, Link: resp.Link}, nil
}

func (w *webhook) Cancel(ctx context.Context, convID string) error {
	err := w.do(ctx, "/cancel", map[string]any{"conv_id": convID}, nil)
	if isNotFound(err) {
		return nil
	}
	return err
}

func (w *webhook) do(ctx context.Context, path string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(w.cfg.URL, "/")+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if key := w.cfg.Key(); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	for k, v := range w.cfg.Headers {
		req.Header.Set(k, v)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNotFound {
		return errNotFound
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook agent %s: HTTP %d: %s", path, resp.StatusCode, trim(string(data)))
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
