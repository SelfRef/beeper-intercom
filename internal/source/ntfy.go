// Package source holds the inbound adapters that are not the native API.
package source

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/notify"
	"github.com/SelfRef/beeper-intercom/internal/store"
)

// Ntfy mirrors ntfy topics into rooms.
//
// ntfy has no outbound webhooks, but a topic is a JSON stream, so the bridge
// subscribes as one more client. The point is that nothing on the publishing
// side changes: existing scripts keep posting to ntfy, ntfy keeps working for
// the out-of-band case (the alarm that has to arrive when the rest of the
// deployment is down), and the same message also shows up in Beeper with
// history, threads and reactions.
type Ntfy struct {
	cfg   config.Source
	store *store.Store
	log   zerolog.Logger
	emit  Emit
}

// Emit is how an adapter hands a notification to the service.
type Emit func(ctx context.Context, n *notify.Notification) error

// NewNtfy builds the mirror.
func NewNtfy(cfg config.Source, st *store.Store, log zerolog.Logger, emit Emit) *Ntfy {
	return &Ntfy{cfg: cfg, store: st, log: log.With().Str("source", "ntfy").Logger(), emit: emit}
}

// Start subscribes to every configured topic, each in its own goroutine, and
// keeps them subscribed.
func (n *Ntfy) Start(ctx context.Context) {
	for _, topic := range config.SortedKeys(n.cfg.Topics) {
		go n.subscribe(ctx, topic, n.cfg.Topics[topic])
	}
}

type ntfyMessage struct {
	ID          string   `json:"id"`
	Time        int64    `json:"time"`
	Event       string   `json:"event"`
	Topic       string   `json:"topic"`
	Title       string   `json:"title"`
	Message     string   `json:"message"`
	Priority    int      `json:"priority"`
	Tags        []string `json:"tags"`
	Click       string   `json:"click"`
	ContentType string   `json:"content_type"`
	Attachment  *struct {
		Name string `json:"name"`
		URL  string `json:"url"`
		Type string `json:"type"`
		Size int64  `json:"size"`
	} `json:"attachment"`
}

func (n *Ntfy) subscribe(ctx context.Context, topic string, route config.NtfyTopic) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := n.stream(ctx, topic, route)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			n.log.Warn().Err(err).Str("topic", topic).Dur("retry_in", backoff).Msg("ntfy subscription dropped")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < time.Minute {
			backoff *= 2
		}
	}
}

func (n *Ntfy) stream(ctx context.Context, topic string, route config.NtfyTopic) error {
	since, err := n.since(ctx, topic)
	if err != nil {
		return err
	}
	resp, err := n.open(ctx, topic, since)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// An id that has fallen out of ntfy's cache is rejected outright rather
	// than treated as "from the beginning", so the fallback is a timestamp.
	if resp.StatusCode == http.StatusBadRequest && strings.HasPrefix(since, "id:") {
		resp.Body.Close()
		fallback := strconv.FormatInt(time.Now().Add(-1*time.Hour).Unix(), 10)
		n.log.Warn().Str("topic", topic).Msg("ntfy rejected the stored since id; falling back to the last hour")
		resp, err = n.open(ctx, topic, fallback)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("ntfy %s: HTTP %d", topic, resp.StatusCode)
	}

	n.log.Info().Str("topic", topic).Str("room", route.Room).Msg("Mirroring ntfy topic")

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var msg ntfyMessage
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			n.log.Debug().Err(err).Str("line", line).Msg("Unparseable ntfy line")
			continue
		}
		// open and keepalive carry no content but do advance the stream.
		if msg.Event != "message" {
			continue
		}
		if route.MinPriority > 0 && msg.Priority > 0 && msg.Priority < route.MinPriority {
			n.remember(ctx, topic, msg.ID)
			continue
		}
		if err := n.emit(ctx, n.convert(&msg, route)); err != nil {
			// Do not advance `since`: a failure here should be retried on the
			// next reconnect rather than silently dropped.
			n.log.Error().Err(err).Str("topic", topic).Msg("Failed to mirror ntfy message")
			continue
		}
		n.remember(ctx, topic, msg.ID)
	}
	return scanner.Err()
}

func (n *Ntfy) open(ctx context.Context, topic, since string) (*http.Response, error) {
	url := fmt.Sprintf("%s/%s/json", strings.TrimSuffix(n.cfg.URL, "/"), topic)
	if since != "" {
		url += "?since=" + strings.TrimPrefix(since, "id:")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if n.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+n.cfg.Token)
	}
	// No client timeout: this is a long-lived stream, and ntfy sends
	// keepalives. Cancellation comes from ctx.
	return (&http.Client{}).Do(req)
}

func (n *Ntfy) since(ctx context.Context, topic string) (string, error) {
	stored, err := n.store.GetKV(ctx, "ntfy_since:"+topic)
	if err != nil {
		return "", err
	}
	if stored == "" {
		// First run: start from now, not from "all". Replaying a month of
		// backlog into a fresh room on first start is not a feature.
		return strconv.FormatInt(time.Now().Unix(), 10), nil
	}
	return "id:" + stored, nil
}

func (n *Ntfy) remember(ctx context.Context, topic, id string) {
	if id == "" {
		return
	}
	if err := n.store.SetKV(ctx, "ntfy_since:"+topic, id); err != nil {
		n.log.Error().Err(err).Msg("Failed to persist the ntfy cursor")
	}
}

// convert maps ntfy's fields onto the internal notification. The mapping is
// the contract: title to a bold first line, priority 4-5 to an @room mention
// in a room that asked for one, tags to emoji, click to a trailing link,
// attachment re-uploaded, and the ntfy message id as the dedupe key.
func (n *Ntfy) convert(msg *ntfyMessage, route config.NtfyTopic) *notify.Notification {
	out := &notify.Notification{
		Room:     route.Room,
		Ghost:    route.Ghost,
		Title:    msg.Title,
		Text:     msg.Message,
		Priority: notify.FromNtfyPriority(msg.Priority),
		URL:      msg.Click,
		Tags:     msg.Tags,
		Dedupe:   "ntfy:" + msg.Topic + ":" + msg.ID,
		Source: &notify.Source{
			Kind: "ntfy",
			ID:   msg.ID,
		},
	}
	if payload, err := json.Marshal(msg); err == nil {
		out.Source.Payload = payload
	}
	if msg.Attachment != nil && msg.Attachment.URL != "" {
		out.Media = []notify.Media{{
			URL:      msg.Attachment.URL,
			Filename: msg.Attachment.Name,
			Mime:     msg.Attachment.Type,
		}}
	}
	// ntfy's own markdown flag decides whether the body is Markdown; without
	// it, a message full of underscores should not come out italic.
	if !strings.Contains(msg.ContentType, "markdown") {
		out.HTML = htmlEscape(msg.Message)
	}
	return out
}

func htmlEscape(text string) string {
	replacer := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\n", "<br/>")
	return replacer.Replace(text)
}
