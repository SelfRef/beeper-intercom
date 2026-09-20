// Package notify holds the one object every source produces and the rendering
// that turns it into what a Beeper client shows.
//
// Keeping this separate from both the HTTP API and the Matrix side is what
// makes a new source (Gotify, MQTT, a webhook shape nobody has invented yet) a
// new adapter rather than a change to rooms, dedupe, threads or actions.
package notify

import (
	"encoding/json"
	"fmt"
	"html"
	"strings"
)

// Priority levels, borrowed from ntfy so the mirror is lossless.
const (
	PriorityMin     = "min"
	PriorityLow     = "low"
	PriorityDefault = "default"
	PriorityHigh    = "high"
	PriorityUrgent  = "urgent"
)

// Notification is the internal shape. Sources fill it, the bridge renders it.
type Notification struct {
	// Room is a config key or a raw !room ID.
	Room string `json:"room"`
	// Ghost is a config key; empty means the room's first ghost.
	Ghost string `json:"ghost,omitempty"`

	Title string `json:"title,omitempty"`
	Text  string `json:"text,omitempty"`
	// HTML overrides the Markdown rendering of Text when set.
	HTML string `json:"html,omitempty"`

	Priority string   `json:"priority,omitempty"`
	URL      string   `json:"url,omitempty"`
	Tags     []string `json:"tags,omitempty"`
	Media    []Media  `json:"media,omitempty"`

	// Source is what made this notification, kept so a later reaction or
	// thread reply has the original payload to work with.
	Source *Source `json:"source,omitempty"`

	// Thread is an event ID, or "kind:id" naming an earlier notification's
	// source. The message becomes a thread reply under it.
	Thread string `json:"thread,omitempty"`

	// Dedupe is an idempotency key. A repeat returns the first event ID.
	Dedupe string `json:"dedupe,omitempty"`

	// Actions maps a reaction key to an action name. Reacting with one of
	// these keys on the delivered message fires that action.
	Actions map[string]string `json:"actions,omitempty"`

	// Profile makes one ghost speak as somebody else for this one message —
	// for a source that is really many authors (commits, forum posts).
	Profile *Profile `json:"profile,omitempty"`

	// Notice sends as m.notice instead of m.text.
	Notice bool `json:"notice,omitempty"`
}

// Media is a file to attach. The bridge downloads it and re-uploads it, so
// the client never reaches back into the sender's network.
type Media struct {
	URL      string `json:"url"`
	Filename string `json:"filename,omitempty"`
	Mime     string `json:"mime,omitempty"`
	Caption  string `json:"caption,omitempty"`
}

// Source identifies what produced a notification.
type Source struct {
	Kind    string          `json:"kind,omitempty"`
	ID      string          `json:"id,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Profile is a per-message identity (com.beeper.per_message_profile).
type Profile struct {
	ID          string `json:"id,omitempty"`
	Displayname string `json:"displayname"`
	AvatarURL   string `json:"avatar_url,omitempty"`
}

// Validate checks what the bridge cannot recover from later.
func (n *Notification) Validate() error {
	if n.Room == "" {
		return fmt.Errorf("room is required")
	}
	if n.Text == "" && n.Title == "" && n.HTML == "" && len(n.Media) == 0 {
		return fmt.Errorf("notification is empty")
	}
	switch n.Priority {
	case "", PriorityMin, PriorityLow, PriorityDefault, PriorityHigh, PriorityUrgent:
	default:
		return fmt.Errorf("unknown priority %q", n.Priority)
	}
	return nil
}

// Urgent reports whether this should raise an @room mention in a room that
// asked for them.
func (n *Notification) Urgent() bool {
	return n.Priority == PriorityHigh || n.Priority == PriorityUrgent
}

// Rendered is the message body pair a Matrix client needs.
type Rendered struct {
	Body      string
	Formatted string
}

// Render builds the plain and HTML bodies: bold title, the text as Markdown,
// the click-through URL as a trailing link, tags as hashtags.
//
// Markdown is rendered by the caller (the bridge has mautrix's Matrix-aware
// renderer); this function assembles the source text and the HTML around it.
func (n *Notification) Render(markdown func(string) (string, string)) Rendered {
	var body, formatted strings.Builder

	if n.Title != "" {
		body.WriteString(n.Title)
		formatted.WriteString("<strong>" + html.EscapeString(n.Title) + "</strong>")
		if n.Text != "" || n.HTML != "" {
			body.WriteString("\n")
			formatted.WriteString("<br/>")
		}
	}

	switch {
	case n.HTML != "":
		// The caller declared HTML; take it as given and derive a plain-text
		// fallback from the text field if there is one.
		if n.Text != "" {
			body.WriteString(n.Text)
		} else {
			body.WriteString(stripTags(n.HTML))
		}
		formatted.WriteString(n.HTML)
	case n.Text != "":
		plain, rich := markdown(n.Text)
		body.WriteString(plain)
		formatted.WriteString(rich)
	}

	if n.URL != "" {
		body.WriteString("\n" + n.URL)
		formatted.WriteString(fmt.Sprintf("<br/><a href=\"%s\">%s</a>",
			html.EscapeString(n.URL), html.EscapeString(n.URL)))
	}

	if tags := n.renderTags(); tags != "" {
		body.WriteString("\n" + tags)
		formatted.WriteString("<br/><em>" + html.EscapeString(tags) + "</em>")
	}

	return Rendered{Body: body.String(), Formatted: formatted.String()}
}

func (n *Notification) renderTags() string {
	if len(n.Tags) == 0 {
		return ""
	}
	parts := make([]string, 0, len(n.Tags))
	for _, tag := range n.Tags {
		if emoji, ok := Emoji(tag); ok {
			parts = append(parts, emoji)
		} else {
			parts = append(parts, "#"+tag)
		}
	}
	return strings.Join(parts, " ")
}

// stripTags is a last-resort plain-text fallback for caller-supplied HTML. It
// is not a sanitiser — the HTML goes to the client as given, because only the
// holder of the ingest token can set it.
func stripTags(in string) string {
	var out strings.Builder
	depth := 0
	for _, r := range in {
		switch {
		case r == '<':
			depth++
		case r == '>':
			if depth > 0 {
				depth--
			}
		case depth == 0:
			out.WriteRune(r)
		}
	}
	return html.UnescapeString(strings.TrimSpace(out.String()))
}

// FromNtfyPriority maps ntfy's 1-5 scale onto the names used here.
func FromNtfyPriority(p int) string {
	switch {
	case p <= 0:
		return ""
	case p == 1:
		return PriorityMin
	case p == 2:
		return PriorityLow
	case p == 3:
		return PriorityDefault
	case p == 4:
		return PriorityHigh
	default:
		return PriorityUrgent
	}
}
