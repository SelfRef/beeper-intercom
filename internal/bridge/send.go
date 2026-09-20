package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/notify"
	"github.com/SelfRef/beeper-intercom/internal/store"
)

// Event types the mautrix constants do not cover.
var (
	eventPollStart    = event.Type{Type: "org.matrix.msc3381.poll.start", Class: event.MessageEventType}
	eventPollResponse = event.Type{Type: "org.matrix.msc3381.poll.response", Class: event.MessageEventType}
	eventPollEnd      = event.Type{Type: "org.matrix.msc3381.poll.end", Class: event.MessageEventType}
)

// SendOptions are the knobs every outgoing message shares.
type SendOptions struct {
	ThreadRoot  id.EventID
	ReplyTo     id.EventID
	Notice      bool
	MentionRoom bool
	Profile     *notify.Profile
}

// Markdown renders Markdown to the Matrix HTML subset. Exported because the
// agent reply path needs exactly the same rendering as a notification.
func Markdown(text string) (plain, formatted string) {
	content := format.RenderMarkdown(text, true, false)
	if content.FormattedBody == "" {
		return content.Body, ""
	}
	return content.Body, content.FormattedBody
}

// SendNotification renders and delivers one announcement, plus any media, and
// returns the event ID of the message itself — the handle a later reaction,
// thread or action refers to.
func (b *Bridge) SendNotification(ctx context.Context, roomKey, ghostKey string, n *notify.Notification, threadRoot id.EventID) (id.EventID, error) {
	roomID, ok := b.RoomID(roomKey)
	if !ok {
		return "", fmt.Errorf("room %q has no Matrix room yet", roomKey)
	}
	room := b.conf().Rooms[roomKey]
	rendered := n.Render(Markdown)

	opts := SendOptions{
		ThreadRoot:  threadRoot,
		Notice:      n.Notice,
		MentionRoom: n.Urgent() && room.UrgentMentions,
		Profile:     n.Profile,
	}

	var eventID id.EventID
	if rendered.Body != "" {
		sent, err := b.sendText(ctx, roomID, ghostKey, rendered.Body, rendered.Formatted, opts)
		if err != nil {
			return "", err
		}
		eventID = sent
	}

	for _, media := range n.Media {
		mediaOpts := opts
		// Media hangs under the message it belongs to, so a notification with
		// a snapshot is one conversation item, not two.
		if mediaOpts.ThreadRoot == "" && eventID != "" {
			mediaOpts.ThreadRoot = eventID
		}
		sent, err := b.sendMedia(ctx, roomID, ghostKey, media, mediaOpts)
		if err != nil {
			b.log.Warn().Err(err).Str("url", media.URL).Msg("Failed to attach media")
			continue
		}
		if eventID == "" {
			eventID = sent
		}
	}
	if eventID == "" {
		return "", fmt.Errorf("nothing was sent")
	}
	return eventID, nil
}

// SendText is the plain path used by the agent reply and by status notices.
func (b *Bridge) SendText(ctx context.Context, roomID id.RoomID, ghostKey, body, formatted string, opts SendOptions) (id.EventID, error) {
	return b.sendText(ctx, roomID, ghostKey, body, formatted, opts)
}

func (b *Bridge) sendText(ctx context.Context, roomID id.RoomID, ghostKey, body, formatted string, opts SendOptions) (id.EventID, error) {
	content := &event.MessageEventContent{
		MsgType: event.MsgText,
		Body:    body,
	}
	if opts.Notice {
		content.MsgType = event.MsgNotice
	}
	if formatted != "" && formatted != body {
		content.Format = event.FormatHTML
		content.FormattedBody = formatted
	}
	if opts.MentionRoom {
		content.Mentions = &event.Mentions{Room: true}
	}
	if opts.Profile != nil {
		content.BeeperPerMessageProfile = &event.BeeperPerMessageProfile{
			ID:          opts.Profile.ID,
			Displayname: opts.Profile.Displayname,
		}
		if opts.Profile.AvatarURL != "" {
			uri := id.ContentURIString(opts.Profile.AvatarURL)
			content.BeeperPerMessageProfile.AvatarURL = &uri
		}
		// Clients that don't know the field still show who spoke.
		content.AddPerMessageProfileFallback()
	}
	applyRelations(content, opts)

	resp, err := b.Intent(ghostKey).SendMessageEvent(ctx, roomID, event.EventMessage, content)
	if err != nil {
		return "", err
	}
	return resp.EventID, nil
}

// SendEdit replaces an earlier message in place, which is how a streamed or
// split answer is finalised without a second bubble.
func (b *Bridge) SendEdit(ctx context.Context, roomID id.RoomID, ghostKey string, target id.EventID, body, formatted string) (id.EventID, error) {
	newContent := &event.MessageEventContent{MsgType: event.MsgText, Body: body}
	if formatted != "" && formatted != body {
		newContent.Format = event.FormatHTML
		newContent.FormattedBody = formatted
	}
	content := &event.MessageEventContent{
		MsgType:    event.MsgText,
		Body:       "* " + body,
		NewContent: newContent,
		RelatesTo:  &event.RelatesTo{Type: event.RelReplace, EventID: target},
	}
	if formatted != "" && formatted != body {
		content.Format = event.FormatHTML
		content.FormattedBody = "* " + formatted
	}
	resp, err := b.Intent(ghostKey).SendMessageEvent(ctx, roomID, event.EventMessage, content)
	if err != nil {
		return "", err
	}
	return resp.EventID, nil
}

func applyRelations(content *event.MessageEventContent, opts SendOptions) {
	switch {
	case opts.ThreadRoot != "":
		content.RelatesTo = &event.RelatesTo{
			Type:    event.RelThread,
			EventID: opts.ThreadRoot,
		}
		// The reply fallback inside a thread is what makes clients without
		// thread support show the message in the right place.
		fallback := opts.ReplyTo
		if fallback == "" {
			fallback = opts.ThreadRoot
		}
		content.RelatesTo.InReplyTo = &event.InReplyTo{EventID: fallback}
		content.RelatesTo.IsFallingBack = true
	case opts.ReplyTo != "":
		content.RelatesTo = &event.RelatesTo{
			InReplyTo: &event.InReplyTo{EventID: opts.ReplyTo},
		}
	}
}

// sendMedia downloads a file and re-uploads it, so the client never has to
// reach into the source's network (and a Frigate snapshot survives the clip
// being rotated away).
func (b *Bridge) sendMedia(ctx context.Context, roomID id.RoomID, ghostKey string, media notify.Media, opts SendOptions) (id.EventID, error) {
	sum := sha256.Sum256([]byte(media.URL))
	hash := hex.EncodeToString(sum[:])

	cached, err := b.store.Media(ctx, hash)
	if err != nil {
		return "", err
	}
	if cached == nil {
		cached, err = b.fetchAndUpload(ctx, ghostKey, media)
		if err != nil {
			return "", err
		}
		if err := b.store.PutMedia(ctx, hash, cached); err != nil {
			return "", err
		}
	}

	filename := media.Filename
	if filename == "" {
		filename = path.Base(strings.SplitN(media.URL, "?", 2)[0])
		if filename == "" || filename == "/" || filename == "." {
			filename = "attachment"
		}
	}
	body := filename
	if media.Caption != "" {
		body = media.Caption
	}

	uri, err := id.ParseContentURI(cached.MXC)
	if err != nil {
		return "", err
	}
	content := &event.MessageEventContent{
		MsgType: msgTypeFor(cached.Mime),
		Body:    body,
		URL:     uri.CUString(),
		Info: &event.FileInfo{
			MimeType: cached.Mime,
			Size:     int(cached.Size),
			Width:    cached.Width,
			Height:   cached.Height,
		},
	}
	if media.Caption != "" && content.MsgType != event.MsgFile {
		content.FileName = filename
	}
	applyRelations(content, opts)

	resp, err := b.Intent(ghostKey).SendMessageEvent(ctx, roomID, event.EventMessage, content)
	if err != nil {
		return "", err
	}
	return resp.EventID, nil
}

func (b *Bridge) fetchAndUpload(ctx context.Context, ghostKey string, media notify.Media) (*store.Media, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, media.URL, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 2 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("download %s: HTTP %d", media.URL, resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, b.conf().Limits.MaxMediaBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > b.conf().Limits.MaxMediaBytes {
		return nil, fmt.Errorf("attachment larger than limit of %d bytes", b.conf().Limits.MaxMediaBytes)
	}

	mime := media.Mime
	if mime == "" {
		mime = resp.Header.Get("Content-Type")
	}
	if mime == "" || strings.HasPrefix(mime, "application/octet-stream") {
		mime = http.DetectContentType(data)
	}
	mime = strings.SplitN(mime, ";", 2)[0]

	width, height := 0, 0
	if strings.HasPrefix(mime, "image/") {
		if cfg, _, err := image.DecodeConfig(strings.NewReader(string(data))); err == nil {
			width, height = cfg.Width, cfg.Height
		}
	}

	filename := media.Filename
	if filename == "" {
		filename = path.Base(strings.SplitN(media.URL, "?", 2)[0])
	}
	uploaded, err := b.Intent(ghostKey).UploadMedia(ctx, mautrix.ReqUploadMedia{
		ContentBytes:  data,
		ContentType:   mime,
		FileName:      filename,
		ContentLength: int64(len(data)),
	})
	if err != nil {
		return nil, err
	}
	return &store.Media{
		MXC:    uploaded.ContentURI.String(),
		Mime:   mime,
		Size:   int64(len(data)),
		Width:  width,
		Height: height,
	}, nil
}

func msgTypeFor(mime string) event.MessageType {
	switch {
	case strings.HasPrefix(mime, "image/"):
		return event.MsgImage
	case strings.HasPrefix(mime, "video/"):
		return event.MsgVideo
	case strings.HasPrefix(mime, "audio/"):
		return event.MsgAudio
	default:
		return event.MsgFile
	}
}

// DownloadMedia fetches an attachment the account owner sent. Rooms are
// unencrypted, so the appservice token can read it directly.
func (b *Bridge) DownloadMedia(ctx context.Context, uri id.ContentURI, limit int64) ([]byte, error) {
	resp, err := b.BotIntent().Download(ctx, uri)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("attachment larger than the %d byte limit", limit)
	}
	return data, nil
}

// React adds a reaction as a ghost — used for the ⚙️ "working on it" marker.
func (b *Bridge) React(ctx context.Context, roomID id.RoomID, ghostKey string, target id.EventID, key string) (id.EventID, error) {
	resp, err := b.Intent(ghostKey).SendReaction(ctx, roomID, target, key)
	if err != nil {
		return "", err
	}
	return resp.EventID, nil
}

// Redact removes an event as a ghost.
func (b *Bridge) Redact(ctx context.Context, roomID id.RoomID, ghostKey string, target id.EventID) error {
	_, err := b.Intent(ghostKey).RedactEvent(ctx, roomID, target)
	return err
}

// Typing shows the ghost as typing. Beeper expires the indicator, so a long
// answer needs this refreshed.
func (b *Bridge) Typing(ctx context.Context, roomID id.RoomID, ghostKey string, typing bool, timeout time.Duration) error {
	_, err := b.Intent(ghostKey).UserTyping(ctx, roomID, typing, timeout)
	return err
}

// SendStatus marks one of MY messages as failed. This is the real red error
// UI in the client, which is better than an apology in a chat bubble.
func (b *Bridge) SendStatus(ctx context.Context, roomID id.RoomID, target id.EventID, st event.MessageStatus, reason event.MessageStatusReason, message, internal string) error {
	content := &event.BeeperMessageStatusEventContent{
		Network:       b.conf().Network.ID,
		RelatesTo:     event.RelatesTo{Type: event.RelReference, EventID: target},
		Status:        st,
		Reason:        reason,
		Message:       message,
		InternalError: internal,
	}
	_, err := b.BotIntent().SendMessageEvent(ctx, roomID, event.BeeperMessageStatus, content)
	return err
}

// SendPoll asks a question the client renders as a poll — the confirmation
// primitive for anything irreversible.
func (b *Bridge) SendPoll(ctx context.Context, roomID id.RoomID, ghostKey, question string, answers []string, opts SendOptions) (id.EventID, error) {
	if len(answers) == 0 {
		answers = []string{"Yes", "No"}
	}
	pollAnswers := make([]map[string]any, 0, len(answers))
	for _, answer := range answers {
		pollAnswers = append(pollAnswers, map[string]any{
			"id":                      answerID(answer),
			"org.matrix.msc1767.text": []map[string]string{{"mimetype": "text/plain", "body": answer}},
		})
	}
	content := map[string]any{
		"org.matrix.msc3381.poll.start": map[string]any{
			"kind":           "org.matrix.msc3381.poll.disclosed",
			"max_selections": 1,
			"question": map[string]any{
				"org.matrix.msc1767.text": []map[string]string{{"mimetype": "text/plain", "body": question}},
			},
			"answers": pollAnswers,
		},
		// Fallback for anything that does not render polls.
		"org.matrix.msc1767.text": []map[string]string{
			{"mimetype": "text/plain", "body": question + "\n" + strings.Join(answers, " / ")},
		},
	}
	if opts.ThreadRoot != "" {
		content["m.relates_to"] = map[string]any{
			"rel_type": "m.thread",
			"event_id": opts.ThreadRoot.String(),
		}
	}
	resp, err := b.Intent(ghostKey).SendMessageEvent(ctx, roomID, eventPollStart, content)
	if err != nil {
		return "", err
	}
	return resp.EventID, nil
}

// ClosePoll ends a poll so the client stops accepting answers.
func (b *Bridge) ClosePoll(ctx context.Context, roomID id.RoomID, ghostKey string, poll id.EventID) error {
	_, err := b.Intent(ghostKey).SendMessageEvent(ctx, roomID, eventPollEnd, map[string]any{
		"org.matrix.msc3381.poll.end": map[string]any{},
		"m.relates_to": map[string]any{
			"rel_type": "m.reference",
			"event_id": poll.String(),
		},
		"org.matrix.msc1767.text": []map[string]string{{"mimetype": "text/plain", "body": "Poll closed"}},
	})
	return err
}

// answerID keeps poll answer ids stable and readable in the webhook payload.
func answerID(answer string) string {
	slug := strings.ToLower(strings.TrimSpace(answer))
	slug = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			return r
		case r == ' ' || r == '-' || r == '_':
			return '-'
		default:
			return -1
		}
	}, slug)
	if slug == "" {
		return uuid.NewString()
	}
	return slug
}

// SplitMessage breaks a long answer at paragraph boundaries. Beeper renders
// very long messages badly and some clients truncate them outright.
func SplitMessage(text string, limit int) []string {
	if len(text) <= limit {
		return []string{text}
	}
	var out []string
	var current strings.Builder
	for _, para := range strings.Split(text, "\n\n") {
		if current.Len() > 0 && current.Len()+len(para)+2 > limit {
			out = append(out, current.String())
			current.Reset()
		}
		// A single paragraph over the limit is split on lines as a last resort.
		for len(para) > limit {
			cut := strings.LastIndex(para[:limit], "\n")
			if cut <= 0 {
				cut = limit
			}
			out = append(out, para[:cut])
			para = strings.TrimPrefix(para[cut:], "\n")
		}
		if current.Len() > 0 {
			current.WriteString("\n\n")
		}
		current.WriteString(para)
	}
	if current.Len() > 0 {
		out = append(out, current.String())
	}
	return out
}

// UploadBytes puts an in-memory file on the media repo as a ghost — used for
// the .md attachment that carries an answer too long for a bubble.
func (b *Bridge) UploadBytes(ctx context.Context, ghostKey, filename, mime string, data []byte) (id.ContentURI, error) {
	resp, err := b.Intent(ghostKey).UploadMedia(ctx, mautrix.ReqUploadMedia{
		ContentBytes:  data,
		ContentType:   mime,
		FileName:      filename,
		ContentLength: int64(len(data)),
	})
	if err != nil {
		return id.ContentURI{}, err
	}
	return resp.ContentURI, nil
}

// SendFile sends an already-uploaded file.
func (b *Bridge) SendFile(ctx context.Context, roomID id.RoomID, ghostKey, filename, mime string, uri id.ContentURI, size int, opts SendOptions) (id.EventID, error) {
	content := &event.MessageEventContent{
		MsgType: msgTypeFor(mime),
		Body:    filename,
		URL:     uri.CUString(),
		Info:    &event.FileInfo{MimeType: mime, Size: size},
	}
	applyRelations(content, opts)
	resp, err := b.Intent(ghostKey).SendMessageEvent(ctx, roomID, event.EventMessage, content)
	if err != nil {
		return "", err
	}
	return resp.EventID, nil
}

// RoomConfig returns the config for a room key, for callers that have the key
// but not the config.
func (b *Bridge) RoomConfig(key string) (config.Room, bool) {
	room, ok := b.conf().Rooms[key]
	return room, ok
}
