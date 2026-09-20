package bridge

import (
	"context"
	"time"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// The appservice does not receive events sent by its own ghosts, so the
// filtering here is belt and braces — but a bridge that echoes its own
// announcements back into an agent turn is a loop, and loops cost money.
func (b *Bridge) handleMessage(ctx context.Context, evt *event.Event) {
	roomKey, ok := b.RoomKey(evt.RoomID)
	if !ok || b.IsOwnGhost(evt.Sender) {
		return
	}
	content, ok := evt.Content.Parsed.(*event.MessageEventContent)
	if !ok || content == nil {
		return
	}
	// An edit the BRIDGE made to one of my own messages (see EditMine): it
	// carries no new instruction, and handling it would re-run the command it
	// is only there to reformat.
	if raw, ok := evt.Content.Raw[CommandFormatKey].(bool); ok && raw {
		return
	}
	if b.handlers.OnMessage == nil {
		return
	}

	msg := &Message{
		RoomKey: roomKey,
		RoomID:  evt.RoomID,
		EventID: evt.ID,
		Sender:  evt.Sender,
		Body:    content.Body,
		Content: content,
		Time:    time.UnixMilli(evt.Timestamp),
	}
	switch content.MsgType {
	case event.MsgImage, event.MsgFile, event.MsgAudio, event.MsgVideo:
		if content.URL != "" {
			if uri, err := content.URL.Parse(); err == nil {
				msg.Attachment = &Attachment{
					URL:   uri,
					Name:  content.FileName,
					Kind:  content.MsgType,
					Voice: content.MSC3245Voice != nil,
				}
				if content.Info != nil {
					msg.Attachment.Mime = content.Info.MimeType
					msg.Attachment.Size = content.Info.Size
				}
				// Without a separate filename the body IS the filename, not a
				// caption; with one, the body is whatever the user typed.
				if content.FileName == "" {
					msg.Attachment.Name = content.Body
					msg.Body = ""
				} else if content.Body == content.FileName {
					msg.Body = ""
				}
			}
		}
	}
	if rel := content.RelatesTo; rel != nil {
		switch rel.Type {
		case event.RelThread:
			msg.ThreadRoot = rel.EventID
			if rel.InReplyTo != nil && !rel.IsFallingBack {
				msg.ReplyTo = rel.InReplyTo.EventID
			}
		case event.RelReplace:
			msg.Edits = rel.EventID
			if content.NewContent != nil {
				msg.Body = content.NewContent.Body
				msg.Content = content.NewContent
			}
		}
		if rel.InReplyTo != nil && msg.ReplyTo == "" && rel.Type != event.RelThread {
			msg.ReplyTo = rel.InReplyTo.EventID
		}
	}
	b.handlers.OnMessage(ctx, msg)
}

func (b *Bridge) handleReaction(ctx context.Context, evt *event.Event) {
	roomKey, ok := b.RoomKey(evt.RoomID)
	if !ok || b.IsOwnGhost(evt.Sender) || b.handlers.OnReaction == nil {
		return
	}
	content, ok := evt.Content.Parsed.(*event.ReactionEventContent)
	if !ok || content == nil {
		return
	}
	b.handlers.OnReaction(ctx, &Reaction{
		RoomKey: roomKey,
		RoomID:  evt.RoomID,
		EventID: evt.ID,
		Sender:  evt.Sender,
		Key:     content.RelatesTo.Key,
		Target:  content.RelatesTo.EventID,
	})
}

func (b *Bridge) handleRedaction(ctx context.Context, evt *event.Event) {
	roomKey, ok := b.RoomKey(evt.RoomID)
	if !ok || b.IsOwnGhost(evt.Sender) || b.handlers.OnRedaction == nil {
		return
	}
	target := evt.Redacts
	if target == "" {
		if content, ok := evt.Content.Parsed.(*event.RedactionEventContent); ok && content != nil {
			target = content.Redacts
		}
	}
	if target == "" {
		return
	}
	b.handlers.OnRedaction(ctx, roomKey, evt.RoomID, target)
}

// handlePollResponse parses the MSC3381 answer. mautrix has no typed struct
// for this, so it is read out of the raw content.
func (b *Bridge) handlePollResponse(ctx context.Context, evt *event.Event) {
	roomKey, ok := b.RoomKey(evt.RoomID)
	if !ok || b.IsOwnGhost(evt.Sender) || b.handlers.OnPollResponse == nil {
		return
	}
	raw := evt.Content.Raw
	if raw == nil {
		return
	}
	response, _ := raw["org.matrix.msc3381.poll.response"].(map[string]any)
	relates, _ := raw["m.relates_to"].(map[string]any)
	if response == nil || relates == nil {
		return
	}
	pollID, _ := relates["event_id"].(string)
	if pollID == "" {
		return
	}
	rawAnswers, _ := response["answers"].([]any)
	answers := make([]string, 0, len(rawAnswers))
	for _, a := range rawAnswers {
		if s, ok := a.(string); ok {
			answers = append(answers, s)
		}
	}
	b.handlers.OnPollResponse(ctx, &PollResponse{
		RoomKey: roomKey,
		RoomID:  evt.RoomID,
		Sender:  evt.Sender,
		PollID:  id.EventID(pollID),
		Answers: answers,
	})
}
