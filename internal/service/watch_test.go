package service

import (
	"testing"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/config"
)

// TestWatchEvent pins the mapping onto beeper-watch's hook format: a rule
// that silently never matches is the failure mode, so every kind is checked.
func TestWatchEvent(t *testing.T) {
	network := config.Network{Bridge: "sh-test", ID: "test"}
	owner := id.UserID("@me:beeper.com")
	mk := func(typ event.Type, sender id.UserID, parsed any) *event.Event {
		return &event.Event{Type: typ, Sender: sender, RoomID: "!r:x", ID: "$e", Timestamp: 42,
			Content: event.Content{Parsed: parsed}}
	}

	msg := watchEvent(network, owner, mk(event.EventMessage, owner, &event.MessageEventContent{MsgType: event.MsgText, Body: "hi"}))
	if msg["kind"] != "message" || msg["direction"] != "out" || msg["is_self"] != true || msg["body"] != "hi" ||
		msg["network"] != "test" || msg["bridge"] != "sh-test" || msg["room_id"] != "!r:x" {
		t.Fatalf("message: %v", msg)
	}

	edit := watchEvent(network, owner, mk(event.EventMessage, owner, &event.MessageEventContent{
		MsgType: event.MsgText, Body: "* new",
		NewContent: &event.MessageEventContent{Body: "new"},
		RelatesTo:  &event.RelatesTo{Type: event.RelReplace, EventID: "$old"},
	}))
	if edit["kind"] != "edit" || edit["edits"] != "$old" || edit["body"] != "new" {
		t.Fatalf("edit: %v", edit)
	}

	reply := watchEvent(network, owner, mk(event.EventMessage, "@other:x", &event.MessageEventContent{
		MsgType: event.MsgText, Body: "ok",
		RelatesTo: &event.RelatesTo{InReplyTo: &event.InReplyTo{EventID: "$q"}},
	}))
	if reply["direction"] != "in" || reply["is_self"] != false || reply["reply_to"] != "$q" {
		t.Fatalf("reply: %v", reply)
	}

	reaction := watchEvent(network, owner, mk(event.EventReaction, owner, &event.ReactionEventContent{
		RelatesTo: event.RelatesTo{Type: event.RelAnnotation, EventID: "$m", Key: "👍"},
	}))
	if reaction["kind"] != "reaction" || reaction["reaction_key"] != "👍" {
		t.Fatalf("reaction: %v", reaction)
	}

	deletion := watchEvent(network, owner, mk(event.EventRedaction, owner, &event.RedactionEventContent{Redacts: "$gone"}))
	if deletion["kind"] != "deletion" || deletion["redacts"] != "$gone" {
		t.Fatalf("deletion: %v", deletion)
	}

	if other := watchEvent(network, owner, mk(event.StateTopic, owner, &event.TopicEventContent{})); other != nil {
		t.Fatalf("a state event has no kind: %v", other)
	}
}
