package bridge

import (
	"fmt"
	"strings"
	"testing"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/config"
)

// The whole point of declaring room features is the tombstone: a redaction in
// a bridge room should leave nothing behind unless the room asked for it.
func TestRoomFeaturesHideDeletePlaceholder(t *testing.T) {
	b := &Bridge{}
	b.cfg.Store(&config.Config{})

	chat := b.roomFeatures(config.Room{Kind: config.KindDM})
	if !chat.DeleteHide {
		t.Error("a dm should hide the delete placeholder by default")
	}
	if chat.Delete != event.CapLevelFullySupported || chat.Edit != event.CapLevelFullySupported {
		t.Error("a conversational room can edit and delete")
	}

	marked := b.roomFeatures(config.Room{Kind: config.KindDM, DeletePlaceholder: true})
	if marked.DeleteHide {
		t.Error("delete_placeholder: true must keep the tombstone")
	}

	// Nothing is sent into a broadcast room, so there is nothing to take back.
	broadcast := b.roomFeatures(config.Room{Kind: config.KindBroadcast})
	if broadcast.Delete != event.CapLevelUnsupported || broadcast.Edit != event.CapLevelUnsupported {
		t.Error("a broadcast room should not offer edit or delete")
	}
	if broadcast.Reaction != event.CapLevelFullySupported {
		t.Error("reactions are how a broadcast room is answered; they must stay")
	}
}

// The tombstone is suppressed by a key on the redaction event, and the room
// decides whether it goes on. A room the bridge does not know keeps its
// markers.
func TestHidesDeletePlaceholder(t *testing.T) {
	b := &Bridge{roomKey: map[id.RoomID]string{
		"!chat:beeper.local":   "chat",
		"!alerts:beeper.local": "alerts",
	}}
	b.cfg.Store(&config.Config{Rooms: map[string]config.Room{
		"chat":   {Kind: config.KindDM},
		"alerts": {Kind: config.KindBroadcast, DeletePlaceholder: true},
	}})

	for roomID, want := range map[id.RoomID]bool{
		"!chat:beeper.local":     true,  // hides it: the bridge tidies up after itself
		"!alerts:beeper.local":   false, // asked for the marker
		"!stranger:beeper.local": false, // not ours to erase from
	} {
		if got := b.hidesDeletePlaceholder(roomID); got != want {
			t.Errorf("hidesDeletePlaceholder(%s) = %v, want %v", roomID, got, want)
		}
	}
}

// Beeper's client reads a poll's text as a plain string and renders an empty
// bubble for anything else — the extensible-events array form included
// (measured 2026-09-20 against the desktop bundle).
func TestPollTextIsAString(t *testing.T) {
	content := pollContent("Coffee or tea?", []string{"Coffee", "Tea"})
	if _, ok := content[pollTextKey].(string); !ok {
		t.Errorf("top-level %s is %T, want a string", pollTextKey, content[pollTextKey])
	}
	start, ok := content["org.matrix.msc3381.poll.start"].(map[string]any)
	if !ok {
		t.Fatal("no poll.start object")
	}
	question, ok := start["question"].(map[string]any)
	if !ok {
		t.Fatal("no question object")
	}
	if text, ok := question[pollTextKey].(string); !ok || text != "Coffee or tea?" {
		t.Errorf("question text = %#v, want the question as a string", question[pollTextKey])
	}
	answers, ok := start["answers"].([]map[string]any)
	if !ok || len(answers) != 2 {
		t.Fatalf("answers = %#v", start["answers"])
	}
	for _, answer := range answers {
		if _, ok := answer[pollTextKey].(string); !ok {
			t.Errorf("answer text is %T, want a string", answer[pollTextKey])
		}
		if answer["id"] == "" {
			t.Error("answer has no id")
		}
	}
	if !strings.Contains(fmt.Sprint(content[pollTextKey]), "Coffee or tea?") {
		t.Errorf("top-level text = %q, want the question", content[pollTextKey])
	}
	// A poll is not a message: a msgtype would make it one.
	if _, present := content["msgtype"]; present {
		t.Error("a poll must not carry a msgtype")
	}
	// body is the fallback for clients with no poll support.
	if body, _ := content["body"].(string); !strings.Contains(body, "Coffee") {
		t.Errorf("fallback body = %q", content["body"])
	}
}
