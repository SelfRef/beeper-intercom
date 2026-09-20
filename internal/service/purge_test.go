package service

import (
	"context"
	"strings"
	"testing"
)

// What /purge refuses, and why. Both cases are about what would be lost:
// a broadcast room holds a record somebody else acts on rather than a
// conversation to start again, and a running turn is still writing the events
// a purge would be redacting.
func TestPurgeRefusal(t *testing.T) {
	svc := newTestService(t)
	cfg := svc.Config()

	if why := svc.purgeRefusal(cfg.Rooms["news"], "news"); why == "" {
		t.Error("a broadcast room should not be purgeable")
	}
	if why := svc.purgeRefusal(cfg.Rooms["chat"], "chat"); why != "" {
		t.Errorf("an idle chat room should be purgeable: %s", why)
	}

	// A turn in a thread still counts: the purge is the whole room.
	svc.turns[sessionKey("chat", "$thread")] = &runningTurn{}
	why := svc.purgeRefusal(cfg.Rooms["chat"], "chat")
	if !strings.Contains(why, "/stop") {
		t.Errorf("a running turn should be refused with a way out: %q", why)
	}
	// A room whose key merely starts the same is a different room.
	if why := svc.purgeRefusal(cfg.Rooms["chat"], "chatter"); why != "" {
		t.Errorf("another room's turn blocked the purge: %s", why)
	}
	delete(svc.turns, sessionKey("chat", "$thread"))
	if why := svc.purgeRefusal(cfg.Rooms["chat"], "chat"); why != "" {
		t.Errorf("the turn is over; the room should be purgeable: %s", why)
	}
}

// Forgetting a room takes the conversation state that names a session with it,
// and leaves the settings that say what the room talks to.
func TestForgetRoomKeepsSettings(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	for key, value := range map[string]string{
		kvToolsetPrefix + "chat\x00:rw":    "12:on:1",
		kvCompactRequest + "chat\x00":      "1",
		kvThinkPending + "chat\x00":        "high",
		kvModelOverride + "chat":           "qwen38",
		kvAgentOverride + "chat":           "main",
		kvToolsetPrefix + "chatter\x00:rw": "13:on:1",
	} {
		if err := svc.store.SetKV(ctx, key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := svc.forgetRoom(ctx, "chat"); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		kvToolsetPrefix + "chat\x00:rw":    "",
		kvCompactRequest + "chat\x00":      "",
		kvThinkPending + "chat\x00":        "",
		kvModelOverride + "chat":           "qwen38",
		kvAgentOverride + "chat":           "main",
		kvToolsetPrefix + "chatter\x00:rw": "13:on:1",
	} {
		got, err := svc.store.GetKV(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%q = %q, want %q", key, got, want)
		}
	}
}
