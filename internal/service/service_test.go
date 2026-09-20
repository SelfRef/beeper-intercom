package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/notify"
	"github.com/SelfRef/beeper-intercom/internal/store"
)

const testConfig = `
network:
  bridge: sh-test
  name: Test
ghosts:
  updates: { name: Updates }
  agent:   { name: Agent }
rooms:
  news:
    name: News
    kind: broadcast
    ghosts: [updates]
  chat:
    name: Chat
    kind: dm
    ghosts: [agent]
    agent: main
agents:
  main:
    type: openai
    url: http://llama.invalid/v1
    model: m
`

// newTestService builds a service without touching the network. Everything
// here is about the state the process is in *before* it reaches Beeper, which
// is where a missing initialisation hides.
func newTestService(t *testing.T) *Service {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(testConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	svc, err := New(cfg, path, st, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestServiceStateBeforeConnecting(t *testing.T) {
	svc := newTestService(t)

	// The bridge spends its first seconds — and a Beeper outage — in exactly
	// this state, with the HTTP API already answering.
	if svc.Ready() {
		t.Error("service claims to be ready before it has connected")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := svc.WaitReady(ctx); err == nil {
		t.Error("WaitReady should honour a cancelled context")
	}
	if svc.Bridge() == nil || svc.Config() == nil || svc.Store() == nil {
		t.Error("service is missing one of its parts")
	}
	if svc.Bridge().BotMXID() != "" {
		t.Error("the bot MXID is only known after registration")
	}
}

func TestNotifyRejectsUnknownRoomWithoutQueueing(t *testing.T) {
	svc := newTestService(t)

	// No worker is running, so anything that reaches the queue would block
	// forever. Validation has to happen in front of it.
	if _, _, err := svc.Notify(context.Background(), &notify.Notification{Room: "nope", Text: "x"}); err == nil {
		t.Fatal("expected an error for an unknown room")
	}
	if _, _, err := svc.Notify(context.Background(), &notify.Notification{Room: "news"}); err == nil {
		t.Fatal("expected an error for an empty notification")
	}
}

func TestStartRefusesWithoutAnAccountToken(t *testing.T) {
	t.Setenv("MATRIX_ACCESS_TOKEN", "")
	svc := newTestService(t)
	if err := svc.Start(context.Background()); err == nil {
		t.Fatal("expected Start to refuse without an account token")
	}
}

func TestRotationReason(t *testing.T) {
	policy := config.Session{IdleMinutes: 60, MaxTurns: 3}

	fresh := &store.Session{Turns: 1, LastActive: nowMillis()}
	if reason := rotationReason(fresh, policy); reason != "" {
		t.Errorf("a fresh session should continue, got %q", reason)
	}
	// The ceiling is a ceiling: compaction has been running long before it.
	full := &store.Session{Turns: 3, LastActive: nowMillis()}
	if reason := rotationReason(full, policy); reason == "" {
		t.Error("a session at max_turns should rotate")
	}
	// A new topic after a gap should not inherit stale context.
	stale := &store.Session{Turns: 1, LastActive: nowMillis() - 2*60*60*1000}
	if reason := rotationReason(stale, policy); reason == "" {
		t.Error("an idle session should rotate")
	}
	// Both limits off means never rotate on its own.
	if reason := rotationReason(stale, config.Session{}); reason != "" {
		t.Errorf("no policy should mean no rotation, got %q", reason)
	}
}

func nowMillis() int64 {
	return time.Now().UnixMilli()
}
