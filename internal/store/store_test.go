package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func open(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestNotificationDedupe(t *testing.T) {
	ctx := context.Background()
	st := open(t)

	n := &Notification{EventID: "$one", RoomKey: "alerts", RoomID: "!r", Ghost: "alerts", Dedupe: "frigate:123"}
	if err := st.PutNotification(ctx, n); err != nil {
		t.Fatal(err)
	}
	found, err := st.NotificationByDedupe(ctx, "frigate:123")
	if err != nil || found == nil {
		t.Fatalf("lookup by dedupe: %v %v", found, err)
	}
	if found.EventID != "$one" {
		t.Errorf("event = %q", found.EventID)
	}
	// The same key twice is the caller retrying, and must not become a second
	// message.
	if err := st.PutNotification(ctx, &Notification{EventID: "$two", RoomKey: "alerts", RoomID: "!r", Ghost: "alerts", Dedupe: "frigate:123"}); err == nil {
		t.Error("expected the unique index to reject a duplicate dedupe key")
	}
	// An empty dedupe key is not a key: many rows may have none.
	for _, id := range []string{"$a", "$b"} {
		if err := st.PutNotification(ctx, &Notification{EventID: id, RoomKey: "news", RoomID: "!r", Ghost: "u"}); err != nil {
			t.Fatalf("empty dedupe should be allowed: %v", err)
		}
	}
	if found, err := st.NotificationByDedupe(ctx, ""); err != nil || found != nil {
		t.Errorf("empty dedupe lookup = %v %v", found, err)
	}
}

func TestNotificationSourceLookup(t *testing.T) {
	ctx := context.Background()
	st := open(t)

	payload, _ := json.Marshal(map[string]string{"camera": "front"})
	err := st.PutNotification(ctx, &Notification{
		EventID: "$e", RoomKey: "alerts", RoomID: "!r", Ghost: "alerts",
		SourceKind: "frigate", SourceID: "abc", SourcePayload: payload,
		Actions: map[string]string{"👍": "ack"},
		Tags:    []string{"door"},
	})
	if err != nil {
		t.Fatal(err)
	}
	found, err := st.NotificationBySource(ctx, "frigate", "abc")
	if err != nil || found == nil {
		t.Fatalf("lookup: %v %v", found, err)
	}
	if found.Actions["👍"] != "ack" {
		t.Errorf("actions did not round-trip: %v", found.Actions)
	}
	if len(found.Tags) != 1 || found.Tags[0] != "door" {
		t.Errorf("tags did not round-trip: %v", found.Tags)
	}
	if string(found.SourcePayload) != string(payload) {
		t.Errorf("payload did not round-trip: %s", found.SourcePayload)
	}
}

func TestSessionLifecycle(t *testing.T) {
	ctx := context.Background()
	st := open(t)

	sess, err := st.CreateSession(ctx, &Session{RoomKey: "chat", Agent: "main", ConvID: "c1", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	// Only one live conversation per room and thread; a second would make the
	// next turn a coin flip over which parent id to use.
	if _, err := st.CreateSession(ctx, &Session{RoomKey: "chat", Agent: "main", ConvID: "c2"}); err == nil {
		t.Error("expected the partial unique index to reject a second live session")
	}
	// A thread is a separate conversation in the same room.
	if _, err := st.CreateSession(ctx, &Session{RoomKey: "chat", ThreadRoot: "$root", Agent: "main", ConvID: "c3"}); err != nil {
		t.Errorf("thread session: %v", err)
	}

	if err := st.AdvanceSession(ctx, sess.ID, "assistant-1"); err != nil {
		t.Fatal(err)
	}
	live, err := st.LiveSession(ctx, "chat", "")
	if err != nil || live == nil {
		t.Fatalf("live session: %v %v", live, err)
	}
	if live.Turns != 1 || live.ParentID != "assistant-1" {
		t.Errorf("session = %+v", live)
	}

	if err := st.CloseSession(ctx, sess.ID); err != nil {
		t.Fatal(err)
	}
	if live, err := st.LiveSession(ctx, "chat", ""); err != nil || live != nil {
		t.Errorf("closed session still live: %v %v", live, err)
	}
	// Closing frees the slot for a new conversation in the same room.
	if _, err := st.CreateSession(ctx, &Session{RoomKey: "chat", Agent: "main", ConvID: "c4"}); err != nil {
		t.Errorf("new session after close: %v", err)
	}
}

// /resume: a closed conversation becomes the live one again, but only after
// the one that holds the slot has let go of it.
func TestReopenSession(t *testing.T) {
	ctx := context.Background()
	st := open(t)

	first, err := st.CreateSession(ctx, &Session{RoomKey: "chat", Agent: "main", ConvID: "c1", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AdvanceSession(ctx, first.ID, "assistant-1"); err != nil {
		t.Fatal(err)
	}
	if err := st.CloseSession(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	second, err := st.CreateSession(ctx, &Session{RoomKey: "chat", Agent: "main", ConvID: "c2"})
	if err != nil {
		t.Fatal(err)
	}
	// While the newer one is live, the slot is taken.
	if err := st.ReopenSession(ctx, first.ID); err == nil {
		t.Error("expected reopening to collide with the live conversation")
	}
	if err := st.CloseSession(ctx, second.ID); err != nil {
		t.Fatal(err)
	}
	if err := st.ReopenSession(ctx, first.ID); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	live, err := st.LiveSession(ctx, "chat", "")
	if err != nil || live == nil {
		t.Fatalf("live session: %v %v", live, err)
	}
	// Everything the next turn needs comes back with it, and the conversation
	// is not idle any more.
	if live.ID != first.ID || live.ConvID != "c1" || live.ParentID != "assistant-1" || live.Turns != 1 {
		t.Errorf("resumed session = %+v", live)
	}
	if live.LastActive < first.LastActive {
		t.Errorf("last_active went backwards: %d < %d", live.LastActive, first.LastActive)
	}
}

func TestTranscriptWindow(t *testing.T) {
	ctx := context.Background()
	st := open(t)

	for i, text := range []string{"one", "two", "three", "four"} {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		if err := st.AppendTranscript(ctx, "conv", role, text); err != nil {
			t.Fatal(err)
		}
	}
	// The window is the most recent N, in chronological order — replaying it
	// backwards would be worse than not replaying it at all.
	entries, err := st.Transcript(ctx, "conv", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Content != "three" || entries[1].Content != "four" {
		t.Errorf("transcript = %+v", entries)
	}
}

func TestOutboxRetry(t *testing.T) {
	ctx := context.Background()
	st := open(t)

	id, err := st.EnqueueDelivery(ctx, "action", "http://n8n.invalid/hook", map[string]any{"kind": "reaction"})
	if err != nil {
		t.Fatal(err)
	}
	due, err := st.DueDeliveries(ctx, 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("due = %v %v", due, err)
	}

	future := time.Now().Add(time.Hour).UnixMilli()
	if err := st.MarkDeliveryFailed(ctx, id, 1, future, "pending", "HTTP 500"); err != nil {
		t.Fatal(err)
	}
	if due, _ := st.DueDeliveries(ctx, 10); len(due) != 0 {
		t.Errorf("a backed-off delivery should not be due yet: %v", due)
	}
	// An operator forcing a retry ignores the backoff.
	if err := st.RetryDelivery(ctx, id); err != nil {
		t.Fatal(err)
	}
	if due, _ := st.DueDeliveries(ctx, 10); len(due) != 1 {
		t.Error("retry did not requeue the delivery")
	}

	if err := st.MarkDelivered(ctx, id); err != nil {
		t.Fatal(err)
	}
	if due, _ := st.DueDeliveries(ctx, 10); len(due) != 0 {
		t.Error("a delivered row is still due")
	}
}

func TestTxnDedupe(t *testing.T) {
	ctx := context.Background()
	st := open(t)

	first, err := st.MarkTxn(ctx, "e663722_d36453_r924934_t432703")
	if err != nil || !first {
		t.Fatalf("first = %v %v", first, err)
	}
	// Hungryserv retries transactions; a retried announcement would be a
	// second message in the room.
	second, err := st.MarkTxn(ctx, "e663722_d36453_r924934_t432703")
	if err != nil || second {
		t.Fatalf("second = %v %v", second, err)
	}
}

func TestPollAnswer(t *testing.T) {
	ctx := context.Background()
	st := open(t)

	if err := st.PutPoll(ctx, &Poll{EventID: "$p", RoomKey: "ops", RoomID: "!r", Question: "Restart?", Answers: []string{"Yes", "No"}}); err != nil {
		t.Fatal(err)
	}
	if err := st.AnswerPoll(ctx, "$p", "yes"); err != nil {
		t.Fatal(err)
	}
	poll, err := st.Poll(ctx, "$p")
	if err != nil || poll == nil {
		t.Fatalf("poll = %v %v", poll, err)
	}
	if poll.Response != "yes" || poll.ClosedAt == nil {
		t.Errorf("poll = %+v", poll)
	}
}

func TestKVRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := open(t)

	if value, err := st.GetKV(ctx, "missing"); err != nil || value != "" {
		t.Errorf("missing key = %q %v", value, err)
	}
	if err := st.SetKV(ctx, "k", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetKV(ctx, "k", "v2"); err != nil {
		t.Fatal(err)
	}
	if value, _ := st.GetKV(ctx, "k"); value != "v2" {
		t.Errorf("value = %q", value)
	}

	type reg struct{ ID string }
	if err := st.SetJSON(ctx, "reg", reg{ID: "abc"}); err != nil {
		t.Fatal(err)
	}
	var out reg
	found, err := st.GetJSON(ctx, "reg", &out)
	if err != nil || !found || out.ID != "abc" {
		t.Errorf("json round trip = %v %v %+v", found, err, out)
	}
}

// A turn has to come back with everything /undo needs: the backend handle for
// the exchange and every room event the answer occupies.
func TestTurnRoundTrip(t *testing.T) {
	st, ctx := open(t), context.Background()
	sess, err := st.CreateSession(ctx, &Session{RoomKey: "chat", Agent: "a", ConvID: "c1", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	want := &Turn{
		EventID:      "$q",
		SessionID:    sess.ID,
		UserMsgID:    "user-uuid",
		ParentID:     "p0",
		ReplyEvent:   "$a",
		AnswerEvents: []string{"$a", "$edit1", "$edit2", "$file"},
		Question:     "why",
		Usage:        `{"prompt_tokens":3}`,
	}
	if err := st.PutTurn(ctx, want); err != nil {
		t.Fatal(err)
	}
	got, err := st.LastTurn(ctx, "chat", "")
	if err != nil || got == nil {
		t.Fatalf("LastTurn = %v, %v", got, err)
	}
	if got.UserMsgID != want.UserMsgID || got.Question != want.Question ||
		strings.Join(got.AnswerEvents, ",") != strings.Join(want.AnswerEvents, ",") {
		t.Errorf("round trip lost something: %+v", got)
	}

	// A turn written before the answer-event list existed still has one event.
	if err := st.PutTurn(ctx, &Turn{EventID: "$q2", SessionID: sess.ID, ReplyEvent: "$a2"}); err != nil {
		t.Fatal(err)
	}
	old, err := st.Turn(ctx, "$q2")
	if err != nil || old == nil {
		t.Fatalf("Turn = %v, %v", old, err)
	}
	if len(old.AnswerEvents) != 1 || old.AnswerEvents[0] != "$a2" {
		t.Errorf("fallback answer events = %v", old.AnswerEvents)
	}

	// Rewinding gives the turn back, so an undone turn cannot rotate the
	// conversation out on the turn ceiling.
	if err := st.AdvanceSession(ctx, sess.ID, "p1"); err != nil {
		t.Fatal(err)
	}
	if err := st.RewindSession(ctx, sess.ID, "p0"); err != nil {
		t.Fatal(err)
	}
	after, err := st.SessionByID(ctx, sess.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Turns != 0 || after.ParentID != "p0" {
		t.Errorf("after rewind: turns = %d, parent = %q", after.Turns, after.ParentID)
	}
}

// Bridge messages are tracked only so they can be taken back: by command, by
// conversation, or by the message that caused them.
func TestNoticeBookkeeping(t *testing.T) {
	st, ctx := open(t), context.Background()
	put := func(id, thread, command string) {
		if err := st.PutNotice(ctx, &Notice{
			EventID: id, RoomKey: "chat", ThreadRoot: thread, RoomID: "!r", Ghost: "", CommandEvent: command,
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // created_at is the ordering key
	}
	put("$n1", "", "$help")
	put("$n2", "", "$help")
	put("$n3", "", "$status")
	put("$t1", "$thread", "$status")

	// The newest first, and a thread is its own conversation.
	last, err := st.Notices(ctx, "chat", "", 1)
	if err != nil || len(last) != 1 || last[0].EventID != "$n3" {
		t.Fatalf("last notice = %v (%v)", last, err)
	}
	if all, err := st.Notices(ctx, "chat", "", 0); err != nil || len(all) != 3 {
		t.Fatalf("main timeline = %d notices (%v)", len(all), err)
	}
	if thread, err := st.Notices(ctx, "chat", "$thread", 0); err != nil || len(thread) != 1 {
		t.Fatalf("thread = %d notices (%v)", len(thread), err)
	}

	// Everything said in answer to one command.
	forHelp, err := st.NoticesForCommand(ctx, "$help")
	if err != nil || len(forHelp) != 2 {
		t.Fatalf("answers to $help = %v (%v)", forHelp, err)
	}

	if err := st.DeleteNotices(ctx, []string{"$n1", "$n2"}); err != nil {
		t.Fatal(err)
	}
	if left, err := st.Notices(ctx, "chat", "", 0); err != nil || len(left) != 1 {
		t.Fatalf("after deleting two, %d left (%v)", len(left), err)
	}
	// Deleting nothing is not an error: /clear with nothing to remove.
	if err := st.DeleteNotices(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

// Whether an edited command may re-run is decided from the commands table, so
// the two things it has to get right are "which is the newest" and "which
// conversation was it part of" — and editing one must not change either.
func TestCommandOrdering(t *testing.T) {
	st, ctx := open(t), context.Background()
	put := func(id, body string, session int64) {
		if err := st.PutCommand(ctx, &Command{
			EventID: id, RoomKey: "chat", RoomID: "!r", Body: body, SessionID: session,
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	put("$c1", "/help", 7)
	put("$c2", "/status", 7)

	last, err := st.LastCommand(ctx, "chat", "")
	if err != nil || last == nil || last.EventID != "$c2" {
		t.Fatalf("last command = %v (%v)", last, err)
	}

	// Editing the OLD one updates its text but must not promote it to newest,
	// or an edit of ancient history would look like a correction.
	put("$c1", "/new", 7)
	last, err = st.LastCommand(ctx, "chat", "")
	if err != nil || last.EventID != "$c2" {
		t.Fatalf("after editing the older command, last = %v (%v)", last, err)
	}
	edited, err := st.Command(ctx, "$c1")
	if err != nil || edited.Body != "/new" {
		t.Fatalf("edited body = %v (%v)", edited, err)
	}
	if edited.SessionID != 7 {
		t.Errorf("session id = %d, want the one it was typed in", edited.SessionID)
	}
	// A thread is its own conversation, so it has its own newest command.
	if last, err := st.LastCommand(ctx, "chat", "$thread"); err != nil || last != nil {
		t.Errorf("thread should have no commands yet: %v (%v)", last, err)
	}
}
