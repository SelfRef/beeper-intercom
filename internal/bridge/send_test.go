package bridge

import (
	"strings"
	"testing"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/notify"
)

func TestSplitMessageKeepsShortTextWhole(t *testing.T) {
	parts := SplitMessage("short answer", 100)
	if len(parts) != 1 || parts[0] != "short answer" {
		t.Errorf("parts = %q", parts)
	}
}

func TestSplitMessagePrefersParagraphs(t *testing.T) {
	text := strings.Repeat("a", 40) + "\n\n" + strings.Repeat("b", 40)
	parts := SplitMessage(text, 50)
	if len(parts) != 2 {
		t.Fatalf("parts = %d: %q", len(parts), parts)
	}
	for _, part := range parts {
		if len(part) > 50 {
			t.Errorf("part over the limit: %d", len(part))
		}
	}
}

func TestSplitMessageBreaksOversizedParagraphs(t *testing.T) {
	// A single paragraph with no blank line still has to be broken, or a wall
	// of log output would go out as one unrenderable message.
	text := strings.Repeat("line\n", 100)
	parts := SplitMessage(text, 60)
	if len(parts) < 2 {
		t.Fatalf("oversized paragraph was not split: %d parts", len(parts))
	}
	for _, part := range parts {
		if len(part) > 60 {
			t.Errorf("part over the limit: %d", len(part))
		}
	}
	if joined := strings.ReplaceAll(strings.Join(parts, "\n"), "\n", ""); joined != strings.ReplaceAll(text, "\n", "") {
		t.Error("splitting lost or duplicated content")
	}
}

func TestApplyRelationsThread(t *testing.T) {
	content := &event.MessageEventContent{}
	applyRelations(content, SendOptions{ThreadRoot: "$root", ReplyTo: "$msg"})

	if content.RelatesTo == nil || content.RelatesTo.Type != event.RelThread {
		t.Fatalf("relates_to = %+v", content.RelatesTo)
	}
	if content.RelatesTo.EventID != id.EventID("$root") {
		t.Errorf("thread root = %q", content.RelatesTo.EventID)
	}
	// The reply fallback is what puts the message in the right place in
	// clients that do not render threads.
	if content.RelatesTo.InReplyTo == nil || content.RelatesTo.InReplyTo.EventID != "$msg" {
		t.Errorf("in_reply_to = %+v", content.RelatesTo.InReplyTo)
	}
	if !content.RelatesTo.IsFallingBack {
		t.Error("is_falling_back must be set on a thread reply")
	}
}

func TestApplyRelationsPlainReply(t *testing.T) {
	content := &event.MessageEventContent{}
	applyRelations(content, SendOptions{ReplyTo: "$msg"})
	if content.RelatesTo == nil || content.RelatesTo.Type == event.RelThread {
		t.Fatalf("relates_to = %+v", content.RelatesTo)
	}
	if content.RelatesTo.InReplyTo.EventID != "$msg" {
		t.Errorf("in_reply_to = %+v", content.RelatesTo.InReplyTo)
	}
}

func TestMsgTypeFor(t *testing.T) {
	cases := map[string]event.MessageType{
		"image/png":       event.MsgImage,
		"video/mp4":       event.MsgVideo,
		"audio/ogg":       event.MsgAudio,
		"application/pdf": event.MsgFile,
		"":                event.MsgFile,
	}
	for mime, want := range cases {
		if got := msgTypeFor(mime); got != want {
			t.Errorf("msgTypeFor(%q) = %q, want %q", mime, got, want)
		}
	}
}

func TestAnswerIDIsStableAndReadable(t *testing.T) {
	if got := answerID("Restart it"); got != "restart-it" {
		t.Errorf("answerID = %q", got)
	}
	if answerID("Yes") != answerID("yes ") {
		t.Error("answer ids should not depend on case or trailing space")
	}
	// Something with no usable characters still needs an id.
	if answerID("🙂") == "" {
		t.Error("answerID returned an empty id")
	}
}

func TestMarkdownRendersToMatrixHTML(t *testing.T) {
	plain, formatted := Markdown("**bold** and `code`")
	if plain == "" {
		t.Error("plain body is empty")
	}
	if !strings.Contains(formatted, "<strong>") || !strings.Contains(formatted, "<code>") {
		t.Errorf("formatted = %q", formatted)
	}
}

func TestNotificationRenderUsesTheRealRenderer(t *testing.T) {
	// End to end through the renderer the bridge actually ships with, which
	// catches a Markdown flavour change in a dependency bump.
	n := &notify.Notification{Title: "Update", Text: "- one\n- two"}
	out := n.Render(Markdown)
	if !strings.Contains(out.Formatted, "<li>") {
		t.Errorf("list did not render: %q", out.Formatted)
	}
	if !strings.HasPrefix(out.Formatted, "<strong>Update</strong>") {
		t.Errorf("title did not render: %q", out.Formatted)
	}
}
