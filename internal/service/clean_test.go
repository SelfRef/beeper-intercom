package service

import (
	"context"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/SelfRef/beeper-intercom/internal/agent"
	"github.com/SelfRef/beeper-intercom/internal/config"
)

// Which reactions sweep a command away. The default is "any", because the
// point is to be faster than typing /clear.
func TestCleanTriggeredBy(t *testing.T) {
	off := false
	for name, tc := range map[string]struct {
		cfg  config.Notices
		key  string
		want bool
	}{
		"default, any emoji":    {config.Notices{}, "🧹", true},
		"default, another one":  {config.Notices{}, "👍", true},
		"disabled":              {config.Notices{CleanOnReaction: &off}, "🧹", false},
		"limited, listed":       {config.Notices{CleanEmoji: []string{"🧹", "🗑️"}}, "🗑️", true},
		"limited, not listed":   {config.Notices{CleanEmoji: []string{"🧹"}}, "👍", false},
		"disabled beats a list": {config.Notices{CleanOnReaction: &off, CleanEmoji: []string{"🧹"}}, "🧹", false},
	} {
		if got := tc.cfg.CleanTriggeredBy(tc.key); got != tc.want {
			t.Errorf("%s: CleanTriggeredBy(%q) = %v, want %v", name, tc.key, got, tc.want)
		}
	}
}

// The poll's question is the header and the question; the options carry their
// own descriptions, so nothing about them belongs in the title.
func TestQuestionText(t *testing.T) {
	text := questionText(agent.Question{
		Header: "Scope",
		Text:   "What should I do with 'lorem ipsum'?",
		Options: []agent.QuestionOption{
			{Label: "Explain it", Description: "Tell me what it is."},
			{Label: "Generate some"},
		},
		AllowOther: true,
	})
	// allow_other adds the only hint a Matrix poll can carry, and it goes on
	// the same line: the question renders as one line whatever is in it.
	if text != "Scope — What should I do with 'lorem ipsum'? (or type answer)" {
		t.Errorf("question text = %q", text)
	}
	fixed := questionText(agent.Question{Text: "Pick one", Options: []agent.QuestionOption{{Label: "a"}}})
	if strings.Contains(fixed, "type answer") {
		t.Errorf("a fixed-option question must not offer typing: %q", fixed)
	}
	// A header that repeats the question is noise.
	plain := questionText(agent.Question{Header: "Pick one", Text: "Pick one"})
	if plain != "Pick one" {
		t.Errorf("repeated header was kept: %q", plain)
	}
}

// What the model said before its question is posted once, and the resumed
// turn repeats it at the front of the finished answer.
func TestWithoutPrefix(t *testing.T) {
	const said = "The tool limits each question to 2-3 options, so I'll go with 3:"
	full := said + "\n\nWorked — you picked **Curry**."
	if got := withoutPrefix(full, said); got != "Worked — you picked **Curry**." {
		t.Errorf("prefix not stripped: %q", got)
	}
	// An answer that merely starts differently is left alone.
	if got := withoutPrefix("Something else entirely", said); got != "Something else entirely" {
		t.Errorf("unrelated answer was cut: %q", got)
	}
	if got := withoutPrefix("anything", ""); got != "anything" {
		t.Errorf("empty prefix changed the text: %q", got)
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("short", 10); got != "short" {
		t.Errorf("truncate(short) = %q", got)
	}
	long := strings.Repeat("x", 120)
	got := truncate(long, 100)
	if len([]rune(got)) != 100 || !strings.HasSuffix(got, "…") {
		t.Errorf("truncate kept %d runes: %q", len([]rune(got)), got[len(got)-5:])
	}
}

// What a typed message means depends on the question that is open: one that
// accepts free text takes it as the answer, one that does not is dropped so
// the message can be a message.
func TestOfferMessage(t *testing.T) {
	s := &Service{pendingAsks: map[string]*openQuestion{}}
	s.log = zerolog.Nop()
	const key = "chat\x00"

	if got := s.offerMessage(context.Background(), key, "hello"); got != messageIsAMessage {
		t.Errorf("with nothing open, message = %d", got)
	}

	ctx := context.Background()
	open := s.awaitingAnswer(key, true, 0, "")
	if got := s.offerMessage(ctx, key, "neither, do the third thing"); got != messageIsTheAnswer {
		t.Fatalf("a free-text question should take the message, got %d", got)
	}
	if answer := <-open.answer; answer != "neither, do the third thing" {
		t.Errorf("answer = %q", answer)
	}
	if got := s.offerMessage(ctx, key, "and another"); got != messageIsAMessage {
		t.Errorf("the question was answered; the next message is a turn, got %d", got)
	}

	// A question with fixed options is dropped instead, and says so by
	// closing, so the turn waiting on it can unwind.
	fixed := s.awaitingAnswer(key, false, 0, "")
	if got := s.offerMessage(ctx, key, "actually, something else"); got != messageDropsTheQuestion {
		t.Fatalf("a fixed-option question should be dropped, got %d", got)
	}
	select {
	case <-fixed.abandoned:
	default:
		t.Error("a dropped question must signal that it was abandoned")
	}

	// An abandoned question unregisters, so a late message is not eaten.
	s.awaitingAnswer(key, true, 0, "")
	s.doneAwaiting(key)
	if got := s.offerMessage(ctx, key, "too late"); got != messageIsAMessage {
		t.Errorf("an abandoned question swallowed a message, got %d", got)
	}
}
