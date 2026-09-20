package notify

import (
	"strings"
	"testing"
)

// markdown stands in for the mautrix renderer: the tests here are about what
// Render assembles around the body, not about Markdown itself.
func markdown(text string) (string, string) {
	return text, "<p>" + text + "</p>"
}

func TestRenderTitleAndBody(t *testing.T) {
	n := &Notification{Title: "Front door", Text: "Person detected."}
	out := n.Render(markdown)

	if out.Body != "Front door\nPerson detected." {
		t.Errorf("body = %q", out.Body)
	}
	if !strings.HasPrefix(out.Formatted, "<strong>Front door</strong><br/>") {
		t.Errorf("formatted = %q", out.Formatted)
	}
}

func TestRenderEscapesTitle(t *testing.T) {
	// A title comes from a source payload. It must not be able to inject
	// markup into the HTML body.
	n := &Notification{Title: `<img src=x onerror="alert(1)">`}
	out := n.Render(markdown)
	if strings.Contains(out.Formatted, "<img") {
		t.Errorf("title was not escaped: %q", out.Formatted)
	}
}

func TestRenderHTMLOverridesMarkdown(t *testing.T) {
	n := &Notification{Text: "plain", HTML: "<b>rich</b>"}
	out := n.Render(markdown)
	if out.Formatted != "<b>rich</b>" {
		t.Errorf("formatted = %q", out.Formatted)
	}
	if out.Body != "plain" {
		t.Errorf("body = %q", out.Body)
	}
}

func TestRenderHTMLWithoutTextDerivesFallback(t *testing.T) {
	n := &Notification{HTML: "<p>two <b>words</b></p>"}
	out := n.Render(markdown)
	if out.Body != "two words" {
		t.Errorf("body fallback = %q", out.Body)
	}
}

func TestRenderTagsAndURL(t *testing.T) {
	n := &Notification{Text: "body", URL: "https://example.invalid/x", Tags: []string{"warning", "custom"}}
	out := n.Render(markdown)

	if !strings.Contains(out.Body, "https://example.invalid/x") {
		t.Errorf("url missing from body: %q", out.Body)
	}
	// A known ntfy shortcode becomes an emoji, an unknown one a hashtag —
	// matching what the ntfy app itself shows.
	if !strings.Contains(out.Body, "⚠️") || !strings.Contains(out.Body, "#custom") {
		t.Errorf("tags = %q", out.Body)
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		n    Notification
		ok   bool
	}{
		{"no room", Notification{Text: "x"}, false},
		{"empty", Notification{Room: "news"}, false},
		{"bad priority", Notification{Room: "news", Text: "x", Priority: "critical"}, false},
		{"media only", Notification{Room: "news", Media: []Media{{URL: "https://x.invalid/a.png"}}}, true},
		{"ok", Notification{Room: "news", Text: "x", Priority: PriorityHigh}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.n.Validate()
			if tc.ok && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tc.ok && err == nil {
				t.Error("expected an error")
			}
		})
	}
}

func TestUrgent(t *testing.T) {
	for priority, want := range map[string]bool{
		PriorityMin: false, PriorityLow: false, PriorityDefault: false,
		PriorityHigh: true, PriorityUrgent: true, "": false,
	} {
		if got := (&Notification{Priority: priority}).Urgent(); got != want {
			t.Errorf("priority %q: urgent = %v", priority, got)
		}
	}
}

func TestFromNtfyPriority(t *testing.T) {
	want := map[int]string{0: "", 1: PriorityMin, 2: PriorityLow, 3: PriorityDefault, 4: PriorityHigh, 5: PriorityUrgent}
	for in, expected := range want {
		if got := FromNtfyPriority(in); got != expected {
			t.Errorf("FromNtfyPriority(%d) = %q, want %q", in, got, expected)
		}
	}
}
