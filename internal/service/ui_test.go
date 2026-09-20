package service

import (
	"strings"
	"testing"

	"github.com/SelfRef/beeper-intercom/internal/bridge"
)

// The dim centred style a bridge message renders in has two hard rules: it
// must survive as a table, and it must never be a list.
func TestNoticeRendering(t *testing.T) {
	md := table([]string{"Level", "Type", "Model"}, [][]string{
		{yes("Medium"), "`medium` `m`", yes("qwen38:m")},
		{"Off", "`off`", code("qwen38")},
		{"a | b", "pipe in a cell", ""},
	})
	_, html := bridge.MarkdownHTML(md)
	for _, want := range []string{"<table>", "<th>Level</th>", `data-mx-color="` + colourOn + `"`, "<code>qwen38</code>"} {
		if !strings.Contains(html, want) {
			t.Errorf("rendered notice is missing %s:\n%s", want, html)
		}
	}
	// A pipe inside a cell must not open a new column.
	if strings.Count(html, "<td>") != 9 {
		t.Errorf("expected 3x3 cells, got:\n%s", html)
	}
	// An agent's answer is not trusted with HTML, a notice is.
	raw := `<span data-mx-color="#fff">x</span>`
	if _, escaped := bridge.Markdown(raw); strings.Contains(escaped, "<span") {
		t.Errorf("Markdown passed raw HTML through; it must escape it: %s", escaped)
	}
	if _, kept := bridge.MarkdownHTML(raw); !strings.Contains(kept, "<span") {
		t.Errorf("MarkdownHTML escaped the bridge's own markup: %s", kept)
	}
}

func TestNoticesAreNeverLists(t *testing.T) {
	_, html := bridge.MarkdownHTML(helpText)
	if strings.Contains(html, "<ul>") || strings.Contains(html, "<ol>") {
		t.Error("/help renders as a list; the centred style strands the markers at the left margin")
	}
	if !strings.Contains(html, "<table>") {
		t.Error("/help lost its table")
	}
}
