package service

import (
	"fmt"
	"strings"
)

// How a bridge message is laid out.
//
// Bridge messages are sent by the bridge bot, which Beeper renders as dim
// centred text with no bubble (BEEPER_REF §5). That rendering decides the
// layout: the whole message is centre-aligned, so a bullet list comes out with
// its markers stranded at the left margin while the text drifts to the middle.
// Tables do not have that problem — their cells left-align inside the centred
// column — and they are also shorter, which matters in a chat where every line
// pushes the conversation up.
//
// So: a table for anything with more than one field, one line of prose for
// anything that is really a sentence. Colour carries the state that would
// otherwise need a word ("on", "current"), because data-mx-color is the one
// presentational attribute the Matrix HTML subset allows.
const (
	colourOn   = "#3ba55d" // enabled, current, yes
	colourOff  = "#8a8f98" // disabled, absent, dim
	colourWarn = "#d99e0b" // temporary, expiring
	colourBad  = "#e06c75" // failed, unreachable
)

func colour(hex, text string) string {
	return fmt.Sprintf(`<span data-mx-color="%s">%s</span>`, hex, text)
}

// table renders a Markdown table. Empty headers still produce a header row,
// which the client draws as a thin label line — fine for a two-column
// key/value block.
func table(headers []string, rows [][]string) string {
	var b strings.Builder
	b.WriteString("| " + strings.Join(headers, " | ") + " |\n")
	b.WriteString("|" + strings.Repeat(" --- |", len(headers)) + "\n")
	for _, row := range rows {
		cells := make([]string, len(headers))
		for i := range cells {
			if i < len(row) {
				cells[i] = strings.ReplaceAll(row[i], "|", "\\|")
			}
		}
		b.WriteString("| " + strings.Join(cells, " | ") + " |\n")
	}
	return b.String()
}

// code wraps an identifier so it is readable as one.
func code(s string) string {
	if s == "" {
		return ""
	}
	return "`" + s + "`"
}

// yes and no are the two states that appear all over these tables.
func yes(text string) string { return colour(colourOn, "**"+text+"**") }
func no(text string) string  { return colour(colourOff, text) }
