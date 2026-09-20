package service

import (
	"testing"
	"time"
)

func TestToolsetRowRoundTrip(t *testing.T) {
	when := time.UnixMilli(1789878000000)
	row := formatToolsetRow("12", true, when)
	session, on, since, ok := parseToolsetRow(row)
	if !ok || session != "12" || !on || !since.Equal(when) {
		t.Fatalf("round trip lost something: %q -> %q %v %v %v", row, session, on, since, ok)
	}
	if _, _, _, ok := parseToolsetRow("nonsense"); ok {
		t.Fatal("a malformed row parsed")
	}
	// A row from a conversation that has ended must not read as a session id
	// that happens to look similar.
	if session, _, _, _ := parseToolsetRow(formatToolsetRow(toolsetNextSession, true, when)); session != toolsetNextSession {
		t.Fatalf("pending row lost its marker: %q", session)
	}
}

func TestHumanDuration(t *testing.T) {
	cases := map[time.Duration]string{
		15 * time.Minute: "15 min",
		time.Hour:        "1 h",
		90 * time.Second: "1m30s",
	}
	for d, want := range cases {
		if got := humanDuration(d); got != want {
			t.Fatalf("humanDuration(%s) = %q, want %q", d, got, want)
		}
	}
}
