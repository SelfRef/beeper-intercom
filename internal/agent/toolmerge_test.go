package agent

import (
	"reflect"
	"testing"
)

func TestMergeTools(t *testing.T) {
	got := mergeTools([]string{"a", "b"}, []string{"b", "c", ""})
	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("mergeTools = %v, want %v", got, want)
	}
	if got := mergeTools([]string{"a"}, nil); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("no extras should return the configured list, got %v", got)
	}
}

func TestToolPhase(t *testing.T) {
	web := []string{"search_web", "fetch_url", "mcphub_firecrawl-scrape", "web_search"}
	for _, name := range web {
		if got := toolPhase(name); got != "web" {
			t.Fatalf("toolPhase(%q) = %q, want web", name, got)
		}
	}
	for _, name := range []string{"mcphub_time-get_current_time", "execute_workflow"} {
		if got := toolPhase(name); got != "tools" {
			t.Fatalf("toolPhase(%q) = %q, want tools", name, got)
		}
	}
}

// A zero Sink is the normal case for anything that runs a turn without
// showing it — answering a question, seeding a conversation, /btw. Calling
// its fields directly panics, which is how an answered question used to take
// the bridge down.
func TestZeroSinkIsSafe(t *testing.T) {
	var sink Sink
	sink.Push("text")
	sink.Report("writing")
	if sink.Streams() {
		t.Error("a zero sink does not want deltas")
	}
	var tracker phaseTracker
	tracker.to(sink, "thinking")

	seen := ""
	live := Sink{Status: func(state string) { seen = state }}
	live.Report("writing")
	live.Report("")
	if seen != "writing" {
		t.Errorf("status = %q", seen)
	}
}
