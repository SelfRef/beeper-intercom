package service

import "testing"

func TestUsageLine(t *testing.T) {
	got := usageLine(`{"prompt_tokens":2519,"completion_tokens":615,"cached_tokens":181,"prompt_per_second":951.1,"tokens_per_second":93.7}`)
	want := "2519 in (181 cached), 615 out, 951 tok/s prefill, 93.7 tok/s decode"
	if got != want {
		t.Fatalf("usageLine =\n%q\nwant\n%q", got, want)
	}
	if usageLine("") != "" || usageLine("not json") != "" {
		t.Fatal("a missing or broken usage row should render nothing")
	}
	if got := usageLine(`{}`); got != "" {
		t.Fatalf("an empty usage row rendered %q", got)
	}
}
