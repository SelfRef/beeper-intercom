package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// write puts a config file in a temp dir and loads it.
func load(t *testing.T, body string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}

const minimal = `
network:
  bridge: sh-test
  name: Test
ghosts:
  bot: { name: Bot }
rooms:
  news:
    name: News
    kind: broadcast
    ghosts: [bot]
`

func TestLoadMinimal(t *testing.T) {
	cfg, err := load(t, minimal)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Network.ID != "intercom" {
		t.Errorf("default protocol id = %q", cfg.Network.ID)
	}
	if cfg.Server.Bind != "0.0.0.0:8080" {
		t.Errorf("default bind = %q", cfg.Server.Bind)
	}
	if cfg.Limits.QueueSize == 0 || cfg.Limits.MessageSplitBytes == 0 {
		t.Error("limits were not defaulted")
	}
}

func TestExampleConfigIsValid(t *testing.T) {
	// The example is documentation people copy; it has to stay loadable.
	cfg, err := Load("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Rooms) == 0 || len(cfg.Agents) == 0 {
		t.Error("example config lost its rooms or agents")
	}
}

func TestEnvExpansion(t *testing.T) {
	t.Setenv("INTERCOM_TEST_URL", "http://ntfy.invalid")
	cfg, err := load(t, minimal+`
sources:
  - type: ntfy
    url: ${INTERCOM_TEST_URL}
    topics:
      t: { room: news, ghost: bot }
`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Sources[0].URL != "http://ntfy.invalid" {
		t.Errorf("url = %q", cfg.Sources[0].URL)
	}
}

func TestUnknownFieldIsRejected(t *testing.T) {
	// A typo in a key is a silent behaviour change otherwise: the bridge would
	// start, look healthy, and quietly ignore what you asked for.
	_, err := load(t, minimal+"\nunexpected_key: 1\n")
	if err == nil {
		t.Fatal("expected an error for an unknown field")
	}
}

func TestValidationErrors(t *testing.T) {
	cases := map[string]string{
		"bad bridge name": `
network: { bridge: aperte, name: X }
ghosts: { bot: { name: Bot } }
rooms: { news: { name: News, kind: broadcast, ghosts: [bot] } }
`,
		"undefined ghost": `
network: { bridge: sh-test, name: X }
ghosts: { bot: { name: Bot } }
rooms: { news: { name: News, kind: broadcast, ghosts: [nobody] } }
`,
		"undefined agent": `
network: { bridge: sh-test, name: X }
ghosts: { bot: { name: Bot } }
rooms: { chat: { name: Chat, kind: dm, ghosts: [bot], agent: ghost } }
`,
		"dm with two ghosts": `
network: { bridge: sh-test, name: X }
ghosts: { a: { name: A }, b: { name: B } }
rooms: { chat: { name: Chat, kind: dm, ghosts: [a, b] } }
`,
		"unknown kind": `
network: { bridge: sh-test, name: X }
ghosts: { bot: { name: Bot } }
rooms: { news: { name: News, kind: shouting, ghosts: [bot] } }
`,
		"agent without url": `
network: { bridge: sh-test, name: X }
ghosts: { bot: { name: Bot } }
rooms: { chat: { name: Chat, kind: dm, ghosts: [bot], agent: main } }
agents: { main: { type: openai, model: m } }
`,
		"ntfy topic to nowhere": `
network: { bridge: sh-test, name: X }
ghosts: { bot: { name: Bot } }
rooms: { news: { name: News, kind: broadcast, ghosts: [bot] } }
sources:
  - type: ntfy
    topics: { t: { room: missing } }
`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := load(t, body); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

func TestGhostForRoom(t *testing.T) {
	cfg, err := load(t, `
network: { bridge: sh-test, name: X }
ghosts: { a: { name: A }, b: { name: B }, c: { name: C } }
rooms: { news: { name: News, kind: broadcast, ghosts: [a, b] } }
`)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := cfg.GhostForRoom("news", ""); err != nil || got != "a" {
		t.Errorf("default ghost = %q, %v", got, err)
	}
	if got, err := cfg.GhostForRoom("news", "b"); err != nil || got != "b" {
		t.Errorf("named ghost = %q, %v", got, err)
	}
	// A ghost that exists but is not in the room would render as a raw MXID.
	if _, err := cfg.GhostForRoom("news", "c"); err == nil {
		t.Error("expected an error for a ghost outside the room")
	}
	if _, err := cfg.GhostForRoom("nope", ""); err == nil || !strings.Contains(err.Error(), "unknown room") {
		t.Errorf("unknown room error = %v", err)
	}
}

func TestTokensPreferEnvironment(t *testing.T) {
	t.Setenv("INTERCOM_TEST_ADMIN", "from-env")
	cfg, err := load(t, minimal+`
server:
  admin_token_env: INTERCOM_TEST_ADMIN
  admin_token: from-file
`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AdminToken() != "from-env" {
		t.Errorf("admin token = %q", cfg.AdminToken())
	}
}

func TestProgressDefaults(t *testing.T) {
	cfg := &Config{}
	cfg.applyDefaults()
	if cfg.Progress.Mode != ProgressOff {
		t.Fatalf("progress mode defaults to %q, want %q", cfg.Progress.Mode, ProgressOff)
	}
	if cfg.Progress.Suffix == "" {
		t.Fatal("progress suffix has no default")
	}
	if got := cfg.Progress.Reactions.Emoji("web"); got == "" {
		t.Fatal("web phase has no default emoji")
	}
	if got := cfg.Progress.Reactions.Emoji("nonsense"); got != "" {
		t.Fatalf("unknown phase returned %q, want empty", got)
	}
	// An operator who names one emoji keeps exactly that one: the defaults are
	// all-or-nothing, so a half-configured block cannot silently regrow.
	custom := &Config{Progress: Progress{Reactions: Reactions{Thinking: "🤖"}}}
	custom.applyDefaults()
	if custom.Progress.Reactions.Queued != "" || custom.Progress.Reactions.Thinking != "🤖" {
		t.Fatalf("custom reactions were overwritten: %+v", custom.Progress.Reactions)
	}
}

func TestProgressValidation(t *testing.T) {
	base := func() *Config {
		c := &Config{
			Network: Network{Bridge: "sh-test", ID: "test"},
			Ghosts:  map[string]Ghost{"bot": {Name: "Bot"}},
			Rooms:   map[string]Room{"chat": {Name: "Chat", Kind: KindChat, Ghosts: []string{"bot"}}},
		}
		c.applyDefaults()
		return c
	}
	c := base()
	c.Progress.Mode = "sometimes"
	if err := c.Validate(); err == nil {
		t.Fatal("an unknown progress mode was accepted")
	}
	c = base()
	c.Progress.Mode = ProgressEdits
	c.Progress.Interval = Duration(10 * time.Millisecond)
	if err := c.Validate(); err == nil {
		t.Fatal("a 10ms edit interval was accepted")
	}
	c = base()
	c.Progress.Mode = ProgressEdits
	c.Progress.Interval = Duration(time.Second)
	if err := c.Validate(); err != nil {
		t.Fatalf("a sane progress block was rejected: %v", err)
	}
}

// Reasoning levels are a naming convention, and the only two things that can
// go wrong with one are reading a suffix that is not there and reading the
// wrong one.
func TestReasoningSplit(t *testing.T) {
	r := Reasoning{Enabled: true, Levels: []ReasoningLevel{
		{Name: "Think", IDs: []string{"think", "t"}, Suffix: ":t"},
		{Name: "Medium", IDs: []string{"medium", "m"}, Suffix: ":m"},
		{Name: "Extra Extra", IDs: []string{"xx"}, Suffix: ":xl"},
		{Name: "Extra High", IDs: []string{"extra", "x"}, Suffix: ":x"},
	}}
	for _, tc := range []struct {
		model string
		base  string
		level string
	}{
		{"qwen38", "qwen38", ""},
		{"qwen38:m", "qwen38", "Medium"},
		{"qwen38:t", "qwen38", "Think"},
		// The longest suffix wins, or ":xl" would read as ":x" plus a stray l.
		{"qwen38:xl", "qwen38", "Extra Extra"},
		{"qwen38:x", "qwen38", "Extra High"},
		// A suffix nobody declared is part of the id, not a level.
		{"qwen38:z", "qwen38:z", ""},
	} {
		base, level := r.Split(tc.model)
		name := ""
		if level != nil {
			name = level.Name
		}
		if base != tc.base || name != tc.level {
			t.Errorf("Split(%q) = %q/%q, want %q/%q", tc.model, base, name, tc.base, tc.level)
		}
	}
	// Every id selects its level, whatever the case, and the first one is the
	// canonical one.
	for _, id := range []string{"extra", "x", "EXTRA", "X"} {
		level, ok := r.Level(id)
		if !ok || level.Name != "Extra High" {
			t.Errorf("Level(%q) did not find Extra High", id)
		} else if level.ID() != "extra" {
			t.Errorf("canonical id = %q, want extra", level.ID())
		}
	}
	if _, ok := r.Level("off"); ok {
		t.Error("off is not a level")
	}
}

func TestReasoningValidation(t *testing.T) {
	levels := func(body string) string {
		return minimal + "\nreasoning:\n  enabled: true\n  levels:\n" + body
	}
	for name, body := range map[string]string{
		"no levels":        minimal + "\nreasoning:\n  enabled: true\n",
		"no suffix":        levels("    - { name: High, ids: [high] }\n"),
		"no ids":           levels("    - { name: High, suffix: \":x\" }\n"),
		"no name":          levels("    - { ids: [high], suffix: \":x\" }\n"),
		"off is reserved":  levels("    - { name: Off, ids: [off], suffix: \":o\" }\n"),
		"suffix is off_suffix": minimal + "\nreasoning:\n  enabled: true\n  off_suffix: \":nt\"\n  levels:\n    - { name: No think, ids: [nope], suffix: \":nt\" }\n",
		"duplicate id":     levels("    - { name: High, ids: [h], suffix: \":x\" }\n    - { name: Huge, ids: [h], suffix: \":h\" }\n"),
		"duplicate suffix": levels("    - { name: High, ids: [high], suffix: \":x\" }\n    - { name: Extra, ids: [extra], suffix: \":x\" }\n"),
		"bad id":           levels("    - { name: High, ids: [\"Very High\"], suffix: \":x\" }\n"),
	} {
		if _, err := load(t, body); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	cfg, err := load(t, levels("    - { name: Low, ids: [low, l], suffix: \":l\" }\n    - { name: Extra High, ids: [extra, x], suffix: \":x\" }\n"))
	if err != nil {
		t.Fatalf("valid reasoning config rejected: %v", err)
	}
	if base, level := cfg.Reasoning.Split("main:x"); base != "main" || level.Label() != "Extra High" {
		t.Errorf("loaded levels do not parse a model id: %q %v", base, level)
	}
}

// A catalogue whose bare id already thinks names its non-thinking variant with
// a suffix like any other level, and /think off has to reach THAT one.
func TestReasoningOffSuffix(t *testing.T) {
	r := Reasoning{Enabled: true, OffSuffix: ":nt", Levels: []ReasoningLevel{
		{Name: "Medium", IDs: []string{"medium", "m"}, Suffix: ":m"},
	}}
	if off := r.Off(); off.Suffix != ":nt" || off.ID() != ReasoningOff {
		t.Errorf("Off() = %q/%q, want %q/%q", off.Suffix, off.ID(), ":nt", ReasoningOff)
	}
	if base, level := r.Split("some-model:nt"); base != "some-model" || level == nil || level.ID() != ReasoningOff {
		t.Errorf("the off variant does not split back to its base: %q %v", base, level)
	}
	if base, level := r.Split("some-model"); base != "some-model" || level != nil {
		t.Errorf("a bare id is no longer the off variant: %q %v", base, level)
	}
	// Without one, the bare id keeps meaning "off" — the default behaviour.
	plain := Reasoning{Enabled: true, Levels: r.Levels}
	if off := plain.Off(); off.Suffix != "" {
		t.Errorf("Off() invented a suffix: %q", off.Suffix)
	}
}
