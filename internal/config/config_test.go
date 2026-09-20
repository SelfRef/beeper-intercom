package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
