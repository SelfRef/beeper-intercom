// Package config is the whole operator-facing surface of the bridge: one YAML
// file that declares the network, its ghosts, its rooms, the agents behind the
// conversational rooms and the sources that feed the broadcast ones.
//
// Nothing here is instance-specific. The public image ships neutral defaults;
// an installation's names, hostnames, tokens and model ids live in its own
// config file and environment. Secrets are never values in the YAML: a field
// that needs one names the environment variable instead (`api_key_env`), and
// `${VAR}` anywhere in the file is expanded from the environment at load time.
package config

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the parsed configuration file.
type Config struct {
	Network Network          `yaml:"network"`
	Matrix  Matrix           `yaml:"matrix"`
	Server  Server           `yaml:"server"`
	Ghosts  map[string]Ghost `yaml:"ghosts"`
	Rooms   map[string]Room  `yaml:"rooms"`
	Agents  map[string]Agent `yaml:"agents"`
	Sources []Source         `yaml:"sources"`
	Actions Actions          `yaml:"actions"`
	Log     Log              `yaml:"log"`
	Limits  Limits           `yaml:"limits"`

	// Dir is the directory the config was loaded from; relative paths in the
	// file (avatars, mostly) resolve against it.
	Dir string `yaml:"-"`
}

// Network is how the installation presents itself inside Beeper: one
// appservice registration shown as one network next to Telegram and Signal.
type Network struct {
	// Bridge is the bbctl registration name. Beeper requires ^sh-[a-z0-9-]+$.
	Bridge string `yaml:"bridge"`
	// ID is the m.bridge protocol id — the key that groups the rooms under
	// their own network in the client.
	ID string `yaml:"id"`
	// Name is what the client displays as the network name.
	Name string `yaml:"name"`
	// Avatar is a path to an image uploaded once and cached as an mxc URI.
	Avatar string `yaml:"avatar"`
	// ExternalURL is the protocol's homepage, shown in some client surfaces.
	ExternalURL string `yaml:"external_url"`
}

// Matrix is the account side: which Beeper deployment, and the account token
// that both registers the appservice and writes per-user room account data
// (tags and mute are the user's state, not the appservice's).
type Matrix struct {
	// BaseDomain is the Beeper deployment: api.<domain> and matrix.<domain>.
	BaseDomain string `yaml:"base_domain"`
	// TokenEnv names the environment variable holding the account token.
	TokenEnv string `yaml:"token_env"`
	// HomeserverDomain is the server_name of the hungryserv. Always
	// beeper.local on Beeper; configurable so a test deployment can differ.
	HomeserverDomain string `yaml:"homeserver_domain"`
	// ReRegister forces a fresh registration call on start even when one is
	// already stored. Harmless (the call is idempotent), but it costs a
	// round-trip to Beeper on every restart.
	ReRegister bool `yaml:"re_register"`
}

// Server is the HTTP side. Three tokens, three roles: ingest can post
// notifications and nothing else, admin can read and rewrite everything, and
// the health endpoint is open.
type Server struct {
	Bind           string `yaml:"bind"`
	IngestTokenEnv string `yaml:"ingest_token_env"`
	AdminTokenEnv  string `yaml:"admin_token_env"`
	IngestToken    string `yaml:"ingest_token"`
	AdminToken     string `yaml:"admin_token"`
}

// Ghost is one identity in the exclusive user namespace: a real room member
// with a profile, not a rendering hint.
type Ghost struct {
	Name   string `yaml:"name"`
	Avatar string `yaml:"avatar"`
}

// Room kinds. The difference that matters is the power levels: a broadcast
// room locks the composer, a conversational one does not.
const (
	KindBroadcast = "broadcast"
	KindChat      = "chat"
	KindDM        = "dm"
)

// Room is one declared room. Rooms are reconciled on every start: created if
// missing, patched if drifted, never deleted.
type Room struct {
	Name  string `yaml:"name"`
	Topic string `yaml:"topic"`
	// Avatar is a path to an image; defaults to the network avatar.
	Avatar string `yaml:"avatar"`
	// Kind is broadcast, chat or dm. A dm is a chat that renders as a direct
	// message, which only makes sense with exactly one ghost.
	Kind string `yaml:"kind"`
	// Ghosts are the config keys of the identities that live in this room.
	Ghosts []string `yaml:"ghosts"`
	// Tags are Matrix room tags without the m. prefix: favourite, lowpriority.
	Tags []string `yaml:"tags"`
	// UrgentMentions turns priority >= high into an @room mention.
	UrgentMentions bool `yaml:"urgent_mentions"`
	// Muted mutes the room for the account forever (com.beeper.mute).
	Muted bool `yaml:"muted"`
	// Agent names the agent config that answers in this room. Empty means the
	// room is announcements only.
	Agent string `yaml:"agent"`
	// ActionsWebhook overrides the global actions webhook for this room.
	ActionsWebhook string `yaml:"actions_webhook"`
	// Invite are extra user IDs to invite besides the account owner.
	Invite []string `yaml:"invite"`
}

// Agent types.
const (
	AgentOpenWebUI = "openwebui"
	AgentOpenAI    = "openai"
	AgentWebhook   = "webhook"
)

// Agent is one conversational backend. The bridge owns session policy,
// rendering, commands and failure reporting; the adapter owns the protocol.
type Agent struct {
	Type string `yaml:"type"`
	URL  string `yaml:"url"`
	// APIKeyEnv names the environment variable holding the key. APIKey is the
	// literal, accepted for test setups and never recommended.
	APIKeyEnv string `yaml:"api_key_env"`
	APIKey    string `yaml:"api_key"`
	Model     string `yaml:"model"`

	// openwebui: the sidebar folder chats are filed under, the per-room tool
	// set, and any direct tool servers the room may reach. ToolServers is
	// passed through verbatim, because Open WebUI wants whole server objects
	// (url, auth, spec) there rather than names.
	Folder      string           `yaml:"folder"`
	ToolIDs     []string         `yaml:"tool_ids"`
	ToolServers []map[string]any `yaml:"tool_servers"`

	// openai: the bridge keeps the transcript, so it needs to know how much of
	// it to replay and what to put in front of it.
	SystemPrompt string `yaml:"system_prompt"`
	MaxHistory   int    `yaml:"max_history"`

	// webhook: extra headers for the sidecar, e.g. a shared secret.
	Headers map[string]string `yaml:"headers"`

	Timeout Duration `yaml:"timeout"`
	Session Session  `yaml:"session"`
}

// Session is when a conversation stops being the same conversation.
type Session struct {
	IdleMinutes  int  `yaml:"idle_minutes"`
	MaxTurns     int  `yaml:"max_turns"`
	CarrySummary bool `yaml:"carry_summary"`
}

// Source types.
const (
	SourceNotify = "notify"
	SourceNtfy   = "ntfy"
)

// Source is one inbound adapter. Every adapter produces the same internal
// notification, so rooms, rendering, dedupe, actions and threads never learn
// where a message came from.
type Source struct {
	Type string `yaml:"type"`

	// ntfy: the server and which topic maps to which room and ghost.
	URL    string               `yaml:"url"`
	Token  string               `yaml:"token"`
	Topics map[string]NtfyTopic `yaml:"topics"`
}

// NtfyTopic routes one ntfy topic into one room, as one ghost.
type NtfyTopic struct {
	Room  string `yaml:"room"`
	Ghost string `yaml:"ghost"`
	// Priority is the minimum ntfy priority (1-5) that is mirrored at all.
	MinPriority int `yaml:"min_priority"`
}

// Actions is where reactions, poll responses and thread events are reported.
type Actions struct {
	Webhook     string   `yaml:"webhook"`
	Timeout     Duration `yaml:"timeout"`
	MaxAttempts int      `yaml:"max_attempts"`
	// ReportUndeclared reports reactions that no notification declared, with a
	// null action name, so conventions can be invented without a redeploy.
	ReportUndeclared bool `yaml:"report_undeclared"`
}

// Log configures the zerolog writer.
type Log struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"` // json or console
}

// Limits are the safety valves: how deep the send queue goes before callers
// are told to back off, and how long history is kept.
type Limits struct {
	QueueSize             int   `yaml:"queue_size"`
	NotifyRetentionDays   int   `yaml:"notify_retention_days"`
	DeliveryRetentionDays int   `yaml:"delivery_retention_days"`
	MaxMediaBytes         int64 `yaml:"max_media_bytes"`
	MessageSplitBytes     int   `yaml:"message_split_bytes"`
}

// Duration is a time.Duration that unmarshals from "30s" as well as a number
// of seconds.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err == nil {
		parsed, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		*d = Duration(parsed)
		return nil
	}
	var secs float64
	if err := node.Decode(&secs); err != nil {
		return fmt.Errorf("invalid duration: %w", err)
	}
	*d = Duration(time.Duration(secs * float64(time.Second)))
	return nil
}

// Or returns the duration, or def when it is unset.
func (d Duration) Or(def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return time.Duration(d)
}

var (
	bridgeNameRe = regexp.MustCompile(`^sh-[a-z0-9-]{1,29}$`)
	keyRe        = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}$`)
	envRe        = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)
)

// Load reads, expands and validates a config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	expanded := envRe.ReplaceAllStringFunc(string(raw), func(m string) string {
		return os.Getenv(envRe.FindStringSubmatch(m)[1])
	})
	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(expanded))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg.Dir = dirOf(path)
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func dirOf(path string) string {
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		return "."
	}
	if idx == 0 {
		return "/"
	}
	return path[:idx]
}

func (c *Config) applyDefaults() {
	if c.Matrix.BaseDomain == "" {
		c.Matrix.BaseDomain = "beeper.com"
	}
	if c.Matrix.HomeserverDomain == "" {
		c.Matrix.HomeserverDomain = "beeper.local"
	}
	if c.Matrix.TokenEnv == "" {
		c.Matrix.TokenEnv = "MATRIX_ACCESS_TOKEN"
	}
	if c.Server.Bind == "" {
		c.Server.Bind = "0.0.0.0:8080"
	}
	if c.Server.IngestTokenEnv == "" {
		c.Server.IngestTokenEnv = "INTERCOM_INGEST_TOKEN"
	}
	if c.Server.AdminTokenEnv == "" {
		c.Server.AdminTokenEnv = "INTERCOM_ADMIN_TOKEN"
	}
	if c.Network.ID == "" {
		c.Network.ID = "intercom"
	}
	if c.Network.Name == "" {
		c.Network.Name = "Intercom"
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "json"
	}
	if c.Limits.QueueSize == 0 {
		c.Limits.QueueSize = 512
	}
	if c.Limits.NotifyRetentionDays == 0 {
		c.Limits.NotifyRetentionDays = 30
	}
	if c.Limits.DeliveryRetentionDays == 0 {
		c.Limits.DeliveryRetentionDays = 14
	}
	if c.Limits.MaxMediaBytes == 0 {
		c.Limits.MaxMediaBytes = 64 << 20
	}
	if c.Limits.MessageSplitBytes == 0 {
		c.Limits.MessageSplitBytes = 16 << 10
	}
	if c.Actions.MaxAttempts == 0 {
		c.Actions.MaxAttempts = 8
	}
	for key, room := range c.Rooms {
		if room.Kind == "" {
			room.Kind = KindChat
		}
		if room.Name == "" {
			room.Name = key
		}
		c.Rooms[key] = room
	}
	for key, agent := range c.Agents {
		if agent.Session.IdleMinutes == 0 {
			agent.Session.IdleMinutes = 120
		}
		if agent.Session.MaxTurns == 0 {
			agent.Session.MaxTurns = 40
		}
		if agent.MaxHistory == 0 {
			agent.MaxHistory = 20
		}
		c.Agents[key] = agent
	}
	for i, src := range c.Sources {
		if src.Type == SourceNtfy && src.URL == "" {
			src.URL = "http://ntfy"
		}
		c.Sources[i] = src
	}
}

// Validate rejects a config that would only fail later, at the point where a
// notification is already in flight and there is nowhere good to report it.
func (c *Config) Validate() error {
	if !bridgeNameRe.MatchString(c.Network.Bridge) {
		return fmt.Errorf("network.bridge %q must match ^sh-[a-z0-9-]+$ (Beeper requirement)", c.Network.Bridge)
	}
	if !keyRe.MatchString(c.Network.ID) {
		return fmt.Errorf("network.id %q must match %s", c.Network.ID, keyRe)
	}
	if len(c.Rooms) == 0 {
		return fmt.Errorf("no rooms configured")
	}
	for _, key := range SortedKeys(c.Ghosts) {
		if !keyRe.MatchString(key) {
			return fmt.Errorf("ghost key %q must match %s (it becomes an MXID localpart)", key, keyRe)
		}
		if c.Ghosts[key].Name == "" {
			return fmt.Errorf("ghost %q has no name", key)
		}
	}
	for _, key := range SortedKeys(c.Rooms) {
		room := c.Rooms[key]
		if !keyRe.MatchString(key) {
			return fmt.Errorf("room key %q must match %s", key, keyRe)
		}
		switch room.Kind {
		case KindBroadcast, KindChat, KindDM:
		default:
			return fmt.Errorf("room %q: unknown kind %q", key, room.Kind)
		}
		if len(room.Ghosts) == 0 {
			return fmt.Errorf("room %q has no ghosts; nothing could speak in it", key)
		}
		if room.Kind == KindDM && len(room.Ghosts) != 1 {
			return fmt.Errorf("room %q is a dm but has %d ghosts; a dm renders as one conversation partner", key, len(room.Ghosts))
		}
		for _, ghost := range room.Ghosts {
			if _, ok := c.Ghosts[ghost]; !ok {
				return fmt.Errorf("room %q references undefined ghost %q", key, ghost)
			}
		}
		for _, tag := range room.Tags {
			if tag != "favourite" && tag != "lowpriority" {
				return fmt.Errorf("room %q: unknown tag %q (favourite or lowpriority)", key, tag)
			}
		}
		if room.Agent != "" {
			if _, ok := c.Agents[room.Agent]; !ok {
				return fmt.Errorf("room %q references undefined agent %q", key, room.Agent)
			}
		}
	}
	for _, key := range SortedKeys(c.Agents) {
		agent := c.Agents[key]
		switch agent.Type {
		case AgentOpenWebUI, AgentOpenAI, AgentWebhook:
		default:
			return fmt.Errorf("agent %q: unknown type %q", key, agent.Type)
		}
		if agent.URL == "" {
			return fmt.Errorf("agent %q has no url", key)
		}
		if agent.Type != AgentWebhook && agent.Model == "" {
			return fmt.Errorf("agent %q has no model", key)
		}
	}
	seenNtfy := false
	for _, src := range c.Sources {
		switch src.Type {
		case SourceNotify:
		case SourceNtfy:
			if seenNtfy {
				return fmt.Errorf("more than one ntfy source; put every topic in one adapter")
			}
			seenNtfy = true
			for topic, route := range src.Topics {
				if _, ok := c.Rooms[route.Room]; !ok {
					return fmt.Errorf("ntfy topic %q routes to undefined room %q", topic, route.Room)
				}
				if route.Ghost != "" {
					if _, ok := c.Ghosts[route.Ghost]; !ok {
						return fmt.Errorf("ntfy topic %q routes to undefined ghost %q", topic, route.Ghost)
					}
				}
			}
		default:
			return fmt.Errorf("unknown source type %q", src.Type)
		}
	}
	return nil
}

// AccountToken reads the Beeper account token from the environment.
func (c *Config) AccountToken() string { return os.Getenv(c.Matrix.TokenEnv) }

// IngestToken and AdminToken prefer the environment over the literal.
func (c *Config) IngestToken() string {
	if v := os.Getenv(c.Server.IngestTokenEnv); v != "" {
		return v
	}
	return c.Server.IngestToken
}

func (c *Config) AdminToken() string {
	if v := os.Getenv(c.Server.AdminTokenEnv); v != "" {
		return v
	}
	return c.Server.AdminToken
}

// Key returns the API key for an agent, environment first.
func (a Agent) Key() string {
	if a.APIKeyEnv != "" {
		if v := os.Getenv(a.APIKeyEnv); v != "" {
			return v
		}
	}
	return a.APIKey
}

// GhostForRoom returns the ghost a notification should speak as: the one it
// asked for, or the room's first ghost.
func (c *Config) GhostForRoom(roomKey, ghost string) (string, error) {
	room, ok := c.Rooms[roomKey]
	if !ok {
		return "", fmt.Errorf("unknown room %q", roomKey)
	}
	if ghost == "" {
		return room.Ghosts[0], nil
	}
	for _, g := range room.Ghosts {
		if g == ghost {
			return ghost, nil
		}
	}
	// A ghost that exists but is not a member of the room would render as a
	// raw MXID, which looks like a bug rather than a misconfiguration.
	return "", fmt.Errorf("ghost %q is not a member of room %q", ghost, roomKey)
}

// SortedKeys makes map iteration deterministic, which matters because room
// and ghost reconciliation order shows up in the timeline.
func SortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
