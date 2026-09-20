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
	Network  Network          `yaml:"network"`
	Matrix   Matrix           `yaml:"matrix"`
	Server   Server           `yaml:"server"`
	Ghosts   map[string]Ghost `yaml:"ghosts"`
	Rooms    map[string]Room  `yaml:"rooms"`
	Agents   map[string]Agent `yaml:"agents"`
	Sources  []Source         `yaml:"sources"`
	Actions  Actions          `yaml:"actions"`
	Progress Progress         `yaml:"progress"`
	// Reasoning is the model-id convention that says how hard a model thinks.
	Reasoning Reasoning `yaml:"reasoning"`
	// Notices is how the bridge's own messages are told apart from the agent's.
	Notices Notices `yaml:"notices"`
	// Questions is what happens when the model asks ME something.
	Questions Questions `yaml:"questions"`
	Log       Log       `yaml:"log"`
	Limits    Limits    `yaml:"limits"`

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
	// StatusRoom, when named, is a bridge-bot room where the bridge itself
	// reports connection changes, reloads and delivery failures as dim
	// centred notices instead of log lines. Empty turns it off.
	StatusRoom string `yaml:"status_room"`
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
	// DeletePlaceholder keeps the "This message has been deleted" tombstone
	// when something in this room is redacted. The bridge hides it by default
	// (com.beeper.room_features delete_hide_placeholder): it deletes its own
	// messages as part of ordinary work — /undo takes back an exchange, a
	// command tidies up after itself — and a tombstone is louder than what it
	// replaced. Turn it on where a disappearance would be worse than a marker,
	// e.g. a room of announcements somebody else acts on.
	DeletePlaceholder bool `yaml:"delete_placeholder"`
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
	// PublicURL is the same backend as a human reaches it. URL is usually a
	// compose hostname, which is exactly the wrong thing to put in a link
	// someone is meant to open — /link, /history and /share use this instead
	// when it is set.
	PublicURL string `yaml:"public_url"`
	// APIKeyEnv names the environment variable holding the key. APIKey is the
	// literal, accepted for test setups and never recommended.
	APIKeyEnv string `yaml:"api_key_env"`
	APIKey    string `yaml:"api_key"`
	Model     string `yaml:"model"`

	// openwebui: the sidebar folder chats are filed under, the per-room tool
	// set, and any direct tool servers the room may reach. ToolServers is
	// passed through verbatim, because Open WebUI wants whole server objects
	// (url, auth, spec) there rather than names.
	//
	// SocketTokenEnv names a SESSION JWT for the user, used only for the
	// socket.io connection that carries live deltas — the socket verifies
	// tokens with the session secret, so the API key is refused there.
	// Without it answers still arrive, just not token by token.
	SocketTokenEnv string           `yaml:"socket_token_env"`
	Folder         string           `yaml:"folder"`
	ToolIDs        []string         `yaml:"tool_ids"`
	ToolServers    []map[string]any `yaml:"tool_servers"`

	// Features are Open WebUI's built-in capabilities: web_search,
	// code_interpreter, image_generation, memory. They are not tools the
	// bridge can name — Open WebUI injects them itself, and ONLY for a request
	// that carries a session id, because it treats anything else as an API
	// caller that "does not expect hidden tools"
	// (utils/middleware.py, measured 2026-09-20). Naming any feature here
	// therefore also makes the adapter send a session id, which puts the turn
	// on Open WebUI's background-task path.
	Features []string `yaml:"features"`

	// Toolsets are tool ids that can be switched on and off in the room with
	// /tools, on top of the always-on ToolIDs. They exist because "which tools
	// does this room have" and "which tools do I want for this question" are
	// different questions: a room can be allowed to write to something without
	// every casual message carrying that authority.
	Toolsets map[string]Toolset `yaml:"toolsets"`

	// openai: the bridge keeps the transcript, so it needs to know how much of
	// it to replay and what to put in front of it.
	SystemPrompt string `yaml:"system_prompt"`
	MaxHistory   int    `yaml:"max_history"`

	// webhook: extra headers for the sidecar, e.g. a shared secret.
	Headers map[string]string `yaml:"headers"`

	// NoStream turns off live streaming for this agent: the answer arrives
	// as one message when it is complete. Streaming is on by default.
	NoStream bool `yaml:"no_stream"`

	Timeout Duration `yaml:"timeout"`
	Session Session  `yaml:"session"`
}

// Toolset is one switchable group of tools. The name is what /tools takes.
//
// A toolset's state belongs to the CONVERSATION, not the room: it starts at
// Default, /tools changes it, and it is back to Default in the next
// conversation. That way an escalation cannot outlive the thing it was for.
type Toolset struct {
	// Description is shown by /tools. Write what it grants, not what it is.
	Description string `yaml:"description"`
	// Tools are backend tool ids, e.g. Open WebUI's server:mcp:<id>.
	Tools []string `yaml:"tools"`
	// Default is the state at the start of a conversation.
	Default bool `yaml:"default"`
	// Idle turns the toolset off again after this long without a turn, and
	// says so in the room. Zero means it lasts as long as the conversation.
	// Only meaningful for a toolset that is off by default: the point is that
	// an escalation closes itself when the reason for it has passed.
	Idle Duration `yaml:"idle"`
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
// Progress modes: how an answer that is still being produced is shown.
const (
	// ProgressOff shows nothing until the answer is finished: the ghost is
	// simply typing the whole time. The default, because it is the only mode
	// that costs no extra events and misleads no client.
	ProgressOff = "off"
	// ProgressEdits posts the first chunk and rewrites it with the answer so
	// far, every Interval. Works in every client.
	ProgressEdits = "edits"
	// ProgressStream publishes com.beeper.stream deltas to subscribed devices
	// instead. Protocol-correct and cheap, but no client painted it when this
	// was measured (2026-09-20) — keep it for when one does.
	ProgressStream = "stream"
)

// Progress is how a running answer is presented: whether the bubble grows
// while the model works, and what the reaction on the question says about
// which phase it is in.
type Progress struct {
	// Mode is off, edits or stream.
	Mode string `yaml:"mode"`
	// Interval is how often the bubble is rewritten in edits mode.
	Interval Duration `yaml:"interval"`
	// Suffix marks the text as unfinished; it is appended to every partial
	// rendering and never to the final one.
	Suffix string `yaml:"suffix"`
	// Reactions are the emoji reacted to the question while the answer is
	// produced, one per phase. A phase set to "" is not marked; a phase the
	// backend never reports is never shown. The reaction is removed when the
	// answer lands.
	Reactions Reactions `yaml:"reactions"`
}

// Reactions name one emoji per phase of a turn. Which of them are ever seen
// depends on what the backend reports: an adapter that only knows "it is
// working" spends the whole turn on Queued.
type Reactions struct {
	// Queued is from the moment the question is accepted until the backend
	// says anything — the queue, the prompt being processed, the wait.
	Queued string `yaml:"queued"`
	// Thinking is reasoning: the model is producing thoughts, not an answer.
	Thinking string `yaml:"thinking"`
	// Tools is any tool call other than a web search.
	Tools string `yaml:"tools"`
	// Web is a web search or fetch specifically, because it is the one tool
	// whose latency is somebody else's.
	Web string `yaml:"web"`
	// Writing is the answer itself being generated.
	Writing string `yaml:"writing"`
	// Asking is the model waiting for me to answer a question of its own.
	Asking string `yaml:"asking"`
	// Compacting is the conversation being summarised in place by /compact.
	// Open WebUI also compacts on its own past its token threshold, but it
	// says nothing when it does, so that one is never marked.
	Compacting string `yaml:"compacting"`
}

// Emoji returns the reaction for a phase, or "" when the phase is not marked.
func (r Reactions) Emoji(phase string) string {
	switch phase {
	case "queued":
		return r.Queued
	case "thinking":
		return r.Thinking
	case "tools":
		return r.Tools
	case "web":
		return r.Web
	case "writing", "generating":
		return r.Writing
	case "asking":
		return r.Asking
	case "compacting":
		return r.Compacting
	}
	return ""
}

// Reasoning describes how this installation names the thinking variants of a
// model, so that /think can move between them.
//
// It is a NAMING convention, not a protocol: catalogues of this kind publish
// one model id per reasoning effort — `some-model`, `some-model:t`,
// `some-model:m` — and the only thing the bridge has to know is which suffix
// means what. Nothing here is specific to a suffix style; a deployment that
// writes `-high` instead of `:x` just says so.
//
// Which id means "does not think" is also the deployment's to say. Usually it
// is the bare id, and OffSuffix is empty. But a catalogue is free to make the
// bare id the thinking one and give the non-thinking variant a suffix of its
// own, in which case OffSuffix names it — otherwise /think off would be a
// button that quietly does nothing.
//
// Which levels a given model actually has is never assumed: the backend's own
// model list decides, so /think offers the variants that exist and says so
// when a model has none.
type Reasoning struct {
	// Enabled turns the feature and the /think command on. Off by default,
	// because a catalogue with no such convention would only get a command that
	// cannot work.
	Enabled bool `yaml:"enabled"`
	// Levels are the suffixes, lowest effort first — the order /think lists
	// them in. "off" is implicit and always first.
	Levels []ReasoningLevel `yaml:"levels"`
	// OffSuffix reaches the non-thinking variant when the bare id is not it.
	// Empty (the default) means the bare id is the off one.
	OffSuffix string `yaml:"off_suffix"`
}

// Off is the non-thinking variant as a level, so that the rest of the code can
// treat it like any other one. Its id is the reserved word /think takes.
func (r Reasoning) Off() ReasoningLevel {
	return ReasoningLevel{Name: "Off", IDs: []string{ReasoningOff}, Suffix: r.OffSuffix}
}

// ReasoningLevel is one suffix, what it is called, and what you may type for
// it. The name is for reading; the ids are for typing, which is why there can
// be several — "Extra High" is `extra` when you are being explicit and `x`
// when you are matching the suffix you already know.
type ReasoningLevel struct {
	// Name is the display name: "Extra High".
	Name string `yaml:"name"`
	// IDs are the words /think accepts for this level, most explicit first.
	// The first one is what error messages and hints suggest.
	IDs []string `yaml:"ids"`
	// Suffix is appended to the bare model id to reach this level, e.g. ":m".
	Suffix string `yaml:"suffix"`
	// Description is shown by /think, when it has something to add.
	Description string `yaml:"description"`
}

// ID is the canonical word for this level: the first of its ids.
func (l ReasoningLevel) ID() string {
	if len(l.IDs) == 0 {
		return strings.ToLower(l.Name)
	}
	return l.IDs[0]
}

// Label is the name, falling back to the id for a level that has none.
func (l ReasoningLevel) Label() string {
	if l.Name == "" {
		return l.ID()
	}
	return l.Name
}

// Split takes a model id apart into its bare id and its reasoning level. The
// level is nil when the id carries no known suffix: with no OffSuffix that is
// the "off" variant, and with one it is the id whose effort the catalogue
// picks by itself.
func (r Reasoning) Split(model string) (string, *ReasoningLevel) {
	candidates := r.Levels
	if r.OffSuffix != "" {
		candidates = append(append([]ReasoningLevel{}, r.Levels...), r.Off())
	}
	var best *ReasoningLevel
	for i := range candidates {
		level := &candidates[i]
		// Longest suffix wins: ":xl" must not be read as ":x" plus a stray l.
		if level.Suffix != "" && strings.HasSuffix(model, level.Suffix) &&
			(best == nil || len(level.Suffix) > len(best.Suffix)) {
			best = level
		}
	}
	if best == nil {
		return model, nil
	}
	return strings.TrimSuffix(model, best.Suffix), best
}

// Level finds a level by any of the words typed into /think.
func (r Reasoning) Level(id string) (*ReasoningLevel, bool) {
	for i := range r.Levels {
		for _, candidate := range r.Levels[i].IDs {
			if strings.EqualFold(candidate, id) {
				return &r.Levels[i], true
			}
		}
	}
	return nil, false
}

// Notice senders.
const (
	// NoticeSenderBot sends bridge messages as the bridge bot, whose m.notice
	// Beeper renders as dim centred text with no bubble — visibly not the
	// agent, in any room. Measured 2026-09-20; it does not need a bridge-bot
	// room, only the bot as sender.
	NoticeSenderBot = "bot"
	// NoticeSenderGhost sends them as the room's ghost, which looks like an
	// ordinary message. With a per-message profile it at least carries its own
	// name; without one it is indistinguishable from the agent.
	NoticeSenderGhost = "ghost"
)

// Notices is how the bridge speaks for itself.
//
// A room has one ghost and two voices in it: the agent answering, and the
// bridge reporting on itself ("Nothing to undo yet.", "`rw` switched off after
// 15 min idle"). Beeper renders m.notice exactly like a normal message inside
// a portal, so telling them apart is the bridge's job:
//
//   - a per-message profile (com.beeper.per_message_profile) gives the notice
//     its own name and avatar without adding a second room member, which would
//     turn a dm into a group;
//   - notices are laid out as TABLES, never as lists: the dim style centres
//     the whole message and a list's markers stay at the left margin;
//   - and a notice caused by a command is sent as a reply to it, so the answer
//     is attached to the question. Notices nobody asked for — a toolset going
//     idle — are not replies to anything and are sent plainly.
type Notices struct {
	// Sender is bot or ghost. The bot is the whole reason a bridge message
	// looks different at all, so it is the default; ghost is for a client that
	// renders the bot's notices badly, and then the profile below matters.
	Sender string `yaml:"sender"`
	// Name is the display name of the per-message profile. Empty turns the
	// profile off and leaves only the emoji and the italics.
	Name string `yaml:"name"`
	// ID is the profile's stable identity. Clients derive the colour and the
	// generated avatar from it, so changing it changes how the notice looks.
	ID string `yaml:"id"`
	// Avatar is a path to an image, as a ghost's is. Without one the client
	// generates one from the name.
	Avatar string `yaml:"avatar"`
	// Reply sends a notice caused by a command as a reply to that command.
	Reply *bool `yaml:"reply"`

	// ClearButton puts a reaction on every command I type, which works as a
	// delete button on the bridge's answer: tapping it adds the same reaction
	// from me, and that is the signal to sweep the command and everything the
	// bridge said in answer to it. It has to be done this way round because a
	// bridge message cannot be reacted to at all — only my own can, and a
	// reaction the bot already placed is one tap away. On by default; `/clear`
	// does the same thing without a reaction, and deleting the command message
	// itself takes the answer with it either way.
	ClearButton *bool `yaml:"clear_button"`
	// ClearEmoji is which reaction that is. A wastebasket by default, because
	// the button has to say what it does with no label.
	ClearEmoji string `yaml:"clear_emoji"`

	// FormatCommands rewrites a command I typed as inline code, so `/help`
	// reads as a command rather than as something I said. The bridge cannot
	// edit somebody else's event, but it holds the account token and the
	// message is mine, so it sends the edit AS me. The cost is the client's
	// "Edited" label on every command.
	FormatCommands bool `yaml:"format_commands"`
}

// HasClearButton defaults to true.
func (n Notices) HasClearButton() bool { return n.ClearButton == nil || *n.ClearButton }

// ClearKey is the reaction the button uses, wastebasket by default.
const ClearKey = "🗑️"

// ClearEmojiOr is the configured button emoji, or the default.
func (n Notices) ClearEmojiOr() string {
	if n.ClearEmoji == "" {
		return ClearKey
	}
	return n.ClearEmoji
}

// ClearTriggeredBy reports whether this reaction key is the button being
// pressed. Emoji reach the bridge with or without the variation selector
// depending on the client, and the two are the same picture.
func (n Notices) ClearTriggeredBy(key string) bool {
	return n.HasClearButton() && sameEmoji(key, n.ClearEmojiOr())
}

func sameEmoji(a, b string) bool {
	return strings.ReplaceAll(a, "\ufe0f", "") == strings.ReplaceAll(b, "\ufe0f", "")
}

// RepliesToCommands defaults to true: a bridge answer belongs to the message
// that asked for it.
func (n Notices) RepliesToCommands() bool { return n.Reply == nil || *n.Reply }

// Questions is the ask_user path: a model that stops to ask something gets a
// poll per question, and the answers resume the turn where it stopped.
type Questions struct {
	// Timeout is how long a question waits. It is generous by default: the
	// turn is paused on the backend, not burning anything, and a question
	// asked while I am away is still worth answering when I come back.
	Timeout Duration `yaml:"timeout"`
	// AfterAnswer is what the room is left with once a question has been
	// answered. The poll card is a control, and a control that can no longer
	// do anything is clutter — but what was asked and what I picked is part of
	// how the conversation went, so the default trades the card for a line.
	// An unanswered question is never touched by this: it stays, closed.
	AfterAnswer AnswerRecord `yaml:"after_answer"`
}

// AnswerRecord is the three ways an answered question can be tidied away.
type AnswerRecord string

const (
	// AnswerReplace removes the poll and posts "what was asked: what I
	// picked" in its place. The default.
	AnswerReplace AnswerRecord = "replace"
	// AnswerKeep leaves the poll (ended, so the answer cannot be changed to
	// one the model never saw) and posts the line as well.
	AnswerKeep AnswerRecord = "keep"
	// AnswerDelete removes the poll and says nothing.
	AnswerDelete AnswerRecord = "delete"
)

// Record is AfterAnswer with the default applied.
func (q Questions) Record() AnswerRecord {
	if q.AfterAnswer == "" {
		return AnswerReplace
	}
	return q.AfterAnswer
}

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
	if c.Progress.Mode == "" {
		c.Progress.Mode = ProgressOff
	}
	if c.Progress.Interval == 0 {
		c.Progress.Interval = Duration(1500 * time.Millisecond)
	}
	if c.Progress.Suffix == "" {
		c.Progress.Suffix = " …"
	}
	if c.Notices.Sender == "" {
		c.Notices.Sender = NoticeSenderBot
	}
	if c.Notices.ID == "" {
		c.Notices.ID = "system"
	}
	if c.Progress.Reactions == (Reactions{}) {
		c.Progress.Reactions = Reactions{
			Queued:     "⏳",
			Thinking:   "🧠",
			Tools:      "🛠️",
			Web:        "🌐",
			Writing:    "✍️",
			Asking:     "❓",
			Compacting: "🗜️",
		}
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
	switch c.Progress.Mode {
	case ProgressOff, ProgressEdits, ProgressStream:
	default:
		return fmt.Errorf("progress.mode %q must be %s, %s or %s", c.Progress.Mode, ProgressOff, ProgressEdits, ProgressStream)
	}
	if c.Progress.Mode == ProgressEdits && c.Progress.Interval < Duration(250*time.Millisecond) {
		return fmt.Errorf("progress.interval %s is too short: an edit per message part is a flood, 250ms is the floor",
			time.Duration(c.Progress.Interval))
	}
	switch c.Notices.Sender {
	case NoticeSenderBot, NoticeSenderGhost:
	default:
		return fmt.Errorf("notices.sender %q must be %s or %s", c.Notices.Sender, NoticeSenderBot, NoticeSenderGhost)
	}
	switch c.Questions.Record() {
	case AnswerReplace, AnswerKeep, AnswerDelete:
	default:
		return fmt.Errorf("questions.after_answer %q must be %s, %s or %s",
			c.Questions.AfterAnswer, AnswerReplace, AnswerKeep, AnswerDelete)
	}
	if c.Reasoning.Enabled && len(c.Reasoning.Levels) == 0 {
		return fmt.Errorf("reasoning.enabled is set but no levels are configured; /think would have nothing to offer")
	}
	seenID := map[string]bool{}
	seenSuffix := map[string]bool{}
	if c.Reasoning.OffSuffix != "" {
		seenSuffix[c.Reasoning.OffSuffix] = true
	}
	for i, level := range c.Reasoning.Levels {
		switch {
		case level.Name == "":
			return fmt.Errorf("reasoning.levels[%d]: no name", i)
		case level.Suffix == "":
			return fmt.Errorf("reasoning.levels[%d] (%s): no suffix; the bare id already means no reasoning", i, level.Name)
		case len(level.IDs) == 0:
			return fmt.Errorf("reasoning.levels[%d] (%s): no ids; there would be no way to select it", i, level.Name)
		case level.Suffix == c.Reasoning.OffSuffix:
			return fmt.Errorf("reasoning.levels[%d] (%s): suffix %q is already reasoning.off_suffix", i, level.Name, level.Suffix)
		case seenSuffix[level.Suffix]:
			return fmt.Errorf("reasoning.levels: suffix %q is declared twice", level.Suffix)
		}
		seenSuffix[level.Suffix] = true
		for _, id := range level.IDs {
			switch {
			case !keyRe.MatchString(id):
				return fmt.Errorf("reasoning.levels[%d] (%s): id %q must match %s (it is typed into /think)", i, level.Name, id, keyRe)
			case strings.EqualFold(id, ReasoningOff):
				return fmt.Errorf("reasoning.levels[%d] (%s): %q is reserved — it always selects the non-thinking variant (reasoning.off_suffix, or the bare id when that is empty)", i, level.Name, ReasoningOff)
			case seenID[strings.ToLower(id)]:
				return fmt.Errorf("reasoning.levels: id %q is declared twice", id)
			}
			seenID[strings.ToLower(id)] = true
		}
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
		for _, name := range SortedKeys(agent.Toolsets) {
			if !keyRe.MatchString(name) {
				return fmt.Errorf("agent %q: toolset %q must match %s (it is typed into /tools)", key, name, keyRe)
			}
			if len(agent.Toolsets[name].Tools) == 0 {
				return fmt.Errorf("agent %q: toolset %q has no tools", key, name)
			}
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

// SocketToken returns the session JWT for the socket, if configured.
func (a Agent) SocketToken() string {
	if a.SocketTokenEnv == "" {
		return ""
	}
	return os.Getenv(a.SocketTokenEnv)
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

// ReasoningOff is the word /think takes for the non-thinking model id.
// It is not a level: there is no suffix to configure for "no suffix".
const ReasoningOff = "off"
