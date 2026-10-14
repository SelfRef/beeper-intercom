package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/loop"
	"github.com/SelfRef/beeper-intercom/internal/store"
)

// tools runs the agent loop in the bridge itself: an OpenAI-compatible
// endpoint for the model, MCP servers for the tools (internal/loop), and the
// transcript in the bridge's store like the openai adapter, whose plumbing —
// conversations, model list, undo — it reuses as is.
//
// What it adds over a chat backend is the part a chat backend hides: every
// tool call is visible as it happens, a call to a server marked `confirm`
// waits for a yes in the room, a model that fails hands the turn to the next
// one, and the model can ask a multiple-choice question that arrives as a
// poll. Between turns only the text is kept, plus a line naming the tools
// that were used — tool results are stale by the next question, and replaying
// them is what makes a long conversation slow.
type tools struct {
	*openAI
	chat *loop.Chat
	skip map[string]*regexp.Regexp

	mu sync.Mutex
	// paused are turns waiting for an answer, by conversation. In memory only:
	// a restart forgets the question, and the next message starts a new turn.
	paused map[string]*turnState
}

// turnState is a turn in progress: enough to resume it after a question.
type turnState struct {
	token    string
	user     string
	messages []loop.Message
	held     []loop.ToolCall
	models   []string
	attached []string
	trusted  map[string]bool
	said     []string
	used     []string
	usage    loop.Usage
}

func newTools(cfg config.Agent, client *http.Client, st *store.Store, log zerolog.Logger) *tools {
	t := &tools{
		openAI: newOpenAI(cfg, client, st, log),
		chat:   &loop.Chat{URL: cfg.URL, Key: cfg.Key(), Headers: cfg.Headers, HTTP: client},
		skip:   make(map[string]*regexp.Regexp),
		paused: make(map[string]*turnState),
	}
	for name, server := range cfg.MCP {
		if server.ConfirmSkip != "" {
			// Validated with the config.
			t.skip[name] = regexp.MustCompile(server.ConfirmSkip)
		}
	}
	return t
}

func (t *tools) Type() string { return config.AgentTools }

func (t *tools) Send(ctx context.Context, conv Conversation, turn Turn, sink Sink) (*Reply, error) {
	if turn.Internal {
		return t.internal(ctx, conv, turn)
	}
	t.abandon(ctx, conv.ID)

	history, err := t.store.Transcript(ctx, conv.ID, t.cfg.MaxHistory)
	if err != nil {
		return nil, err
	}
	messages := t.system()
	for _, entry := range history {
		messages = append(messages, loop.Message{Role: entry.Role, Content: entry.Content})
	}
	messages = append(messages, loop.Message{Role: "user", Content: userContent(turn)})

	model := conv.Model
	if model == "" {
		model = t.cfg.Model
	}
	state := &turnState{
		user:     turn.Text,
		messages: messages,
		models:   t.models(model, sink),
		attached: mergeTools(t.cfg.ToolIDs, turn.Tools),
		trusted:  make(map[string]bool, len(turn.Tools)),
	}
	for _, name := range turn.Tools {
		state.trusted[name] = true
	}
	sink.Report("queued")
	return t.run(ctx, conv, state, nil, sink)
}

// run opens the turn's servers, settles any held calls with answers, and
// lets the model continue.
func (t *tools) run(ctx context.Context, conv Conversation, state *turnState, answers map[string]string, sink Sink) (*Reply, error) {
	servers := make([]loop.Server, 0, len(state.attached))
	for _, name := range state.attached {
		server := t.cfg.MCP[name]
		servers = append(servers, loop.Server{Name: name, URL: server.URL, Headers: server.AllHeaders()})
	}
	box, problems := loop.Open(ctx, servers, t.client)
	defer box.Close()
	for _, problem := range problems {
		sink.Note(problem)
	}

	var phase phaseTracker
	runner := &loop.Runner{
		Chat:   t.chat,
		Models: state.models,
		Tools:  box,
		Ask:    true,
		Hold:   func(call loop.ToolCall) bool { return t.held(call, state.trusted) },
		Events: loop.Events{
			Thinking: func() { phase.to(sink, "thinking") },
			Text: func(text string) {
				phase.to(sink, "writing")
				sink.Push(text)
			},
			Tool: func(call loop.ToolCall) {
				name := loop.Describe(call)
				state.used = append(state.used, name)
				phase.to(sink, toolPhase(name))
				line := "🔧 " + name
				if args := loop.Arguments(call, 80); args != "" {
					line += " " + args
				}
				sink.Note(line)
			},
			Note: sink.Note,
		},
	}

	for _, call := range state.held {
		state.messages = append(state.messages, t.settle(ctx, runner, call, answers))
	}
	state.held = nil

	result, err := runner.Run(ctx, state.messages)
	if err != nil {
		return nil, err
	}
	state.models = runner.Models
	state.messages = result.Messages
	state.usage.Add(result.Usage)
	if text := strings.TrimSpace(result.Text); text != "" {
		state.said = append(state.said, text)
	}
	reply := &Reply{Text: result.Text, Usage: usageOf(state.usage)}

	if len(result.Held) > 0 {
		state.held = result.Held
		state.token = uuid.NewString()
		reply.Ask = t.askFor(state)
		t.mu.Lock()
		t.paused[conv.ID] = state
		t.mu.Unlock()
		return reply, nil
	}
	if err := t.persist(ctx, conv.ID, state, ""); err != nil {
		return nil, err
	}
	return reply, nil
}

// held decides whether a call waits for a yes in the room.
func (t *tools) held(call loop.ToolCall, trusted map[string]bool) bool {
	server, tool := loop.SplitName(call.Function.Name)
	cfg, ok := t.cfg.MCP[server]
	if !ok || !cfg.Confirm || trusted[server] {
		return false
	}
	if skip := t.skip[server]; skip != nil && skip.MatchString(tool) {
		return false
	}
	return true
}

// Labels of a confirmation poll.
const (
	confirmAllow = "Allow"
	confirmDeny  = "Deny"
)

// askFor turns held calls into questions: ask_user's own, and one yes/no per
// call that needs confirming. Question ids carry the call id so the answers
// find their way back.
func (t *tools) askFor(state *turnState) *Ask {
	ask := &Ask{CallID: state.token, MessageID: state.token}
	for _, call := range state.held {
		if call.Function.Name != loop.AskUser {
			text := "Allow " + loop.Describe(call)
			if args := loop.Arguments(call, 160); args != "" {
				text += " " + args
			}
			ask.Questions = append(ask.Questions, Question{
				ID:      call.ID,
				Text:    text + "?",
				Options: []QuestionOption{{Label: confirmAllow}, {Label: confirmDeny}},
			})
			continue
		}
		var args loop.AskUserArgs
		_ = json.Unmarshal([]byte(call.Function.Arguments), &args)
		for _, q := range args.Questions {
			question := Question{ID: call.ID + "/" + q.ID, Text: q.Question, AllowOther: q.AllowOther}
			for _, option := range q.Options {
				question.Options = append(question.Options, QuestionOption{Label: option.Label, Description: option.Description})
			}
			ask.Questions = append(ask.Questions, question)
		}
	}
	return ask
}

// settle answers one held call: runs it when it was allowed, and tells the
// model what happened otherwise.
func (t *tools) settle(ctx context.Context, runner *loop.Runner, call loop.ToolCall, answers map[string]string) loop.Message {
	if call.Function.Name != loop.AskUser {
		if answers[call.ID] == confirmAllow {
			return runner.Exec(ctx, call)
		}
		return loop.ToolResult(call, "The user did not allow this call, so it was not run. Do not try it again unless they ask.")
	}
	var args loop.AskUserArgs
	_ = json.Unmarshal([]byte(call.Function.Arguments), &args)
	got := make(map[string]string, len(args.Questions))
	for _, q := range args.Questions {
		answer, ok := answers[call.ID+"/"+q.ID]
		if !ok {
			answer = "(no answer)"
		}
		got[q.ID] = answer
	}
	raw, _ := json.Marshal(map[string]any{"answers": got})
	return loop.ToolResult(call, string(raw))
}

// Answer resumes a turn that stopped at a question. No answers at all — the
// question timed out, or a new message replaced it — ends the turn without
// asking the model again: a confirmation nobody gave is a no, and the turn's
// text so far is what goes into the transcript.
func (t *tools) Answer(ctx context.Context, conv Conversation, ask *Ask, answers map[string]string, sink Sink) (*Reply, error) {
	t.mu.Lock()
	state := t.paused[conv.ID]
	if state == nil || ask == nil || state.token != ask.MessageID {
		t.mu.Unlock()
		return nil, nil
	}
	delete(t.paused, conv.ID)
	t.mu.Unlock()

	if len(answers) == 0 {
		const stopped = "Nothing was run: the question went unanswered."
		if err := t.persist(ctx, conv.ID, state, stopped); err != nil {
			return nil, err
		}
		return &Reply{Text: stopped, Usage: usageOf(state.usage)}, nil
	}
	return t.run(ctx, conv, state, answers, sink)
}

// abandon closes a turn left waiting at a question when the next message
// arrives instead of an answer.
func (t *tools) abandon(ctx context.Context, convID string) {
	t.mu.Lock()
	state := t.paused[convID]
	delete(t.paused, convID)
	t.mu.Unlock()
	if state != nil {
		if err := t.persist(ctx, convID, state, "(stopped at a question that was not answered)"); err != nil {
			t.log.Warn().Err(err).Msg("Could not record an abandoned turn")
		}
	}
}

// persist writes the finished turn to the transcript: my message, what the
// model said, and which tools it used.
func (t *tools) persist(ctx context.Context, convID string, state *turnState, extra string) error {
	if convID == "" {
		return nil
	}
	answer := strings.Join(append(append([]string{}, state.said...), extra), "\n\n")
	if len(state.used) > 0 {
		answer += "\n\n(Tools used for this answer: " + strings.Join(dedupe(state.used), ", ") + ".)"
	}
	if err := t.store.AppendTranscript(ctx, convID, "user", state.user); err != nil {
		return err
	}
	return t.store.AppendTranscript(ctx, convID, "assistant", strings.TrimSpace(answer))
}

// internal answers the bridge's own request — the summary at a rotation —
// from the transcript, with the helper model and no tools, and leaves no trace
// in the conversation.
func (t *tools) internal(ctx context.Context, conv Conversation, turn Turn) (*Reply, error) {
	messages := t.system()
	if conv.ID != "" {
		history, err := t.store.Transcript(ctx, conv.ID, t.cfg.MaxHistory)
		if err != nil {
			return nil, err
		}
		for _, entry := range history {
			messages = append(messages, loop.Message{Role: entry.Role, Content: entry.Content})
		}
	}
	messages = append(messages, loop.Message{Role: "user", Content: turn.Text})
	model := conv.Model
	if model == "" {
		model = t.cfg.Model
	}
	if t.cfg.HelperModel != "" {
		model = t.cfg.HelperModel
	}
	return t.plain(ctx, messages, t.models(model, Sink{}))
}

// Ask is /btw: one question, no history, no tools.
func (t *tools) Ask(ctx context.Context, model, question string) (string, error) {
	if model == "" {
		model = t.cfg.Model
	}
	reply, err := t.plain(ctx, append(t.system(), loop.Message{Role: "user", Content: question}), t.models(model, Sink{}))
	if err != nil {
		return "", err
	}
	return reply.Text, nil
}

func (t *tools) plain(ctx context.Context, messages []loop.Message, models []string) (*Reply, error) {
	runner := &loop.Runner{Chat: t.chat, Models: models}
	result, err := runner.Run(ctx, messages)
	if err != nil {
		return nil, err
	}
	return &Reply{Text: result.Text, Usage: usageOf(result.Usage)}, nil
}

func (t *tools) Transcribe(ctx context.Context, att Attachment) (string, error) {
	if t.cfg.TranscribeModel == "" {
		return "", ErrUnsupported
	}
	return postTranscription(ctx, t.client, strings.TrimSuffix(t.cfg.URL, "/")+"/audio/transcriptions", t.cfg.Key(), t.cfg.Headers, att, t.cfg.TranscribeModel)
}

func (t *tools) system() []loop.Message {
	if t.cfg.SystemPrompt == "" {
		return nil
	}
	return []loop.Message{{Role: "system", Content: t.cfg.SystemPrompt}}
}

// models is the order to try models in: the conversation's, then the
// fallbacks — minus the ones a held yield lock takes out, as long as that
// leaves any.
func (t *tools) models(model string, sink Sink) []string {
	chain := dedupe(append([]string{model}, t.cfg.Fallback...))
	reason := t.yielding()
	if reason == "" {
		return chain
	}
	var kept, skipped []string
	for _, m := range chain {
		base, _, _ := strings.Cut(m, ":")
		if slices.Contains(t.cfg.Yield.Models, base) {
			skipped = append(skipped, m)
		} else {
			kept = append(kept, m)
		}
	}
	if len(skipped) == 0 || len(kept) == 0 {
		return chain
	}
	sink.Note(fmt.Sprintf("`%s` steps aside (%s) → `%s`", skipped[0], reason, kept[0]))
	return kept
}

// yielding reads the yield lock and says why it is held, or "" when it is
// not (absent, unreadable, or past its `until`).
func (t *tools) yielding() string {
	if t.cfg.Yield.File == "" {
		return ""
	}
	data, err := os.ReadFile(t.cfg.Yield.File)
	if err != nil {
		return ""
	}
	reason := "locked"
	for _, line := range strings.Split(string(data), "\n") {
		key, value, _ := strings.Cut(strings.TrimSpace(line), "=")
		switch key {
		case "until":
			if until, err := strconv.ParseInt(value, 10, 64); err == nil && time.Now().Unix() > until {
				t.log.Debug().Str("file", t.cfg.Yield.File).Msg("Ignoring a stale yield lock")
				return ""
			}
		case "reason":
			if value != "" {
				reason = value
			}
		}
	}
	return reason
}

// userContent is my message, with images as vision parts.
func userContent(turn Turn) any {
	if len(turn.Attachments) == 0 {
		return turn.Text
	}
	text := turn.Text
	parts := []map[string]any{}
	for _, att := range turn.Attachments {
		if strings.HasPrefix(att.Mime, "image/") {
			parts = append(parts, map[string]any{"type": "image_url", "image_url": map[string]any{"url": dataURL(att)}})
		} else {
			text += fmt.Sprintf("\n\n(attached file %q, %s, %d bytes — not readable by this backend)", att.Name, att.Mime, len(att.Data))
		}
	}
	return append([]map[string]any{{"type": "text", "text": text}}, parts...)
}

func usageOf(u loop.Usage) *Usage {
	if u == (loop.Usage{}) {
		return nil
	}
	return &Usage{
		PromptTokens:     u.PromptTokens,
		CompletionTokens: u.CompletionTokens,
		CachedTokens:     u.CachedTokens,
		PromptPerSecond:  u.PromptPerSecond,
		TokensPerSecond:  u.TokensPerSecond,
	}
}

func dedupe(list []string) []string {
	return mergeTools(nil, list)
}
