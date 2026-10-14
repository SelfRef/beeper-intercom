package loop

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// AskUser is the one tool that does not run: the model uses it to ask the
// human a multiple-choice question, and the run stops until it is answered.
const AskUser = "ask_user"

var askUserTool = Tool{Type: "function", Function: ToolFunction{
	Name: AskUser,
	Description: "Ask the user one or more multiple-choice questions and wait for the answers. " +
		"Use it only when you cannot go on without their decision, not to confirm what they already asked for.",
	Parameters: map[string]any{
		"type":     "object",
		"required": []string{"questions"},
		"properties": map[string]any{
			"questions": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type":     "object",
					"required": []string{"id", "question", "options"},
					"properties": map[string]any{
						"id":       map[string]any{"type": "string"},
						"question": map[string]any{"type": "string"},
						"options": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type":     "object",
								"required": []string{"label"},
								"properties": map[string]any{
									"label":       map[string]any{"type": "string"},
									"description": map[string]any{"type": "string"},
								},
							},
						},
						"allow_other": map[string]any{"type": "boolean", "description": "the user may type a free-form answer"},
					},
				},
			},
		},
	},
}}

// AskUserArgs is the ask_user call's payload.
type AskUserArgs struct {
	Questions []struct {
		ID         string `json:"id"`
		Question   string `json:"question"`
		AllowOther bool   `json:"allow_other"`
		Options    []struct {
			Label       string `json:"label"`
			Description string `json:"description"`
		} `json:"options"`
	} `json:"questions"`
}

// Events report what a run is doing. Every field is optional.
type Events struct {
	Thinking func()
	Text     func(string)
	// Tool fires before a call runs.
	Tool func(ToolCall)
	// Note is something worth telling the human: a fallback, a dead server.
	Note func(string)
}

// Runner drives one turn: completion, tool calls, completion, … until the
// model answers without calling anything, or a call has to wait for a human.
type Runner struct {
	Chat *Chat
	// Models are tried in order. A model that fails before it has said
	// anything hands the request to the next one, and the one that answers
	// stays first for the rest of the run.
	Models []string
	Tools  *Toolbox
	// Ask offers ask_user.
	Ask bool
	// Hold reports whether a call must wait for a human before it runs.
	Hold      func(ToolCall) bool
	Events    Events
	MaxRounds int
}

// Result is where a run stopped.
type Result struct {
	// Messages is the conversation including everything this run added.
	Messages []Message
	// Text is everything the model said to the user in this run.
	Text string
	// Held are the calls waiting for a human: ask_user, and calls Hold
	// stopped. Resume answers each with a tool message and runs again.
	Held  []ToolCall
	Usage Usage
	// Model is the one that produced the last completion.
	Model string
}

// Run continues the conversation in messages until there is an answer or a
// held call.
func (r *Runner) Run(ctx context.Context, messages []Message) (*Result, error) {
	res := &Result{Messages: messages}
	var said strings.Builder
	maxRounds := r.MaxRounds
	if maxRounds <= 0 {
		maxRounds = 12
	}
	for round := 0; ; round++ {
		tools := r.Tools.Tools()
		if r.Ask {
			tools = append(append([]Tool{}, tools...), askUserTool)
		}
		if round >= maxRounds {
			// Out of rounds: one last completion with nothing to call, so the
			// turn ends with an answer instead of an error.
			tools = nil
			res.Messages = append(res.Messages, Message{Role: "user",
				Content: "(The tool budget for this turn is used up. Answer now with what you have.)"})
		}
		msg, usage, model, err := r.complete(ctx, res.Messages, tools, &said)
		if err != nil {
			return nil, err
		}
		res.Usage.Add(usage)
		res.Model = model
		res.Text = said.String()
		res.Messages = append(res.Messages, msg)
		if len(msg.ToolCalls) == 0 || tools == nil {
			return res, nil
		}
		for _, call := range msg.ToolCalls {
			if call.Function.Name == AskUser || (r.Hold != nil && r.Hold(call)) {
				res.Held = append(res.Held, call)
				continue
			}
			res.Messages = append(res.Messages, r.Exec(ctx, call))
		}
		if len(res.Held) > 0 {
			return res, nil
		}
	}
}

// Exec runs one call and returns its tool message.
func (r *Runner) Exec(ctx context.Context, call ToolCall) Message {
	if r.Events.Tool != nil {
		r.Events.Tool(call)
	}
	return ToolResult(call, r.Tools.Call(ctx, call))
}

// ToolResult is the message that answers a call.
func ToolResult(call ToolCall, text string) Message {
	return Message{Role: "tool", ToolCallID: call.ID, Content: text}
}

func (r *Runner) complete(ctx context.Context, messages []Message, tools []Tool, said *strings.Builder) (Message, Usage, string, error) {
	var lastErr error
	for i, model := range r.Models {
		emitted := false
		ev := streamEvents{thinking: r.Events.Thinking, text: func(text string) {
			if !emitted {
				// Models open with blank lines after their reasoning; a
				// bubble that starts empty looks like a broken one.
				if text = strings.TrimLeft(text, " \t\r\n"); text == "" {
					return
				}
			}
			if !emitted && said.Len() > 0 {
				// A new round's text is a new paragraph of the same answer.
				r.emit("\n\n")
				said.WriteString("\n\n")
			}
			emitted = true
			said.WriteString(text)
			r.emit(text)
		}}
		msg, usage, err := r.Chat.complete(ctx, model, messages, tools, ev)
		if err == nil {
			if i > 0 {
				r.Models = append([]string{model}, append(append([]string{}, r.Models[:i]...), r.Models[i+1:]...)...)
			}
			return msg, usage, model, nil
		}
		if ctx.Err() != nil || emitted {
			// Half an answer cannot be handed to another model.
			return Message{}, Usage{}, model, err
		}
		lastErr = fmt.Errorf("%s: %w", model, err)
		if i+1 < len(r.Models) {
			r.note(fmt.Sprintf("`%s` failed (%s) → `%s`", model, short(err.Error(), 100), r.Models[i+1]))
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no model to ask")
	}
	return Message{}, Usage{}, "", lastErr
}

func (r *Runner) emit(text string) {
	if r.Events.Text != nil {
		r.Events.Text(text)
	}
}

func (r *Runner) note(text string) {
	if r.Events.Note != nil {
		r.Events.Note(text)
	}
}

// Describe names a call for a human: the server, the tool, and — for a hub's
// generic call_tool — the tool it is really calling.
func Describe(call ToolCall) string {
	server, tool := SplitName(call.Function.Name)
	var args map[string]any
	_ = json.Unmarshal([]byte(call.Function.Arguments), &args)
	for _, key := range []string{"toolName", "tool_name", "tool"} {
		if inner, ok := args[key].(string); ok && inner != "" {
			tool = inner
			break
		}
	}
	if server == "" {
		return tool
	}
	return server + ": " + tool
}

// Arguments is a call's arguments without the part Describe already shows,
// cut to a length that fits on a line.
func Arguments(call ToolCall, limit int) string {
	var args map[string]any
	if json.Unmarshal([]byte(call.Function.Arguments), &args) != nil {
		return short(call.Function.Arguments, limit)
	}
	for _, key := range []string{"toolName", "tool_name", "tool"} {
		if _, ok := args[key].(string); ok {
			delete(args, key)
			// MCPHub's call_tool nests the real arguments one level down.
			if inner, ok := args["arguments"].(map[string]any); ok && len(args) == 1 {
				args = inner
			}
			break
		}
	}
	if len(args) == 0 {
		return ""
	}
	raw, _ := json.Marshal(args)
	return short(string(raw), limit)
}
