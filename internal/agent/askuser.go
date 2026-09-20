package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// ask_user, Open WebUI's built-in clarifying-question tool.
//
// It is the one tool that cannot simply run: it stops the turn and waits for
// an answer. Open WebUI does that by STAGING the call — the turn ends early
// with a `function_call` item named ask_user, status "pending", carrying the
// questions — and the web UI draws a card for it. Nothing renders it in a
// chat client, so the bridge asks the questions as polls and then resolves the
// call, which resumes the same turn where it stopped
// (`POST /api/v1/chats/{id}/messages/{message_id}/resolve`, read from the
// 0.11.3 source on 2026-09-20).
const askUserTool = "ask_user"

// Question is one clarifying question with the options offered for it.
type Question struct {
	ID      string
	Header  string
	Text    string
	Options []QuestionOption
	// AllowOther means a free-form answer is acceptable. A poll cannot offer
	// one, so it only decides whether "Something else" is worth an option.
	AllowOther bool
}

type QuestionOption struct {
	Label       string
	Description string
}

// Ask is a turn that stopped to ask something. Resolve it with Answer.
type Ask struct {
	// CallID and MessageID are the handles the backend needs back.
	CallID    string
	MessageID string
	Questions []Question
}

// askUserArguments is the tool call's own payload, as Open WebUI normalises it.
type askUserArguments struct {
	Questions []struct {
		ID         string `json:"id"`
		Header     string `json:"header"`
		Question   string `json:"question"`
		AllowOther bool   `json:"allow_other"`
		Options    []struct {
			Label       string `json:"label"`
			Description string `json:"description"`
		} `json:"options"`
	} `json:"questions"`
	TimeoutMS int `json:"timeout_ms"`
}

// pendingAsk reads the staged ask_user call out of a stored message, if there
// is one. A turn that asked nothing returns nil.
func (o *openWebUI) pendingAsk(ctx context.Context, chatID, messageID string) (*Ask, error) {
	var resp struct {
		Chat struct {
			History struct {
				Messages map[string]struct {
					Output []owuiOutputItem `json:"output"`
				} `json:"messages"`
			} `json:"history"`
		} `json:"chat"`
	}
	if err := o.do(ctx, http.MethodGet, "/api/v1/chats/"+chatID, nil, &resp); err != nil {
		return nil, err
	}
	message, ok := resp.Chat.History.Messages[messageID]
	if !ok {
		return nil, nil
	}
	return askFromOutput(message.Output, messageID)
}

// askFromOutput finds a staged ask_user call in a message's output. Open WebUI
// ends the turn for one WITHOUT done:true — the questions sitting in the
// output are the only signal that it is over and waiting for me.
func askFromOutput(output []owuiOutputItem, messageID string) (*Ask, error) {
	for _, item := range output {
		if item.Type != "function_call" || item.Name != askUserTool {
			continue
		}
		// queued and requires_approval are the other states the resolve
		// endpoint accepts; anything else has already been answered.
		switch item.Status {
		case "pending", "queued", "requires_approval":
		default:
			continue
		}
		var args askUserArguments
		if err := json.Unmarshal([]byte(item.Arguments), &args); err != nil {
			return nil, fmt.Errorf("ask_user arguments: %w", err)
		}
		callID := item.CallID
		if callID == "" {
			callID = item.ID
		}
		ask := &Ask{CallID: callID, MessageID: messageID}
		for _, q := range args.Questions {
			question := Question{ID: q.ID, Header: q.Header, Text: q.Question, AllowOther: q.AllowOther}
			for _, option := range q.Options {
				question.Options = append(question.Options, QuestionOption{
					Label:       option.Label,
					Description: option.Description,
				})
			}
			ask.Questions = append(ask.Questions, question)
		}
		return ask, nil
	}
	return nil, nil
}

// Answer resolves a staged ask_user call and waits for the rest of the turn.
// answers are keyed by question id; an empty map means nobody answered in
// time, which the backend is told so the model can carry on without it.
func (o *openWebUI) Answer(ctx context.Context, conv Conversation, ask *Ask, answers map[string]string, sink Sink) (*Reply, error) {
	if ask == nil || ask.CallID == "" {
		return nil, ErrUnsupported
	}
	body := map[string]any{
		"call_id":   ask.CallID,
		"action":    "answer",
		"answers":   answers,
		"timed_out": len(answers) == 0,
	}
	path := "/api/v1/chats/" + conv.ID + "/messages/" + ask.MessageID + "/resolve"

	// The resume runs as the same message, so the answer arrives the way any
	// answer does — which means the socket has to be listening before the
	// call, exactly as for a fresh turn.
	return o.awaitAnswer(ctx, conv, ask.MessageID, sink, func(ctx context.Context) error {
		return o.do(ctx, http.MethodPost, path, body, nil)
	})
}

// finish builds the Reply for a completed turn — and notices when the turn did
// not complete at all but stopped to ask something, which is the one case
// where the caller has to do more than post an answer.
// finish builds the Reply for a completed turn. asked is the staged question
// the turn stopped on, when one was seen live; without it the stored message
// is checked once, because the socket can miss the event that carries it.
func (o *openWebUI) finish(ctx context.Context, conv Conversation, assistantID, text string, usage *Usage, asked *Ask) (*Reply, error) {
	reply := &Reply{Text: text, Usage: usage, Parent: assistantID, Link: o.Link(conv.ID)}
	if asked != nil {
		reply.Ask = asked
		return reply, nil
	}
	ask, err := o.pendingAsk(ctx, conv.ID, assistantID)
	if err != nil {
		// A question we cannot read is not worth failing the turn over; the
		// answer, if any, still stands.
		o.log.Debug().Err(err).Msg("Could not read a staged ask_user call")
		return reply, nil
	}
	reply.Ask = ask
	return reply, nil
}
