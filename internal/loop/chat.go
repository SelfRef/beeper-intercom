// Package loop is a small tool-using agent: an OpenAI-compatible chat
// endpoint, a set of MCP servers, and the loop between them. It knows nothing
// about Matrix, rooms or the store — the bridge adapts it to its Agent
// interface — so it can move to a service of its own without a rewrite.
package loop

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Message is one chat message in the OpenAI shape. Content is a string, or a
// list of parts for a user message that carries images.
type Message struct {
	Role       string     `json:"role"`
	Content    any        `json:"content"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// ToolCall is a function call the model asked for. Arguments is the raw JSON
// string the model produced, passed on as it came.
type ToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// Tool is a function definition offered to the model.
type Tool struct {
	Type     string       `json:"type"`
	Function ToolFunction `json:"function"`
}

type ToolFunction struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters"`
}

// Usage is what the endpoint reported, summed over the rounds of a turn.
// The speeds are the last round's: llama.cpp reports them per request.
type Usage struct {
	PromptTokens     int
	CompletionTokens int
	CachedTokens     int
	PromptPerSecond  float64
	TokensPerSecond  float64
}

// Add sums another round into u.
func (u *Usage) Add(o Usage) {
	u.PromptTokens += o.PromptTokens
	u.CompletionTokens += o.CompletionTokens
	u.CachedTokens += o.CachedTokens
	if o.PromptPerSecond > 0 {
		u.PromptPerSecond = o.PromptPerSecond
	}
	if o.TokensPerSecond > 0 {
		u.TokensPerSecond = o.TokensPerSecond
	}
}

// Chat is an OpenAI-compatible /chat/completions endpoint.
type Chat struct {
	URL     string
	Key     string
	Headers map[string]string
	HTTP    *http.Client
}

// streamEvents are what a completion reports while it runs.
type streamEvents struct {
	thinking func()
	text     func(string)
}

// complete runs one streamed completion and returns the assistant message.
func (c *Chat) complete(ctx context.Context, model string, messages []Message, tools []Tool, ev streamEvents) (Message, Usage, error) {
	body := map[string]any{
		"model":          model,
		"messages":       messages,
		"stream":         true,
		"stream_options": map[string]any{"include_usage": true},
	}
	if len(tools) > 0 {
		body["tools"] = tools
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return Message{}, Usage{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(c.URL, "/")+"/chat/completions", bytes.NewReader(raw))
	if err != nil {
		return Message{}, Usage{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Key != "" {
		req.Header.Set("Authorization", "Bearer "+c.Key)
	}
	for k, v := range c.Headers {
		req.Header.Set(k, v)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Message{}, Usage{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return Message{}, Usage{}, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return readStream(resp.Body, ev)
}

type chunk struct {
	Choices []struct {
		Delta struct {
			Content          string `json:"content"`
			ReasoningContent string `json:"reasoning_content"`
			ToolCalls        []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens        int `json:"prompt_tokens"`
		CompletionTokens    int `json:"completion_tokens"`
		PromptTokensDetails *struct {
			CachedTokens int `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
	// llama.cpp's own accounting, which is where the speeds are.
	Timings *struct {
		CacheN             int     `json:"cache_n"`
		PromptPerSecond    float64 `json:"prompt_per_second"`
		PredictedPerSecond float64 `json:"predicted_per_second"`
	} `json:"timings"`
	Error any `json:"error"`
}

func readStream(body io.Reader, ev streamEvents) (Message, Usage, error) {
	msg := Message{Role: "assistant"}
	var text strings.Builder
	var usage Usage
	var calls []ToolCall
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 16<<20)
	for scanner.Scan() {
		payload, ok := strings.CutPrefix(scanner.Text(), "data:")
		payload = strings.TrimSpace(payload)
		if !ok || payload == "" {
			continue
		}
		if payload == "[DONE]" {
			break
		}
		var c chunk
		if err := json.Unmarshal([]byte(payload), &c); err != nil {
			return msg, usage, fmt.Errorf("bad chunk: %w", err)
		}
		if c.Error != nil {
			return msg, usage, fmt.Errorf("%v", c.Error)
		}
		for _, choice := range c.Choices {
			d := choice.Delta
			if d.ReasoningContent != "" && ev.thinking != nil {
				ev.thinking()
			}
			if d.Content != "" {
				text.WriteString(d.Content)
				if ev.text != nil {
					ev.text(d.Content)
				}
			}
			for _, tc := range d.ToolCalls {
				for len(calls) <= tc.Index {
					calls = append(calls, ToolCall{Type: "function"})
				}
				call := &calls[tc.Index]
				if tc.ID != "" {
					call.ID = tc.ID
				}
				call.Function.Name += tc.Function.Name
				call.Function.Arguments += tc.Function.Arguments
			}
		}
		if c.Usage != nil {
			usage.PromptTokens = c.Usage.PromptTokens
			usage.CompletionTokens = c.Usage.CompletionTokens
			if c.Usage.PromptTokensDetails != nil {
				usage.CachedTokens = c.Usage.PromptTokensDetails.CachedTokens
			}
		}
		if c.Timings != nil {
			usage.PromptPerSecond = c.Timings.PromptPerSecond
			usage.TokensPerSecond = c.Timings.PredictedPerSecond
			if usage.CachedTokens == 0 {
				usage.CachedTokens = c.Timings.CacheN
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return msg, usage, err
	}
	msg.Content = text.String()
	for i := range calls {
		if calls[i].ID == "" {
			calls[i].ID = fmt.Sprintf("call_%d", i)
		}
		if calls[i].Function.Arguments == "" {
			calls[i].Function.Arguments = "{}"
		}
	}
	msg.ToolCalls = calls
	return msg, usage, nil
}
