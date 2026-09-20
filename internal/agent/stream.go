package agent

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"
)

// Everything in this file is shared by the streaming paths of the adapters:
// how server-sent events are read, and how text that was never meant for a
// chat bubble is kept out of one.

// readSSE calls fn for every `data:` payload of a server-sent event stream and
// stops at EOF or at the OpenAI-style "[DONE]" sentinel.
func readSSE(body io.Reader, fn func(data []byte) error) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 16<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" {
			continue
		}
		if payload == "[DONE]" {
			return nil
		}
		if err := fn([]byte(payload)); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// openAIChunk is the part of a chat.completion.chunk this bridge cares about.
type openAIChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Error any `json:"error"`
}

func parseOpenAIChunk(data []byte) (openAIChunk, error) {
	var chunk openAIChunk
	err := json.Unmarshal(data, &chunk)
	return chunk, err
}

// StripDetails removes Open WebUI's <details> widgets — reasoning, tool calls,
// code interpreter output — from message content. In the web UI they are
// collapsible panels; in a chat bubble they are noise, and a half-streamed one
// is an unclosed tag.
//
// An unclosed <details> (the model is still thinking) is cut to the end, so a
// streamed bubble stays empty during the reasoning phase instead of filling
// with a chain of thought that then vanishes.
func StripDetails(text string) string {
	for {
		start := strings.Index(text, "<details")
		if start < 0 {
			return strings.TrimLeft(text, "\n")
		}
		end := strings.Index(text[start:], "</details>")
		if end < 0 {
			return strings.TrimLeft(text[:start], "\n")
		}
		text = text[:start] + text[start+end+len("</details>"):]
	}
}

// accumulator turns "the full text so far" into "what is new since last time",
// which is what a stream delta is. If the text is rewritten rather than
// extended (a backend that revises), nothing is emitted for that step — the
// final edit carries the truth either way.
type accumulator struct {
	seen string
}

func (a *accumulator) delta(full string) (string, bool) {
	if len(full) <= len(a.seen) || !strings.HasPrefix(full, a.seen) {
		if full != a.seen && !strings.HasPrefix(full, a.seen) {
			// Diverged: resynchronise silently.
			a.seen = full
		}
		return "", false
	}
	delta := full[len(a.seen):]
	a.seen = full
	return delta, true
}
