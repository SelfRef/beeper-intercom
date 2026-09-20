package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/SelfRef/beeper-intercom/internal/config"
)

// The attachment contract with Open WebUI has two halves that fail silently
// if wrong: an image that is not in user_message.files as {type: image, url:
// data:…} is never seen by the model, and a document that is not referenced
// by its uploaded id never reaches RAG. Pin both.
func TestOpenWebUIAttachmentsRequestShape(t *testing.T) {
	var completion map[string]any
	var uploadedName, uploadedMime string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/files/" && r.Method == http.MethodPost:
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Errorf("upload is not multipart: %v", err)
			}
			file, header, err := r.FormFile("file")
			if err != nil {
				t.Errorf("no file field: %v", err)
				return
			}
			defer file.Close()
			data, _ := io.ReadAll(file)
			uploadedName, uploadedMime = header.Filename, header.Header.Get("Content-Type")
			if string(data) != "hello,world" {
				t.Errorf("uploaded bytes = %q", data)
			}
			_, _ = w.Write([]byte(`{"id":"file-1","filename":"data.csv","meta":{"size":11,"content_type":"text/csv"}}`))
		case r.URL.Path == "/api/chat/completions":
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &completion)
			_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	adapter := newOpenWebUI(config.Agent{URL: server.URL, Model: "m", NoStream: true}, server.Client(), zerolog.Nop())
	_, err := adapter.Send(context.Background(), Conversation{ID: "chat-1"}, Turn{
		Text: "look",
		Attachments: []Attachment{
			{Name: "photo.png", Mime: "image/png", Data: []byte{0x89, 'P', 'N', 'G'}},
			{Name: "data.csv", Mime: "text/csv", Data: []byte("hello,world")},
		},
	}, Sink{})
	if err != nil {
		t.Fatal(err)
	}

	if uploadedName != "data.csv" || uploadedMime != "text/csv" {
		t.Errorf("uploaded %q as %q", uploadedName, uploadedMime)
	}

	userMessage, _ := completion["user_message"].(map[string]any)
	stored, _ := userMessage["files"].([]any)
	if len(stored) != 2 {
		t.Fatalf("user_message.files = %v", userMessage["files"])
	}
	image, _ := stored[0].(map[string]any)
	if image["type"] != "image" || !strings.HasPrefix(image["url"].(string), "data:image/png;base64,") {
		t.Errorf("image entry = %v", image)
	}
	doc, _ := stored[1].(map[string]any)
	if doc["type"] != "file" || doc["id"] != "file-1" || doc["url"] != "/api/v1/files/file-1" {
		t.Errorf("file entry = %v", doc)
	}

	// Only real files go in the top-level list the RAG handler reads; an
	// image there would be sent through retrieval as if it were a document.
	meta, _ := completion["files"].([]any)
	if len(meta) != 1 || meta[0].(map[string]any)["id"] != "file-1" {
		t.Errorf("files = %v", completion["files"])
	}
}

func TestOpenWebUITranscribe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/audio/transcriptions" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, header, err := r.FormFile("file")
		if err != nil || header.Header.Get("Content-Type") != "audio/ogg" {
			t.Errorf("file part = %v %v", header, err)
		}
		_, _ = w.Write([]byte(`{"text":"  turn the lights off "}`))
	}))
	defer server.Close()

	adapter := newOpenWebUI(config.Agent{URL: server.URL, Model: "m"}, server.Client(), zerolog.Nop())
	text, err := adapter.Transcribe(context.Background(), Attachment{Name: "voice.ogg", Mime: "audio/ogg", Data: []byte("opus")})
	if err != nil {
		t.Fatal(err)
	}
	if text != "turn the lights off" {
		t.Errorf("transcript = %q", text)
	}
}

func TestOpenAIImagesBecomeVisionParts(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"a cat"}}]}`))
	}))
	defer server.Close()

	adapter := newOpenAI(config.Agent{URL: server.URL, Model: "m", MaxHistory: 5, NoStream: true}, server.Client(), openTestStore(t), zerolog.Nop())
	_, err := adapter.Send(context.Background(), Conversation{ID: "c"}, Turn{
		Text:        "what is this",
		Attachments: []Attachment{{Name: "x.jpg", Mime: "image/jpeg", Data: []byte("jpg")}},
	}, Sink{})
	if err != nil {
		t.Fatal(err)
	}
	messages, _ := body["messages"].([]any)
	last, _ := messages[len(messages)-1].(map[string]any)
	parts, _ := last["content"].([]any)
	if len(parts) != 2 {
		t.Fatalf("content parts = %v", last["content"])
	}
	if parts[1].(map[string]any)["type"] != "image_url" {
		t.Errorf("second part = %v", parts[1])
	}
}
