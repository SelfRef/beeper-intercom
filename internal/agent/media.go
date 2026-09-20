package agent

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
)

// dataURL inlines an attachment the way a browser would.
func dataURL(att Attachment) string {
	mime := att.Mime
	if mime == "" {
		mime = "application/octet-stream"
	}
	return "data:" + mime + ";base64," + base64.StdEncoding.EncodeToString(att.Data)
}

// postTranscription is the OpenAI-shaped /audio/transcriptions call: multipart
// with the audio as "file", JSON {"text": ...} back.
func postTranscription(ctx context.Context, client *http.Client, url, key string, headers map[string]string, att Attachment, model string) (string, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	header := textproto.MIMEHeader{}
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, strings.ReplaceAll(att.Name, `"`, "")))
	header.Set("Content-Type", att.Mime)
	part, err := writer.CreatePart(header)
	if err != nil {
		return "", err
	}
	if _, err := part.Write(att.Data); err != nil {
		return "", err
	}
	if model != "" {
		_ = writer.WriteField("model", model)
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &buf)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode == http.StatusNotFound {
		return "", ErrUnsupported
	}
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("transcription: HTTP %d: %s", resp.StatusCode, trim(string(data)))
	}
	var parsed struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return "", fmt.Errorf("transcription: bad response: %w", err)
	}
	return strings.TrimSpace(parsed.Text), nil
}
