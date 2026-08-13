package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"fs_websocket_stream/pipeline"
)

// OpenAILLM uses the OpenAI chat completions API.
type OpenAILLM struct {
	APIKey string
	Model  string // default "gpt-4o-mini"
	// BaseURL overrides the API base (for OpenAI-compatible endpoints).
	BaseURL string
	Client  *http.Client
}

func (o OpenAILLM) httpClient() *http.Client {
	if o.Client != nil {
		return o.Client
	}
	return &http.Client{Timeout: 30 * time.Second}
}

type chatRequest struct {
	Model    string             `json:"model"`
	Messages []pipeline.Message `json:"messages"`
}

type chatResponse struct {
	Choices []struct {
		Message pipeline.Message `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// Respond returns the assistant reply for the conversation.
func (o OpenAILLM) Respond(ctx context.Context, history []pipeline.Message) (string, error) {
	if o.APIKey == "" {
		return "", fmt.Errorf("openai: API key required")
	}
	model := o.Model
	if model == "" {
		model = "gpt-4o-mini"
	}
	base := o.BaseURL
	if base == "" {
		base = "https://api.openai.com"
	}

	body, err := json.Marshal(chatRequest{Model: model, Messages: history})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+o.APIKey)

	resp, err := o.httpClient().Do(req)
	if err != nil {
		return "", fmt.Errorf("openai request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}

	var parsed chatResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", fmt.Errorf("openai: bad response: %w", err)
	}
	if parsed.Error != nil {
		return "", fmt.Errorf("openai: %s", parsed.Error.Message)
	}
	if len(parsed.Choices) == 0 {
		return "", pipeline.ErrNoReply
	}
	return parsed.Choices[0].Message.Content, nil
}
