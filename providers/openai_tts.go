package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"fs_websocket_stream/pipeline"
)

// openAITTSRate is the sample rate of the PCM returned by the OpenAI speech
// API (response_format=pcm is always 24 kHz). The provider resamples to the
// session rate.
const openAITTSRate = 24000

// OpenAITTS streams speech audio from the OpenAI speech API as raw PCM.
// It works against any OpenAI-compatible /v1/audio/speech endpoint, though
// response_format=pcm support varies by provider.
type OpenAITTS struct {
	APIKey     string
	Model      string // default "gpt-4o-mini-tts"
	Voice      string // default "alloy"
	SampleRate int    // session rate, e.g. 8000 or 16000
	// BaseURL overrides the API base (for OpenAI-compatible endpoints).
	BaseURL string
	Client  *http.Client
}

func (o OpenAITTS) httpClient() *http.Client {
	if o.Client != nil {
		return o.Client
	}
	return http.DefaultClient // no timeout: streaming response
}

type speechRequest struct {
	Model          string `json:"model"`
	Voice          string `json:"voice"`
	Input          string `json:"input"`
	ResponseFormat string `json:"response_format"`
}

// Synthesize starts streaming PCM for the given text.
func (o OpenAITTS) Synthesize(ctx context.Context, text string) (<-chan []byte, error) {
	if o.APIKey == "" {
		return nil, fmt.Errorf("openai tts: API key required")
	}
	model := o.Model
	if model == "" {
		model = "gpt-4o-mini-tts"
	}
	voice := o.Voice
	if voice == "" {
		voice = "alloy"
	}
	rate := o.SampleRate
	if rate == 0 {
		rate = 16000
	}
	base := o.BaseURL
	if base == "" {
		base = "https://api.openai.com"
	}

	body, err := json.Marshal(speechRequest{
		Model: model, Voice: voice, Input: text, ResponseFormat: "pcm",
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/audio/speech", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+o.APIKey)

	resp, err := o.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai tts request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("openai tts: HTTP %d: %s", resp.StatusCode, raw)
	}

	out := make(chan []byte, 64)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		// Resample in whole blocks, carrying the remainder across reads so
		// chunk boundaries stay artifact-free. On EOF, flush what's left.
		block := ResampleBlockBytes(openAITTSRate, rate)
		var pending []byte
		emit := func(pcm []byte) bool {
			if rate != openAITTSRate {
				pcm = ResamplePCM16(pcm, openAITTSRate, rate)
			}
			if len(pcm) == 0 {
				return true
			}
			select {
			case <-ctx.Done():
				return false
			case out <- pcm:
				return true
			}
		}
		buf := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				pending = append(pending, buf[:n]...)
				usable := len(pending)
				if err == nil { // not EOF: keep the partial block for next read
					usable -= usable % block
				}
				if usable > 0 {
					chunk := make([]byte, usable)
					copy(chunk, pending[:usable])
					pending = pending[:copy(pending, pending[usable:])]
					if !emit(chunk) {
						return
					}
				}
			}
			if err != nil {
				return
			}
		}
	}()
	return out, nil
}

// OpenAITTSFactory returns a pipeline.TTSFactory for OpenAI speech.
func OpenAITTSFactory(apiKey, voice, baseURL string) pipeline.TTSFactory {
	return func(sampleRate int) (pipeline.TTS, error) {
		return OpenAITTS{APIKey: apiKey, Voice: voice, BaseURL: baseURL, SampleRate: sampleRate}, nil
	}
}
