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

// ElevenLabsTTS streams speech audio from the ElevenLabs API as raw PCM.
type ElevenLabsTTS struct {
	APIKey     string
	VoiceID    string // e.g. "21m00Tcm4TlvDq8ikWAM" (Rachel)
	Model      string // default "eleven_turbo_v2_5" (low latency)
	SampleRate int    // 8000 or 16000
	Client     *http.Client
}

func (e ElevenLabsTTS) httpClient() *http.Client {
	if e.Client != nil {
		return e.Client
	}
	return http.DefaultClient // no timeout: streaming response
}

type ttsRequest struct {
	Text    string `json:"text"`
	ModelID string `json:"model_id"`
}

// Synthesize starts streaming PCM for the given text.
func (e ElevenLabsTTS) Synthesize(ctx context.Context, text string) (<-chan []byte, error) {
	if e.APIKey == "" {
		return nil, fmt.Errorf("elevenlabs: API key required")
	}
	if e.VoiceID == "" {
		return nil, fmt.Errorf("elevenlabs: voice ID required")
	}
	model := e.Model
	if model == "" {
		model = "eleven_turbo_v2_5"
	}
	rate := e.SampleRate
	if rate == 0 {
		rate = 16000
	}
	format := fmt.Sprintf("pcm_%d", rate)

	body, err := json.Marshal(ttsRequest{Text: text, ModelID: model})
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("https://api.elevenlabs.io/v1/text-to-speech/%s/stream?output_format=%s", e.VoiceID, format)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("xi-api-key", e.APIKey)

	resp, err := e.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("elevenlabs request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("elevenlabs: HTTP %d: %s", resp.StatusCode, raw)
	}

	out := make(chan []byte, 64)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		buf := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				select {
				case <-ctx.Done():
					return
				case out <- chunk:
				}
			}
			if err != nil {
				return
			}
		}
	}()
	return out, nil
}

// ElevenLabsTTSFactory returns a pipeline.TTSFactory for ElevenLabs.
func ElevenLabsTTSFactory(apiKey, voiceID string) pipeline.TTSFactory {
	return func(sampleRate int) (pipeline.TTS, error) {
		return ElevenLabsTTS{APIKey: apiKey, VoiceID: voiceID, SampleRate: sampleRate}, nil
	}
}
