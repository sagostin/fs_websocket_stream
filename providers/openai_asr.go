package providers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"fs_websocket_stream/pipeline"
	"github.com/coder/websocket"
)

// openAIRealtimeRate is the sample rate the OpenAI Realtime API expects for
// audio/pcm input. The provider resamples the session rate to this.
const openAIRealtimeRate = 24000

// OpenAIASR streams caller audio to the OpenAI Realtime transcription API
// (or any endpoint speaking the same WebSocket protocol).
type OpenAIASR struct {
	conn   *websocket.Conn
	events chan pipeline.ASREvent
	rate   int // session sample rate (e.g. 16000)

	writeMu sync.Mutex
	once    sync.Once
}

// OpenAIASRConfig tunes the OpenAI transcription connection.
type OpenAIASRConfig struct {
	APIKey string
	Model  string // default "gpt-4o-transcribe"
	// BaseURL overrides the API base (for OpenAI-compatible endpoints),
	// e.g. "https://api.openai.com" or an Azure/gateway URL. The ws(s)
	// scheme is derived from it.
	BaseURL string
	// Prompt gives the transcriber optional context (domain vocabulary,
	// setting). Empty to omit.
	Prompt string
}

// NewOpenAIASR dials the Realtime API with a transcription session and
// starts the read loop. Server VAD is enabled so speech-start (barge-in)
// and turn completion are detected server-side.
func NewOpenAIASR(ctx context.Context, cfg OpenAIASRConfig, sampleRate int) (*OpenAIASR, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("openai asr: API key required")
	}
	model := cfg.Model
	if model == "" {
		model = "gpt-4o-transcribe"
	}
	base := cfg.BaseURL
	if base == "" {
		base = "https://api.openai.com"
	}
	wsBase := strings.NewReplacer("https://", "wss://", "http://", "ws://").Replace(strings.TrimRight(base, "/"))
	url := wsBase + "/v1/realtime?intent=transcription"

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dialCtx, url, &websocket.DialOptions{
		HTTPHeader: map[string][]string{"Authorization": {"Bearer " + cfg.APIKey}},
	})
	if err != nil {
		return nil, fmt.Errorf("openai asr dial: %w", err)
	}

	update := map[string]any{
		"type": "session.update",
		"session": map[string]any{
			"type": "transcription",
			"audio": map[string]any{
				"input": map[string]any{
					"format":         map[string]any{"type": "audio/pcm", "rate": openAIRealtimeRate},
					"transcription":  transcriptionConfig(model, cfg.Prompt),
					"turn_detection": map[string]any{"type": "server_vad"},
				},
			},
		},
	}
	body, err := json.Marshal(update)
	if err != nil {
		conn.Close(websocket.StatusInternalError, "")
		return nil, err
	}
	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	err = conn.Write(wctx, websocket.MessageText, body)
	wcancel()
	if err != nil {
		conn.Close(websocket.StatusInternalError, "")
		return nil, fmt.Errorf("openai asr session.update: %w", err)
	}

	o := &OpenAIASR{
		conn:   conn,
		events: make(chan pipeline.ASREvent, 32),
		rate:   sampleRate,
	}
	go o.readLoop()
	return o, nil
}

func transcriptionConfig(model, prompt string) map[string]any {
	cfg := map[string]any{"model": model}
	if prompt != "" {
		cfg["prompt"] = prompt
	}
	return cfg
}

// realtimeEvent is the subset of Realtime server events we consume.
type realtimeEvent struct {
	Type       string `json:"type"`
	Delta      string `json:"delta"`      // ...input_audio_transcription.delta
	Transcript string `json:"transcript"` // ...input_audio_transcription.completed
	Error      *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (o *OpenAIASR) readLoop() {
	defer close(o.events)
	for {
		_, data, err := o.conn.Read(context.Background())
		if err != nil {
			return
		}
		var ev realtimeEvent
		if err := json.Unmarshal(data, &ev); err != nil {
			continue
		}
		switch ev.Type {
		case "input_audio_buffer.speech_started":
			o.emit(pipeline.ASREvent{Type: pipeline.EventSpeechStarted})
		case "conversation.item.input_audio_transcription.delta":
			// Deltas are increments, not cumulative partials.
			if ev.Delta != "" {
				o.emit(pipeline.ASREvent{Type: pipeline.EventTranscript, Text: ev.Delta})
			}
		case "conversation.item.input_audio_transcription.completed":
			if ev.Transcript != "" {
				o.emit(pipeline.ASREvent{Type: pipeline.EventTranscript, Text: ev.Transcript, Final: true})
			}
		}
	}
}

func (o *OpenAIASR) emit(ev pipeline.ASREvent) {
	select {
	case o.events <- ev:
	default: // consumer stalled; drop rather than block the read loop
	}
}

// Write resamples a PCM chunk to 24 kHz and appends it to the input buffer.
func (o *OpenAIASR) Write(pcm []byte) error {
	if o.rate != openAIRealtimeRate {
		pcm = ResamplePCM16(pcm, o.rate, openAIRealtimeRate)
	}
	if len(pcm) == 0 {
		return nil
	}
	body, err := json.Marshal(map[string]string{
		"type":  "input_audio_buffer.append",
		"audio": base64.StdEncoding.EncodeToString(pcm),
	})
	if err != nil {
		return err
	}
	o.writeMu.Lock()
	defer o.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return o.conn.Write(ctx, websocket.MessageText, body)
}

// Events streams ASR events.
func (o *OpenAIASR) Events() <-chan pipeline.ASREvent { return o.events }

// Close terminates the transcription session.
func (o *OpenAIASR) Close() error {
	var err error
	o.once.Do(func() {
		err = o.conn.Close(websocket.StatusNormalClosure, "done")
	})
	return err
}

// OpenAIASRFactory returns a pipeline.ASRFactory for OpenAI transcription.
func OpenAIASRFactory(cfg OpenAIASRConfig) pipeline.ASRFactory {
	return func(sampleRate int) (pipeline.ASR, error) {
		return NewOpenAIASR(context.Background(), cfg, sampleRate)
	}
}
