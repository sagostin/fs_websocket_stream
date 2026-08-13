// Package providers contains concrete ASR/LLM/TTS implementations for the
// pipeline package: Deepgram (streaming ASR), OpenAI (LLM) and ElevenLabs
// (streaming TTS).
package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"fs_websocket_stream/pipeline"
	"github.com/coder/websocket"
)

// DeepgramASR streams caller audio to Deepgram's realtime API.
type DeepgramASR struct {
	conn   *websocket.Conn
	events chan pipeline.ASREvent

	writeMu sync.Mutex
	closeCh chan struct{}
	once    sync.Once
}

// DeepgramASRConfig tunes the Deepgram connection.
type DeepgramASRConfig struct {
	APIKey string
	Model  string // default "nova-2"
	// EndpointingMs of silence before Deepgram finalizes an utterance.
	// Default 300.
	EndpointingMs int
}

// NewDeepgramASR dials Deepgram and starts the read loop.
func NewDeepgramASR(ctx context.Context, cfg DeepgramASRConfig, sampleRate int) (*DeepgramASR, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("deepgram: API key required")
	}
	model := cfg.Model
	if model == "" {
		model = "nova-2"
	}
	endpointing := cfg.EndpointingMs
	if endpointing == 0 {
		endpointing = 300
	}

	url := fmt.Sprintf("wss://api.deepgram.com/v1/listen?encoding=linear16&sample_rate=%d&channels=1&model=%s&punctuate=true&vad_events=true&endpointing=%d",
		sampleRate, model, endpointing)

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dialCtx, url, &websocket.DialOptions{
		HTTPHeader: map[string][]string{"Authorization": {"Token " + cfg.APIKey}},
	})
	if err != nil {
		return nil, fmt.Errorf("deepgram dial: %w", err)
	}

	d := &DeepgramASR{
		conn:    conn,
		events:  make(chan pipeline.ASREvent, 32),
		closeCh: make(chan struct{}),
	}
	go d.readLoop()
	return d, nil
}

// deepgramResult is the subset of Deepgram's message schema we consume.
type deepgramResult struct {
	Type    string `json:"type"` // Results | SpeechStarted | UtteranceEnd | Metadata
	Channel struct {
		Alternatives []struct {
			Transcript string `json:"transcript"`
		} `json:"alternatives"`
	} `json:"channel"`
	IsFinal     bool `json:"is_final"`
	SpeechFinal bool `json:"speech_final"`
}

func (d *DeepgramASR) readLoop() {
	defer close(d.events)
	for {
		_, data, err := d.conn.Read(context.Background())
		if err != nil {
			return
		}
		var msg deepgramResult
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		switch msg.Type {
		case "SpeechStarted":
			d.emit(pipeline.ASREvent{Type: pipeline.EventSpeechStarted})
		case "Results":
			if len(msg.Channel.Alternatives) == 0 {
				continue
			}
			text := msg.Channel.Alternatives[0].Transcript
			if text == "" {
				continue
			}
			d.emit(pipeline.ASREvent{
				Type:  pipeline.EventTranscript,
				Text:  text,
				Final: msg.IsFinal || msg.SpeechFinal,
			})
		}
	}
}

func (d *DeepgramASR) emit(ev pipeline.ASREvent) {
	select {
	case d.events <- ev:
	default: // consumer stalled; drop rather than block the read loop
	}
}

// Write sends a PCM chunk to Deepgram.
func (d *DeepgramASR) Write(pcm []byte) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return d.conn.Write(ctx, websocket.MessageBinary, pcm)
}

// Events streams ASR events.
func (d *DeepgramASR) Events() <-chan pipeline.ASREvent { return d.events }

// Close terminates the stream.
func (d *DeepgramASR) Close() error {
	var err error
	d.once.Do(func() {
		close(d.closeCh)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = d.conn.Write(ctx, websocket.MessageText, []byte(`{"type":"CloseStream"}`))
		err = d.conn.Close(websocket.StatusNormalClosure, "done")
	})
	return err
}

// DeepgramASRFactory returns a pipeline.ASRFactory for Deepgram.
func DeepgramASRFactory(cfg DeepgramASRConfig) pipeline.ASRFactory {
	return func(sampleRate int) (pipeline.ASR, error) {
		return NewDeepgramASR(context.Background(), cfg, sampleRate)
	}
}
