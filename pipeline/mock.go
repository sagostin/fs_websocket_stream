package pipeline

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// MockLLM replies with a canned echo of the user's last message.
type MockLLM struct {
	// Prefix prepended to the echoed user text. Default "You said: ".
	Prefix string
}

func (m MockLLM) Respond(ctx context.Context, history []Message) (string, error) {
	if len(history) == 0 {
		return "", ErrNoReply
	}
	last := history[len(history)-1].Content
	prefix := m.Prefix
	if prefix == "" {
		prefix = "You said: "
	}
	return prefix + last, nil
}

// MockASR is a scripted ASR for testing: after SpeechAfter bytes of audio
// it emits EventSpeechStarted once, and after FinalAfter bytes it emits a
// final transcript with Transcript, then resets and repeats.
type MockASR struct {
	Transcript string // default "hello world"
	// SpeechAfter is the number of audio bytes before EventSpeechStarted.
	// Default 6400 (200ms at 16kHz L16 mono).
	SpeechAfter int
	// FinalAfter is the number of audio bytes before a final transcript.
	// Default 32000 (1s at 16kHz L16 mono).
	FinalAfter int

	mu       sync.Mutex
	events   chan ASREvent
	buffered int
	spoke    bool
	closed   bool
}

func (m *MockASR) Write(pcm []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("mock ASR closed")
	}
	m.buffered += len(pcm)

	speechAfter := m.SpeechAfter
	if speechAfter == 0 {
		speechAfter = 6400
	}
	finalAfter := m.FinalAfter
	if finalAfter == 0 {
		finalAfter = 32000
	}

	if !m.spoke && m.buffered >= speechAfter {
		m.spoke = true
		m.events <- ASREvent{Type: EventSpeechStarted}
	}
	if m.buffered >= finalAfter {
		text := m.Transcript
		if text == "" {
			text = "hello world"
		}
		m.events <- ASREvent{Type: EventTranscript, Text: text, Final: true}
		m.buffered = 0
		m.spoke = false
	}
	return nil
}

func (m *MockASR) Events() <-chan ASREvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.events == nil {
		m.events = make(chan ASREvent, 16)
	}
	return m.events
}

func (m *MockASR) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		if m.events != nil {
			close(m.events)
		}
	}
	return nil
}

// MockASRFactory returns an ASRFactory producing MockASR instances.
func MockASRFactory(transcript string) ASRFactory {
	return func(sampleRate int) (ASR, error) {
		m := &MockASR{Transcript: transcript}
		m.Events() // pre-create channel
		return m, nil
	}
}

// MockTTS synthesizes a sine tone instead of speech, so tests and manual
// calls can verify audibility without a real TTS. DurationMs of tone is
// emitted in 20ms chunks at the given sample rate.
type MockTTS struct {
	SampleRate int
	// DurationMs of tone to generate. Default 1000.
	DurationMs int
	// Freq of the tone in Hz. Default 440.
	Freq float64
	// Realtime paces chunk emission to wall-clock duration (simulating a
	// streaming TTS). Off by default.
	Realtime bool
}

func (t MockTTS) Synthesize(ctx context.Context, text string) (<-chan []byte, error) {
	rate := t.SampleRate
	if rate == 0 {
		rate = 16000
	}
	dur := t.DurationMs
	if dur == 0 {
		dur = 1000
	}
	freq := t.Freq
	if freq == 0 {
		freq = 440
	}

	out := make(chan []byte, 64)
	go func() {
		defer close(out)
		frameSamples := rate / 50 // 20ms
		total := rate * dur / 1000
		for pos := 0; pos < total; pos += frameSamples {
			n := frameSamples
			if pos+n > total {
				n = total - pos
			}
			buf := make([]byte, n*2)
			for i := 0; i < n; i++ {
				v := int16(6000 * math.Sin(2*math.Pi*freq*float64(pos+i)/float64(rate)))
				buf[2*i] = byte(v)
				buf[2*i+1] = byte(v >> 8)
			}
			if t.Realtime {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(n) * time.Second / time.Duration(rate)):
				}
			}
			select {
			case <-ctx.Done():
				return
			case out <- buf:
			}
		}
	}()
	return out, nil
}

// MockTTSFactory returns a TTSFactory producing tone-generating MockTTS
// instances at the session's sample rate.
func MockTTSFactory(durationMs int) TTSFactory {
	return func(sampleRate int) (TTS, error) {
		return MockTTS{SampleRate: sampleRate, DurationMs: durationMs}, nil
	}
}
