// Package pipeline wires the audio bridge to AI stages: streaming ASR,
// an LLM, and streaming TTS, with barge-in support.
//
// The stages are interfaces; the cascade glues a per-call ASR -> LLM -> TTS
// flow onto a bridge.Session. Mocks allow testing the full path with no
// external API keys; real providers live in the providers package.
package pipeline

import (
	"context"
	"errors"
	"io"
)

// EventType classifies ASR events.
type EventType int

const (
	// EventSpeechStarted indicates the caller started speaking (VAD/ASR
	// signal). Used for barge-in.
	EventSpeechStarted EventType = iota
	// EventTranscript carries a partial or final transcript.
	EventTranscript
)

// ASREvent is emitted by a streaming ASR.
type ASREvent struct {
	Type  EventType
	Text  string // transcript text for EventTranscript
	Final bool   // true for final transcripts
}

// ASR consumes caller audio (L16 PCM at the session's sample rate) and
// emits speech/transcript events. Implementations must be safe for Write to
// be called from the session read pump while Events is consumed elsewhere.
type ASR interface {
	io.Closer
	// Write feeds an uplink PCM chunk.
	Write(pcm []byte) error
	// Events streams ASR events until Close.
	Events() <-chan ASREvent
}

// Message is a single conversational turn.
type Message struct {
	Role    string `json:"role"` // "user" | "assistant" | "system"
	Content string `json:"content"`
}

// LLM produces an assistant reply for the conversation so far.
type LLM interface {
	// Respond returns the assistant's reply text for the given history.
	Respond(ctx context.Context, history []Message) (string, error)
}

// TTS synthesizes text into L16 PCM chunks at the given sample rate.
// The returned channel yields chunks until synthesis completes, ctx is
// cancelled, or an error occurs (reported via the last error if any).
type TTS interface {
	Synthesize(ctx context.Context, text string) (<-chan []byte, error)
}

// ErrNoReply indicates the LLM produced no reply for the turn.
var ErrNoReply = errors.New("pipeline: empty LLM reply")

// ASRFactory builds a per-call ASR for the given sample rate.
type ASRFactory func(sampleRate int) (ASR, error)

// TTSFactory builds a per-call TTS for the given sample rate.
type TTSFactory func(sampleRate int) (TTS, error)
