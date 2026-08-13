package pipeline

import (
	"context"
	"log/slog"
	"sync"

	"fs_websocket_stream/bridge"
)

// Cascade implements bridge.Handler, wiring ASR -> LLM -> TTS per call with
// barge-in: when the caller starts speaking while a reply is playing, the
// reply is cancelled and the module's playback buffer is flushed.
type Cascade struct {
	// NewASR builds a per-call streaming ASR for the session sample rate.
	NewASR ASRFactory
	// LLM generates replies.
	LLM LLM
	// NewTTS builds a per-call TTS for the session sample rate.
	NewTTS TTSFactory
	// SystemPrompt seeds the conversation history. Optional.
	SystemPrompt string
	// Logger for lifecycle events. Defaults to slog.Default().
	Logger *slog.Logger
	// Bus, when set, receives transcript and speech events per call.
	Bus *bridge.EventBus

	mu sync.Map // *bridge.Session -> *callState
}

type callState struct {
	asr     ASR
	history []Message
	logger  *slog.Logger

	mu       sync.Mutex
	cancel   context.CancelFunc // cancels in-flight reply playback
	speaking bool               // reply playback in progress
	turn     int                // increments per reply turn
}

// OnStart sets up the per-call pipeline.
func (c *Cascade) OnStart(s *bridge.Session) {
	logger := c.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("id", s.ID())

	asr, err := c.NewASR(s.SampleRate())
	if err != nil {
		logger.Error("ASR init failed", "err", err)
		s.Close()
		return
	}

	st := &callState{asr: asr, logger: logger}
	if c.SystemPrompt != "" {
		st.history = append(st.history, Message{Role: "system", Content: c.SystemPrompt})
	}
	c.mu.Store(s, st)

	go c.consumeASR(s, st)
}

// OnAudio feeds uplink audio to the ASR.
func (c *Cascade) OnAudio(s *bridge.Session, pcm []byte) {
	if v, ok := c.mu.Load(s); ok {
		_ = v.(*callState).asr.Write(pcm)
	}
}

// OnText logs metadata/control frames.
func (c *Cascade) OnText(s *bridge.Session, data []byte) {
	if c.Logger != nil {
		c.Logger.Info("session text frame", "id", s.ID(), "data", string(data))
	}
}

// OnEnd tears down the per-call pipeline.
func (c *Cascade) OnEnd(s *bridge.Session, err error) {
	if v, ok := c.mu.LoadAndDelete(s); ok {
		st := v.(*callState)
		st.mu.Lock()
		if st.cancel != nil {
			st.cancel()
		}
		st.mu.Unlock()
		_ = st.asr.Close()
	}
}

// consumeASR processes ASR events for the life of the call.
func (c *Cascade) consumeASR(s *bridge.Session, st *callState) {
	for ev := range st.asr.Events() {
		switch ev.Type {
		case EventSpeechStarted:
			if c.Bus != nil {
				c.Bus.Publish(bridge.Event{Name: "speech.start", UUID: s.FSUUID()})
			}
			c.bargeIn(s, st)
		case EventTranscript:
			if c.Bus != nil && ev.Text != "" {
				c.Bus.Publish(bridge.Event{Name: "transcript", UUID: s.FSUUID(), Data: map[string]any{
					"text": ev.Text, "final": ev.Final,
				}})
			}
			if !ev.Final || ev.Text == "" {
				continue
			}
			st.logger.Info("final transcript", "text", ev.Text)
			go c.respond(s, st, ev.Text)
		}
	}
}

// bargeIn cancels any in-flight reply and flushes the module's playback
// buffer. The clear is sent unconditionally: the module may still hold
// buffered audio even after the bridge finished sending a reply.
func (c *Cascade) bargeIn(s *bridge.Session, st *callState) {
	st.mu.Lock()
	if st.cancel != nil {
		st.cancel()
		st.cancel = nil
	}
	st.mu.Unlock()

	st.logger.Debug("barge-in: flushing playback")
	_ = s.ClearPlayback()
}

// respond runs one LLM -> TTS turn and plays the reply.
func (c *Cascade) respond(s *bridge.Session, st *callState, userText string) {
	ctx, cancel := context.WithCancel(context.Background())

	st.mu.Lock()
	if st.cancel != nil {
		st.cancel() // supersede any previous turn
	}
	st.cancel = cancel
	st.speaking = true
	st.turn++
	turn := st.turn
	st.history = append(st.history, Message{Role: "user", Content: userText})
	history := append([]Message(nil), st.history...)
	st.mu.Unlock()

	defer func() {
		st.mu.Lock()
		if st.turn == turn {
			st.cancel = nil
			st.speaking = false
		}
		st.mu.Unlock()
	}()

	reply, err := c.LLM.Respond(ctx, history)
	if err != nil {
		if ctx.Err() == nil {
			st.logger.Error("LLM failed", "err", err)
		}
		return
	}
	if reply == "" {
		return
	}

	st.mu.Lock()
	st.history = append(st.history, Message{Role: "assistant", Content: reply})
	st.mu.Unlock()

	tts, err := c.NewTTS(s.SampleRate())
	if err != nil {
		st.logger.Error("TTS init failed", "err", err)
		return
	}

	chunks, err := tts.Synthesize(ctx, reply)
	if err != nil {
		st.logger.Error("TTS synthesize failed", "err", err)
		return
	}

	for chunk := range chunks {
		if ctx.Err() != nil {
			return // barged in
		}
		if err := s.SendAudio(chunk); err != nil {
			return
		}
	}
}
