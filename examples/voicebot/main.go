// Command voicebot is an example external AI voice agent. It demonstrates
// the agent-side of the fsbridge agent protocol: fsbridge dials out to this
// app once per call and forwards caller audio; the app runs the AI pipeline
// (ASR -> LLM -> TTS) and streams reply audio back.
//
// Run fsbridge in agent mode:
//
//	fsbridge -mode agent -agent-url ws://localhost:9000/call -esl-addr localhost:8021
//
// Then run this app:
//
//	voicebot -mode mock   # tone replies, no API keys
//	voicebot -mode ai     # Deepgram + OpenAI + ElevenLabs (env keys required)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"fs_websocket_stream/bridge"
	"fs_websocket_stream/pipeline"
	"fs_websocket_stream/providers"
	"github.com/coder/websocket"
)

var logger = slog.New(slog.NewTextHandler(os.Stderr, nil))

// call is one agent session: a WebSocket from fsbridge carrying one call.
type call struct {
	conn    *websocket.Conn
	uuid    string // FreeSWITCH call UUID
	session string // bridge audio session id
	rate    int

	writeMu sync.Mutex

	asr     pipeline.ASR
	llm     pipeline.LLM
	newTTS  pipeline.TTSFactory
	history []pipeline.Message

	mu     sync.Mutex
	cancel context.CancelFunc
}

func (c *call) writeBinary(pcm []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.conn.Write(ctx, websocket.MessageBinary, pcm)
}

func (c *call) writeJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.conn.Write(ctx, websocket.MessageText, data)
}

// run is the per-call agent loop.
func (c *call) run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// ASR event loop: transcripts drive turns; speech-start is barge-in.
	go func() {
		for ev := range c.asr.Events() {
			switch ev.Type {
			case pipeline.EventSpeechStarted:
				// Caller interrupted: stop synthesizing and tell fsbridge to
				// flush what the caller hasn't heard yet.
				c.mu.Lock()
				if c.cancel != nil {
					c.cancel()
					c.cancel = nil
				}
				c.mu.Unlock()
				_ = c.writeJSON(map[string]string{"type": "clear"})
			case pipeline.EventTranscript:
				// Share transcripts with any /control subscribers via the bus.
				_ = c.writeJSON(bridge.AgentMessage{
					Type: bridge.AgentMsgEvent, Name: "transcript",
					Data: json.RawMessage(mustJSON(map[string]any{"text": ev.Text, "final": ev.Final})),
				})
				if ev.Final && ev.Text != "" {
					go c.respond(ctx, ev.Text)
				}
			}
		}
	}()

	// Audio loop: caller PCM -> ASR. Text frames: metadata/stop.
	for {
		mt, data, err := c.conn.Read(ctx)
		if err != nil {
			return
		}
		if mt == websocket.MessageBinary {
			_ = c.asr.Write(data)
			continue
		}
		var msg bridge.AgentMessage
		if json.Unmarshal(data, &msg) != nil {
			continue
		}
		switch msg.Type {
		case bridge.AgentMsgMetadata:
			logger.Info("call metadata", "uuid", c.uuid, "data", string(msg.Data))
		case bridge.AgentMsgStop:
			return
		}
	}
}

// respond runs one LLM -> TTS turn and streams the reply to fsbridge.
func (c *call) respond(parent context.Context, userText string) {
	ctx, cancel := context.WithCancel(parent)
	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
	}
	c.cancel = cancel
	c.history = append(c.history, pipeline.Message{Role: "user", Content: userText})
	history := append([]pipeline.Message(nil), c.history...)
	c.mu.Unlock()

	reply, err := c.llm.Respond(ctx, history)
	if err != nil || reply == "" {
		if err != nil && ctx.Err() == nil {
			logger.Error("llm failed", "err", err)
		}
		return
	}
	logger.Info("reply", "uuid", c.uuid, "text", reply)

	c.mu.Lock()
	c.history = append(c.history, pipeline.Message{Role: "assistant", Content: reply})
	c.mu.Unlock()

	tts, err := c.newTTS(c.rate)
	if err != nil {
		logger.Error("tts init failed", "err", err)
		return
	}
	chunks, err := tts.Synthesize(ctx, reply)
	if err != nil {
		logger.Error("tts failed", "err", err)
		return
	}
	for chunk := range chunks {
		if ctx.Err() != nil {
			return // barged in
		}
		if err := c.writeBinary(chunk); err != nil {
			return
		}
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	path := flag.String("path", "/call", "websocket endpoint path")
	mode := flag.String("mode", "mock", "mock | ai")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc(*path, func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			logger.Warn("accept failed", "err", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")

		q := r.URL.Query()
		c := &call{
			conn:    conn,
			uuid:    q.Get("uuid"),
			session: q.Get("session"),
			rate:    16000,
		}
		if n := q.Get("rate"); n != "" {
			var v int
			if _, err := fmt.Sscanf(n, "%d", &v); err == nil && v > 0 {
				c.rate = v
			}
		}

		// Build the pipeline stages for this call.
		switch *mode {
		case "ai":
			asr, err := providers.NewDeepgramASR(r.Context(), providers.DeepgramASRConfig{
				APIKey: os.Getenv("DEEPGRAM_API_KEY"),
			}, c.rate)
			if err != nil {
				logger.Error("deepgram init failed", "err", err)
				return
			}
			c.asr = asr
			c.llm = providers.OpenAILLM{APIKey: os.Getenv("OPENAI_API_KEY"), Model: os.Getenv("OPENAI_MODEL")}
			c.newTTS = providers.ElevenLabsTTSFactory(os.Getenv("ELEVENLABS_API_KEY"), os.Getenv("ELEVENLABS_VOICE_ID"))
			c.history = append(c.history, pipeline.Message{Role: "system", Content: "You are a helpful voice assistant. Keep replies short."})
		default: // mock
			m := &pipeline.MockASR{Transcript: "hello from the voicebot"}
			m.Events()
			c.asr = m
			c.llm = pipeline.MockLLM{}
			c.newTTS = pipeline.MockTTSFactory(2000)
		}
		defer c.asr.Close()

		logger.Info("call started", "uuid", c.uuid, "session", c.session, "rate", c.rate, "mode", *mode)
		c.run(r.Context())
		logger.Info("call ended", "uuid", c.uuid)
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()

	logger.Info("voicebot listening", "addr", *addr, "path", *path, "mode", *mode)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server failed", "err", err)
		os.Exit(1)
	}
}
