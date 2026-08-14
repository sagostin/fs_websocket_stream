// Command openaibot is a fully OpenAI-compatible example AI agent. Every
// pipeline stage speaks an OpenAI-compatible API:
//
//	ASR: OpenAI Realtime transcription (WebSocket, gpt-4o-transcribe)
//	LLM: OpenAI chat completions
//	TTS: OpenAI /v1/audio/speech (streaming PCM)
//
// Each stage honors OPENAI_BASE_URL (or its own flag), so the whole stack
// can be pointed at api.openai.com, Azure, a gateway, or a local
// OpenAI-compatible server.
//
// The bot is a first-line dispatcher: the LLM is prompted to answer with
// ONLY a JSON object naming an action and the spoken reply, e.g.
//
//	{"destination": "support", "response": "Transferring you to support now."}
//
// The app speaks "response" via TTS; when "destination" is set and maps to
// a dialplan extension (see -routes), it transfers the call through the
// bridge's /control endpoint.
//
// Run fsbridge in agent mode:
//
//	fsbridge -addr :8090 -mode agent -agent-url ws://localhost:9000/call -esl-addr localhost:8021
//
// Then run this app:
//
//	openaibot -mode mock   # tone replies + scripted actions, no API keys
//	openaibot -mode ai     # full OpenAI-compatible stack (env keys required)
//
// Env (ai mode): OPENAI_API_KEY; optional OPENAI_BASE_URL, OPENAI_MODEL,
// OPENAI_ASR_MODEL, OPENAI_ASR_PROMPT, OPENAI_TTS_MODEL, OPENAI_TTS_VOICE.
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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"fs_websocket_stream/bridge"
	"fs_websocket_stream/pipeline"
	"fs_websocket_stream/providers"
	"github.com/coder/websocket"
)

var logger = slog.New(slog.NewTextHandler(os.Stderr, nil))

// action is the LLM's structured reply: what to say and where to send the
// call. Destination is a logical name ("support"); -routes maps names to
// dialplan extensions.
type action struct {
	Destination string `json:"destination"`
	Response    string `json:"response"`
}

// extractAction parses the model's reply tolerantly: strips markdown code
// fences and any prose around the JSON object.
func extractAction(text string) (action, error) {
	var a action
	s := strings.TrimSpace(text)
	if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```")
		s = strings.TrimPrefix(strings.TrimSpace(s), "json")
		s = strings.TrimSuffix(strings.TrimSpace(s), "```")
	}
	start := strings.Index(s, "{")
	end := strings.LastIndex(s, "}")
	if start < 0 || end <= start {
		return a, fmt.Errorf("no JSON object in reply: %q", text)
	}
	if err := json.Unmarshal([]byte(s[start:end+1]), &a); err != nil {
		return a, fmt.Errorf("bad action JSON: %w", err)
	}
	if a.Response == "" && a.Destination == "" {
		return a, fmt.Errorf("action has neither response nor destination: %q", s[start:end+1])
	}
	return a, nil
}

// systemPrompt instructs the model to act as the TOPS Telecom dispatcher
// and to reply with strict JSON only.
func systemPrompt(routes map[string]string) string {
	names := make([]string, 0, len(routes))
	for name := range routes {
		names = append(names, name)
	}
	sort.Strings(names)
	return fmt.Sprintf(`You are a helpful assistant for TOPS Telecom, the first line support dispatcher.
Your job is to understand the caller's issue and transfer them to the correct department.
Available departments: %s.

Respond with ONLY a JSON object, no markdown, no prose:
{"destination": "<department name or null>", "response": "<short spoken reply>"}

Set destination only when you are confident which department the caller needs;
otherwise set it to null and ask a brief clarifying question in response.
Keep response under two sentences — it will be spoken aloud.`,
		strings.Join(names, ", "))
}

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

	controlURL string            // bridge /control endpoint, for transfers
	routes     map[string]string // department name -> dialplan extension

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
				c.mu.Lock()
				if c.cancel != nil {
					c.cancel()
					c.cancel = nil
				}
				c.mu.Unlock()
				_ = c.writeJSON(map[string]string{"type": "clear"})
			case pipeline.EventTranscript:
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

// respond runs one LLM -> TTS turn, then executes the action the model
// returned (a transfer, when destination is set).
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

	act, err := extractAction(reply)
	if err != nil {
		// Model broke the JSON contract; speak the raw reply rather than
		// leaving the caller in silence.
		logger.Warn("unparsable llm reply, speaking raw text", "err", err)
		act = action{Response: reply}
	}
	logger.Info("action", "uuid", c.uuid, "destination", act.Destination, "response", act.Response)

	c.mu.Lock()
	c.history = append(c.history, pipeline.Message{Role: "assistant", Content: reply})
	c.mu.Unlock()

	if act.Response != "" {
		if !c.speak(ctx, act.Response) {
			return // barged in or write failed
		}
	}
	if act.Destination != "" && ctx.Err() == nil {
		c.transfer(act.Destination)
	}
}

// speak synthesizes text and streams it to fsbridge. It returns false when
// the turn was cancelled (barge-in) or the socket write failed.
func (c *call) speak(ctx context.Context, text string) bool {
	tts, err := c.newTTS(c.rate)
	if err != nil {
		logger.Error("tts init failed", "err", err)
		return true
	}
	chunks, err := tts.Synthesize(ctx, text)
	if err != nil {
		logger.Error("tts failed", "err", err)
		return true
	}
	start := time.Now()
	var written int
	for chunk := range chunks {
		if ctx.Err() != nil {
			return false // barged in
		}
		if err := c.writeBinary(chunk); err != nil {
			return false
		}
		written += len(chunk)
	}
	// TTS streams faster than realtime; give the caller time to hear the
	// reply before any follow-up action (e.g. transfer) moves the call.
	played := time.Since(start)
	total := time.Duration(written/2) * time.Second / time.Duration(c.rate)
	if rem := total - played; rem > 0 {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(rem):
		}
	}
	return true
}

// transfer moves the call to the extension mapped to dest via the bridge's
// /control endpoint (ESL uuid_transfer under the hood).
func (c *call) transfer(dest string) {
	ext, ok := c.routes[dest]
	if !ok {
		logger.Warn("unknown destination, call stays here", "uuid", c.uuid, "destination", dest)
		return
	}
	if c.controlURL == "" || c.uuid == "" {
		logger.Warn("transfer unavailable (no -control-url or call uuid)", "uuid", c.uuid)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, c.controlURL, nil)
	if err != nil {
		logger.Error("control dial failed", "err", err)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	args := json.RawMessage(mustJSON(map[string]string{"uuid": c.uuid, "dest": ext}))
	cmd := bridge.ControlCommand{ID: "transfer-" + c.uuid, Cmd: "transfer", Args: args}
	if err := conn.Write(ctx, websocket.MessageText, []byte(mustJSON(cmd))); err != nil {
		logger.Error("transfer send failed", "err", err)
		return
	}

	okTransfer := false
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			break
		}
		var reply bridge.ControlReply
		if json.Unmarshal(data, &reply) != nil || reply.ID != cmd.ID {
			continue // bus event or unrelated reply
		}
		okTransfer = reply.OK
		if !reply.OK {
			logger.Error("transfer failed", "uuid", c.uuid, "dest", ext, "err", reply.Error)
		}
		break
	}

	logger.Info("transfer", "uuid", c.uuid, "destination", dest, "extension", ext, "ok", okTransfer)
	_ = c.writeJSON(bridge.AgentMessage{
		Type: bridge.AgentMsgEvent, Name: "transfer",
		Data: json.RawMessage(mustJSON(map[string]any{
			"destination": dest, "extension": ext, "ok": okTransfer,
		})),
	})
	// On success FreeSWITCH moves the leg out of the streaming extension;
	// the module ends the stream and the bridge's stop frame closes this
	// session naturally.
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// mockActionLLM speaks JSON actions without an API key: it echoes the
// caller, and "transfers" when the transcript names a known route.
type mockActionLLM struct {
	routes map[string]string
}

func (m mockActionLLM) Respond(ctx context.Context, history []pipeline.Message) (string, error) {
	if len(history) == 0 {
		return "", pipeline.ErrNoReply
	}
	last := strings.ToLower(history[len(history)-1].Content)
	for name := range m.routes {
		if strings.Contains(last, strings.ToLower(name)) {
			return mustJSON(action{
				Destination: name,
				Response:    "Of course, transferring you to " + name + " now.",
			}), nil
		}
	}
	return mustJSON(action{Response: "You said: " + history[len(history)-1].Content}), nil
}

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	path := flag.String("path", "/call", "websocket endpoint path")
	mode := flag.String("mode", "mock", "mock | ai")
	controlURL := flag.String("control-url", "ws://localhost:8090/control", "bridge /control endpoint for transfers")
	routesFlag := flag.String("routes", `{"support":"1002","billing":"1003"}`, "JSON map of department name -> dialplan extension")
	flag.Parse()

	routes := map[string]string{}
	if err := json.Unmarshal([]byte(*routesFlag), &routes); err != nil {
		logger.Error("bad -routes JSON", "err", err)
		os.Exit(1)
	}

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
			conn:       conn,
			uuid:       q.Get("uuid"),
			session:    q.Get("session"),
			rate:       16000,
			controlURL: *controlURL,
			routes:     routes,
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
			apiKey := os.Getenv("OPENAI_API_KEY")
			baseURL := os.Getenv("OPENAI_BASE_URL")
			asr, err := providers.NewOpenAIASR(r.Context(), providers.OpenAIASRConfig{
				APIKey:  apiKey,
				Model:   os.Getenv("OPENAI_ASR_MODEL"),
				BaseURL: baseURL,
				Prompt:  os.Getenv("OPENAI_ASR_PROMPT"),
			}, c.rate)
			if err != nil {
				logger.Error("openai asr init failed", "err", err)
				return
			}
			c.asr = asr
			c.llm = providers.OpenAILLM{
				APIKey:  apiKey,
				Model:   os.Getenv("OPENAI_MODEL"),
				BaseURL: baseURL,
			}
			c.newTTS = providers.OpenAITTSFactory(apiKey, os.Getenv("OPENAI_TTS_VOICE"), baseURL)
			c.history = append(c.history, pipeline.Message{Role: "system", Content: systemPrompt(routes)})
		default: // mock
			m := &pipeline.MockASR{Transcript: "my phone is broken, can you transfer me to support"}
			m.Events()
			c.asr = m
			c.llm = mockActionLLM{routes: routes}
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

	logger.Info("openaibot listening", "addr", *addr, "path", *path, "mode", *mode, "routes", routes)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server failed", "err", err)
		os.Exit(1)
	}
}
