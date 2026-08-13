// Command fsbridge runs the WebSocket audio bridge server.
//
// Modes:
//
//	echo  - return caller audio unchanged (connectivity testing)
//	mock  - run the mock ASR/LLM/TTS cascade (pipeline testing, no API keys)
//	ai    - Deepgram ASR -> OpenAI LLM -> ElevenLabs TTS in-process
//	agent - forward each call to an external agent app (-agent-url)
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"fs_websocket_stream/bridge"
	"fs_websocket_stream/pipeline"
	"fs_websocket_stream/providers"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	path := flag.String("path", "/stream", "websocket audio endpoint path")
	mode := flag.String("mode", "echo", "handler mode: echo | mock | ai | agent")
	agentURL := flag.String("agent-url", "", "agent app WebSocket URL, e.g. ws://localhost:9000/call (agent mode)")
	systemPrompt := flag.String("system", "You are a helpful voice assistant. Keep replies short and conversational.", "system prompt (ai mode)")
	eslAddr := flag.String("esl-addr", "", "FreeSWITCH event socket address (e.g. localhost:8021); enables the control plane")
	eslPassword := flag.String("esl-password", "ClueCon", "ESL password")
	controlPath := flag.String("control-path", "/control", "websocket control endpoint path (requires -esl-addr)")
	recordDir := flag.String("record-dir", "", "directory for per-call recording bundles (enables recording + /recordings API)")
	recordRetention := flag.Duration("record-retention", 24*time.Hour, "how long to keep recording bundles")
	recordMaxMB := flag.Int("record-max-mb", 1024, "max total recording size in MB (oldest deleted first)")
	authToken := flag.String("auth-token", "", "if set, require Bearer token on /control and /recordings")
	lokiURL := flag.String("loki-url", "", "Loki push URL (e.g. http://loki:3100); enables log shipping")
	lokiJob := flag.String("loki-job", "fsbridge", "Loki `job` label")
	lokiTenant := flag.String("loki-tenant", "", "Loki X-Scope-OrgID (Grafana Cloud)")
	lokiUser := flag.String("loki-user", "", "Loki basic auth username")
	lokiPass := flag.String("loki-pass", "", "Loki basic auth password")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if *lokiURL != "" {
		loki, err := bridge.NewLokiClient(bridge.LokiConfig{
			URL: *lokiURL, Job: *lokiJob, TenantID: *lokiTenant, Username: *lokiUser, Password: *lokiPass,
		})
		if err != nil {
			logger.Error("loki init failed", "err", err)
			os.Exit(1)
		}
		logger = slog.New(bridge.NewLokiHandler(slog.NewJSONHandler(os.Stderr, nil), loki))
		logger.Info("loki shipping enabled", "url", *lokiURL)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var bus *bridge.EventBus
	if *eslAddr != "" || *recordDir != "" {
		bus = bridge.NewEventBus()
	}

	var recorder *bridge.Recorder
	if *recordDir != "" {
		var err error
		recorder, err = bridge.NewRecorder(*recordDir)
		if err != nil {
			logger.Error("recorder init failed", "err", err)
			os.Exit(1)
		}
		recorder.Bus = bus
		recorder.Logger = logger
		recorder.Retention = *recordRetention
		recorder.MaxBytes = int64(*recordMaxMB) << 20
		recorder.Start()
		defer recorder.Close()
	}

	// ESL client first: the control plane and agent mode both use it.
	var eslClient *bridge.ESLClient
	if *eslAddr != "" {
		eslClient = bridge.NewESLClient(*eslAddr, *eslPassword, []string{
			"CHANNEL_CREATE", "CHANNEL_ANSWER", "CHANNEL_HANGUP_COMPLETE",
			"CHANNEL_DESTROY", "CHANNEL_BRIDGE", "CHANNEL_UNBRIDGE",
		})
		go eslClient.Run(ctx)
		go bridge.ESLToBus(ctx, eslClient, bus)
	}

	var handler bridge.Handler
	switch *mode {
	case "echo":
		handler = &statsHandler{logger: logger}
	case "mock":
		handler = &pipeline.Cascade{
			NewASR: pipeline.MockASRFactory("hello world"),
			LLM:    pipeline.MockLLM{},
			NewTTS: pipeline.MockTTSFactory(2000),
			Logger: logger,
			Bus:    bus,
		}
	case "ai":
		handler = &pipeline.Cascade{
			NewASR: providers.DeepgramASRFactory(providers.DeepgramASRConfig{
				APIKey: os.Getenv("DEEPGRAM_API_KEY"),
			}),
			LLM: providers.OpenAILLM{
				APIKey: os.Getenv("OPENAI_API_KEY"),
				Model:  os.Getenv("OPENAI_MODEL"),
			},
			NewTTS: providers.ElevenLabsTTSFactory(
				os.Getenv("ELEVENLABS_API_KEY"),
				os.Getenv("ELEVENLABS_VOICE_ID"),
			),
			SystemPrompt: *systemPrompt,
			Logger:       logger,
			Bus:          bus,
		}
	case "agent":
		if *agentURL == "" {
			logger.Error("agent mode requires -agent-url")
			os.Exit(2)
		}
		handler = &bridge.AgentForwarder{
			URL:    *agentURL,
			ESL:    eslClient,
			Bus:    bus,
			Logger: logger,
		}
	default:
		logger.Error("unknown mode", "mode", *mode)
		os.Exit(2)
	}

	srv := bridge.NewServer(handler, &bridge.Options{
		Path:     *path,
		Logger:   logger,
		Bus:      bus,
		Recorder: recorder,
	})

	mux := http.NewServeMux()
	mux.Handle(*path, srv)
	mux.HandleFunc("/healthz", bridge.HealthzHandler(srv))

	if recorder != nil {
		recorder.RegisterRoutes(mux)
		logger.Info("recording enabled", "dir", *recordDir, "retention", *recordRetention)
	}

	if eslClient != nil {
		control := &bridge.ControlServer{
			ESL:   eslClient,
			Bus:   bus,
			Audio: srv,
			Log:   logger,
		}
		mux.Handle(*controlPath, bridge.AuthMiddleware(*authToken, control))
		logger.Info("control plane enabled", "esl", *eslAddr, "path", *controlPath,
			"auth", *authToken != "")
	}

	if recorder != nil && *authToken != "" {
		// Wrap the recordings routes with the same auth middleware.
		recordingMux := http.NewServeMux()
		recorder.RegisterRoutes(recordingMux)
		mux.Handle("/recordings/", bridge.AuthMiddleware(*authToken, recordingMux))
	}

	httpSrv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		<-ctx.Done()
		logger.Info("shutting down")

		// Stop accepting new connections; let existing requests complete.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)

		// Drain active sessions so recorder finalizes, handlers run OnEnd,
		// and bus subscribers see audio.end events.
		srv.CloseAllSessions()

		// Wait briefly for OnEnd goroutines to finish (recorder writes,
		// Finish() finalizes, captureEvents drains).
		time.Sleep(2 * time.Second)

		if eslClient != nil {
			eslClient.Close()
		}
		if recorder != nil {
			recorder.Close()
		}
	}()

	logger.Info("bridge listening", "addr", *addr, "path", *path, "mode", *mode)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server failed", "err", err)
		os.Exit(1)
	}
}

// statsHandler wraps EchoHandler and periodically logs frame counts so
// audio flow can be verified from the bridge logs.
type statsHandler struct {
	bridge.BaseHandler
	logger  *slog.Logger
	frames  uint64
	bytesIn uint64
}

func (h *statsHandler) OnAudio(s *bridge.Session, pcm []byte) {
	n := atomic.AddUint64(&h.frames, 1)
	atomic.AddUint64(&h.bytesIn, uint64(len(pcm)))
	if n%500 == 0 {
		h.logger.Info("audio flowing", "id", s.ID(), "frames", n, "bytes_in", atomic.LoadUint64(&h.bytesIn))
	}
	_ = s.SendAudio(pcm)
}

func (h *statsHandler) OnText(s *bridge.Session, data []byte) {
	h.logger.Info("metadata", "id", s.ID(), "data", string(data))
}
