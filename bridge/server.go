package bridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// Handler receives callbacks for a call session. Callbacks for a given
// session are invoked sequentially from the session's read pump, so a
// Handler does not need additional locking for per-session state. SendAudio
// and SendControl may be called from any goroutine.
type Handler interface {
	// OnStart is called when the module connects, before any frames arrive.
	OnStart(s *Session)
	// OnAudio is called for each uplink L16 PCM frame from the caller.
	OnAudio(s *Session, pcm []byte)
	// OnText is called for each JSON text frame from the module. The first
	// one is typically call metadata; it is also stored on the Session.
	OnText(s *Session, data []byte)
	// OnEnd is called exactly once when the session terminates.
	OnEnd(s *Session, err error)
}

// BaseHandler provides no-op implementations for embedding in handlers that
// only need a subset of the callbacks.
type BaseHandler struct{}

func (BaseHandler) OnStart(s *Session)             {}
func (BaseHandler) OnAudio(s *Session, pcm []byte) {}
func (BaseHandler) OnText(s *Session, data []byte) {}
func (BaseHandler) OnEnd(s *Session, err error)    {}

// Options configures a Server. Nil Options yield defaults.
type Options struct {
	// Path is the HTTP path the WebSocket endpoint listens on.
	// Default "/stream".
	Path string
	// PlaybackQueueSize bounds queued downlink frames per session.
	// Default 100 (about 2s of 20ms frames).
	PlaybackQueueSize int
	// Logger receives connection lifecycle logs. Default slog.Default().
	Logger *slog.Logger
	// Bus, when set, receives audio.start / audio.end events per session.
	Bus *EventBus
	// Recorder, when set, captures every session to disk.
	Recorder *Recorder
}

const (
	defaultPath              = "/stream"
	defaultPlaybackQueueSize = 100
)

// Server accepts mod_ws_bridge WebSocket connections and dispatches each to
// the configured Handler.
type Server struct {
	handler  Handler
	opts     Options
	sessions sync.Map // id -> *Session
}

// NewServer creates a Server dispatching sessions to h.
func NewServer(h Handler, opts *Options) *Server {
	o := Options{Path: defaultPath, PlaybackQueueSize: defaultPlaybackQueueSize, Logger: slog.Default()}
	if opts != nil {
		if opts.Path != "" {
			o.Path = opts.Path
		}
		if opts.PlaybackQueueSize > 0 {
			o.PlaybackQueueSize = opts.PlaybackQueueSize
		}
		if opts.Logger != nil {
			o.Logger = opts.Logger
		}
		if opts.Bus != nil {
			o.Bus = opts.Bus
		}
		if opts.Recorder != nil {
			o.Recorder = opts.Recorder
		}
	}
	return &Server{handler: h, opts: o}
}

// HTTPHandler returns an http.Handler serving the WebSocket endpoint.
func (srv *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(srv.opts.Path, srv)
	return mux
}

// ServeHTTP upgrades the request and runs a session; it implements
// http.Handler so Server can be mounted on any mux/path.
func (srv *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	srv.serveWS(w, r)
}

// ListenAndServe runs the server until ctx is cancelled or the server fails.
func (srv *Server) ListenAndServe(ctx context.Context, addr string) error {
	httpSrv := &http.Server{Addr: addr, Handler: srv.HTTPHandler()}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()
	srv.opts.Logger.Info("bridge listening", "addr", addr, "path", srv.opts.Path)
	err := httpSrv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// SessionCount returns the number of active sessions.
func (srv *Server) SessionCount() int {
	n := 0
	srv.sessions.Range(func(_, _ any) bool { n++; return true })
	return n
}

// SessionByFSUUID returns the active audio session for a FreeSWITCH call
// UUID, or nil if none is streaming.
func (srv *Server) SessionByFSUUID(uuid string) *Session {
	var found *Session
	srv.sessions.Range(func(_, v any) bool {
		if s := v.(*Session); s.fsUUID == uuid {
			found = s
			return false
		}
		return true
	})
	return found
}

// SessionsSnapshot returns a copy of currently active sessions.
func (srv *Server) SessionsSnapshot() []*Session {
	var out []*Session
	srv.sessions.Range(func(_, v any) bool {
		out = append(out, v.(*Session))
		return true
	})
	return out
}

// CloseAllSessions terminates every active session. Used during graceful
// shutdown so handlers run their OnEnd and the recorder finalizes bundles.
func (srv *Server) CloseAllSessions() {
	srv.sessions.Range(func(_, v any) bool {
		v.(*Session).Close()
		return true
	})
}

func (srv *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// mod_audio_stream enables permessage-deflate by default.
		CompressionMode: websocket.CompressionContextTakeover,
	})
	if err != nil {
		srv.opts.Logger.Warn("websocket accept failed", "err", err)
		return
	}

	s := &Session{
		id:         newSessionID(),
		conn:       conn,
		sampleRate: queryInt(r, "rate", 16000),
		mixType:    queryDefault(r, "mix", "mono"),
		fsUUID:     r.URL.Query().Get("uuid"),
		writeQueue: make(chan writeRequest, srv.opts.PlaybackQueueSize),
		done:       make(chan struct{}),
	}
	srv.sessions.Store(s.id, s)
	srv.opts.Logger.Info("session started", "id", s.id, "remote", r.RemoteAddr,
		"rate", s.sampleRate, "mix", s.mixType, "fs_uuid", s.fsUUID)
	if srv.opts.Recorder != nil {
		srv.opts.Recorder.Begin(s)
		s.downlinkTap = func(pcm []byte) { srv.opts.Recorder.Downlink(s, pcm) }
	}
	if srv.opts.Bus != nil {
		srv.opts.Bus.Publish(Event{Name: "audio.start", UUID: s.fsUUID, Data: map[string]any{
			"session": s.id, "rate": s.sampleRate, "mix": s.mixType,
		}})
	}

	go s.writePump(r.Context())
	srv.handler.OnStart(s)

	readErr := srv.readPump(r.Context(), s)

	s.Close()
	srv.sessions.Delete(s.id)
	srv.handler.OnEnd(s, readErr)
	if srv.opts.Recorder != nil {
		srv.opts.Recorder.Finish(s)
	}
	if srv.opts.Bus != nil {
		srv.opts.Bus.Publish(Event{Name: "audio.end", UUID: s.fsUUID, Data: map[string]any{
			"session": s.id,
		}})
	}
	srv.opts.Logger.Info("session ended", "id", s.id, "err", readErr)
}

func (srv *Server) readPump(ctx context.Context, s *Session) error {
	for {
		mt, data, err := s.conn.Read(ctx)
		if err != nil {
			return err
		}
		switch mt {
		case websocket.MessageBinary:
			if srv.opts.Recorder != nil {
				srv.opts.Recorder.Uplink(s, data)
			}
			srv.handler.OnAudio(s, data)
		case websocket.MessageText:
			s.setMetadata(data)
			srv.handler.OnText(s, data)
		}
	}
}

func newSessionID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func queryInt(r *http.Request, key string, def int) int {
	if v := r.URL.Query().Get(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func queryDefault(r *http.Request, key, def string) string {
	if v := r.URL.Query().Get(key); v != "" {
		return v
	}
	return def
}
