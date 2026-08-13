package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// dial connects a test client to srv and returns the connection.
func dial(t *testing.T, srv *Server, url string) *websocket.Conn {
	t.Helper()
	httpSrv := httptest.NewServer(srv.HTTPHandler())
	t.Cleanup(httpSrv.Close)
	wsURL := "ws" + httpSrv.URL[4:] + url
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close(websocket.StatusNormalClosure, "") })
	return conn
}

func TestEchoRoundTrip(t *testing.T) {
	srv := NewServer(EchoHandler{}, nil)
	conn := dial(t, srv, "/stream?rate=16000&mix=mono")

	// First text frame: call metadata, as the module sends on start.
	meta := `{"caller":"1000","destination":"9999"}`
	if err := conn.Write(context.Background(), websocket.MessageText, []byte(meta)); err != nil {
		t.Fatalf("write metadata: %v", err)
	}

	// 20ms of 16kHz L16 PCM = 640 bytes.
	frame := bytes.Repeat([]byte{0x01, 0x02}, 320)
	if err := conn.Write(context.Background(), websocket.MessageBinary, frame); err != nil {
		t.Fatalf("write audio: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	mt, got, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if mt != websocket.MessageBinary {
		t.Fatalf("echo frame type = %v, want binary", mt)
	}
	if !bytes.Equal(got, frame) {
		t.Fatalf("echo mismatch: got %d bytes, want %d", len(got), len(frame))
	}
}

// recordingHandler captures callbacks for assertions.
type recordingHandler struct {
	BaseHandler
	meta    chan []byte
	onStart chan *Session
	onEnd   chan error
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{
		meta:    make(chan []byte, 1),
		onStart: make(chan *Session, 1),
		onEnd:   make(chan error, 1),
	}
}

func (h *recordingHandler) OnStart(s *Session)          { h.onStart <- s }
func (h *recordingHandler) OnEnd(s *Session, err error) { h.onEnd <- err }
func (h *recordingHandler) OnText(s *Session, d []byte) {
	h.meta <- append([]byte(nil), d...)
}

func TestMetadataAndLifecycle(t *testing.T) {
	h := newRecordingHandler()
	srv := NewServer(h, nil)
	conn := dial(t, srv, "/stream?rate=8000&mix=stereo")

	var s *Session
	select {
	case s = <-h.onStart:
	case <-time.After(2 * time.Second):
		t.Fatal("OnStart not called")
	}
	if s.SampleRate() != 8000 {
		t.Errorf("SampleRate = %d, want 8000", s.SampleRate())
	}
	if s.MixType() != "stereo" {
		t.Errorf("MixType = %q, want stereo", s.MixType())
	}
	if srv.SessionCount() != 1 {
		t.Errorf("SessionCount = %d, want 1", srv.SessionCount())
	}

	meta := `{"uuid":"abc-123"}`
	if err := conn.Write(context.Background(), websocket.MessageText, []byte(meta)); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	select {
	case got := <-h.meta:
		if !json.Valid(got) || string(got) != meta {
			t.Errorf("OnText got %q, want %q", got, meta)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnText not called")
	}
	if string(s.Metadata()) != meta {
		t.Errorf("Session.Metadata() = %q, want %q", s.Metadata(), meta)
	}

	conn.Close(websocket.StatusNormalClosure, "bye")
	select {
	case err := <-h.onEnd:
		if err == nil {
			t.Error("OnEnd err = nil, want close error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnEnd not called")
	}

	// Give the server a moment to deregister.
	deadline := time.Now().Add(2 * time.Second)
	for srv.SessionCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if srv.SessionCount() != 0 {
		t.Errorf("SessionCount after close = %d, want 0", srv.SessionCount())
	}
}

func TestClearPlaybackSendsControlFrame(t *testing.T) {
	h := newRecordingHandler()
	srv := NewServer(h, nil)
	conn := dial(t, srv, "/stream")

	var s *Session
	select {
	case s = <-h.onStart:
	case <-time.After(2 * time.Second):
		t.Fatal("OnStart not called")
	}

	if err := s.ClearPlayback(); err != nil {
		t.Fatalf("ClearPlayback: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	mt, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read control: %v", err)
	}
	if mt != websocket.MessageText {
		t.Fatalf("control frame type = %v, want text", mt)
	}
	var msg ControlMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal control: %v", err)
	}
	if msg.Type != ControlClear {
		t.Errorf("control type = %q, want %q", msg.Type, ControlClear)
	}
}

func TestPlaybackQueueDropsOldest(t *testing.T) {
	s := &Session{
		writeQueue: make(chan writeRequest, 2),
		done:       make(chan struct{}),
	}
	defer close(s.done)

	frame := func(b byte) []byte { return bytes.Repeat([]byte{b}, 4) }

	if err := s.SendAudio(frame(1)); err != nil {
		t.Fatal(err)
	}
	if err := s.SendAudio(frame(2)); err != nil {
		t.Fatal(err)
	}
	// Queue now full; this must not block and must evict frame(1).
	done := make(chan error, 1)
	go func() { done <- s.SendAudio(frame(3)) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SendAudio on full queue: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SendAudio blocked on full queue")
	}

	first := <-s.writeQueue
	second := <-s.writeQueue
	if !bytes.Equal(first.data, frame(2)) || !bytes.Equal(second.data, frame(3)) {
		t.Fatalf("queue = [%v %v], want [frame(2) frame(3)]", first.data, second.data)
	}
}

func TestSendAfterClose(t *testing.T) {
	srv := NewServer(EchoHandler{}, nil)
	conn := dial(t, srv, "/stream")

	httpHandler := srv.HTTPHandler()
	_ = httpHandler // server already running via dial

	conn.Close(websocket.StatusNormalClosure, "")
	// There is no direct handle to the server-side session here; instead
	// verify a hand-built closed session errors.
	s := &Session{
		writeQueue: make(chan writeRequest, 1),
		done:       make(chan struct{}),
	}
	s.closeOnce.Do(func() { close(s.done) })
	if err := s.SendAudio([]byte{1, 2}); err != ErrSessionClosed {
		t.Fatalf("SendAudio after close = %v, want ErrSessionClosed", err)
	}
}
