package bridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// fakeAgent records what an agent app would see and lets the test drive
// agent -> bridge messages.
type fakeAgent struct {
	started  chan AgentMessage
	audioIn  chan []byte
	mu       sync.Mutex
	conn     *websocket.Conn
	metadata [][]byte
}

func newFakeAgent(t *testing.T) (*fakeAgent, string) {
	t.Helper()
	fa := &fakeAgent{
		started: make(chan AgentMessage, 1),
		audioIn: make(chan []byte, 64),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		fa.mu.Lock()
		fa.conn = conn
		fa.mu.Unlock()
		defer conn.Close(websocket.StatusNormalClosure, "")
		for {
			mt, data, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if mt == websocket.MessageBinary {
				fa.audioIn <- data
				continue
			}
			var msg AgentMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			switch msg.Type {
			case AgentMsgStart:
				fa.started <- msg
			case AgentMsgMetadata:
				fa.mu.Lock()
				fa.metadata = append(fa.metadata, msg.Data)
				fa.mu.Unlock()
			case AgentMsgStop:
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return fa, "ws" + strings.TrimPrefix(srv.URL, "http")
}

func (fa *fakeAgent) sendJSON(t *testing.T, v any) {
	t.Helper()
	fa.mu.Lock()
	conn := fa.conn
	fa.mu.Unlock()
	if conn == nil {
		t.Fatal("agent not connected")
	}
	data, _ := json.Marshal(v)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("agent write: %v", err)
	}
}

// moduleClient simulates mod_ws_bridge against a bridge server.
func moduleClient(t *testing.T, srv *Server) *websocket.Conn {
	t.Helper()
	httpSrv := httptest.NewServer(srv.HTTPHandler())
	t.Cleanup(httpSrv.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	conn, _, err := websocket.Dial(ctx, "ws"+httpSrv.URL[4:]+"/stream?rate=16000&mix=mono&uuid=call-42", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close(websocket.StatusNormalClosure, "") })
	return conn
}

func TestAgentForwarderAudioAndControl(t *testing.T) {
	agent, agentURL := newFakeAgent(t)
	esl := &fakeESL{reply: "+OK", events: make(chan ESLEvent)}
	fwd := &AgentForwarder{URL: agentURL, ESL: esl}

	srv := NewServer(fwd, nil)
	conn := moduleClient(t, srv)

	// Agent should receive the start frame with correlation ids.
	select {
	case msg := <-agent.started:
		if msg.UUID != "call-42" || msg.Rate != 16000 || msg.Session == "" {
			t.Fatalf("start frame = %+v", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent got no start frame")
	}

	// Module metadata is forwarded.
	if err := conn.Write(context.Background(), websocket.MessageText, []byte(`{"cid":"1000"}`)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		agent.mu.Lock()
		n := len(agent.metadata)
		agent.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	agent.mu.Lock()
	if len(agent.metadata) == 0 || string(agent.metadata[0]) != `{"cid":"1000"}` {
		t.Fatalf("metadata = %v", agent.metadata)
	}
	agent.mu.Unlock()

	// Uplink audio reaches the agent.
	pcm := make([]byte, 640)
	if err := conn.Write(context.Background(), websocket.MessageBinary, pcm); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-agent.audioIn:
		if len(got) != 640 {
			t.Fatalf("agent audio = %d bytes", len(got))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("agent got no audio")
	}

	// Agent clear -> module receives a clear control frame.
	agent.sendJSON(t, AgentMessage{Type: AgentMsgClear})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, data, err := conn.Read(ctx)
	cancel()
	if err != nil {
		t.Fatalf("read clear: %v", err)
	}
	if !strings.Contains(string(data), `"clear"`) {
		t.Fatalf("module got %q, want clear", data)
	}

	// Agent hangup -> ESL uuid_kill for the call.
	agent.sendJSON(t, AgentMessage{Type: AgentMsgHangup})
	deadline = time.Now().Add(2 * time.Second)
	for esl.lastCmd != "uuid_kill call-42" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if esl.lastCmd != "uuid_kill call-42" {
		t.Fatalf("ESL cmd = %q, want uuid_kill call-42", esl.lastCmd)
	}
}

func TestAgentForwarderDownlinkAudio(t *testing.T) {
	agent, agentURL := newFakeAgent(t)
	fwd := &AgentForwarder{URL: agentURL}
	srv := NewServer(fwd, nil)
	conn := moduleClient(t, srv)

	select {
	case <-agent.started:
	case <-time.After(3 * time.Second):
		t.Fatal("agent got no start frame")
	}

	// Agent sends downlink PCM; the module client receives it as binary.
	agent.mu.Lock()
	agentConn := agent.conn
	agent.mu.Unlock()
	if agentConn == nil {
		t.Fatal("agent not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := agentConn.Write(ctx, websocket.MessageBinary, make([]byte, 640)); err != nil {
		t.Fatal(err)
	}

	rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
	mt, data, err := conn.Read(rctx)
	rcancel()
	if err != nil {
		t.Fatalf("module read: %v", err)
	}
	if mt != websocket.MessageBinary || len(data) != 640 {
		t.Fatalf("module got type=%v len=%d", mt, len(data))
	}
}

func TestAgentForwarderDialFailureClosesSession(t *testing.T) {
	fwd := &AgentForwarder{URL: "ws://127.0.0.1:1/none", DialTimeout: 500 * time.Millisecond}
	srv := NewServer(fwd, nil)
	conn := moduleClient(t, srv)

	// The session should be torn down: reads fail once the server closes.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	if err == nil {
		t.Fatal("expected read error after agent dial failure")
	}
}
