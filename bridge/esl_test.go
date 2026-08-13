package bridge

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// fakeESLServer implements just enough of the event socket protocol to test
// the client: auth handshake, api replies, and pushed event-json messages.
type fakeESLServer struct {
	ln       net.Listener
	gotAuths chan string
	gotCmds  chan string
}

func startFakeESL(t *testing.T) *fakeESLServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeESLServer{
		ln:       ln,
		gotAuths: make(chan string, 1),
		gotCmds:  make(chan string, 8),
	}
	t.Cleanup(func() { ln.Close() })
	go s.serve()
	return s
}

func (s *fakeESLServer) addr() string { return s.ln.Addr().String() }

func (s *fakeESLServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *fakeESLServer) handle(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	fmt.Fprint(conn, "auth/request\n\n")

	for {
		msg, err := readESLMessage(r)
		if err != nil {
			return
		}
		cmd := msg.headers["Content-Type"] // bare command line lands here
		switch {
		case strings.HasPrefix(cmd, "auth "):
			s.gotAuths <- strings.TrimPrefix(cmd, "auth ")
			fmt.Fprint(conn, "Content-Type: command/reply\nReply-Text: +OK accepted\n\n")
		case strings.HasPrefix(cmd, "event json"):
			fmt.Fprint(conn, "Content-Type: command/reply\nReply-Text: +OK event listener enabled json\n\n")
		case strings.HasPrefix(cmd, "api "):
			c := strings.TrimPrefix(cmd, "api ")
			s.gotCmds <- c
			body := "+OK " + c
			fmt.Fprintf(conn, "Content-Type: api/response\nContent-Length: %d\n\n%s", len(body), body)
		}
	}
}

// pushEvent sends a text/event-json message to all connected clients.
func (s *fakeESLServer) pushEvent(conn net.Conn, jsonBody string) {
	fmt.Fprintf(conn, "Content-Type: text/event-json\nContent-Length: %d\n\n%s", len(jsonBody), jsonBody)
}

func TestESLClientAuthAndAPI(t *testing.T) {
	srv := startFakeESL(t)

	client := NewESLClient(srv.addr(), "ClueCon", []string{"CHANNEL_ANSWER"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer client.Close()
	go client.Run(ctx)

	// Wait for auth to happen.
	select {
	case pass := <-srv.gotAuths:
		if pass != "ClueCon" {
			t.Fatalf("auth password = %q", pass)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no auth received")
	}

	// Wait for the client to be connected and subscribed.
	deadline := time.Now().Add(3 * time.Second)
	for !client.Ready() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !client.Ready() {
		t.Fatal("client not ready")
	}

	reply, err := client.API(context.Background(), "status")
	if err != nil {
		t.Fatalf("API: %v", err)
	}
	if reply != "+OK status" {
		t.Fatalf("reply = %q", reply)
	}

	select {
	case cmd := <-srv.gotCmds:
		if cmd != "status" {
			t.Fatalf("server got cmd %q", cmd)
		}
	case <-time.After(time.Second):
		t.Fatal("server saw no api command")
	}
}

func TestESLClientReconnects(t *testing.T) {
	srv := startFakeESL(t)

	client := NewESLClient(srv.addr(), "ClueCon", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer client.Close()
	go client.Run(ctx)

	select {
	case <-srv.gotAuths:
	case <-time.After(3 * time.Second):
		t.Fatal("no auth received")
	}

	// Kill the connection; the client should reconnect and re-auth.
	client.connMu.Lock()
	if client.conn != nil {
		client.conn.Close()
	}
	client.connMu.Unlock()

	select {
	case <-srv.gotAuths:
		// re-authed: reconnect works
	case <-time.After(5 * time.Second):
		t.Fatal("client did not reconnect")
	}
}

func TestESLClientAPIErrorsWhenDisconnected(t *testing.T) {
	client := NewESLClient("127.0.0.1:1", "x", nil) // nothing listening
	defer client.Close()
	_, err := client.API(context.Background(), "status")
	if err == nil {
		t.Fatal("expected error when not connected")
	}
}
