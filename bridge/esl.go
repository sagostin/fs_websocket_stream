package bridge

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ESLEvent is a parsed FreeSWITCH event (from `event json`).
type ESLEvent struct {
	Name    string            // e.g. CHANNEL_ANSWER
	Headers map[string]string // full header set (Unique-ID, Caller-Caller-ID-Number, ...)
}

// UUID returns the channel Unique-ID header, if present.
func (e ESLEvent) UUID() string { return e.Headers["Unique-ID"] }

// ESLAPI is the subset of the ESL client used by the control server.
type ESLAPI interface {
	// API executes a FreeSWITCH API command (e.g. "uuid_kill <uuid>") and
	// returns the reply text (e.g. "+OK ...").
	API(ctx context.Context, cmd string) (string, error)
	// Events streams subscribed channel events for the life of the client.
	Events() <-chan ESLEvent
}

// eslMessage is one framed ESL protocol message.
type eslMessage struct {
	headers map[string]string
	body    []byte
}

// ESLClient is a minimal inbound Event Socket Library client with automatic
// reconnection. One instance serves the whole bridge process.
type ESLClient struct {
	addr     string
	password string
	events   []string

	conn   net.Conn
	connMu sync.RWMutex
	ready  atomic.Bool // handshake + subscription complete

	apiMu        sync.Mutex // serializes API request/reply
	pendingReply chan eslMessage

	eventsCh chan ESLEvent
	done     chan struct{}
}

// NewESLClient creates a client for the given FreeSWITCH event socket.
// eventNames are subscribed after every (re)connect, e.g.
// []string{"CHANNEL_CREATE", "CHANNEL_ANSWER", "CHANNEL_HANGUP_COMPLETE"}.
func NewESLClient(addr, password string, eventNames []string) *ESLClient {
	return &ESLClient{
		addr:         addr,
		password:     password,
		events:       eventNames,
		pendingReply: make(chan eslMessage, 1),
		eventsCh:     make(chan ESLEvent, 256),
		done:         make(chan struct{}),
	}
}

// Run connects and keeps the connection alive until ctx is cancelled,
// reconnecting with jittered exponential backoff on failure.
func (c *ESLClient) Run(ctx context.Context) {
	const maxBackoff = 30 * time.Second
	backoff := 2 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		err := c.connectAndServe(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			fmt.Printf("esl disconnected: %v\n", err) // stderr-equivalent minimal
		}
		// Jittered backoff so reconnects don't thunder against the FS.
		jitter := time.Duration(rand.Int63n(int64(backoff)))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff + jitter):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// Close stops the client.
func (c *ESLClient) Close() {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.connMu.Unlock()
}

func (c *ESLClient) connectAndServe(ctx context.Context) error {
	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return fmt.Errorf("esl dial: %w", err)
	}

	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
	defer func() {
		c.connMu.Lock()
		if c.conn == conn {
			c.conn = nil
		}
		c.connMu.Unlock()
		conn.Close()
	}()

	r := bufio.NewReader(conn)

	// Greeting: "auth/request"
	greet, err := readESLMessage(r)
	if err != nil {
		return fmt.Errorf("esl greeting: %w", err)
	}
	if !strings.Contains(greet.headers["Content-Type"], "auth/request") && len(greet.headers) > 0 {
		return fmt.Errorf("esl: unexpected greeting: %v", greet.headers)
	}

	if err := writeESLCommand(conn, "auth "+c.password); err != nil {
		return err
	}
	reply, err := readESLMessage(r)
	if err != nil {
		return fmt.Errorf("esl auth reply: %w", err)
	}
	if !strings.Contains(reply.headers["Reply-Text"], "+OK") {
		return fmt.Errorf("esl auth failed: %s", reply.headers["Reply-Text"])
	}

	// Subscribe to events in JSON form.
	if len(c.events) > 0 {
		if err := writeESLCommand(conn, "event json "+strings.Join(c.events, " ")); err != nil {
			return err
		}
		if _, err := readESLMessage(r); err != nil { // subscribe ack
			return fmt.Errorf("esl subscribe reply: %w", err)
		}
	}

	c.ready.Store(true)
	defer c.ready.Store(false)

	// Read loop until failure.
	for {
		msg, err := readESLMessage(r)
		if err != nil {
			return err
		}
		switch msg.headers["Content-Type"] {
		case "command/reply", "api/response":
			select {
			case c.pendingReply <- msg:
			default: // nobody waiting; drop
			}
		case "text/event-json":
			var headers map[string]string
			if err := json.Unmarshal(msg.body, &headers); err != nil {
				continue
			}
			ev := ESLEvent{Name: headers["Event-Name"], Headers: headers}
			select {
			case c.eventsCh <- ev:
			default:
			}
		case "text/disconnect-notice":
			return errors.New("esl: server disconnect notice")
		}
	}
}

// API executes a FreeSWITCH API command and returns its reply text.
func (c *ESLClient) API(ctx context.Context, cmd string) (string, error) {
	c.apiMu.Lock()
	defer c.apiMu.Unlock()

	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()
	if conn == nil || !c.ready.Load() {
		return "", errors.New("esl: not connected")
	}

	// Drain any stale reply.
	select {
	case <-c.pendingReply:
	default:
	}

	if err := writeESLCommand(conn, "api "+cmd); err != nil {
		return "", err
	}

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-c.done:
			return "", errors.New("esl: client closed")
		case <-time.After(15 * time.Second):
			return "", errors.New("esl: api reply timeout")
		case msg := <-c.pendingReply:
			ct := msg.headers["Content-Type"]
			if ct == "api/response" {
				return strings.TrimSpace(string(msg.body)), nil
			}
			// command/reply
			replyText := msg.headers["Reply-Text"]
			if strings.HasPrefix(replyText, "-ERR") {
				return "", errors.New(strings.TrimSpace(replyText))
			}
			if len(msg.body) > 0 {
				return strings.TrimSpace(string(msg.body)), nil
			}
			return replyText, nil
		}
	}
}

// Events streams subscribed channel events.
func (c *ESLClient) Events() <-chan ESLEvent { return c.eventsCh }

// Ready reports whether the client is connected and subscribed.
func (c *ESLClient) Ready() bool { return c.ready.Load() }

// readESLMessage reads one ESL message: header lines until a blank line,
// then an optional body per Content-Length.
func readESLMessage(r *bufio.Reader) (eslMessage, error) {
	msg := eslMessage{headers: map[string]string{}}
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return msg, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if k, v, ok := strings.Cut(line, ": "); ok {
			msg.headers[k] = v
		} else {
			// Bare command lines like "auth/request".
			msg.headers["Content-Type"] = line
		}
	}
	if cl, ok := msg.headers["Content-Length"]; ok {
		n, err := strconv.Atoi(strings.TrimSpace(cl))
		if err != nil {
			return msg, fmt.Errorf("esl: bad content-length %q", cl)
		}
		msg.body = make([]byte, n)
		if _, err := readFull(r, msg.body); err != nil {
			return msg, err
		}
	}
	return msg, nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func writeESLCommand(conn net.Conn, cmd string) error {
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := conn.Write([]byte(cmd + "\n\n"))
	return err
}
