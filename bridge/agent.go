package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// Agent protocol (fsbridge -> agent app, one WebSocket per call).
//
//	fsbridge connects to the agent URL with query params:
//	  ?uuid=<fs-call-uuid>&session=<id>&rate=16000&mix=mono
//	fsbridge -> agent:
//	  {"type":"start","uuid":...,"session":...,"rate":16000,"mix":"mono",
//	   "caller":...,"destination":...,"direction":...}  (call context via ESL uuid_dump, when available)
//	  {"type":"metadata","data":<call metadata JSON>}   (if the module sent any)
//	  binary frames: uplink L16 PCM from the caller
//	  {"type":"stop"}                                   (call/stream ended)
//	agent -> fsbridge:
//	  binary frames: downlink L16 PCM to play to the caller
//	  {"type":"clear"}                                  (flush caller playback; barge-in)
//	  {"type":"hangup"}                                 (end the call; requires ESL)
//	  {"type":"event","name":...,"data":{...}}          (republished to the event bus)
const (
	AgentMsgStart    = "start"
	AgentMsgMetadata = "metadata"
	AgentMsgStop     = "stop"
	AgentMsgClear    = "clear"
	AgentMsgHangup   = "hangup"
	AgentMsgEvent    = "event"
)

// AgentMessage is a JSON text frame on the agent connection.
type AgentMessage struct {
	Type    string          `json:"type"`
	UUID    string          `json:"uuid,omitempty"`
	Session string          `json:"session,omitempty"`
	Rate    int             `json:"rate,omitempty"`
	Mix     string          `json:"mix,omitempty"`
	Name    string          `json:"name,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`

	// Call context, populated on the "start" frame when ESL is available.
	Caller      string `json:"caller,omitempty"`
	Destination string `json:"destination,omitempty"`
	Direction   string `json:"direction,omitempty"`
}

// CallInfo describes the FreeSWITCH side of a call. It is resolved via ESL
// uuid_dump when the agent socket opens; fields are empty when ESL is not
// configured or the lookup fails.
type CallInfo struct {
	UUID        string
	Caller      string // Caller-Caller-ID-Number
	Destination string // Caller-Destination-Number
	Direction   string // Call-Direction ("inbound" | "outbound")
}

// AgentForwarder is a bridge.Handler that forwards each call's audio to an
// external agent application over a per-call WebSocket (the "media streams"
// pattern). The agent app owns the AI logic; fsbridge owns telephony.
type AgentForwarder struct {
	// URL of the agent app's WebSocket endpoint, e.g. "ws://agent:9000/call".
	// Used when Route is nil or returns "".
	URL string
	// Route optionally resolves the agent URL per call (e.g. by DID or
	// caller). Return "" to fall back to URL.
	Route func(CallInfo) string
	// ESL enables agent-issued call control (hangup) and call context lookup
	// (uuid_dump) for the start frame. Optional.
	ESL ESLAPI
	// Bus receives agent-published events. Optional.
	Bus *EventBus
	// Logger for lifecycle logs. Default slog.Default().
	Logger *slog.Logger
	// DialTimeout for connecting to the agent. Default 5s.
	DialTimeout time.Duration

	mu sync.Map // *Session -> *agentConn
}

type agentConn struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func (a *AgentForwarder) logger() *slog.Logger {
	if a.Logger != nil {
		return a.Logger
	}
	return slog.Default()
}

func (ac *agentConn) writeBinary(pcm []byte) error {
	ac.writeMu.Lock()
	defer ac.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ac.conn.Write(ctx, websocket.MessageBinary, pcm)
}

func (ac *agentConn) writeJSON(msg AgentMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	ac.writeMu.Lock()
	defer ac.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ac.conn.Write(ctx, websocket.MessageText, data)
}

// callInfo resolves the FreeSWITCH call context via ESL uuid_dump. The
// lookup is bounded to 2s so a slow ESL never stalls media setup; any
// failure just yields an info with only UUID set.
func (a *AgentForwarder) callInfo(log *slog.Logger, s *Session) CallInfo {
	ci := CallInfo{UUID: s.FSUUID()}
	if a.ESL == nil || s.FSUUID() == "" {
		return ci
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	dump, err := a.ESL.API(ctx, "uuid_dump "+s.FSUUID())
	cancel()
	if err != nil {
		log.Warn("uuid_dump failed; start frame lacks call context", "err", err)
		return ci
	}
	h := parseKeyValueLines(dump)
	ci.Caller = h["Caller-Caller-ID-Number"]
	ci.Destination = h["Caller-Destination-Number"]
	ci.Direction = h["Call-Direction"]
	return ci
}

// parseKeyValueLines parses ESL-style "Key: value" lines (uuid_dump output).
func parseKeyValueLines(s string) map[string]string {
	out := make(map[string]string)
	for _, line := range strings.Split(s, "\n") {
		if k, v, ok := strings.Cut(line, ": "); ok {
			out[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return out
}

// OnStart dials the agent app and starts forwarding.
func (a *AgentForwarder) OnStart(s *Session) {
	log := a.logger().With("id", s.ID(), "fs_uuid", s.FSUUID())

	ci := a.callInfo(log, s)

	agentURL := a.URL
	if a.Route != nil {
		if r := a.Route(ci); r != "" {
			agentURL = r
		}
	}

	u, err := url.Parse(agentURL)
	if err != nil {
		log.Error("bad agent URL", "err", err)
		s.Close()
		return
	}
	q := u.Query()
	q.Set("session", s.ID())
	q.Set("rate", fmt.Sprint(s.SampleRate()))
	q.Set("mix", s.MixType())
	if s.FSUUID() != "" {
		q.Set("uuid", s.FSUUID())
	}
	u.RawQuery = q.Encode()

	timeout := a.DialTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	conn, _, err := websocket.Dial(ctx, u.String(), nil)
	cancel()
	if err != nil {
		log.Error("agent dial failed", "url", u.String(), "err", err)
		s.Close()
		return
	}

	ac := &agentConn{conn: conn}
	a.mu.Store(s, ac)

	if err := ac.writeJSON(AgentMessage{
		Type: AgentMsgStart, UUID: s.FSUUID(), Session: s.ID(),
		Rate: s.SampleRate(), Mix: s.MixType(),
		Caller: ci.Caller, Destination: ci.Destination, Direction: ci.Direction,
	}); err != nil {
		log.Error("agent start frame failed", "err", err)
		a.mu.Delete(s)
		conn.Close(websocket.StatusInternalError, "")
		s.Close()
		return
	}

	log.Info("agent connected", "url", u.String())
	go a.readFromAgent(s, ac)
}

// OnAudio forwards an uplink PCM chunk to the agent.
func (a *AgentForwarder) OnAudio(s *Session, pcm []byte) {
	v, ok := a.mu.Load(s)
	if !ok {
		return
	}
	if err := v.(*agentConn).writeBinary(pcm); err != nil {
		a.logger().Warn("agent write failed, ending session", "id", s.ID(), "err", err)
		s.Close()
	}
}

// OnText forwards module metadata/control text frames to the agent.
func (a *AgentForwarder) OnText(s *Session, data []byte) {
	v, ok := a.mu.Load(s)
	if !ok {
		return
	}
	_ = v.(*agentConn).writeJSON(AgentMessage{
		Type: AgentMsgMetadata, UUID: s.FSUUID(), Session: s.ID(), Data: data,
	})
}

// OnEnd notifies the agent and closes its connection.
func (a *AgentForwarder) OnEnd(s *Session, err error) {
	v, ok := a.mu.LoadAndDelete(s)
	if !ok {
		return
	}
	ac := v.(*agentConn)
	_ = ac.writeJSON(AgentMessage{Type: AgentMsgStop, UUID: s.FSUUID(), Session: s.ID()})
	ac.conn.Close(websocket.StatusNormalClosure, "call ended")
}

// readFromAgent consumes agent -> fsbridge frames for the call's lifetime.
func (a *AgentForwarder) readFromAgent(s *Session, ac *agentConn) {
	log := a.logger().With("id", s.ID(), "fs_uuid", s.FSUUID())
	for {
		mt, data, err := ac.conn.Read(context.Background())
		if err != nil {
			s.Close()
			return
		}
		if mt == websocket.MessageBinary {
			_ = s.SendAudio(data)
			continue
		}

		var msg AgentMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Warn("bad agent message", "err", err)
			continue
		}
		switch msg.Type {
		case AgentMsgClear:
			_ = s.ClearPlayback()
		case AgentMsgHangup:
			if a.ESL == nil || s.FSUUID() == "" {
				log.Warn("agent hangup ignored (no ESL or unknown call uuid)")
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := a.ESL.API(ctx, "uuid_kill "+s.FSUUID())
			cancel()
			if err != nil {
				log.Warn("agent hangup failed", "err", err)
			}
		case AgentMsgEvent:
			if a.Bus != nil {
				ev := Event{Name: msg.Name, UUID: s.FSUUID()}
				if len(msg.Data) > 0 {
					var m map[string]any
					if json.Unmarshal(msg.Data, &m) == nil {
						ev.Data = m
					}
				}
				a.Bus.Publish(ev)
			}
		default:
			log.Warn("unknown agent message type", "type", msg.Type)
		}
	}
}
