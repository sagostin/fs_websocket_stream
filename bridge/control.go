package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// ControlCommand is a JSON message from the application to the control
// endpoint.
type ControlCommand struct {
	ID   string          `json:"id"`
	Cmd  string          `json:"cmd"`
	Args json.RawMessage `json:"args"`
}

// ControlReply answers a ControlCommand.
type ControlReply struct {
	ID     string `json:"id"`
	OK     bool   `json:"ok"`
	Result string `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

// ControlServer exposes call control and the event stream to the external
// application over a single WebSocket.
//
// Commands (args are command-specific):
//
//	{"id":"1","cmd":"originate","args":{"dest":"sofia/gateway/x/18005551212","ext":"9999"}}
//	{"id":"2","cmd":"hangup","args":{"uuid":"..."}}
//	{"id":"3","cmd":"transfer","args":{"uuid":"...","dest":"1002"}}
//	{"id":"4","cmd":"hold","args":{"uuid":"..."}}   / "unhold"
//	{"id":"5","cmd":"dtmf","args":{"uuid":"...","digits":"1234"}}
//	{"id":"6","cmd":"clear_playback","args":{"uuid":"..."}}
//
// Every command gets a ControlReply with the same id. All bus events (call
// state from ESL, audio.start/end, transcripts) are pushed as bridge.Event
// JSON messages.
type ControlServer struct {
	ESL   ESLAPI
	Bus   *EventBus
	Audio *Server // audio-plane server, for session lookup
	Log   *slog.Logger
}

func (cs *ControlServer) logger() *slog.Logger {
	if cs.Log != nil {
		return cs.Log
	}
	return slog.Default()
}

// ServeHTTP upgrades the connection and runs the control session.
func (cs *ControlServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		cs.logger().Warn("control accept failed", "err", err)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	cs.logger().Info("control client connected", "remote", r.RemoteAddr)

	// Forward bus events until the client goes away.
	if cs.Bus != nil {
		events, unsub := cs.Bus.Subscribe(256)
		defer unsub()
		go func() {
			for ev := range events {
				data, err := json.Marshal(ev)
				if err != nil {
					continue
				}
				wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
				err = conn.Write(wctx, websocket.MessageText, data)
				wcancel()
				if err != nil {
					return
				}
			}
		}()
	}

	// Also forward raw ESL channel events as call.* bus-independent events
	// when no bus is configured.
	if cs.Bus == nil && cs.ESL != nil {
		go func() {
			for ev := range cs.ESL.Events() {
				e := eslToCallEvent(ev)
				if e == nil {
					continue
				}
				data, _ := json.Marshal(e)
				wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
				_ = conn.Write(wctx, websocket.MessageText, data)
				wcancel()
			}
		}()
	}

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var cmd ControlCommand
		if err := json.Unmarshal(data, &cmd); err != nil {
			cs.writeReply(ctx, conn, ControlReply{OK: false, Error: "bad command JSON: " + err.Error()})
			continue
		}
		reply := cs.execute(ctx, cmd)
		cs.writeReply(ctx, conn, reply)
	}
}

func (cs *ControlServer) writeReply(ctx context.Context, conn *websocket.Conn, reply ControlReply) {
	data, _ := json.Marshal(reply)
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = conn.Write(wctx, websocket.MessageText, data)
}

// execute maps a control command to ESL (or local audio) operations.
func (cs *ControlServer) execute(ctx context.Context, cmd ControlCommand) ControlReply {
	reply := ControlReply{ID: cmd.ID}

	fail := func(err error) ControlReply {
		reply.OK = false
		reply.Error = err.Error()
		return reply
	}

	var eslCmd string

	switch cmd.Cmd {
	case "originate":
		var args struct {
			Dest string `json:"dest"` // e.g. "sofia/gateway/trunk/18005551212" or "loopback/9999"
			Ext  string `json:"ext"`  // dialplan extension the called leg lands on; default 9999
			App  string `json:"app"`  // optional app instead of dialplan (e.g. "park")
			CID  string `json:"cid"`  // optional caller ID number
		}
		if err := json.Unmarshal(cmd.Args, &args); err != nil || args.Dest == "" {
			return fail(fmt.Errorf("originate requires args.dest"))
		}
		vars := ""
		if args.CID != "" {
			vars = "{origination_caller_id_number=" + args.CID + "}"
		}
		if args.App != "" {
			eslCmd = fmt.Sprintf("originate %s%s &%s", vars, args.Dest, strings.TrimPrefix(args.App, "&"))
		} else {
			ext := args.Ext
			if ext == "" {
				ext = "9999"
			}
			eslCmd = fmt.Sprintf("originate %s%s %s XML default", vars, args.Dest, ext)
		}

	case "hangup":
		uuid, err := uuidArg(cmd.Args)
		if err != nil {
			return fail(err)
		}
		eslCmd = "uuid_kill " + uuid

	case "transfer":
		var args struct {
			UUID string `json:"uuid"`
			Dest string `json:"dest"`
		}
		if err := json.Unmarshal(cmd.Args, &args); err != nil || args.UUID == "" || args.Dest == "" {
			return fail(fmt.Errorf("transfer requires args.uuid and args.dest"))
		}
		eslCmd = fmt.Sprintf("uuid_transfer %s %s", args.UUID, args.Dest)

	case "hold", "unhold":
		uuid, err := uuidArg(cmd.Args)
		if err != nil {
			return fail(err)
		}
		if cmd.Cmd == "hold" {
			eslCmd = "uuid_hold " + uuid
		} else {
			eslCmd = "uuid_hold off " + uuid
		}

	case "dtmf":
		var args struct {
			UUID   string `json:"uuid"`
			Digits string `json:"digits"`
		}
		if err := json.Unmarshal(cmd.Args, &args); err != nil || args.UUID == "" || args.Digits == "" {
			return fail(fmt.Errorf("dtmf requires args.uuid and args.digits"))
		}
		eslCmd = fmt.Sprintf("uuid_send_dtmf %s %s", args.UUID, args.Digits)

	case "clear_playback":
		uuid, err := uuidArg(cmd.Args)
		if err != nil {
			return fail(err)
		}
		if cs.Audio == nil {
			return fail(fmt.Errorf("no audio server configured"))
		}
		s := cs.Audio.SessionByFSUUID(uuid)
		if s == nil {
			return fail(fmt.Errorf("no active audio session for call %s", uuid))
		}
		if err := s.ClearPlayback(); err != nil {
			return fail(err)
		}
		reply.OK = true
		reply.Result = "playback cleared"
		return reply

	default:
		return fail(fmt.Errorf("unknown command %q", cmd.Cmd))
	}

	if cs.ESL == nil {
		return fail(fmt.Errorf("ESL not configured"))
	}
	result, err := cs.ESL.API(ctx, eslCmd)
	if err != nil {
		return fail(err)
	}
	reply.OK = true
	reply.Result = result
	return reply
}

func uuidArg(args json.RawMessage) (string, error) {
	var a struct {
		UUID string `json:"uuid"`
	}
	if err := json.Unmarshal(args, &a); err != nil || a.UUID == "" {
		return "", fmt.Errorf("command requires args.uuid")
	}
	return a.UUID, nil
}

// eslToCallEvent maps a raw ESL channel event to a bus Event. Returns nil
// for event types we don't surface.
func eslToCallEvent(ev ESLEvent) *Event {
	var name string
	switch ev.Name {
	case "CHANNEL_CREATE":
		name = "call.create"
	case "CHANNEL_ANSWER":
		name = "call.answer"
	case "CHANNEL_HANGUP", "CHANNEL_HANGUP_COMPLETE":
		name = "call.hangup"
	case "CHANNEL_DESTROY":
		name = "call.destroy"
	case "CHANNEL_BRIDGE":
		name = "call.bridge"
	case "CHANNEL_UNBRIDGE":
		name = "call.unbridge"
	default:
		return nil
	}
	data := map[string]any{
		"caller":      ev.Headers["Caller-Caller-ID-Number"],
		"destination": ev.Headers["Caller-Destination-Number"],
		"direction":   ev.Headers["Call-Direction"],
	}
	if cause := ev.Headers["Hangup-Cause"]; cause != "" {
		data["hangup_cause"] = cause
	}
	return &Event{Name: name, UUID: ev.UUID(), Data: data}
}

// ESLToBus forwards ESL channel events onto the bus as call.* events.
// Run for the life of the ESL client.
func ESLToBus(ctx context.Context, esl ESLAPI, bus *EventBus) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-esl.Events():
			if !ok {
				return
			}
			if e := eslToCallEvent(ev); e != nil {
				bus.Publish(*e)
			}
		}
	}
}
