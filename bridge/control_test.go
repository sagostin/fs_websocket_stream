package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// fakeESL implements ESLAPI for control tests.
type fakeESL struct {
	lastCmd string
	reply   string
	err     error
	events  chan ESLEvent
}

func (f *fakeESL) API(ctx context.Context, cmd string) (string, error) {
	f.lastCmd = cmd
	return f.reply, f.err
}

func (f *fakeESL) Events() <-chan ESLEvent { return f.events }

func dialControl(t *testing.T, cs *ControlServer) *websocket.Conn {
	t.Helper()
	httpSrv := httptest.NewServer(cs)
	t.Cleanup(httpSrv.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	conn, _, err := websocket.Dial(ctx, "ws"+httpSrv.URL[4:], nil)
	if err != nil {
		t.Fatalf("dial control: %v", err)
	}
	t.Cleanup(func() { conn.Close(websocket.StatusNormalClosure, "") })
	return conn
}

func sendCommand(t *testing.T, conn *websocket.Conn, cmd ControlCommand) ControlReply {
	t.Helper()
	data, _ := json.Marshal(cmd)
	if err := conn.Write(context.Background(), websocket.MessageText, data); err != nil {
		t.Fatalf("write command: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, raw, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	var reply ControlReply
	if err := json.Unmarshal(raw, &reply); err != nil {
		t.Fatalf("reply JSON: %v", err)
	}
	return reply
}

func TestControlCommands(t *testing.T) {
	esl := &fakeESL{reply: "+OK", events: make(chan ESLEvent)}
	cs := &ControlServer{ESL: esl}
	conn := dialControl(t, cs)

	cases := []struct {
		cmd  ControlCommand
		want string
	}{
		{ControlCommand{ID: "1", Cmd: "hangup", Args: json.RawMessage(`{"uuid":"abc"}`)}, "uuid_kill abc"},
		{ControlCommand{ID: "2", Cmd: "transfer", Args: json.RawMessage(`{"uuid":"abc","dest":"1002"}`)}, "uuid_transfer abc 1002"},
		{ControlCommand{ID: "3", Cmd: "hold", Args: json.RawMessage(`{"uuid":"abc"}`)}, "uuid_hold abc"},
		{ControlCommand{ID: "4", Cmd: "unhold", Args: json.RawMessage(`{"uuid":"abc"}`)}, "uuid_hold off abc"},
		{ControlCommand{ID: "5", Cmd: "dtmf", Args: json.RawMessage(`{"uuid":"abc","digits":"123"}`)}, "uuid_send_dtmf abc 123"},
		{ControlCommand{ID: "6", Cmd: "originate", Args: json.RawMessage(`{"dest":"sofia/gateway/twilio/18005551212","ext":"9999","cid":"15551234567"}`)},
			"originate {origination_caller_id_number=15551234567}sofia/gateway/twilio/18005551212 9999 XML default"},
		{ControlCommand{ID: "7", Cmd: "originate", Args: json.RawMessage(`{"dest":"loopback/9999","app":"park"}`)},
			"originate loopback/9999 &park"},
		{ControlCommand{ID: "8", Cmd: "originate", Args: json.RawMessage(
			`{"dest":"sofia/gateway/twilio/18005551212","cid":"15551234567","vars":{"account":"acme","tries":"1"},"metadata":"{\"customer_id\":\"42\"}"}`)},
			`originate {origination_caller_id_number=15551234567,account=acme,tries=1,ws_bridge_metadata={"customer_id":"42"}}sofia/gateway/twilio/18005551212 9999 XML default`},
	}
	for _, tc := range cases {
		reply := sendCommand(t, conn, tc.cmd)
		if !reply.OK {
			t.Fatalf("%s: reply not ok: %s", tc.cmd.Cmd, reply.Error)
		}
		if reply.ID != tc.cmd.ID {
			t.Fatalf("%s: reply id = %q, want %q", tc.cmd.Cmd, reply.ID, tc.cmd.ID)
		}
		if esl.lastCmd != tc.want {
			t.Errorf("%s: ESL cmd = %q, want %q", tc.cmd.Cmd, esl.lastCmd, tc.want)
		}
	}
}

func TestControlOriginateUUIDReply(t *testing.T) {
	esl := &fakeESL{reply: "+OK 7c1c9f2e-dead-beef-0000-1234567890ab", events: make(chan ESLEvent)}
	cs := &ControlServer{ESL: esl}
	conn := dialControl(t, cs)

	reply := sendCommand(t, conn, ControlCommand{
		ID: "o1", Cmd: "originate", Args: json.RawMessage(`{"dest":"loopback/9999"}`),
	})
	if !reply.OK {
		t.Fatalf("originate failed: %s", reply.Error)
	}
	if reply.UUID != "7c1c9f2e-dead-beef-0000-1234567890ab" {
		t.Errorf("reply uuid = %q", reply.UUID)
	}

	// Non-originate replies carry no uuid.
	reply = sendCommand(t, conn, ControlCommand{
		ID: "h1", Cmd: "hangup", Args: json.RawMessage(`{"uuid":"abc"}`),
	})
	if !reply.OK || reply.UUID != "" {
		t.Errorf("hangup reply = %+v", reply)
	}
}

func TestParseOriginateUUID(t *testing.T) {
	if got := parseOriginateUUID("+OK abc-123\n"); got != "abc-123" {
		t.Errorf("parseOriginateUUID ok = %q", got)
	}
	if got := parseOriginateUUID("-ERR NO_ANSWER"); got != "" {
		t.Errorf("parseOriginateUUID err = %q", got)
	}
}

func TestControlCommandErrors(t *testing.T) {
	esl := &fakeESL{err: errors.New("-ERR no reply"), events: make(chan ESLEvent)}
	cs := &ControlServer{ESL: esl}
	conn := dialControl(t, cs)

	// Unknown command.
	reply := sendCommand(t, conn, ControlCommand{ID: "x", Cmd: "bogus"})
	if reply.OK {
		t.Error("bogus command unexpectedly ok")
	}

	// Missing args.
	reply = sendCommand(t, conn, ControlCommand{ID: "y", Cmd: "hangup", Args: json.RawMessage(`{}`)})
	if reply.OK {
		t.Error("hangup without uuid unexpectedly ok")
	}

	// ESL failure surfaces.
	reply = sendCommand(t, conn, ControlCommand{ID: "z", Cmd: "hangup", Args: json.RawMessage(`{"uuid":"abc"}`)})
	if reply.OK || reply.Error == "" {
		t.Errorf("ESL error not surfaced: %+v", reply)
	}
}

func TestControlEventStream(t *testing.T) {
	bus := NewEventBus()
	cs := &ControlServer{Bus: bus}
	conn := dialControl(t, cs)

	// Republish until the server-side subscription is live, then stop.
	// (coder/websocket read errors are permanent, so the client must use a
	// single read with a generous deadline rather than short retry timeouts.)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			bus.Publish(Event{Name: "call.answer", UUID: "abc-123", Data: map[string]any{"caller": "1000"}})
			time.Sleep(50 * time.Millisecond)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, data, err := conn.Read(ctx)
	cancel()
	if err != nil {
		t.Fatalf("no event received: %v", err)
	}
	var ev Event
	if err := json.Unmarshal(data, &ev); err != nil {
		t.Fatalf("event JSON: %v", err)
	}
	if ev.Name != "call.answer" || ev.UUID != "abc-123" {
		t.Errorf("event = %+v", ev)
	}
	if ev.Data["caller"] != "1000" {
		t.Errorf("event data = %+v", ev.Data)
	}
}

func TestESLToCallEventMapping(t *testing.T) {
	ev := ESLEvent{Name: "CHANNEL_ANSWER", Headers: map[string]string{
		"Unique-ID":                 "u1",
		"Caller-Caller-ID-Number":   "1000",
		"Caller-Destination-Number": "9999",
	}}
	e := eslToCallEvent(ev)
	if e == nil || e.Name != "call.answer" || e.UUID != "u1" {
		t.Fatalf("mapping = %+v", e)
	}
	if e.Data["caller"] != "1000" || e.Data["destination"] != "9999" {
		t.Errorf("data = %+v", e.Data)
	}

	if eslToCallEvent(ESLEvent{Name: "SOME_OTHER_EVENT"}) != nil {
		t.Error("unmapped event should return nil")
	}
}
