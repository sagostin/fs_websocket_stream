# Control plane: `/control` WebSocket

The `/control` WebSocket is the bridge's call-control and event-stream
endpoint for external applications. With `-esl-addr` set, the bridge
exposes it at the configured path (default `/control`). One JSON
message per frame in each direction.

Use it to:

- Drive calls from outside the bridge (originate, hangup, transfer).
- Subscribe to call lifecycle and pipeline events for monitoring or
  evaluation.
- Implement "outbound dialer" services that the AI agent triggers via
  webhook.

For the high-level overview see the top-level
[Control plane section](../README.md#control-plane-control). For the
implementation see [`bridge/control.go`](../bridge/control.go) and
[`bridge/esl.go`](../bridge/esl.go).

## Enable

```sh
fsbridge -addr :8090 -mode agent \
    -esl-addr localhost:8021 -esl-password ClueCon \
    -control-path /control
```

The control plane requires ESL — commands are translated into FreeSWITCH
API calls and subscribed events flow from ESL channel events.

Optional:

- `-auth-token <secret>` — wrap `/control` (and `/recordings`) with bearer
  auth. See [`logging.md`](./logging.md) and
  [`bridge/ops.go`](../bridge/ops.go) for the middleware.

## Connecting

Any WebSocket client works. The simplest is [`cmd/fsctl`](../cmd/fsctl/):

```sh
go run ./cmd/fsctl events
```

In code:

```go
conn, _, err := websocket.Dial(ctx, "ws://localhost:8090/control", nil)
if err != nil { /* ... */ }
defer conn.Close(websocket.StatusNormalClosure, "")
```

You immediately receive events as the bridge publishes them. Send
commands as JSON text frames; replies come back as JSON text frames with
the matching `id`.

## Commands

All commands have the same envelope:

```jsonc
{"id":"<your-correlation-id>","cmd":"<name>","args":{...}}
```

You get exactly one reply per command with the same `id`. Events flow
independently on the same socket.

### `originate`

Place a call. `dest` is any FreeSWITCH dial string; the called leg
lands on `ext` (default `9999`, i.e. the AI bridge) in context
`default`. Or run an app instead of hunting the dialplan via `app`.

```jsonc
// dial out via SIP gateway
{"id":"1","cmd":"originate","args":{"dest":"sofia/gateway/trunk/18005551212","ext":"9999","cid":"15551234567"}}

// run an app instead (park, voicemail, conference, ...)
{"id":"2","cmd":"originate","args":{"dest":"loopback/9999","app":"park"}}
```

Args:

| Field | Required | Meaning |
|---|---|---|
| `dest` | yes | Dial string (`sofia/...`, `loopback/...`, etc.) |
| `ext` | when no `app` | Dialplan extension. Defaults to `9999`. |
| `app` | no | Run an application on answer instead of the dialplan (e.g. `park`, `voicemail`). |
| `cid` | no | Outbound caller ID number (sets `origination_caller_id_number`). |

The bridge returns the FreeSWITCH API reply as `result` (e.g.
`+OK <uuid>...` for a successful originate). The bridge does **not**
wait for the new call to answer; if you need to know that, watch the
`call.answer` event.

### `hangup`

```jsonc
{"id":"2","cmd":"hangup","args":{"uuid":"<fs-call-uuid>"}}
```

ESL command: `uuid_kill <uuid>`. The call ends and you receive
`call.hangup` then `call.destroy`.

### `transfer`

```jsonc
{"id":"3","cmd":"transfer","args":{"uuid":"<fs-call-uuid>","dest":"1002"}}
```

ESL command: `uuid_transfer <uuid> <dest>`. **Caveat:** transferring an
AI call to a non-media-producing endpoint (a SIP phone with no echo
canceller configured, or a simple extension that doesn't keep audio
playing) will leave the caller hearing nothing — the bridge's
`SMBF_WRITE_REPLACE` bug needs an active media stream to replace. The
robust pattern is `hangup + originate` rather than `transfer` for AI
calls. See the top-level
[Operational edge cases](../README.md#operational-edge-cases).

### `hold` / `unhold`

```jsonc
{"id":"4","cmd":"hold",   "args":{"uuid":"<fs-call-uuid>"}}
{"id":"5","cmd":"unhold", "args":{"uuid":"<fs-call-uuid>"}}
```

ESL: `uuid_hold <uuid>` / `uuid_hold off <uuid>`. While on hold, the
audio session continues but the caller hears MOH (FreeSWITCH default).

### `dtmf`

```jsonc
{"id":"6","cmd":"dtmf","args":{"uuid":"<fs-call-uuid>","digits":"1234#"}}
```

ESL: `uuid_send_dtmf <uuid> <digits>`. Useful for IVR navigation or
testing.

### `clear_playback`

```jsonc
{"id":"7","cmd":"clear_playback","args":{"uuid":"<fs-call-uuid>"}}
```

Flushes the module's playback buffer for the call. Does not require
ESL — it operates on the in-memory session. Use it to manually trigger
barge-in (the bridge normally sends `clear` automatically when the ASR
detects speech; this is the explicit version).

## Events

The bridge pushes events as JSON text frames. Subscribers see them all
in one stream — there's no per-event subscription. Filter on `event` /
`uuid` / `data` in your consumer.

| Event | Trigger | Data |
|---|---|---|
| `call.create` | `CHANNEL_CREATE` | `{caller, destination, direction}` |
| `call.answer` | `CHANNEL_ANSWER` | same |
| `call.hangup` | `CHANNEL_HANGUP_COMPLETE` | same + `hangup_cause` |
| `call.destroy` | `CHANNEL_DESTROY` | same |
| `call.bridge` / `call.unbridge` | `CHANNEL_BRIDGE` / `CHANNEL_UNBRIDGE` | same |
| `audio.start` | First binary frame from module | `{session, rate, mix}` |
| `audio.end` | Session ends | `{session}` |
| `transcript` | ASR partial or final | `{text, final}` |
| `speech.start` | VAD/ASR detected caller speech | none |
| `recording.complete` | Recording bundle finalized | `{path, session, duration_ms}` |

Field reference:

- `caller`: `Caller-Caller-ID-Number` ESL header
- `destination`: `Caller-Destination-Number` ESL header
- `direction`: `Call-Direction` ESL header
- `hangup_cause`: `Hangup-Cause` ESL header (only on `call.hangup`)

Mapping logic: [`bridge/control.go:255`](../bridge/control.go:255).

### Custom events from agents

In `agent` mode your external agent app can publish its own events
via the agent protocol. They land on `/control` with the agent's
call UUID. See [`agent-apps.md`](./agent-apps.md#event-republishing).

## Reply correlation

Every command has an `id` (a string you choose). The reply echoes it:

```jsonc
{"id":"1","ok":true,"result":"+OK abc-123-def"}
{"id":"1","ok":false,"error":"-ERR no reply"}
```

Generate unique `id`s — `cmd/fsctl` uses
`fmt.Sprintf("fsctl-%d", time.Now().UnixNano())`. Concurrent commands
need unique ids.

Replies don't block subsequent reads; events and replies can arrive in
any order on the same socket. Match by `id`.

## Library: composing the pieces

If you're building a service that uses fsbridge as a library rather than
running the binary, the relevant types are:

- `bridge.ESLClient` — ESL connection with auto-reconnect and JSON
  event subscription. See [`bridge/esl.go`](../bridge/esl.go).
- `bridge.EventBus` — pub/sub for in-process events. See
  [`bridge/eventbus.go`](../bridge/eventbus.go).
- `bridge.ControlServer` — the WebSocket control plane. Implements
  `http.Handler`; mount on your mux. See
  [`bridge/control.go:46`](../bridge/control.go:46).
- `bridge.ESLToBus` — goroutine that fans ESL events onto the bus as
  `call.*` events. Run it for the lifetime of your process.

Reference wiring in [`cmd/fsbridge/main.go`](../cmd/fsbridge/main.go):
ESL client creation at line 87, control server mounting at line 157.

## Patterns

### Outbound dialer

A service that originates calls based on an external trigger (CRM
record, calendar reminder, scheduled job):

```go
func dialCustomer(record Customer) error {
    cmd := bridge.ControlCommand{
        ID:  uuid.NewString(),
        Cmd: "originate",
        Args: json.RawMessage(fmt.Sprintf(
            `{"dest":"sofia/gateway/trunk/%s","ext":"9999","cid":"%s"}`,
            record.Phone, record.CallerID,
        )),
    }
    reply := sendCommand(cmd)
    if !reply.OK {
        return errors.New(reply.Error)
    }
    return nil
}
```

Then subscribe to `/control` and react to `call.answer`, transcripts,
and `recording.complete` for that UUID.

### Monitoring dashboard

A read-only consumer that shows live call state:

```go
conn, _, _ := websocket.Dial(ctx, "ws://localhost:8090/control", nil)
defer conn.Close(websocket.StatusNormalClosure, "")

for {
    _, raw, _ := conn.Read(ctx)
    var ev bridge.Event
    json.Unmarshal(raw, &ev)
    switch ev.Name {
    case "call.answer":
        dashboard.CallStarted(ev.UUID, ev.Data)
    case "call.hangup":
        dashboard.CallEnded(ev.UUID, ev.Data["hangup_cause"])
    case "transcript":
        if final, _ := ev.Data["final"].(bool); final {
            dashboard.Transcript(ev.UUID, ev.Data["text"].(string))
        }
    case "speech.start":
        dashboard.BargeIn(ev.UUID)
    case "recording.complete":
        dashboard.RecordingReady(ev.UUID, ev.Data["path"])
    }
}
```

### Reconnecting

`/control` is a long-lived WebSocket; if it drops, reconnect with
backoff:

```go
for {
    if err := runControl(ctx, conn, handle); err != nil {
        log.Warn("control disconnected", "err", err)
    }
    select {
    case <-ctx.Done():
        return
    case <-time.After(2 * time.Second):
    }
}
```

`examples/s3uploader/main.go:58`](../examples/s3uploader/main.go:58)
shows the same pattern in production.

## Common pitfalls

- **Originating from `/control` and expecting the call to immediately
  stream audio.** Originate is async — the call is placed and you get
  an ESL reply. Audio starts when FreeSWITCH answers the call and the
  dialplan fires `uuid_ws_bridge start`. Listen for `audio.start` to
  know when the bridge session is live.
- **Watching `call.create` and treating it as "the call answered".**
  `call.create` fires on `CHANNEL_CREATE`, which is before the call is
  actually answered. Use `call.answer` for that.
- **Forgetting `-esl-addr`.** Without it, the control plane isn't
  enabled at all — the binary won't accept the flag combination. The
  bridge exits with an error on startup.
- **Originating to the same extension as the dialplan you came in
  on.** If you originate an AI call from inside another AI call's
  handler, you'll recurse. Track call origin and don't originate from
  an AI-driven call.

## See also

- [`bridge/control.go`](../bridge/control.go) — `ControlServer` and
  command implementations.
- [`bridge/esl.go`](../bridge/esl.go) — `ESLClient` with reconnect.
- [`cmd/fsctl/`](../cmd/fsctl/) — CLI client.
- [`logging.md`](./logging.md) — event vocabulary and Loki correlation.
- [`recordings.md`](./recordings.md) — `recording.complete` arrives
  here.
- [`agent-apps.md`](./agent-apps.md) — how external agents publish
  events onto this stream.
