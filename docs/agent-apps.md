# Building an external AI voice agent app

In `agent` mode, `fsbridge` does **not** run an AI pipeline itself. For
every call it dials out to your application — one WebSocket per call —
and forwards audio and lifecycle events. Your app owns the AI logic;
fsbridge owns telephony. This is the self-hosted equivalent of Twilio
Media Streams.

If you're not sure whether to use this or run the AI in-process, start
with [Choosing in-process vs agent mode](#choosing-in-process-vs-agent-mode)
below.

## When to use agent mode

Use `agent` mode when any of the following apply:

- Your agent is its own service with its own release cycle (separate
  from telephony).
- Your agent is written in a different language (Python, Node, Rust).
- You need stateful per-call logic that's too heavy to embed in the
  bridge: tool calls, RAG, CRM lookups, transfer decisions, multiple
  LLM providers per tenant.
- You want to scale agent replicas independently of fsbridge replicas.
- You want hot-reload of agent code without restarting telephony.

Use in-process `ai` mode (`-mode ai`) when one binary is fine and your
provider stack is fixed.

## The agent protocol

The bridge dials your app once per call at `-agent-url`:

```
ws://your-agent:9000/call?uuid=<fs-call-uuid>&session=<id>&rate=16000&mix=mono
```

Query params:

| Param | Meaning |
|---|---|
| `uuid` | FreeSWITCH call UUID. Stable for the call's lifetime; the same value appears on `/control` events. |
| `session` | Bridge-assigned session id (16-char hex). Distinct from `uuid`; used to correlate this agent socket with the bridge's audio session. |
| `rate` | PCM sample rate in Hz (e.g. `16000`). |
| `mix` | `mono`, `mixed`, or `stereo`. |

The protocol is symmetrical JSON text frames plus binary PCM.

**Bridge → agent**

```jsonc
{"type":"start","uuid":"<fs-uuid>","session":"<id>","rate":16000,"mix":"mono"}
{"type":"metadata","data":{...}}            // dialplan metadata (may be absent)
<binary frames: caller audio, L16 PCM>
{"type":"stop"}                             // call/stream ended
```

**Agent → bridge**

```jsonc
<binary frames: AI audio, L16 PCM, to play to the caller>
{"type":"clear"}                            // flush caller playback (barge-in)
{"type":"hangup"}                           // end the call (needs -esl-addr on the bridge)
{"type":"event","name":"transcript","data":{...}}   // republished to /control subscribers
```

Constants live in [`bridge/agent.go:29`](../bridge/agent.go:29). The
`AgentForwarder` ([`bridge/agent.go:52`](../bridge/agent.go:52)) is the
bridge-side implementation — read it to understand edge cases
(reconnect, dial-timeout, agent-side `hangup` mapping to ESL).

### `event` republishing

The `event` message lets your agent publish events onto the bridge's
event bus. They flow to every `/control` subscriber as `bridge.Event`
JSON, with `uuid` set to the FS call UUID. Use this for transcripts,
tool calls, sentiment scores, anything an external observer might want.

Names are arbitrary dotted strings; the bridge doesn't interpret them,
just fans them out. Stick to a stable vocabulary so consumers can
filter predictably.

## Canonical reference: `examples/voicebot`

[`examples/voicebot/`](../examples/voicebot/) is the canonical
implementation of an agent app. It's ~250 lines, single file, runs in
two modes (`mock` for tone replies with no API keys, `ai` for the full
Deepgram + OpenAI + ElevenLabs stack). Read it first.

Key file paths:

- [`examples/voicebot/main.go:38`](../examples/voicebot/main.go:38) — the
  per-call `call` struct: connection, UUID/session/rate, write mutex,
  ASR/LLM/TTS, conversation history, in-flight cancel.
- [`examples/voicebot/main.go:55`](../examples/voicebot/main.go:55) —
  write helpers with a per-call mutex so the agent never interleaves
  binary and text frames on the wire.
- [`examples/voicebot/main.go:76`](../examples/voicebot/main.go:76) —
  the per-call run loop: ASR event goroutine + read pump on the agent
  socket.
- [`examples/voicebot/main.go:131`](../examples/voicebot/main.go:131) —
  `respond()`: one LLM → TTS turn, cancels on barge-in.

### Per-call state pattern

Each accepted WebSocket becomes a `*call` value that owns the per-call
state:

```go
type call struct {
    conn    *websocket.Conn
    uuid    string
    session string
    rate    int

    writeMu sync.Mutex

    asr     pipeline.ASR
    llm     pipeline.LLM
    newTTS  pipeline.TTSFactory
    history []pipeline.Message

    mu     sync.Mutex
    cancel context.CancelFunc  // cancels in-flight reply turn (for barge-in)
}
```

Two mutexes for two distinct things:

- `writeMu` serializes `Write()` on the WebSocket so binary and text
  frames don't interleave.
- `mu` protects `call.history` and `call.cancel` from concurrent
  updates as multiple turns race.

### ASR event loop drives turns

The voicebot runs one goroutine per call that consumes the ASR event
stream and reacts to `speech.started` (barge-in) and `transcript`
(turn completion):

```go
go func() {
    for ev := range c.asr.Events() {
        switch ev.Type {
        case pipeline.EventSpeechStarted:
            c.bargeIn()                       // cancel current turn + send clear
        case pipeline.EventTranscript:
            c.publishTranscript(ev.Text, ev.Final)
            if ev.Final && ev.Text != "" {
                go c.respond(ev.Text)         // LLM + TTS
            }
        }
    }
}()
```

Each `respond` builds its own cancellable context; if barge-in fires
while a turn is in flight, that context is cancelled, the in-flight
LLM/TTS goroutine sees `ctx.Err() != nil` and exits, and the
in-progress TTS chunk is no longer written to the socket.

### Republishing transcripts to the bridge bus

To get your transcripts onto `/control` (for external observers), wrap
them as `event` messages:

```go
_ = c.writeJSON(bridge.AgentMessage{
    Type: bridge.AgentMsgEvent,
    Name: "transcript",
    Data: json.RawMessage(mustJSON(map[string]any{
        "text":  ev.Text,
        "final": ev.Final,
    })),
})
```

The bridge republishes these onto its bus (see
[`bridge/agent.go:217`](../bridge/agent.go:217)) and every connected
`/control` client receives them.

## End the call cleanly

When the call ends (the agent socket receives `{"type":"stop"}`), the
canonical pattern is to close your per-call resources and return:

```go
for {
    mt, data, err := c.conn.Read(ctx)
    if err != nil {
        return
    }
    if mt == websocket.MessageBinary {
        _ = c.asr.Write(data)
        continue
    }
    var msg bridge.AgentMessage
    if json.Unmarshal(data, &msg) != nil {
        continue
    }
    switch msg.Type {
    case bridge.AgentMsgMetadata:
        // pass to your per-call state
    case bridge.AgentMsgStop:
        return
    }
}
```

Closing the agent socket makes `conn.Read` return, the run goroutine
exits, the deferred `asr.Close()` runs, and the per-call goroutine
fades. The bridge sees the closed socket, runs its `OnEnd`, the
recorder finalizes the bundle (if enabled), and `recording.complete`
fires.

If you want to hang up from inside the agent (the user said
"goodbye, end the call"), send `{"type":"hangup"}`. The bridge maps
that to an ESL `uuid_kill <uuid>` — which requires `-esl-addr` on the
bridge. If ESL isn't configured, the message is logged and dropped
(see [`bridge/agent.go:206`](../bridge/agent.go:206)).

## Reconnect / retry

`AgentForwarder` dials once per call. If the dial fails (your service
is down), the audio session is closed and recorded as `audio.end`. The
FreeSWITCH dialplan continues — a parked call with no audio is the
visible result. See [`README.md` Operational edge cases](../README.md#operational-edge-cases).

If you want calls to survive a brief agent outage, put your agent
service behind a load balancer that returns 200 quickly and then
opens the WebSocket — or add a queueing layer in front.

## Deployment topology

```
              ┌──────────────────┐
FreeSWITCH ───┤  fsbridge  ──────┼───ws://─── agent replicas (LB in front)
              │  (-mode agent)   │
              └──────────────────┘
                       │
                       └─── /control (events) ─── your monitoring / eval pipeline
```

- **Single agent replica**: fine for low call volume.
- **Multiple replicas**: put an LB in front (nginx, Envoy, ALB). Each
  call lands on one replica and stays there for the call's life (the
  bridge dials a new WebSocket per call, no affinity needed on the LB).
  Configure the LB to send `Connection: upgrade` and a long
  `proxy_read_timeout`.
- **Multi-tenant / multi-region**: run one agent service per tenant or
  per region. The bridge just dials the URL you give it; routing is
  your responsibility.

## Run the canonical example

In one terminal:

```sh
docker compose -f examples/freeswitch/docker-compose.yml up   # FreeSWITCH + module
```

In a second:

```sh
go run ./cmd/fsbridge -addr :8090 -mode agent \
    -agent-url ws://localhost:9000/call \
    -esl-addr localhost:8021 -esl-password ClueCon
```

In a third:

```sh
go run ./examples/voicebot -addr :9000 -path /call -mode mock
```

Register any SIP phone and dial **9999**. You'll hear a tone reply (mock
mode) and see transcripts in the voicebot logs.

## Common pitfalls

- **Forgetting `writeMu`.** Two goroutines writing concurrently to the
  same `*websocket.Conn` will interleave binary and text frames and
  confuse the bridge. Wrap every write.
- **Blocking on `c.asr.Events()`.** If your ASR implementation has a
  bounded channel and your consumer is slow, it blocks the read loop
  on the ASR's side. Either consume fast or use an unbounded buffer
  for events you don't want to drop.
- **Holding the agent-side connection open across restarts.** The
  bridge doesn't reconnect agent sockets mid-call. If you restart your
  agent service mid-call, the call continues silently until you hang
  it up via ESL.
- **Not publishing events.** If you don't republish transcripts as
  `event` messages, external observers can't see what was said. Almost
  always worth doing.

## See also

- [`bridge/agent.go`](../bridge/agent.go) — `AgentForwarder`, the
  bridge side of the protocol.
- [`examples/voicebot/main.go`](../examples/voicebot/main.go) — full
  reference implementation.
- [`pipeline-stages.md`](./pipeline-stages.md) — the ASR/LLM/TTS
  interfaces the voicebot composes.
- [`control-plane.md`](./control-plane.md) — events the agent publishes
  land here for external consumers.
- [`logging.md`](./logging.md) — log conventions for per-call
  correlation.
