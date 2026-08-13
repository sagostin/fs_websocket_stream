# Logging for voice AI agent apps

Logging for an AI voice agent has three concerns regular service logs
don't: correlating logs across many processes by a single call, knowing
which calls were "good" or "bad" after the fact, and reproducing a call
from its timeline. This doc covers the conventions, the bridge's event
bus, the optional Loki sink, and per-call log correlation.

## The three log planes

When a call is in flight, you have logs in three places:

1. **fsbridge** — the Go server. Per-call lifecycle (`session started`,
   `session ended`), recorder events, ESL reconnects, and whatever
   your handler logs.
2. **External agent app** (in `agent` mode) — your voicebot or agent
   service. Per-call ASR/LLM/TTS logs, conversation history, tool
   calls.
3. **Provider APIs** — Deepgram, OpenAI, ElevenLabs have their own
   logs; you can't see inside them, but you can match by request id if
   your provider sends it back.

To follow a call end-to-end you need a stable identifier that all three
planes share. That's the FreeSWITCH call UUID (`fs_uuid`), which the
bridge sets on every event, every log line you write from your handler,
and every metadata frame you pass into your agent.

## Field conventions

Use these field names consistently. Every consumer (Grafana queries,
log scrapers, your own search tools) will benefit.

| Field | Type | Meaning |
|---|---|---|
| `fs_uuid` | string | FreeSWITCH call UUID. Stable for the call's lifetime. Appears on every `bridge.Event`, every metadata frame, every `/control` event. |
| `session` | string | Bridge-assigned session id (16-char hex). Distinct from `fs_uuid`. Per audio socket, in case a single call ever gets re-bridged. |
| `direction` | string | `"inbound"` or `"outbound"` — which way the call came in. Comes from the `Call-Direction` ESL header. |
| `caller` | string | Caller ID number (`Caller-Caller-ID-Number` ESL header). |
| `destination` | string | Destination number (`Caller-Destination-Number`). |

In Go, use structured `slog`:

```go
logger.Info("call answered",
    "fs_uuid",     s.FSUUID(),
    "session",     s.ID(),
    "rate",        s.SampleRate(),
    "mix",         s.MixType(),
    "caller",      meta.Caller,
    "destination", meta.Destination,
)
```

The bridge's own code follows this convention — see
[`bridge/server.go:184`](../bridge/server.go:184) and
[`bridge/agent.go:101`](../bridge/agent.go:101).

## Event bus: logs as events

The bridge has an in-process pub/sub `bridge.EventBus` that receives
domain events. These aren't log lines — they're structured `bridge.Event`
values with a name, UUID, and data. Subscribers see them on `/control`
and on `bridge.EventBus.Subscribe(...)`.

| Event | When | Data |
|---|---|---|
| `call.create` | FreeSWITCH `CHANNEL_CREATE` | `{caller, destination, direction}` |
| `call.answer` | `CHANNEL_ANSWER` | same |
| `call.hangup` | `CHANNEL_HANGUP_COMPLETE` | same + `hangup_cause` |
| `call.destroy` | `CHANNEL_DESTROY` | same |
| `call.bridge` / `call.unbridge` | `CHANNEL_BRIDGE` / `CHANNEL_UNBRIDGE` | same |
| `audio.start` | First binary frame from module | `{session, rate, mix}` |
| `audio.end` | Session terminates | `{session}` |
| `transcript` | ASR partial or final | `{text, final}` |
| `speech.start` | VAD/ASR detected caller speech (barge-in trigger) | none |
| `recording.complete` | Recording bundle finalized | `{path, session, duration_ms}` |

Publishing is non-blocking: events are telemetry, not state, and a
slow subscriber won't slow the bridge. `bridge.EventBus.Publish` drops
events to full subscriber channels (see
[`bridge/eventbus.go:41`](../bridge/eventbus.go:41)).

If you want every event of a call in your log pipeline, subscribe once
and `slog.Info` them:

```go
ch, unsub := bus.Subscribe(256)
defer unsub()
for ev := range ch {
    logger.Info("bridge event",
        "fs_uuid",  ev.UUID,
        "event",    ev.Name,
        "data",     ev.Data,
    )
}
```

This is the canonical pattern; the recorder does the same to write
`events.jsonl` per call ([`bridge/recorder.go:226`](../bridge/recorder.go:226)).

## Custom events from your handler

If your handler emits domain events (e.g. tool calls, sentiment,
transfers), publish them so external observers see them:

```go
bus.Publish(bridge.Event{
    Name: "tool.called",
    UUID: s.FSUUID(),
    Data: map[string]any{
        "name":      "check_order_status",
        "args":      args,
        "result_ok": true,
    },
})
```

The recorder will capture them in the call's `events.jsonl` if
recording is enabled. Any `/control` subscriber will see them as JSON
events.

## Custom events from an external agent

In `agent` mode, your voicebot can't reach the bridge's in-process bus.
But the bridge's `AgentForwarder` lets you publish events over the
WebSocket and republishes them onto the bus. See
[`agent-apps.md`](./agent-apps.md#event-republishing):

```go
_ = c.writeJSON(bridge.AgentMessage{
    Type: bridge.AgentMsgEvent,
    Name: "tool.called",
    Data: json.RawMessage(`{"name":"check_order_status","result_ok":true}`),
})
```

End up on the bus and on `/control` as a `tool.called` event with the
agent's call UUID. This is how an external agent joins the same event
plane as in-process handlers — use it liberally.

## Loki shipping

The bridge ships every `slog` record to Loki via the push API when
`-loki-url` is set. Records go to stderr in parallel; you don't lose
local logs.

```sh
fsbridge -loki-url http://loki:3100 -loki-job fsbridge-prod
# Grafana Cloud:
fsbridge -loki-url https://logs-prod-eu-west-0.grafana.net \
    -loki-tenant <tenant> -loki-user <id> -loki-pass <token>
```

Labels: `{job, host, level}`. The `host` is `os.Hostname()` unless
overridden (`bridge.LokiConfig.Host`). Records are batched — default
100 / 5 s — and pushed as one `/loki/api/v1/push` per batch. Under
pressure the queue drops new entries rather than block log emission
(see [`bridge/loki.go:97`](../bridge/loki.go:97)).

For your agent app, do the same thing — the Loki push endpoint doesn't
care who writes to it:

```go
client, _ := bridge.NewLokiClient(bridge.LokiConfig{
    URL:   "http://loki:3100",
    Job:   "voicebot",
    Host:  os.Hostname(),
})
defer client.Close()

inner := slog.NewJSONHandler(os.Stderr, nil)
logger := slog.New(bridge.NewLokiHandler(inner, client))

logger.Info("reply",
    "fs_uuid", c.uuid,
    "turn",    turn,
    "text",    reply,
    "ms",      time.Since(started).Milliseconds(),
)
```

Same labels (`{job=voicebot, host=..., level=info}`), same payload
format. Now `fsbridge` and your voicebot show up in the same Grafana
query, correlated by `fs_uuid`.

### Note on `Job` per service

Use a different `Job` per service (`fsbridge`, `voicebot`, `eval-pipeline`)
so you can scope Grafana queries. Use the same `fs_uuid` field name
everywhere so cross-service queries work.

## Per-call log query (LogQL)

Once both services write to Loki with consistent fields, you can pull
a full call's log trail with one query:

```logql
{job=~"fsbridge|voicebot"} | json | fs_uuid="abc-123-def-456"
```

Add `| line_format "{{.msg}} {{.data}}"` for compact display, or split
by `event` to visualize lifecycle:

```logql
sum by (event) (
  count_over_time({job=~"fsbridge|voicebot"} | json | fs_uuid="abc-123-def-456" [1h])
)
```

## Healthcheck and metrics

The bridge exposes `GET /healthz` returning `{ok: true, sessions: N}`.
Wire it to your load balancer. No Prometheus endpoint yet — see the
top-level README "Production considerations" for the seam.

For agent apps, expose your own `/healthz` returning at minimum the
active-call count and a flag for whether your upstream providers are
reachable. Don't return 200 if Deepgram is down — your calls will
silently fail.

## What to log per turn

A useful minimum for debugging a single turn in your agent:

```go
started := time.Now()
asrText, final := waitForTranscript(ctx)
logger.Info("asr",
    "fs_uuid", c.uuid,
    "text",    asrText,
    "final",   final,
    "ms",      time.Since(started).Milliseconds(),
)

started = time.Now()
reply, err := c.llm.Respond(ctx, history)
logger.Info("llm",
    "fs_uuid", c.uuid,
    "reply",   reply,
    "err",     err,
    "ms",      time.Since(started).Milliseconds(),
)

started = time.Now()
chunks, _ := c.newTTS(c.rate).Synthesize(ctx, reply)
n := 0
for range chunks { n++ }
logger.Info("tts",
    "fs_uuid", c.uuid,
    "chunks",  n,
    "ms",      time.Since(started).Milliseconds(),
)
```

Latency breakdown per stage is what you need to debug "the agent feels
slow" — without it you can't tell whether the bottleneck is ASR, LLM, or
TTS.

## Common pitfalls

- **Logging PCM frames.** Even at info level, that's megabytes per
  minute per call. Log counts and durations, not bytes.
- **Different UUID field names in different services.** If `voicebot`
  logs `call_uuid` and `fsbridge` logs `fs_uuid`, you can't correlate
  them in Loki. Pick a name and stick with it. The bridge uses
  `fs_uuid`.
- **Logging on the hot path without sampling.** If your agent logs at
  debug level for every ASR frame, you will melt Loki. Sample or
  rate-limit per call.
- **Forgetting to log the call UUID.** Every line should have it. Add
  a per-call logger: `logger = logger.With("fs_uuid", c.uuid)` and
  pass that around.

## See also

- [`bridge/loki.go`](../bridge/loki.go) — `LokiClient` and `LokiHandler`.
- [`bridge/eventbus.go`](../bridge/eventbus.go) — `EventBus`.
- [`bridge/agent.go:217`](../bridge/agent.go:217) — agent event
  republishing.
- [`examples/voicebot/main.go:149`](../examples/voicebot/main.go:149) —
  per-turn logging in the reference agent app.
- [`recordings.md`](./recordings.md) — `events.jsonl` is the per-call
  log timeline on disk.
