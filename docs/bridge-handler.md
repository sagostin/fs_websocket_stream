# Implementing a custom `bridge.Handler`

The bridge calls into your code via the `bridge.Handler` interface for
every audio session (one per FreeSWITCH call). This doc covers what you
can do in a handler, the `Session` API you have available, and the common
patterns.

If you want the AI pipeline built in, see
[`pipeline-stages.md`](./pipeline-stages.md). If you want to forward the
call to a separate agent service, see [`agent-apps.md`](./agent-apps.md).

## The interface

```go
type Handler interface {
    OnStart(s *Session)
    OnAudio(s *Session, pcm []byte)
    OnText(s *Session, data []byte)
    OnEnd(s *Session, err error)
}
```

For a given session the bridge invokes the callbacks sequentially from
the session's read pump, so you don't need extra locking on per-call
state. Send-side calls (`SendAudio`, `SendControl`, `ClearPlayback`) are
safe to call from any goroutine — they enqueue on a per-session channel
and drop oldest frames under backpressure.

`OnStart` fires once when the module's WebSocket connects (before any
audio). `OnAudio` fires for each 20 ms binary PCM frame. `OnText` fires
for each JSON text frame; the first one is typically the call metadata
your dialplan passes to `uuid_ws_bridge ... start`. `OnEnd` fires exactly
once when the session terminates; the `err` is the read-pump error (often
`nil` for a clean hangup).

If you only need a subset of the callbacks, embed `bridge.BaseHandler`
for the no-op defaults:

```go
type myHandler struct {
    bridge.BaseHandler
    // ... your fields
}
func (h *myHandler) OnAudio(s *bridge.Session, pcm []byte) { /* ... */ }
```

`BaseHandler` is defined in [`bridge/server.go`](../bridge/server.go:35).

## Minimal example: echo

The shortest handler — return each frame to the caller. Useful for
validating full-duplex transport with no AI involved. The whole thing
fits in a few lines:

```go
type EchoHandler struct{ bridge.BaseHandler }

func (EchoHandler) OnAudio(s *bridge.Session, pcm []byte) {
    _ = s.SendAudio(pcm)
}
```

This is exactly the reference implementation in
[`bridge/echo.go`](../bridge/echo.go). The bridge's `-mode echo` binary
in [`cmd/fsbridge/main.go`](../cmd/fsbridge/main.go:208) wraps it with a
frame counter so you can confirm audio is flowing from the bridge logs.

## The `Session` API

Every callback receives a `*bridge.Session`. Useful methods
([`bridge/session.go`](../bridge/session.go)):

| Method | What it gives you |
|---|---|
| `s.ID()` | Bridge-assigned session id (16-char hex). Distinct from the FS call UUID. |
| `s.FSUUID()` | FreeSWITCH call UUID (the `?uuid=...` query param). `""` if the module didn't supply one. |
| `s.SampleRate()` | Negotiated sample rate in Hz (e.g. `16000`). |
| `s.MixType()` | `"mono"`, `"mixed"` or `"stereo"`. |
| `s.Metadata()` | The latest JSON text frame from the module (the dialplan metadata). Returns `nil` until the first text frame arrives. |
| `s.SendAudio(pcm)` | Queue raw L16 PCM for downlink playback. Copies the buffer. Drops oldest under backpressure. |
| `s.SendControl(msg)` | Queue a JSON control frame (e.g. for surface to FreeSWITCH). |
| `s.ClearPlayback()` | Send `{"type":"clear"}` to the module — flushes its playback buffer (barge-in). |
| `s.Close()` | Terminate the session; the read pump will exit and `OnEnd` will fire. |

`SendAudio` is non-blocking and loss-tolerant: the playback queue is
bounded (`Options.PlaybackQueueSize`, default 100 ≈ 2 s of 20 ms frames),
and on overflow it drops the oldest queued frame so a stalled consumer
can't make audio latency grow without bound. See
[`bridge/session.go:95`](../bridge/session.go:95).

## Per-call state

Handlers are stateless across calls by convention — store per-call state
in a `sync.Map` keyed by `*Session` (the pointer is stable for the
session's lifetime). The `pipeline.Cascade` does exactly this
([`pipeline/cascade.go:28`](../pipeline/cascade.go:28)). Skeleton:

```go
type callState struct {
    started  time.Time
    frames   uint64
    bytesIn  uint64
    metadata json.RawMessage
}

type myHandler struct {
    bridge.BaseHandler
    logger *slog.Logger

    mu sync.Map // *bridge.Session -> *callState
}

func (h *myHandler) OnStart(s *bridge.Session) {
    st := &callState{started: time.Now()}
    h.mu.Store(s, st)
    h.logger.Info("call started",
        "fs_uuid", s.FSUUID(),
        "session", s.ID(),
        "rate", s.SampleRate(),
    )
}

func (h *myHandler) OnAudio(s *bridge.Session, pcm []byte) {
    v, ok := h.mu.Load(s)
    if !ok {
        return
    }
    st := v.(*callState)
    atomic.AddUint64(&st.frames, 1)
    atomic.AddUint64(&st.bytesIn, uint64(len(pcm)))
    // ... your per-frame logic
}

func (h *myHandler) OnEnd(s *bridge.Session, err error) {
    v, ok := h.mu.LoadAndDelete(s)
    if !ok {
        return
    }
    st := v.(*callState)
    h.logger.Info("call ended",
        "fs_uuid", s.FSUUID(),
        "frames",  atomic.LoadUint64(&st.frames),
        "bytes",   atomic.LoadUint64(&st.bytesIn),
        "err",     err,
    )
}
```

## Custom handler: transcript collector

A common use case is to add a transcript overlay to whatever pipeline you
run — record every utterance to your own datastore. Use `bridge.EventBus`
to receive transcripts emitted by `pipeline.Cascade`:

```go
type collector struct {
    bridge.BaseHandler
    sink func(fsUUID, text string, final bool) // your DB write
}

func (c *collector) Start(bus *bridge.EventBus) {
    if bus == nil {
        return
    }
    ch, unsub := bus.Subscribe(256)
    go func() {
        defer unsub()
        for ev := range ch {
            if ev.Name != "transcript" {
                continue
            }
            text, _ := ev.Data["text"].(string)
            final, _ := ev.Data["final"].(bool)
            c.sink(ev.UUID, text, final)
        }
    }()
}
```

`transcript` events are published by `pipeline.Cascade` for both partial
and final utterances; `speech.start` fires on the VAD/ASR signal that
triggers barge-in. See [`logging.md`](./logging.md) for the full event
list.

To wire it into the server, attach the bus:

```go
bus := bridge.NewEventBus()
go collector.Start(bus)

srv := bridge.NewServer(&pipeline.Cascade{
    NewASR: providers.DeepgramASRFactory(providers.DeepgramASRConfig{
        APIKey: os.Getenv("DEEPGRAM_API_KEY"),
    }),
    LLM:    providers.OpenAILLM{APIKey: os.Getenv("OPENAI_API_KEY")},
    NewTTS: providers.ElevenLabsTTSFactory(os.Getenv("ELEVENLABS_API_KEY"), os.Getenv("ELEVENLABS_VOICE_ID")),
    Bus:    bus,
    Logger: logger,
}, &bridge.Options{Bus: bus, Logger: logger})
```

## Custom handler: barge-in with a non-Cascade pipeline

If you're running a custom pipeline (e.g. your own streaming LLM that
already manages turn-taking) but want the bridge to handle barge-in,
send `ClearPlayback` whenever the caller starts speaking:

```go
func (h *myHandler) OnAudio(s *bridge.Session, pcm []byte) {
    if h.vad.DetectSpeech(pcm) {
        _ = s.ClearPlayback()   // module flushes its playback buffer
        h.cancelCurrentTurn()   // your pipeline cancels in-flight synthesis
    }
    h.feedASR(pcm)
}
```

The module's playback buffer caps at 10 s with drop-oldest; `ClearPlayback`
forces an immediate flush so barge-in is heard at the next frame boundary.
Don't sleep waiting for the flush — the next `SendAudio` from your
pipeline will land in a freshly-cleared buffer.

## Custom handler: sending control frames

`SendControl` lets you emit any `bridge.ControlMessage`. The module
treats `{"type":"clear"}` specially (flush). Anything else is surfaced
as a `mod_ws_bridge::json` FreeSWITCH event, so it's useful as a
two-way signaling channel when you need the dialplan to react:

```go
_ = s.SendControl(bridge.ControlMessage{
    Type:    "mark",
    Name:    "turn-end",
    Message: "agent finished speaking",
})
```

In the dialplan:

```xml
<action application="set" data="api_on_answer=..."/>
<action application="answer"/>
<action application="endless_playback" data="/tmp/silence.wav"/>
<!-- handle mod_ws_bridge::json custom events here -->
```

## Mounting the handler

The simplest mount, when you don't need ESL, recording, or a control
plane:

```go
srv := bridge.NewServer(myHandler{}, &bridge.Options{Logger: logger})
srv.ListenAndServe(ctx, ":8080")
```

For the full picture — ESL events into the bus, per-call recording,
agent forwarding, a control plane — see [`cmd/fsbridge/main.go`](../cmd/fsbridge/main.go),
which composes every layer. The function is the canonical recipe.

## Common pitfalls

- **Sending before `OnStart`.** You can't — `SendAudio` and friends
  return `bridge.ErrSessionClosed` if the session is gone, but the more
  common bug is a handler that forgets to wait for `OnStart` and tries
  to write before the session exists. Build state in `OnStart`.
- **Assuming the metadata frame is the first thing.** The module sends
  the metadata frame before any audio, so in practice it is the first
  `OnText` call. But the bridge doesn't enforce ordering; treat it as
  "eventually consistent" and keep working if `Metadata()` returns `nil`.
- **Blocking in `OnAudio`.** It runs on the read pump. Long work stalls
  the whole session — including the caller audio ingest. Use a goroutine
  for anything I/O bound.
- **Forgetting `BaseHandler`.** If you implement one method on a
  pointer receiver, embedding `BaseHandler` keeps the other three as
  no-ops. Without it, the type won't satisfy `Handler` and the bridge
  won't compile.

## See also

- [`pipeline/cascade.go`](../pipeline/cascade.go) — the default handler that wires ASR → LLM → TTS with barge-in.
- [`bridge/server.go`](../bridge/server.go) — `Options`, `NewServer`, session lifecycle.
- [`bridge/agent.go`](../bridge/agent.go) — `AgentForwarder`, the out-of-process alternative.
- [`agent-apps.md`](./agent-apps.md) — when to put your handler in a separate service instead.
- [`control-plane.md`](./control-plane.md) — driving calls from outside the bridge.
- [`recordings.md`](./recordings.md) — capturing every session to disk.
