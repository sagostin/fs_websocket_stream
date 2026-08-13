# Pipeline stages: ASR, LLM, TTS, and the cascade

The `pipeline` package wires an AI voice agent: streaming ASR (speech to
text), an LLM (reply text), and streaming TTS (reply text to speech),
with barge-in so the caller can interrupt. This doc covers the three
interfaces, the `Cascade` that glues them together, the bundled
providers (`Deepgram`, `OpenAI`, `ElevenLabs`), how to write your own
stages, and how to tune the cascade for latency vs. naturalness.

For the high-level overview see the top-level
[External agent apps section](../README.md#external-agent-apps-agent-mode)
and
[Wire protocol](../README.md#wire-protocol). For implementation see
[`pipeline/`](../pipeline/) and [`providers/`](../providers/).

## The three interfaces

Defined in [`pipeline/pipeline.go`](../pipeline/pipeline.go):

```go
type ASR interface {
    io.Closer
    Write(pcm []byte) error
    Events() <-chan ASREvent
}

type LLM interface {
    Respond(ctx context.Context, history []Message) (string, error)
}

type TTS interface {
    Synthesize(ctx context.Context, text string) (<-chan []byte, error)
}
```

The shapes aren't accidental:

- **ASR is streaming.** It accepts caller PCM chunks over time and
  emits partial and final transcripts. `Events()` returns a channel
  that closes when `Close()` is called.
- **LLM is request/response.** One `Respond` per turn. The cascade
  passes the full conversation history each time — your provider can
  stream internally, but the interface returns one final string.
- **TTS is streaming.** It returns a channel of L16 PCM chunks that
  the cascade writes to the call as they arrive, allowing first-audio
  latency as low as ~150 ms on a good day.

`ASRFactory` and `TTSFactory` build per-call instances:

```go
type ASRFactory func(sampleRate int) (ASR, error)
type TTSFactory func(sampleRate int) (TTS, error)
```

Factories exist because each call gets its own ASR/TTS connection — you
don't want to share a Deepgram WebSocket across calls.

## The cascade

`pipeline.Cascade` ([`pipeline/cascade.go`](../pipeline/cascade.go))
implements `bridge.Handler` and wires the three stages:

```
caller audio --> ASR --events--> consumeASR()
                                    |
                                    |  final transcript
                                    v
                                LLM.Respond(ctx, history)
                                    |
                                    v
                                TTS.Synthesize(ctx, reply)
                                    |
                                    v
                                Session.SendAudio(chunk)
```

Barge-in is the loop's safety valve. On `EventSpeechStarted` the
cascade cancels the in-flight LLM/TTS context and calls
`Session.ClearPlayback()` so the module flushes its playback buffer.
The next TTS chunks will land in a freshly-cleared buffer.

```go
type Cascade struct {
    NewASR       ASRFactory
    LLM          LLM
    NewTTS       TTSFactory
    SystemPrompt string
    Logger       *slog.Logger
    Bus          *bridge.EventBus
}
```

Wire it up:

```go
cascade := &pipeline.Cascade{
    NewASR: providers.DeepgramASRFactory(providers.DeepgramASRConfig{
        APIKey: os.Getenv("DEEPGRAM_API_KEY"),
    }),
    LLM: providers.OpenAILLM{
        APIKey: os.Getenv("OPENAI_API_KEY"),
        Model:  "gpt-4o-mini",
    },
    NewTTS: providers.ElevenLabsTTSFactory(
        os.Getenv("ELEVENLABS_API_KEY"),
        os.Getenv("ELEVENLABS_VOICE_ID"),
    ),
    SystemPrompt: "You are a helpful voice assistant. Keep replies short and conversational.",
    Logger:       logger,
    Bus:          bus,
}
srv := bridge.NewServer(cascade, &bridge.Options{Bus: bus, Logger: logger})
```

Or run it via the binary: `fsbridge -mode ai` (env vars as above).

## Bundled providers

### Deepgram ASR ([`providers/deepgram.go`](../providers/deepgram.go))

Streams caller audio to Deepgram's realtime WebSocket API. Config:

| Field | Default | Meaning |
|---|---|---|
| `APIKey` | — | Required. |
| `Model` | `"nova-2"` | Deepgram model. `nova-2` is the latency/quality sweet spot. |
| `EndpointingMs` | `300` | ms of silence before Deepgram finalizes an utterance. Lower = faster turns, higher = fewer mid-thought cuts. |

Emits `EventSpeechStarted` on `SpeechStarted`, `EventTranscript` with
`Final: true` when `is_final` or `speech_final` arrives. Connection
lifetime is the call's lifetime — one Deepgram WebSocket per call.

### OpenAI LLM ([`providers/openai.go`](../providers/openai.go))

Standard Chat Completions call. Config:

| Field | Default | Meaning |
|---|---|---|
| `APIKey` | — | Required. |
| `Model` | `"gpt-4o-mini"` | Any Chat Completions model. `gpt-4o-mini` is the latency/cost sweet spot for voice. |
| `BaseURL` | `https://api.openai.com` | Override for OpenAI-compatible endpoints (Together, Anyscale, vLLM). |

The cascade calls `Respond` synchronously per turn. If you want
streaming (token-by-token), you'll need to write a custom LLM that
buffers to text — for TTS the cascade needs the full reply text to
start synthesis, so partial streaming buys you little.

### ElevenLabs TTS ([`providers/elevenlabs.go`](../providers/elevenlabs.go))

Streaming TTS via ElevenLabs' `/stream` endpoint. Config:

| Field | Default | Meaning |
|---|---|---|
| `APIKey` | — | Required. |
| `VoiceID` | — | Required. The voice to use (e.g. `"21m00Tcm4TlvDq8ikWAM"` for Rachel). |
| `Model` | `"eleven_turbo_v2_5"` | Latency-optimized. The non-turbo model has better quality but ~3× the latency. |
| `SampleRate` | `16000` | `8000` or `16000`. Should match the session rate. |

PCM chunks flow as ElevenLabs emits them; the cascade writes each to
the session as it arrives. `ctx` cancellation cuts the synthesis
mid-stream — essential for barge-in.

## Mocks

`pipeline/mock.go` ships mock implementations for testing without API
keys:

- `pipeline.MockLLM` — echoes the user's last message back as `"You said: <text>"`.
- `pipeline.MockASR` — after `SpeechAfter` bytes of audio it emits
  `EventSpeechStarted` once; after `FinalAfter` bytes it emits a final
  transcript with the configured text; then resets and repeats. Defaults
  are tuned for 16 kHz mono (200 ms speech-start, 1 s final).
- `pipeline.MockTTS` — synthesizes a sine tone. Useful to verify audio
  flows end-to-end without paying for real TTS. `-mode mock` in
  `fsbridge` and `examples/voicebot` use these.

```go
cascade := &pipeline.Cascade{
    NewASR: pipeline.MockASRFactory("hello world"),
    LLM:    pipeline.MockLLM{},
    NewTTS: pipeline.MockTTSFactory(2000), // 2s tone
}
```

## Writing a custom stage

### Custom ASR

Implement the `ASR` interface. The two design points are: how you
buffer PCM before sending, and how you map provider events to
`ASREvent`. For a polling-based provider, drain the events channel
in a goroutine and translate.

```go
type myASR struct {
    conn   net.Conn
    events chan pipeline.ASREvent
    closed atomic.Bool
}

func (a *myASR) Write(pcm []byte) error {
    if a.closed.Load() { return io.EOF }
    _, err := a.conn.Write(pcm)
    return err
}

func (a *myASR) Events() <-chan pipeline.ASREvent { return a.events }

func (a *myASR) Close() error {
    if a.closed.CompareAndSwap(false, true) {
        close(a.events)
        return a.conn.Close()
    }
    return nil
}

// In your constructor, kick off a read goroutine that parses provider
// events and translates them to ASREvent.
func newMyASR(ctx context.Context, cfg Config, sampleRate int) (*myASR, error) {
    conn, err := net.Dial("tcp", cfg.Endpoint)
    if err != nil { return nil, err }
    a := &myASR{
        conn:   conn,
        events: make(chan pipeline.ASREvent, 32),
    }
    go a.readLoop() // provider protocol -> ASREvent translation
    return a, nil
}
```

The events channel is the cascade's clock — if it's slow to read, your
provider's read goroutine will block. Either buffer generously
(`make(chan ..., 256)`) or drop on overflow. The bundled providers
drop (see [`providers/deepgram.go:114`](../providers/deepgram.go:114)).

### Custom LLM

Implement `Respond(ctx, history)`. Three things to get right:

1. **`ctx` cancellation must abort the request.** Use
   `req.WithContext(ctx)` so the HTTP request is cancelled when the
   caller barges in. Don't leak goroutines after cancellation.
2. **Empty history should return an error or empty string.** The
   cascade filters empty replies, but a panic on a zero-length history
   will crash the call.
3. **Don't hold large history indefinitely.** Most voice calls are
   short. If you must keep long history, summarize or truncate — costs
   grow linearly with input tokens.

```go
func (l myLLM) Respond(ctx context.Context, history []pipeline.Message) (string, error) {
    req, _ := http.NewRequestWithContext(ctx, "POST", l.URL, bytes.NewReader(toJSON(history)))
    req.Header.Set("Authorization", "Bearer "+l.APIKey)
    resp, err := l.http.Do(req)
    if err != nil { return "", err }
    defer resp.Body.Close()
    // ... parse, return text
}
```

### Custom TTS

Implement `Synthesize(ctx, text)`. The cascade writes chunks to the
session as they arrive. Two design points:

1. **Chunk size.** 20 ms frames (320 samples @ 16 kHz, 640 bytes) is
   what the module expects. Bigger chunks add latency; smaller chunks
   add CPU. Most streaming TTS providers will give you ~4 KB chunks
   that you can pass through unchanged.
2. **`ctx` cancellation.** Close the response body when ctx is
   cancelled so the upstream provider stops generating audio and the
   connection can be reused.

```go
func (t myTTS) Synthesize(ctx context.Context, text string) (<-chan []byte, error) {
    req, _ := http.NewRequestWithContext(ctx, "POST", t.URL, strings.NewReader(text))
    resp, err := t.http.Do(req)
    if err != nil { return nil, err }
    out := make(chan []byte, 64)
    go func() {
        defer close(out)
        defer resp.Body.Close()
        buf := make([]byte, 4096)
        for {
            n, err := resp.Body.Read(buf)
            if n > 0 {
                chunk := make([]byte, n)
                copy(chunk, buf[:n])
                select {
                case <-ctx.Done(): return
                case out <- chunk:
                }
            }
            if err != nil { return }
        }
    }()
    return out, nil
}
```

## Cascade tuning

### Latency levers (top to bottom)

1. **Deepgram `EndpointingMs`.** Lower = faster turn boundary
   detection = lower reply latency, at the risk of cutting off
   mid-thought utterances. 200–400 ms is the practical range for
   natural conversation.
2. **OpenAI `Model`.** `gpt-4o-mini` is much faster than `gpt-4o` and
   quality is fine for most voice-agent tasks. Drop to `gpt-4.1-nano`
   if you need even less latency.
3. **ElevenLabs `Model`.** `eleven_turbo_v2_5` is the latency-optimized
   variant. Non-turbo sounds slightly better but adds ~500 ms.
4. **System prompt length.** Every token in the prompt adds to LLM
   TTFT. Keep the system prompt terse.
5. **First-audio latency in TTS.** ElevenLabs streams — first chunk
   arrives in ~150–300 ms. If you swap to a non-streaming TTS, this
   becomes the dominant latency component.

### Naturalness levers

1. **TTS voice choice.** Most naturalness lives in the voice; pick a
   voice that fits the persona.
2. **System prompt detail.** Tell the LLM how to format replies
   ("keep under 30 words", "use contractions", "no markdown"). Voice
   replies with `*asterisks*` or numbered lists sound terrible.
3. **Deepgram `Model`.** `nova-2` and `nova-3` are best-in-class;
   older models are noticeably worse on noisy audio.

### Barge-in tuning

Barge-in is automatic — when Deepgram emits `SpeechStarted`, the
cascade cancels the in-flight LLM/TTS context and clears the module's
playback buffer. Two failure modes to watch for:

1. **Barge-in too sensitive.** Background noise from the caller's
   environment triggers speech-started, killing the reply mid-sentence.
   Fix: increase `EndpointingMs` (forces the caller to talk longer
   before speech-start fires), or use a custom VAD with debouncing.
2. **Barge-in too slow.** Caller talks for 500 ms before the bridge
   notices; the reply keeps playing. Fix: lower `EndpointingMs`, or
   add a custom VAD that emits `EventSpeechStarted` faster.

If your ASR provider doesn't emit `SpeechStarted`, the cascade never
barges in — every call will hear the full reply regardless of the
caller talking over it. Implement `EventSpeechStarted` in your custom
ASR if you want barge-in.

## Per-call resources

The cascade builds one ASR, runs one LLM call per turn, and builds
one TTS per reply. All per-call state lives in a `callState` struct on
a `sync.Map` keyed by `*Session`:

```go
type callState struct {
    asr     ASR
    history []Message
    logger  *slog.Logger

    mu       sync.Mutex
    cancel   context.CancelFunc  // cancels in-flight reply
    speaking bool                // reply playback in progress
    turn     int                 // increments per reply turn
}
```

If you wrap or extend the cascade, keep this map pattern. The session
pointer is stable for the call's lifetime, so it's a safe key.

## Logging from a stage

Stages don't get a logger passed in. Log from the cascade or your
handler, not from inside an ASR/LLM/TTS implementation. The cascade's
`Logger` is already wired with the session id; let it handle the per-call
fields. See [`logging.md`](./logging.md) for the conventions.

If you absolutely need logs from inside a stage, add a logger to your
factory closure and capture `s.ID()` in your constructor if you have
access. Most stage implementations don't need it — the cascade logs
the events that matter (`barge-in: flushing playback`, `final
transcript`, `LLM failed`).

## Common pitfalls

- **Reusing one ASR across calls.** Each call needs its own ASR
  connection. Use `ASRFactory` — the cascade calls it once per
  `OnStart`.
- **Forgetting to `Close()` your ASR.** Leaks provider connections.
  The cascade handles this in `OnEnd` — verify your custom ASR's
  `Close` is idempotent (the bundled Deepgram one uses `sync.Once`).
- **Returning a non-empty LLM string with whitespace.** `reply ==
  ""` is the cascade's "skip this turn" signal. Returning `"  \n"`
  will cause TTS to synthesize silence and play it — surprising and
  hard to debug. Trim before returning.
- **TTS that doesn't honor `ctx` cancellation.** The cascade cancels
  ctx on barge-in. If your TTS ignores it, synthesis continues to
  produce chunks that get written to a session that's already
  terminated, causing errors on `SendAudio`.
- **Mixing sample rates.** The cascade passes `s.SampleRate()` to your
  factories. Make sure your TTS emits PCM at the rate you asked for
  (or resamples). The module's playback buffer resamples, but it
  costs CPU and produces artifacts at the boundaries.

## See also

- [`pipeline/`](../pipeline/) — interfaces, cascade, mocks.
- [`providers/`](../providers/) — Deepgram, OpenAI, ElevenLabs.
- [`cmd/fsbridge/main.go`](../cmd/fsbridge/main.go) — `-mode ai`
  wiring with env vars.
- [`bridge-handler.md`](./bridge-handler.md) — for when you want a
  custom handler instead of the cascade.
- [`agent-apps.md`](./agent-apps.md) — for when you want the cascade
  in a separate process (with `examples/voicebot`).
