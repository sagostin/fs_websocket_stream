# fs_websocket_stream

An open, bidirectional audio bridge between **FreeSWITCH** and AI voice
pipelines (ASR → LLM → TTS).

Two components:

| Component | What it is |
|---|---|
| `module/` | **`mod_ws_bridge`** — a FreeSWITCH `.so` module. Streams call audio to a WebSocket as binary L16 PCM, and injects audio received back from the WebSocket into the call (true full-duplex, via `SMBF_WRITE_REPLACE` + a bounded, resampling playback buffer). |
| `bridge/`, `pipeline/`, `providers/`, `cmd/fsbridge` | a **Go** WebSocket server + pluggable AI pipeline **+ call control plane**. Importable as a library or run as a standalone binary. |

The module is derived from the MIT-licensed community edition of
[mod_audio_stream](https://github.com/amigniter/mod_audio_stream), extended
with the downlink playback half (a feature that upstream only ships in its
closed-source commercial edition, capped at 10 concurrent channels). No such
limits here.

## Documentation

How-to docs for building on top of `fs_websocket_stream`. The body of
this README is the architecture and reference; [`docs/`](./docs/) is the
"how do I implement X" layer — concrete patterns, working code snippets,
and pointers to the canonical implementations in [`examples/`](./examples/).

| Doc | When you need it |
|---|---|
| [`docs/deployment.md`](./docs/deployment.md) | Deploy for real — Docker/compose, full flag & env reference, FreeSWITCH-side config, TLS/LB, health checks, production checklist. |
| [`docs/bridge-handler.md`](./docs/bridge-handler.md) | Implement your own `bridge.Handler` (custom audio processing, transcript collection, integration with another system). |
| [`docs/agent-apps.md`](./docs/agent-apps.md) | Build an external AI voice-agent service (your own voicebot, an agent framework integration, a multi-language service). |
| [`docs/logging.md`](./docs/logging.md) | Per-call logging strategy — `slog` field conventions, log correlation by FreeSWITCH UUID, Loki shipping, publishing custom events. |
| [`docs/recordings.md`](./docs/recordings.md) | Fetch recorded call bundles, build post-call pipelines (S3, eval, support UI), retention, auth. |
| [`docs/control-plane.md`](./docs/control-plane.md) | Drive call control from an external app — originate, hangup, transfer, hold, DTMF, clear playback — and consume the event stream. |
| [`docs/pipeline-stages.md`](./docs/pipeline-stages.md) | Swap in custom ASR/LLM/TTS implementations, tune the cascade (barge-in, system prompt, Deepgram endpointing). |

Inspired by
[this blog post](https://www.cyberpunk.tools/jekyll/update/2025/11/18/add-ai-voice-agent-to-freeswitch.html).

## Architecture

The bridge is the glue between telephony and your application: it owns SIP
and media, and exposes audio, call control and call state in a clean form so
an external AI voice-agent app never touches SIP.

```
AI voice agent app (external)
   │  control: originate / hangup / transfer / hold / dtmf   (WS, /control)
   │  events:   call state, audio sessions, transcripts      (WS, /control)
   ▼
fsbridge (Go)
   ├── audio plane:   WS full-duplex L16 PCM        ← mod_ws_bridge
   ├── control plane: ESL (event socket) → FreeSWITCH
   └── event bus:     ESL channel events + pipeline transcripts
   ▼
FreeSWITCH ↔ SIP trunks / carriers / phones
```

Every audio session is correlated with its FreeSWITCH call UUID (the module
sends it as the `uuid` query param), so `call.*`, `audio.*`, `transcript`
and `speech.start` events can all be joined by UUID.

## Wire protocol

One WebSocket connection per call.

**Module → bridge**

- Optional first text frame: JSON call metadata (the `metadata` argument to
  `uuid_ws_bridge ... start`).
- Binary frames: raw L16 PCM (16-bit signed LE) at the negotiated rate
  (20 ms chunks by default; `STREAM_BUFFER_SIZE` adjusts). The module appends
  `?rate=<hz>&mix=<mono|mixed|stereo>&uuid=<call-uuid>` to the URL query so
  the server knows the format and the call correlation ID.

**Bridge → module**

- Binary frames: raw L16 PCM at the same rate — played to the caller. The
  module buffers (10 s cap, drop-oldest) and resamples to the channel rate.
- Text frames: JSON control. `{"type":"clear"}` flushes the playback buffer
  immediately (barge-in). Any other JSON is surfaced as a
  `mod_ws_bridge::json` FreeSWITCH event.

## Quickstart

### 1. Build & run FreeSWITCH with the module

```sh
docker compose -f examples/freeswitch/docker-compose.yml build
docker compose -f examples/freeswitch/docker-compose.yml up
```

(This builds FreeSWITCH 1.10 from source — first build takes a while — and
compiles `mod_ws_bridge` against it. The dialplan routes extension **9999**
to the bridge.)

To build just the module on a machine with FreeSWITCH dev headers:

```sh
cd module
mkdir build && cd build
export PKG_CONFIG_PATH=/usr/local/freeswitch/lib/pkgconfig  # if FS built from source
cmake -DCMAKE_BUILD_TYPE=Release ..
make && sudo make install
```

Then `<load module="mod_ws_bridge"/>` in `modules.conf.xml`.

### 2. Run the bridge

```sh
go run ./cmd/fsbridge -addr :8090 -mode echo   # or: mock | ai | agent
```

Modes:

- **echo** — caller audio is returned unchanged (validates full-duplex transport).
- **mock** — mock ASR/LLM/TTS cascade; a tone reply, no API keys needed.
- **ai** — Deepgram ASR → OpenAI LLM → ElevenLabs TTS *in-process*. Env:
  `DEEPGRAM_API_KEY`, `OPENAI_API_KEY` (+ optional `OPENAI_MODEL`),
  `ELEVENLABS_API_KEY`, `ELEVENLABS_VOICE_ID`.
- **agent** — forward each call to an **external agent app** (below). This is
  the mode for running your own voice-agent application as a separate service.

Enable the control plane by pointing the bridge at FreeSWITCH's event socket:

```sh
go run ./cmd/fsbridge -addr :8090 -mode agent \
  -agent-url ws://localhost:9000/call \
  -esl-addr localhost:8021 -esl-password ClueCon
```

### 3. Call 9999

Register any SIP phone (or `originate loopback/9999 ...` from `fs_cli`) and
dial **9999**.

## External agent apps (agent mode)

In agent mode fsbridge does **not** run an AI pipeline itself. Instead, for
every call it dials out to your application — one WebSocket per call — and
forwards audio and lifecycle events. Your app owns the AI logic; fsbridge
owns telephony. This is the self-hosted equivalent of Twilio Media Streams.

```sh
go run ./examples/voicebot -mode mock   # reference agent app
```

### Agent protocol

fsbridge connects to `-agent-url` with `?uuid=<fs-call-uuid>&session=<id>&rate=16000&mix=mono`.

**fsbridge → agent**

```jsonc
{"type":"start","uuid":"...","session":"...","rate":16000,"mix":"mono",
 "caller":"+15551234567","destination":"9999","direction":"inbound"}  // context via ESL uuid_dump, when -esl-addr is set
{"type":"metadata","data":{...}}   // call metadata from the dialplan, if any
// binary frames: caller audio, L16 PCM
{"type":"stop"}                    // call/stream ended
```

**agent → fsbridge**

```jsonc
// binary frames: AI audio (L16 PCM) to play to the caller
{"type":"clear"}                                // flush caller playback (barge-in)
{"type":"hangup"}                               // end the call (needs -esl-addr)
{"type":"event","name":"transcript","data":{...}} // republished to /control subscribers
```

`examples/voicebot` is a complete reference implementation: per-call state,
barge-in, conversation history, and the same ASR/LLM/TTS stages (Deepgram /
OpenAI / ElevenLabs, or mocks) running as a standalone service. Flags:
`-addr :9000`, `-path /call`, `-mode mock|ai`.

### Placing outbound calls as an agent

The agent app (or a sidecar) sends `originate` on `/control`; the reply
carries the new call's `uuid`. When the called leg lands on the AI
extension and answers, fsbridge dials your agent exactly like an inbound
call — the `start` frame's `direction` is `outbound` and `destination` is
the dialed number, and any `metadata` you passed to `originate` arrives in
the `metadata` frame. Full walkthrough:
[docs/agent-apps.md](docs/agent-apps.md#placing-calls-as-an-agent).

### Choosing in-process (`ai`) vs external (`agent`) mode

- **ai**: simplest deployment — one binary, pipeline configured by env vars.
  Good for a fixed provider stack.
- **agent**: your agent is its own service with its own release cycle,
  language, scaling, and business logic (tools, RAG, CRM lookups, call
  transfers). fsbridge stays a dumb, stable telephony relay.

## Control plane (`/control`)

When `-esl-addr` is set, the bridge exposes a second WebSocket endpoint for
the external application: call control commands in, call/pipeline events
out. One JSON message per frame.

**Commands** (each gets a reply with the same `id`):

```jsonc
// Place a call. dest is any FS dial string; the called leg lands on
// extension ext (default 9999, i.e. the AI bridge) in context default.
// The reply carries the new call's uuid; vars/metadata are set as channel
// variables (metadata reaches the agent via the dialplan's metadata arg).
{"id":"1","cmd":"originate","args":{"dest":"sofia/gateway/trunk/18005551212","ext":"9999","cid":"15551234567",
  "vars":{"account":"acme"},"metadata":"{\"customer_id\":\"42\"}"}}
// ...or run an app instead of hunting the dialplan:
{"id":"1","cmd":"originate","args":{"dest":"loopback/9999","app":"park"}}

{"id":"2","cmd":"hangup","args":{"uuid":"<call-uuid>"}}
{"id":"3","cmd":"transfer","args":{"uuid":"<call-uuid>","dest":"1002"}}
{"id":"4","cmd":"hold","args":{"uuid":"<call-uuid>"}}
{"id":"5","cmd":"unhold","args":{"uuid":"<call-uuid>"}}
{"id":"6","cmd":"dtmf","args":{"uuid":"<call-uuid>","digits":"1234"}}
{"id":"7","cmd":"clear_playback","args":{"uuid":"<call-uuid>"}}  // flush buffered AI audio
```

**Events** pushed to every connected control client:

```jsonc
{"event":"call.create"|"call.answer"|"call.hangup"|"call.destroy"|"call.bridge"|"call.unbridge",
 "uuid":"...", "data":{"caller":"...","destination":"...","direction":"...","hangup_cause":"..."}}
{"event":"audio.start"|"audio.end","uuid":"...","data":{"session":"...","rate":16000,"mix":"mono"}}
{"event":"speech.start","uuid":"..."}                                   // barge-in signal
{"event":"transcript","uuid":"...","data":{"text":"...","final":true}}  // from the pipeline
```

`cmd/fsctl` is a tiny CLI client for this endpoint (handy for testing):

```sh
go run ./cmd/fsctl originate loopback/9999 park
go run ./cmd/fsctl hangup <uuid>
go run ./cmd/fsctl events                     # stream events (-follow 30s to bound)
```

For library use, the pieces are composable: `bridge.ESLClient` (event
socket client with reconnect), `bridge.EventBus`, `bridge.ControlServer`,
and `bridge.ESLToBus`.

## Using the Go bridge as a library

```go
handler := &pipeline.Cascade{
    NewASR: myASRFactory,          // pipeline.ASRFactory
    LLM:    myLLM,                 // pipeline.LLM
    NewTTS: myTTSFactory,          // pipeline.TTSFactory
    SystemPrompt: "You are a helpful voice assistant.",
}
srv := bridge.NewServer(handler, nil)
srv.ListenAndServe(ctx, ":8080")
```

Or implement `bridge.Handler` directly for full control
(`OnStart / OnAudio / OnText / OnEnd`, with `Session.SendAudio`,
`Session.ClearPlayback`, `Session.SendControl`).

## FreeSWITCH API

```
uuid_ws_bridge <uuid> start <ws-url> <mono|mixed|stereo> <8000|16000> [metadata]
uuid_ws_bridge <uuid> stop [metadata]
uuid_ws_bridge <uuid> pause | resume          # pause/resume uplink
uuid_ws_bridge <uuid> send_text <text>        # send a text frame to the bridge
```

Channel variables (same semantics as mod_audio_stream):
`STREAM_BUFFER_SIZE`, `STREAM_HEART_BEAT`, `STREAM_SUPPRESS_LOG`,
`STREAM_MESSAGE_DEFLATE`, `STREAM_EXTRA_HEADERS`, `STREAM_TLS_*`.

Events: `mod_ws_bridge::connect` / `::disconnect` / `::error` / `::json`.

## Recording

Enable with `-record-dir <dir>`; every call is captured automatically, in
any mode (echo/mock/ai/agent), because the tap sits below the handler.

Each call produces a bundle at `<record-dir>/<call-uuid>/`:

| File | Contents |
|---|---|
| `audio.wav` | Stereo 16-bit WAV at the session rate — **caller = left, AI = right**. The AI track is silence-padded to call length, so playback is time-aligned. |
| `meta.json` | uuid, session id, rate, mix, start/end, duration, dialplan metadata |
| `events.jsonl` | this call's bus events: transcripts, `speech.start`, `call.*` |

Fetch API (same HTTP port as the bridge):

```
GET    /recordings                     → JSON list of call metas
GET    /recordings/{uuid}/audio.wav    → stereo WAV
GET    /recordings/{uuid}/meta.json    → call metadata
GET    /recordings/{uuid}/events.jsonl → event timeline
DELETE /recordings/{uuid}              → remove a bundle
```

Retention: `-record-retention 24h` (default) and `-record-max-mb 1024`
(oldest bundles swept first).

**Post-call integration**: on finalize the bridge publishes a
`recording.complete` bus event (`{uuid, path, duration_ms}`) to `/control`
subscribers. `examples/s3uploader` shows the intended pattern — subscribe,
upload the bundle to S3 — which is equally the seam for eval pipelines,
data lakes, or a support/debug UI. Since everything is keyed by the call
UUID your agent already sees in its `start` frame, pulling the full debug
bundle for "call X" is a single lookup.

## TLS & load balancing

Recommended posture: **terminate TLS at a load balancer** in front of
fsbridge (`wss://` from FreeSWITCH/agents → LB → plain `ws://` internally).
`examples/lb/nginx.conf` is a working config: WebSocket upgrade headers,
`proxy_read_timeout` longer than your longest call, and
`hash $arg_uuid consistent` so each call's WebSocket lands on a stable
bridge instance.

If you need encryption directly between FreeSWITCH and the bridge (no LB),
rebuild the module with TLS (`cmake -DUSE_TLS=ON ...`) and use a `wss://`
URL; tune with the `STREAM_TLS_*` channel variables. Set
`STREAM_HEART_BEAT` (e.g. 30s) to keep idle-but-open calls alive through
proxies.

## Scaling topologies

**Phase 1 — one box**: 1 FreeSWITCH + 1 fsbridge on the same host (module
dials `ws://127.0.0.1:8090/stream`), agent app replicas behind an LB
(fsbridge dials the agent LB once per call). The bridge is a cheap relay;
FreeSWITCH CPU and ASR/TTS spend will bottleneck long before it.

**Phase 2 — bridge pool**: N fsbridge replicas behind the nginx config in
`examples/lb/nginx.conf`. Caveats:
- `clear_playback` and anything touching in-memory session state must
  reach the instance holding the call — the `$arg_uuid` hash handles this.
  Agent-issued `clear` needs no affinity (it travels over the call's own
  socket). ESL commands (`hangup`, `transfer`, ...) work from any instance.
- Each instance subscribes to ESL independently, so `call.*` events appear
  on every instance, but `audio.*`/`transcript` events exist only on the
  instance owning the call. Subscribe your app to all instances, or add a
  NATS/Redis fan-out later.

**Phase 3 — multi-node FreeSWITCH**: one fsbridge per FS node (localhost),
with events tagged by node. The ESL client layer is the seam for this; not
built yet.

## Operational edge cases

- **Transferring an AI call.** `uuid_transfer` keeps the media bug attached
  to the original channel, but the *write* path of the channel after
  transfer depends on the destination. Transferring to an extension that
  doesn't keep media alive (e.g. a pure SIP endpoint with no echo) will
  leave the caller hearing nothing. For clean handoffs, transfer to an
  app that keeps media producing (a queue, music-on-hold, or a
  continue-AI-with-this-call app). The robust pattern is
  `hangup + originate` rather than `transfer`.
- **WS drop mid-call.** The module's `ws_bridge` does not auto-reconnect;
  a dropped audio connection means the call continues silently until the
  bridge or FreeSWITCH hangs it up. The ESL hangup command is the
  recommended recovery — issue it from `/control` or an external monitor.
- **Agent dial failure.** If the agent app isn't reachable when a call
  starts, the audio session is closed (and recorded as `audio.end`); the
  FreeSWITCH dialplan continues — typically a parked call with no audio
  is the visible result.
- **Recording length.** The stereo WAV uses the caller track's length
  and silence-pads the AI track. If your AI produces longer than the
  caller (e.g., the AI keeps talking after the caller hung up), the
  tail is truncated at finalize.

## Notes & caveats

- **Keep the write path alive.** A parked FreeSWITCH channel produces no
  outbound media, so the module's injection would never run. The example
  dialplan plays endless silence (`endless_playback`) after answer; the
  module replaces those frames with AI audio. Any media-producing app works.
- **Barge-in**: the Go pipeline sends `{"type":"clear"}` when the ASR reports
  new speech; the module flushes its playback buffer instantly.
- **Hostname resolution**: libwsc's async DNS (evdns) can fail in some
  container/NAT setups. The module pre-resolves hostnames for plain `ws://`
  URLs before connecting, so this is handled; `wss://` keeps the hostname
  for SNI/certificate validation.
- **ESL reachability**: mod_event_socket binds loopback by default. In
  Docker, publish it with the provided `event_socket.conf.xml` (listens on
  0.0.0.0) and `acl.conf.xml` (allows loopback + private/container nets) —
  both mounted by `docker-compose.yml`. Restrict appropriately in production.
- **License**: MIT (module code derived from mod_audio_stream © AMSOFTSWITCH
  LTD; libwsc © AMSOFTSWITCH LTD — both MIT).

## Observability (Loki)

Set `-loki-url` (with `-loki-tenant` / `-loki-user` / `-loki-pass` as needed)
to ship every slog record to a Loki push endpoint. Logs continue to print
to stderr in parallel.

```sh
fsbridge -loki-url http://loki:3100 -loki-job fsbridge-prod
# Grafana Cloud:
fsbridge -loki-url https://logs-prod-eu-west-0.grafana.net -loki-tenant <tenant> -loki-user <id> -loki-pass <token>
```

Records are batched (default 100 entries / 5s flush) and pushed as one
`/loki/api/v1/push` request per batch with labels `{job, host, level}`.
Backpressure drops new entries rather than blocking log emission.

**`/healthz`** — `GET /healthz` returns `{ "ok": true, "sessions": N }`.
Wire to your load balancer's health check.

## Authentication

`-auth-token` enables shared-secret bearer auth on `/control` and
`/recordings` (no-op when unset). Accepted:

```
Authorization: Bearer <token>
?token=<token>   (for browsers)
```

`/stream` (the audio plane) is **not** auth-protected by this flag — the
module does not speak it. Use FreeSWITCH channel variables to add the token
to the module's outbound HTTP headers:

```
<action application="set" data="STREAM_EXTRA_HEADERS=Authorization:Bearer%20my-token"/>
```

Then terminate auth at the LB (recommended) or use the module-side headers
through it. See `examples/lb/nginx.conf` for an ACL example.

## Production considerations (not yet implemented)

Deliberately out of scope for now; the seams are in place for each:

- **Observability.** Loki log shipping is implemented; what remains is
  Prometheus-style metrics endpoints (active sessions, frames in/out,
  queue drops) and per-call latency histograms.
- **Graceful deploys.** On SIGTERM the server stops accepting new
  connections, drains active sessions (so the recorder finalizes bundles
  and handlers run their `OnEnd`), and closes the ESL connection. There's
  still no drain-time window passed at the LB (stop sending traffic, wait
  for `sessions: 0`, then redeploy).
- **Endpointing tuning.** Deepgram `endpointing`, VAD sensitivity and
  TTS pacing are the main latency/naturalness levers — see
  `DeepgramASRConfig`.

## Development

```sh
go test ./...          # Go unit tests (bridge protocol, cascade, barge-in)

# Module compile + live loopback test:
docker build -f examples/freeswitch/Dockerfile -t fs-ws-bridge .
docker run -d --name fs fs-ws-bridge
```
