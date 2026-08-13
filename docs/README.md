# Documentation

How-to docs for building on top of `fs_websocket_stream`. The top-level
[README](../README.md) covers architecture, wire protocol, and quickstart;
this folder is the second layer: concrete patterns, working code snippets,
and references to the canonical implementations in `examples/`.

## Pick a doc

| Doc | When you need it |
|---|---|
| [bridge-handler.md](./bridge-handler.md) | Implement your own `bridge.Handler` (custom audio processing, transcript collection, integration with another system). |
| [agent-apps.md](./agent-apps.md) | Build an external AI voice-agent service (e.g. your own voicebot, an agent framework integration, multi-language service). |
| [logging.md](./logging.md) | Wire per-call logs through `slog` and Loki, correlate by FreeSWITCH UUID, publish custom events to the bus. |
| [recordings.md](./recordings.md) | Fetch recorded call bundles, build post-call pipelines (S3, eval, support UI), set retention, delete bundles. |
| [control-plane.md](./control-plane.md) | Drive call control from an external app — originate, hangup, transfer, hold, DTMF, clear playback — and consume the event stream. |
| [pipeline-stages.md](./pipeline-stages.md) | Swap in custom ASR/LLM/TTS implementations, tune the cascade (barge-in, system prompt, Deepgram endpointing). |

## Canonical implementations

The docs reference these as the canonical, runnable patterns. Treat them
as the source of truth — docs describe the intent, code shows the reality.

| Reference | What it shows |
|---|---|
| [`examples/voicebot/`](../examples/voicebot/) | Full external agent app: per-call state, barge-in, ASR/LLM/TTS, conversation history. |
| [`examples/s3uploader/`](../examples/s3uploader/) | Subscribe to `/control`, on `recording.complete` upload the bundle to S3. |
| [`examples/lb/nginx.conf`](../examples/lb/nginx.conf) | Production-ready nginx config: TLS termination, per-call affinity, WebSocket upgrade. |
| [`cmd/fsctl/`](../cmd/fsctl/) | Tiny CLI client for the control plane — handy for shell scripting and ad-hoc testing. |
| [`bridge/echo.go`](../bridge/echo.go) | The minimal `bridge.Handler` (10 lines). |
| [`pipeline/cascade.go`](../pipeline/cascade.go) | The default `pipeline.Cascade` — ASR → LLM → TTS with barge-in. |
