# Deploying fsbridge

Everything needed to run the stack in production: components, ports, the
full `fsbridge` flag/env reference, FreeSWITCH-side configuration, and a
production checklist. For architecture and scaling *phases*, see the
top-level [README](../README.md#scaling-topologies); this doc is the
concrete "how do I run it" guide.

## Components and ports

```
SIP phones / carriers
      │ SIP 5060 (udp+tcp), RTP 16384-16484/udp
      ▼
FreeSWITCH + mod_ws_bridge
      │  ws(s):// per call (uplink/downlink PCM)      ──▶ fsbridge :8080 /stream
      ◀── ESL :8021 (call control, channel events) ──       :8080 /control ◀── your app
                                                            :8080 /healthz
                                                            :8080 /recordings/*
      fsbridge ──ws:// per call──▶ agent app (agent mode)
```

| Component | Port | Purpose |
|---|---|---|
| FreeSWITCH SIP | 5060 udp/tcp | SIP signaling |
| FreeSWITCH RTP | 16384–16484 udp | Media |
| FreeSWITCH ESL | 8021 tcp | Event socket — fsbridge connects *inbound* here |
| fsbridge HTTP | 8080 (compose maps 8090→8080) | `/stream`, `/control`, `/healthz`, `/recordings` |
| Agent app | yours (e.g. 9000) | fsbridge dials *outbound* to it, one WS per call |

Only the fsbridge HTTP port and the FreeSWITCH SIP/RTP ports need to be
reachable from outside your network. ESL must be reachable **by fsbridge**,
never by the public internet.

## Run with Docker Compose (quickest)

```sh
docker compose -f examples/freeswitch/docker-compose.yml build   # first build compiles FreeSWITCH — slow
docker compose -f examples/freeswitch/docker-compose.yml up
```

This starts:

- **freeswitch** — FreeSWITCH 1.10 built from source with `mod_ws_bridge`,
  the 9999 dialplan, and an ESL config that listens on `0.0.0.0:8021`
  (ACL-restricted to loopback + private nets).
- **fsbridge** — the Go bridge from `examples/freeswitch/Dockerfile.fsbridge`,
  defaulting to `-mode echo` on `:8080`, published as `localhost:8090`.

Then run an agent app (agent mode) and configure the bridge flags per the
reference below. For a bare-metal FreeSWITCH, build just the module:

```sh
cd module && mkdir build && cd build
export PKG_CONFIG_PATH=/usr/local/freeswitch/lib/pkgconfig
cmake -DCMAKE_BUILD_TYPE=Release ..
make && sudo make install
# then: <load module="mod_ws_bridge"/> in modules.conf.xml
```

## `fsbridge` flag reference

| Flag | Default | Purpose |
|---|---|---|
| `-addr` | `:8080` | HTTP listen address (all endpoints share one server) |
| `-path` | `/stream` | WebSocket audio endpoint path |
| `-mode` | `echo` | Handler: `echo` \| `mock` \| `ai` \| `agent` |
| `-agent-url` | — | Agent app WS URL, e.g. `ws://agent:9000/call` (**required** for `-mode agent`) |
| `-system` | generic prompt | System prompt (`ai` mode) |
| `-esl-addr` | — | FreeSWITCH event socket, e.g. `localhost:8021`. Enables the control plane, agent hangup, and `uuid_dump` call context on agent `start` frames |
| `-esl-password` | `ClueCon` | ESL password |
| `-control-path` | `/control` | Control WS endpoint (requires `-esl-addr`) |
| `-record-dir` | — | Per-call recording bundles directory (enables recording + `/recordings` API) |
| `-record-retention` | `24h` | How long to keep bundles |
| `-record-max-mb` | `1024` | Max total recording size; oldest deleted first |
| `-auth-token` | — | If set, require `Authorization: Bearer <token>` on `/control` and `/recordings` |
| `-loki-url` | — | Loki push URL; enables log shipping |
| `-loki-job` | `fsbridge` | Loki `job` label |
| `-loki-tenant` | — | Loki `X-Scope-OrgID` (Grafana Cloud) |
| `-loki-user` / `-loki-pass` | — | Loki basic auth |

### Environment variables (`ai` mode)

| Variable | Purpose |
|---|---|
| `DEEPGRAM_API_KEY` | Deepgram ASR |
| `OPENAI_API_KEY` | OpenAI LLM |
| `OPENAI_MODEL` | Optional model override |
| `ELEVENLABS_API_KEY` | ElevenLabs TTS |
| `ELEVENLABS_VOICE_ID` | ElevenLabs voice |

`agent` mode needs no provider keys — your agent app owns the AI stack.

## FreeSWITCH-side configuration

Three files, all provided under `examples/freeswitch/` and mounted by the
compose file:

1. **Module** — `<load module="mod_ws_bridge"/>` in `modules.conf.xml`.
2. **Dialplan** — `dialplan-9999.xml` in `conf/dialplan/default/`. Two
   extensions:
   - `ws_bridge_9999` — inbound calls to **9999**: answers, streams audio to
     the bridge, plays endless silence to keep the write path alive.
   - `ws_bridge_9999_agent_call` — calls carrying the `ws_bridge_metadata`
     channel variable (set by `/control` `originate`): forwards that JSON
     verbatim as stream metadata so the agent app receives its call context.
3. **Event socket** — `event_socket.conf.xml` listens on `0.0.0.0:8021`;
   `acl.conf.xml` restricts it to loopback + container/private networks.
   **Tighten this for production** (your fsbridge host's IP only) — ESL is
   full administrative control of the switch.

The dialplan's `endless_playback` of silence is load-bearing: a parked
channel produces no outbound media, and the module's downlink injection
only runs while the write path is active. See
[README — Notes & caveats](../README.md#notes--caveats).

## HTTP endpoints

| Endpoint | Auth | Purpose |
|---|---|---|
| `GET/WS /stream` | LB/ACL (see below) | Per-call audio WebSocket from `mod_ws_bridge` |
| `WS /control` | Bearer if `-auth-token` | Commands (`originate`, `hangup`, `transfer`, `hold`, `dtmf`, `clear_playback`) + event stream |
| `GET /healthz` | none | `{"ok":true,"sessions":N}` — wire to your LB health check |
| `GET /recordings/*` | Bearer if `-auth-token` | Recording bundle listing/download (requires `-record-dir`) |

`/stream` is intentionally not token-protected by the bridge (the module
doesn't speak bearer auth). Protect it at the LB — ACL to your FreeSWITCH
IPs — or pass `STREAM_EXTRA_HEADERS` from the dialplan and enforce at the
LB. `examples/lb/nginx.conf` shows both.

## TLS and load balancing

Recommended: terminate TLS at an LB (`wss://` outside → `ws://` inside).
`examples/lb/nginx.conf` is a working config: WebSocket upgrade headers,
long `proxy_read_timeout`, and `hash $arg_uuid consistent` so each call
lands on a stable bridge instance (required for `clear_playback` and other
session-local operations). Details and the no-LB alternative (module-side
TLS, `STREAM_TLS_*` vars) are in [README — TLS & load balancing](../README.md#tls--load-balancing).

## Lifecycle

- **Startup**: fsbridge connects to ESL with jittered exponential backoff
  and re-subscribes after every reconnect; `/control` and agent `hangup`
  return an error while ESL is down, but `/stream` keeps serving.
- **Shutdown**: on SIGTERM/SIGINT the server stops accepting new
  connections, drains active sessions (handlers run `OnEnd`, the recorder
  finalizes bundles), waits ~2s, then closes ESL and the recorder.
- **Deploys**: point the LB health check at `/healthz`; for a clean drain,
  remove the instance from the pool, poll `/healthz` until `sessions` is 0,
  then stop the process.

## Observability

- **Logs** — structured `slog` on stderr; every per-call record carries the
  FreeSWITCH `uuid`. Ship to Loki with `-loki-url` (+ `-loki-job`,
  `-loki-tenant`, `-loki-user`, `-loki-pass`); see [logging.md](./logging.md).
- **Events** — `call.*`, `audio.*`, `transcript`, `recording.complete` on
  `/control`; see [control-plane.md](./control-plane.md#events).
- **Metrics** — Prometheus-style metrics are not implemented yet; the
  per-call seams (session counts in `/healthz`, bus events) are the
  interim answer.

## Production checklist

- [ ] `-auth-token` set; LB TLS termination in front of fsbridge.
- [ ] `/stream` ACL'd to FreeSWITCH hosts only (LB or firewall).
- [ ] ESL ACL restricted to fsbridge hosts (`acl.conf.xml`), non-default
      password.
- [ ] `-record-dir` on persistent, monitored storage if recording;
      `-record-retention` / `-record-max-mb` tuned.
- [ ] `/healthz` wired to LB health checks; drain procedure rehearsed.
- [ ] Loki shipping enabled; `uuid`-correlated log queries verified.
- [ ] Agent app behind an LB with WebSocket upgrade + long read timeout
      (agent mode).
- [ ] Outbound dialing tested end-to-end: `originate` → `reply.uuid` →
      `call.answer` → agent `start` frame with `direction:"outbound"` and
      your `metadata` in the `metadata` frame.

## See also

- [README — Scaling topologies](../README.md#scaling-topologies) — when and how to run multiple bridge replicas.
- [README — Operational edge cases](../README.md#operational-edge-cases) — transfer caveats, WS drops, agent dial failure.
- [agent-apps.md](./agent-apps.md) — the agent protocol, placing calls, routing hooks.
- [control-plane.md](./control-plane.md) — full command/event reference.
