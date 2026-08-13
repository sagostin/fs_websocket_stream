# Recordings: fetch API and post-call pipelines

Every call that hits fsbridge with `-record-dir` set is captured
automatically, regardless of mode (`echo`, `mock`, `ai`, `agent`),
because the recorder tap sits below the handler. Each call produces a
self-contained bundle on disk; this doc covers the bundle format, the
HTTP fetch API, retention, and how to build post-call pipelines.

For the high-level overview see the top-level
[Recording section](../README.md#recording). For the implementation see
[`bridge/recorder.go`](../bridge/recorder.go) and
[`bridge/recordings.go`](../bridge/recordings.go).

## Enable

```sh
fsbridge -record-dir /var/spool/fsbridge/recordings
```

With `-record-dir`, every audio session is recorded. The recorder
launches a sweeper goroutine on `Start()` and finalizes bundles on
session end.

Optional flags:

| Flag | Default | Meaning |
|---|---|---|
| `-record-retention` | `24h` | Bundle age at which it becomes eligible for sweep. |
| `-record-max-mb` | `1024` | Max total disk usage in MiB. Oldest bundles swept first once exceeded. |

## Bundle layout

```
<record-dir>/
└── <fs-uuid>/                # the bundle directory
    ├── audio.wav             # stereo 16-bit L16 PCM: caller=L, AI=R
    ├── meta.json             # call metadata
    └── events.jsonl          # per-call event timeline
```

If the FS UUID is empty (rare — module didn't set the `?uuid=` query),
the bundle is named `session-<bridge-session-id>` instead. `sanitizeName`
strips anything outside `[A-Za-z0-9._-]` so the bundle name is safe to
use as a path component.

### `audio.wav`

Stereo 16-bit PCM at the session sample rate (typically 16 kHz mono
input → 16 kHz stereo output). Channels:

- **Left** = caller (uplink).
- **Right** = AI (downlink).

Both tracks advance at the session rate from session start. The shorter
(AI) track is silence-padded to the caller's length at finalize time
so playback is time-aligned. If your AI produces longer than the
caller (e.g. the AI keeps talking after the caller hung up), the tail
is truncated at finalize.

Implementation: [`bridge/recorder.go:238`](../bridge/recorder.go:238).
Recording assumes mono sessions; stereo (`mixed`) sessions record both
channels interleaved per track.

### `meta.json`

```jsonc
{
  "uuid":          "<fs-uuid>",
  "session":       "<bridge-session-id>",
  "rate":          16000,
  "mix":           "mono",
  "started":       "2025-08-13T14:23:11.456Z",
  "ended":         "2025-08-13T14:24:33.821Z",
  "duration_ms":   82365,
  "caller_bytes":  2635680,
  "agent_bytes":   2416640,
  "metadata":      { ... dialplan metadata, if any ... }
}
```

`metadata` is the JSON text frame the module sent as call metadata
(if any). It's whatever your dialplan passed to
`uuid_ws_bridge ... start`. Type: `json.RawMessage`.

### `events.jsonl`

Newline-delimited JSON, one event per line. Only events whose UUID
matches the call's UUID (or whose data carries the bridge session id)
are captured. Names follow the same vocabulary as `/control` events:

```jsonc
{"event":"call.answer","uuid":"...","data":{"caller":"1000","destination":"9999"}}
{"event":"audio.start","uuid":"...","data":{"session":"...","rate":16000,"mix":"mono"}}
{"event":"transcript","uuid":"...","data":{"text":"hi","final":false}}
{"event":"transcript","uuid":"...","data":{"text":"hi there","final":true}}
{"event":"speech.start","uuid":"..."}
{"event":"audio.end","uuid":"...","data":{"session":"..."}}
{"event":"recording.complete","uuid":"...","data":{"path":"/var/.../recordings/...","session":"...","duration_ms":82365}}
```

This is the per-call log timeline. For external observers, fetch
`events.jsonl` and replay — you have everything that happened in order.

## HTTP fetch API

All routes are mounted by `Recorder.RegisterRoutes(mux)` (see
[`bridge/recordings.go:23`](../bridge/recordings.go:23)). Same HTTP
port as the bridge.

| Method | Path | Response |
|---|---|---|
| `GET` | `/recordings` | JSON array of `meta.json` objects, newest first |
| `GET` | `/recordings/{uuid}/audio.wav` | `audio/wav` |
| `GET` | `/recordings/{uuid}/meta.json` | `application/json` |
| `GET` | `/recordings/{uuid}/events.jsonl` | `application/x-ndjson` |
| `DELETE` | `/recordings/{uuid}` | `204 No Content` |

`{uuid}` is sanitized — anything outside `[A-Za-z0-9._-]` returns
404. The file name must be exactly one of `audio.wav`, `meta.json`,
`events.jsonl`; anything else returns 404. This blocks path traversal
and arbitrary file disclosure.

### Auth

When `-auth-token` is set, the `/recordings/...` routes are wrapped
with `bridge.AuthMiddleware`. Accepted:

- `Authorization: Bearer <token>`
- `?token=<token>` (for browsers)

The recording fetch API is the right place to require auth — recordings
contain both sides of every call. The audio plane (`/stream`) is **not**
covered by `-auth-token`; that one is authenticated via
`STREAM_EXTRA_HEADERS` on the FreeSWITCH side, terminated at the LB.

### `cmd/fsctl` doesn't cover recordings

`fsctl` is for `/control`. For `/recordings`, just `curl` — see the
recipes below.

## Subscribing to `recording.complete`

For post-call processing, subscribe to `/control` and react to the
`recording.complete` event:

```jsonc
{"event":"recording.complete","uuid":"...","data":{"path":"/var/spool/.../recordings/<uuid>","session":"...","duration_ms":82365}}
```

The `path` is the bundle directory. You can read `audio.wav`,
`meta.json`, and `events.jsonl` directly from it.

The canonical implementation of this pattern is
[`examples/s3uploader/`](../examples/s3uploader/) — ~120 lines, single
file, pushes every bundle to S3 as soon as it's complete. Read it
first; below is a condensed recipe.

### Recipe: post-call pipeline

```go
type recorderEvt struct {
    Event string         `json:"event"`
    UUID  string         `json:"uuid"`
    Data  map[string]any `json:"data"`
}

func main() {
    conn, _, _ := websocket.Dial(ctx, "ws://localhost:8090/control", nil)
    defer conn.Close(websocket.StatusNormalClosure, "")

    for {
        _, raw, err := conn.Read(ctx)
        if err != nil {
            return
        }
        var ev recorderEvt
        if json.Unmarshal(raw, &ev) != nil || ev.Event != "recording.complete" {
            continue
        }
        dir, _ := ev.Data["path"].(string)
        go processBundle(dir, ev.UUID)
    }
}

func processBundle(dir, uuid string) {
    // Walk the directory, upload each file to your destination.
    entries, _ := os.ReadDir(dir)
    for _, e := range entries {
        if e.IsDir() { continue }
        f, _ := os.Open(filepath.Join(dir, e.Name()))
        defer f.Close()
        uploadToS3(uuid+"/"+e.Name(), f) // or EvalAPI.UploadBundle, or...
    }
}
```

[`examples/s3uploader/main.go:91`](../examples/s3uploader/main.go:91)
shows the actual S3 path. Replace `s3.PutObject` with whatever sink you
have: an eval pipeline's API, a data lake, an SFTP to a long-term
archive, a support-tool webhook.

### Alternative: fetch on demand

If you don't need post-call processing, just fetch from disk:

```sh
# list recent calls
curl -s http://localhost:8090/recordings | jq '.[0:5]'

# download the audio for one call
curl -O http://localhost:8090/recordings/<uuid>/audio.wav

# download the event timeline
curl -O http://localhost:8090/recordings/<uuid>/events.jsonl
```

## Retention and sweep

The recorder runs a sweeper goroutine that runs every
`Recorder.SweepInterval` (default `1m`). On each pass:

1. Walks `<record-dir>`, summing each bundle's size and the latest
   mtime.
2. Sorts oldest first.
3. Deletes bundles whose latest mtime is older than `Retention` (or
   when total exceeds `MaxBytes`, oldest first).

This is best-effort — it doesn't run if the bridge is down, so plan
for an external cleanup job if you have hard retention requirements.

## When to delete

After you've successfully uploaded a bundle (or run it through your eval
pipeline), `DELETE` it from the bridge to free disk:

```sh
curl -X DELETE http://localhost:8090/recordings/<uuid>
```

204 No Content on success. The local file is gone; if your post-call
pipeline already copied it elsewhere, you've kept the bridge's disk
usage bounded.

In the `s3uploader` example, deletion isn't done automatically — the
sweeper handles it. Add the `DELETE` call if you want immediate
cleanup. Trade-off: deleting on upload success is faster cleanup but
riskier (a successful upload doesn't guarantee the S3 object is
durable yet); letting the sweeper handle it is safer.

## Reproducing a call

For support / debugging, given a call UUID, you can pull the full
record in three requests:

```sh
UUID=abc-123-def-456
curl -s http://localhost:8090/recordings/$UUID/meta.json    | jq .metadata
curl -s http://localhost:8090/recordings/$UUID/events.jsonl | jq -c '.event + " " + (.data.text // "")'
curl -O http://localhost:8090/recordings/$UUID/audio.wav
```

Now you have the call metadata, the full transcript with timing, and
the stereo recording. Open `audio.wav` in any audio player; left channel
is the caller, right channel is the AI.

## Common pitfalls

- **Forgetting to enable recording before launching.** Recording is
  silent — you won't see "recording is happening" anywhere. Check
  `<record-dir>` after a test call.
- **Mounting `/recordings` over NFS.** Works for reads but the
  recorder does many small appends during the call, which is slow on
  NFS. Local disk or a fast attached volume is better.
- **Treating `audio.wav` as a regular mono file.** It's stereo. Tools
  that expect mono will sound wrong or skip a channel. Use any audio
  editor that respects channels.
- **Listening to `events.jsonl` in a tool that requires valid JSON per
  file.** It's newline-delimited; use `jq -c .` line by line, or open
  in a tool that understands NDJSON.
- **Running on a host with `tmpfs` for `/var`.** You will lose all
  recordings on reboot. Mount a persistent volume.

## See also

- [`bridge/recorder.go`](../bridge/recorder.go) — recorder
  implementation, retention sweep.
- [`bridge/recordings.go`](../bridge/recordings.go) — HTTP route
  registration.
- [`examples/s3uploader/`](../examples/s3uploader/) — canonical
  post-call pipeline implementation.
- [`logging.md`](./logging.md) — how `events.jsonl` events are
  produced (bus subscription).
- [`control-plane.md`](./control-plane.md) — `recording.complete` is
  delivered on `/control`.
