# mod_ws_bridge

Bidirectional WebSocket audio bridge module for FreeSWITCH.

Derived from [mod_audio_stream](https://github.com/amigniter/mod_audio_stream)
(MIT, AMSOFTSWITCH LTD), extended with full-duplex playback: audio received
from the WebSocket server is injected into the call via `SMBF_WRITE_REPLACE`
with a bounded, resampling playback buffer.

## API

```
uuid_ws_bridge <uuid> start <ws-url> <mono|mixed|stereo> <8000|16000> [metadata]
uuid_ws_bridge <uuid> stop [metadata]
uuid_ws_bridge <uuid> pause | resume
uuid_ws_bridge <uuid> send_text <text>
```

## Wire protocol

- **Module -> bridge**: optional JSON metadata text frame first, then binary
  L16 PCM frames at the negotiated rate. The module appends
  `?rate=<hz>&mix=<mono|mixed|stereo>&uuid=<call-uuid>` query params to the
  URL automatically, so the bridge knows the audio format and can correlate
  the audio session with the FreeSWITCH call (call control / ESL events).
- **Bridge -> module**: binary L16 PCM frames are played to the caller;
  text frame `{"type":"clear"}` flushes the playback buffer (barge-in).

## Build

Requires FreeSWITCH development headers (`libfreeswitch-dev`) plus
`libspeexdsp-dev`, `libevent-dev`, `zlib1g-dev`, `libssl-dev`.

```sh
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make && make install
```

Or use `build-mod-ws-bridge.sh`. A `cpack -G DEB` package can be produced
after building.

For `wss://` (TLS) endpoints, build with `-DUSE_TLS=ON` and install
`libssl-dev`; tune with the `STREAM_TLS_*` channel variables. Usually you
will instead terminate TLS at a load balancer in front of the bridge (see
`examples/lb/nginx.conf` in the repository root).

## Events

`mod_ws_bridge::connect`, `mod_ws_bridge::disconnect`,
`mod_ws_bridge::error`, `mod_ws_bridge::json` (non-control text frames).
