/*
 * mod_ws_bridge - Bidirectional WebSocket audio bridge for FreeSWITCH
 *
 * Derived from mod_audio_stream (MIT License, Copyright AMSOFTSWITCH LTD),
 * extended with full-duplex playback: downlink audio received over the
 * WebSocket is injected into the channel via SMBF_WRITE_REPLACE.
 */
#ifndef MOD_WS_BRIDGE_H
#define MOD_WS_BRIDGE_H

#include <switch.h>
#include <speex/speex_resampler.h>

#define MY_BUG_NAME "ws_bridge"
#define MAX_SESSION_ID (256)
#define MAX_WS_URI (4096)
#define MAX_METADATA_LEN (8192)

/* Playback buffer cap: 10 seconds of PCM at the streaming rate. */
#define MAX_PLAY_BUFFER_MS (10000)

#define EVENT_CONNECT           "mod_ws_bridge::connect"
#define EVENT_DISCONNECT        "mod_ws_bridge::disconnect"
#define EVENT_ERROR             "mod_ws_bridge::error"
#define EVENT_JSON              "mod_ws_bridge::json"

typedef void (*responseHandler_t)(switch_core_session_t* session, const char* eventName, const char* json);

struct private_data {
    switch_mutex_t *mutex;
    char sessionId[MAX_SESSION_ID];
    SpeexResamplerState *resampler;        /* uplink: channel rate -> stream rate */
    responseHandler_t responseHandler;
    void *pAudioStreamer;
    char ws_uri[MAX_WS_URI];
    int sampling;                          /* stream (websocket) sample rate */
    int channels;
    int audio_paused:1;
    int close_requested:1;
    int cleanup_started:1;
    char initialMetadata[MAX_METADATA_LEN];
    switch_buffer_t *sbuffer;              /* uplink chunk assembly buffer */
    int rtp_packets;

    /* downlink (playback) state */
    switch_mutex_t *play_mutex;
    switch_buffer_t *play_buffer;          /* PCM at stream rate, as received */
    switch_buffer_t *dl_stash;             /* PCM resampled to channel write rate */
    SpeexResamplerState *dl_resampler;     /* stream rate -> channel write rate */
    int dl_rate;                           /* channel write rate, learned from frames */
    uint32_t underruns;
    uint32_t replaced;                     /* frames where playback audio was injected */
};

typedef struct private_data private_t;

enum notifyEvent_t {
    CONNECT_SUCCESS,
    CONNECT_ERROR,
    CONNECTION_DROPPED,
    MESSAGE
};

#endif //MOD_WS_BRIDGE_H
