/*
 * mod_ws_bridge - Bidirectional WebSocket audio bridge for FreeSWITCH
 *
 * Derived from mod_audio_stream (MIT License, Copyright AMSOFTSWITCH LTD).
 */
#ifndef WS_BRIDGE_GLUE_H
#define WS_BRIDGE_GLUE_H

#include "mod_ws_bridge.h"

#ifdef __cplusplus
extern "C" {
#endif

int validate_ws_uri(const char* url, char* wsUri);
switch_status_t is_valid_utf8(const char *str);
switch_status_t stream_session_init(switch_core_session_t *session,
                                    responseHandler_t responseHandler,
                                    uint32_t samples_per_second,
                                    char *wsUri,
                                    int sampling,
                                    int channels,
                                    char* metadata,
                                    void **ppUserData);
switch_bool_t stream_frame(switch_media_bug_t *bug);
switch_bool_t stream_playback_frame(switch_media_bug_t *bug);
switch_status_t stream_session_pauseresume(switch_core_session_t *session, int pause);
switch_status_t stream_session_send_text(switch_core_session_t *session, char* text);
switch_status_t stream_session_cleanup(switch_core_session_t *session, char* text, int channelIsClosing);

#ifdef __cplusplus
}
#endif

#endif //WS_BRIDGE_GLUE_H
