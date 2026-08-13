/*
 * mod_ws_bridge - Bidirectional WebSocket audio bridge for FreeSWITCH
 *
 * Derived from mod_audio_stream (MIT License, Copyright AMSOFTSWITCH LTD),
 * extended with full-duplex playback:
 *   - binary WebSocket frames from the server are buffered per-session and
 *     injected into the channel via SMBF_WRITE_REPLACE
 *   - {"type":"clear"} control frames flush the playback buffer (barge-in)
 */
#include <string>
#include <cstring>
#include <algorithm>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include "mod_ws_bridge.h"
#include "WebSocketClient.h"
#include <switch_json.h>
#include <switch_buffer.h>
#include <atomic>
#include <vector>
#include <memory>
#include <mutex>

#define FRAME_SIZE_8000  320 /* 1000x0.02 (20ms)= 160 x(16bit= 2 bytes) 320 frame size*/

class AudioStreamer {
public:
    // Factory
    static std::shared_ptr<AudioStreamer> create(
        private_t *tech_pvt, const char* wsUri, responseHandler_t callback, int deflate, int heart_beat,
        bool suppressLog, const char* extra_headers, const char* tls_cafile, const char* tls_keyfile,
        const char* tls_certfile, bool tls_disable_hostname_validation) {

        std::shared_ptr<AudioStreamer> sp(new AudioStreamer(
            tech_pvt, wsUri, callback, deflate, heart_beat,
            suppressLog, extra_headers, tls_cafile, tls_keyfile,
            tls_certfile, tls_disable_hostname_validation
        ));

        sp->bindCallbacks(std::weak_ptr<AudioStreamer>(sp));

        sp->client.connect();

        return sp;
    }

    ~AudioStreamer()= default;

    void disconnect() {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "disconnecting...\n");
        client.disconnect();
    }

    bool isConnected() {
        return client.isConnected();
    }

    void writeBinary(uint8_t* buffer, size_t len) {
        if(!this->isConnected()) return;
        client.sendBinary(buffer, len);
    }

    void writeText(const char* text) {
        if(!this->isConnected()) return;
        client.sendMessage(text, strlen(text));
    }

    void markCleanedUp() {
        m_cleanedUp.store(true, std::memory_order_release);
        client.setMessageCallback({});
        client.setBinaryCallback({});
        client.setOpenCallback({});
        client.setErrorCallback({});
        client.setCloseCallback({});
    }

    bool isCleanedUp() const {
        return m_cleanedUp.load(std::memory_order_acquire);
    }

private:
    // Ctor
    AudioStreamer(
        private_t *tech_pvt, const char* wsUri, responseHandler_t callback, int deflate, int heart_beat,
        bool suppressLog, const char* extra_headers, const char* tls_cafile, const char* tls_keyfile,
        const char* tls_certfile, bool tls_disable_hostname_validation
    ) : m_tech_pvt(tech_pvt), m_sessionId(tech_pvt->sessionId), m_notify(callback), m_suppress_log(suppressLog),
        m_extra_headers(extra_headers) {

        WebSocketHeaders hdrs;
        WebSocketTLSOptions tls;

        if (m_extra_headers) {
            cJSON *headers_json = cJSON_Parse(m_extra_headers);
            if (headers_json) {
                cJSON *iterator = headers_json->child;
                while (iterator) {
                    if (iterator->type == cJSON_String && iterator->valuestring != nullptr) {
                        hdrs.set(iterator->string, iterator->valuestring);
                    }
                    iterator = iterator->next;
                }
                cJSON_Delete(headers_json);
            }
        }

        client.setUrl(wsUri);

        // Setup TLS options
        // NONE - disables validation
        // SYSTEM - uses the system CAs bundle
        if (tls_cafile) {
            tls.caFile = tls_cafile;
        }

        if (tls_keyfile) {
            tls.keyFile = tls_keyfile;
        }

        if (tls_certfile) {
            tls.certFile = tls_certfile;
        }

        tls.disableHostnameValidation = tls_disable_hostname_validation;
        client.setTLSOptions(tls);

        // Optional heart beat, sent every xx seconds when there is not any traffic
        // to make sure that load balancers do not kill an idle connection.
        if(heart_beat)
            client.setPingInterval(heart_beat);

        // Per message deflate connection is enabled by default. You can tweak its parameters or disable it
        if(deflate)
            client.enableCompression(false);

        // Set extra headers if any
        if(!hdrs.empty())
            client.setHeaders(hdrs);
    }

    /* Append downlink PCM to the playback buffer, dropping the oldest audio
     * when the cap is reached so playback latency stays bounded. Called from
     * the libevent thread. */
    void onBinary(const void* data, size_t len) {
        private_t *tp = m_tech_pvt;
        if (!tp || !tp->play_buffer || tp->cleanup_started) return;

        switch_mutex_lock(tp->play_mutex);

        while (switch_buffer_freespace(tp->play_buffer) < len &&
               switch_buffer_inuse(tp->play_buffer) > 0) {
            uint8_t scratch[4096];
            switch_size_t inuse = switch_buffer_inuse(tp->play_buffer);
            switch_buffer_read(tp->play_buffer, scratch,
                               std::min((switch_size_t)sizeof(scratch), inuse));
        }
        if (switch_buffer_freespace(tp->play_buffer) >= len) {
            switch_buffer_write(tp->play_buffer, data, len);
        }

        switch_mutex_unlock(tp->play_mutex);
    }

    /* Flush all queued playback audio (barge-in). */
    void clearPlayback() {
        private_t *tp = m_tech_pvt;
        if (!tp) return;

        switch_mutex_lock(tp->play_mutex);
        if (tp->play_buffer) switch_buffer_zero(tp->play_buffer);
        if (tp->dl_stash) switch_buffer_zero(tp->dl_stash);
        if (tp->dl_resampler) speex_resampler_reset_mem(tp->dl_resampler);
        switch_mutex_unlock(tp->play_mutex);

        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
                          "(%s) playback buffer cleared\n", m_sessionId.c_str());
    }

    void bindCallbacks(std::weak_ptr<AudioStreamer> wp) {
        client.setMessageCallback([wp](const std::string& message) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            // Control frames: {"type":"clear"} flushes the playback buffer.
            cJSON *root = cJSON_Parse(message.c_str());
            if (root) {
                const char *type = cJSON_GetObjectCstr(root, "type");
                if (type && std::strcmp(type, "clear") == 0) {
                    self->clearPlayback();
                    cJSON_Delete(root);
                    return;
                }
                cJSON_Delete(root);
            }

            self->eventCallback(MESSAGE, message.c_str());
        });

        client.setBinaryCallback([wp](const void* data, size_t len) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;
            self->onBinary(data, len);
        });

        client.setOpenCallback([wp]() {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "connected");
            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECT_SUCCESS, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });

        client.setErrorCallback([wp](int code, const std::string& msg) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "error");
            cJSON* message = cJSON_CreateObject();
            cJSON_AddNumberToObject(message, "code", code);
            cJSON_AddStringToObject(message, "error", msg.c_str());
            cJSON_AddItemToObject(root, "message", message);

            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECT_ERROR, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });

        client.setCloseCallback([wp](int code, const std::string& reason) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "disconnected");
            cJSON* message = cJSON_CreateObject();
            cJSON_AddNumberToObject(message, "code", code);
            cJSON_AddStringToObject(message, "reason", reason.c_str());
            cJSON_AddItemToObject(root, "message", message);

            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECTION_DROPPED, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });
    }

    switch_media_bug_t *get_media_bug(switch_core_session_t *session) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        if(!channel) {
            return nullptr;
        }
        auto *bug = (switch_media_bug_t *) switch_channel_get_private(channel, MY_BUG_NAME);
        return bug;
    }

    inline void media_bug_close(switch_core_session_t *session) {
        auto *bug = get_media_bug(session);
        if(bug) {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            tech_pvt->close_requested = 1;
            switch_core_media_bug_close(&bug, SWITCH_FALSE);
        }
    }

    inline void send_initial_metadata(switch_core_session_t *session) {
        auto *bug = get_media_bug(session);
        if(bug) {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            if(tech_pvt && strlen(tech_pvt->initialMetadata) > 0) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                                          "sending initial metadata %s\n", tech_pvt->initialMetadata);
                writeText(tech_pvt->initialMetadata);
            }
        }
    }

    void eventCallback(notifyEvent_t event, const char* message) {
        std::string msg = message ? message : "";

        switch_core_session_t* psession = switch_core_session_locate(m_sessionId.c_str());
        if (!psession) {
            return;
        }

        switch (event) {
            case CONNECT_SUCCESS:
                send_initial_metadata(psession);
                m_notify(psession, EVENT_CONNECT, msg.c_str());
                break;

            case CONNECTION_DROPPED:
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO, "connection closed\n");
                m_notify(psession, EVENT_DISCONNECT, msg.c_str());
                break;

            case CONNECT_ERROR:
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO, "connection error: %s\n", msg.c_str());
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_DEBUG, "ws_uri was: %s\n", m_tech_pvt ? m_tech_pvt->ws_uri : "?");
                m_notify(psession, EVENT_ERROR, msg.c_str());
                media_bug_close(psession);
                break;

            case MESSAGE:
                m_notify(psession, EVENT_JSON, msg.c_str());

                if (!m_suppress_log) {
                    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_DEBUG,
                                    "response: %s\n", msg.c_str());
                }
                break;
        }

        switch_core_session_rwunlock(psession);
    }

private:
    private_t *m_tech_pvt;
    std::string m_sessionId;
    responseHandler_t m_notify;
    WebSocketClient client;
    bool m_suppress_log;
    const char* m_extra_headers;
    std::atomic<bool> m_cleanedUp{false};
};


namespace {

    switch_status_t stream_data_init(private_t *tech_pvt, switch_core_session_t *session, char *wsUri,
                                     uint32_t sampling, int desiredSampling, int channels, char *metadata, responseHandler_t responseHandler,
                                     int deflate, int heart_beat, bool suppressLog, int rtp_packets, const char* extra_headers,
                                     const char *tls_cafile, const char* tls_keyfile, const char *tls_certfile,
                                     bool tls_disable_hostname_validation)
    {
        int err; //speex

        switch_memory_pool_t *pool = switch_core_session_get_pool(session);

        memset(tech_pvt, 0, sizeof(private_t));

        strncpy(tech_pvt->sessionId, switch_core_session_get_uuid(session), MAX_SESSION_ID);

        /* Robustness: libevent's async evdns resolver fails outright in
         * some container/network setups (observed: EAGAIN under Docker
         * Desktop). Pre-resolve the host for plain ws:// URLs so the
         * connect uses a numeric address. wss:// keeps the hostname for
         * SNI/certificate validation. */
        std::string uri(wsUri);
        if (uri.rfind("ws://", 0) == 0) {
            const size_t hostStart = 5;
            const size_t hostEnd = uri.find_first_of(":/", hostStart);
            const std::string host = uri.substr(hostStart, hostEnd - hostStart);
            struct in_addr a4;
            if (!host.empty() && inet_pton(AF_INET, host.c_str(), &a4) != 1) {
                struct addrinfo hints{}, *res = nullptr;
                hints.ai_family = AF_INET;
                hints.ai_socktype = SOCK_STREAM;
                if (getaddrinfo(host.c_str(), nullptr, &hints, &res) == 0 && res) {
                    char ip[INET_ADDRSTRLEN];
                    auto *sa = (struct sockaddr_in*)res->ai_addr;
                    if (inet_ntop(AF_INET, &sa->sin_addr, ip, sizeof(ip))) {
                        uri.replace(hostStart, host.size(), ip);
                        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                                          "(%s) pre-resolved %s -> %s\n", tech_pvt->sessionId, host.c_str(), ip);
                    }
                    freeaddrinfo(res);
                }
            }
        }

        /* Append the negotiated stream parameters as query params so the
         * bridge knows how to interpret the PCM frames. */
        {
            const char *mix = channels == 2 ? "stereo" : "mono";
            uri += (uri.find('?') == std::string::npos) ? '?' : '&';
            uri += "rate=" + std::to_string(desiredSampling);
            uri += "&mix=";
            uri += mix;
            uri += "&uuid=";
            uri += tech_pvt->sessionId;
            strncpy(tech_pvt->ws_uri, uri.c_str(), MAX_WS_URI - 1);
        }

        tech_pvt->sampling = desiredSampling;
        tech_pvt->responseHandler = responseHandler;
        tech_pvt->rtp_packets = rtp_packets;
        tech_pvt->channels = channels;
        tech_pvt->audio_paused = 0;

        if (metadata) strncpy(tech_pvt->initialMetadata, metadata, MAX_METADATA_LEN);

        const size_t buflen = (FRAME_SIZE_8000 * desiredSampling / 8000 * channels * rtp_packets);

        auto sp = AudioStreamer::create(tech_pvt, tech_pvt->ws_uri, responseHandler, deflate, heart_beat,
                                        suppressLog, extra_headers, tls_cafile, tls_keyfile,
                                        tls_certfile, tls_disable_hostname_validation);

        tech_pvt->pAudioStreamer = new std::shared_ptr<AudioStreamer>(sp);

        switch_mutex_init(&tech_pvt->mutex, SWITCH_MUTEX_NESTED, pool);
        switch_mutex_init(&tech_pvt->play_mutex, SWITCH_MUTEX_NESTED, pool);

        if (switch_buffer_create(pool, &tech_pvt->sbuffer, buflen) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                "%s: Error creating switch buffer.\n", tech_pvt->sessionId);
            return SWITCH_STATUS_FALSE;
        }

        /* Downlink playback buffer: capped at MAX_PLAY_BUFFER_MS of PCM at
         * the streaming rate; dl_stash holds audio already resampled to the
         * channel write rate (sized for the same duration at up to 48kHz). */
        const size_t play_buflen = (size_t)desiredSampling * 2 * channels * (MAX_PLAY_BUFFER_MS / 1000);
        const size_t stash_buflen = (size_t)48000 * 2 * channels * (MAX_PLAY_BUFFER_MS / 1000);
        if (switch_buffer_create(pool, &tech_pvt->play_buffer, play_buflen) != SWITCH_STATUS_SUCCESS ||
            switch_buffer_create(pool, &tech_pvt->dl_stash, stash_buflen) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                "%s: Error creating playback buffers.\n", tech_pvt->sessionId);
            return SWITCH_STATUS_FALSE;
        }

        if (desiredSampling != sampling) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) resampling from %u to %u\n", tech_pvt->sessionId, sampling, desiredSampling);
            tech_pvt->resampler = speex_resampler_init(channels, sampling, desiredSampling, SWITCH_RESAMPLE_QUALITY, &err);
            if (0 != err) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Error initializing resampler: %s.\n", speex_resampler_strerror(err));
                return SWITCH_STATUS_FALSE;
            }
        }
        else {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) no resampling needed for this call\n", tech_pvt->sessionId);
        }

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) stream_data_init\n", tech_pvt->sessionId);

        return SWITCH_STATUS_SUCCESS;
    }

    void destroy_tech_pvt(private_t* tech_pvt) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "%s destroy_tech_pvt (playback frames replaced: %u, underruns: %u)\n",
                          tech_pvt->sessionId, tech_pvt->replaced, tech_pvt->underruns);
        if (tech_pvt->resampler) {
            speex_resampler_destroy(tech_pvt->resampler);
            tech_pvt->resampler = nullptr;
        }
        if (tech_pvt->dl_resampler) {
            speex_resampler_destroy(tech_pvt->dl_resampler);
            tech_pvt->dl_resampler = nullptr;
        }
        if (tech_pvt->mutex) {
            switch_mutex_destroy(tech_pvt->mutex);
            tech_pvt->mutex = nullptr;
        }
        if (tech_pvt->play_mutex) {
            switch_mutex_destroy(tech_pvt->play_mutex);
            tech_pvt->play_mutex = nullptr;
        }
    }

}

extern "C" {
    int validate_ws_uri(const char* url, char* wsUri) {
        const char* scheme = nullptr;
        const char* hostStart = nullptr;
        const char* hostEnd = nullptr;
        const char* portStart = nullptr;

        // Check scheme
        if (strncmp(url, "ws://", 5) == 0) {
            scheme = "ws";
            hostStart = url + 5;
        } else if (strncmp(url, "wss://", 6) == 0) {
            scheme = "wss";
            hostStart = url + 6;
        } else {
            return 0;
        }

        // Find host end or port start
        hostEnd = hostStart;
        while (*hostEnd && *hostEnd != ':' && *hostEnd != '/') {
            if (!std::isalnum(*hostEnd) && *hostEnd != '-' && *hostEnd != '.') {
                return 0;
            }
            ++hostEnd;
        }

        // Check if host is empty
        if (hostStart == hostEnd) {
            return 0;
        }

        // Check for port
        if (*hostEnd == ':') {
            portStart = hostEnd + 1;
            while (*portStart && *portStart != '/') {
                if (!std::isdigit(*portStart)) {
                    return 0;
                }
                ++portStart;
            }
        }

        // Copy valid URI to wsUri
        std::strncpy(wsUri, url, MAX_WS_URI);
        return 1;
    }

    switch_status_t is_valid_utf8(const char *str) {
        switch_status_t status = SWITCH_STATUS_FALSE;
        while (*str) {
            if ((*str & 0x80) == 0x00) {
                // 1-byte character
                str++;
            } else if ((*str & 0xE0) == 0xC0) {
                // 2-byte character
                if ((str[1] & 0xC0) != 0x80) {
                    return status;
                }
                str += 2;
            } else if ((*str & 0xF0) == 0xE0) {
                // 3-byte character
                if ((str[1] & 0xC0) != 0x80 || (str[2] & 0xC0) != 0x80) {
                    return status;
                }
                str += 3;
            } else if ((*str & 0xF8) == 0xF0) {
                // 4-byte character
                if ((str[1] & 0xC0) != 0x80 || (str[2] & 0xC0) != 0x80 || (str[3] & 0xC0) != 0x80) {
                    return status;
                }
                str += 4;
            } else {
                // invalid character
                return status;
            }
        }
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_send_text(switch_core_session_t *session, char* text) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "stream_session_send_text failed because no bug\n");
            return SWITCH_STATUS_FALSE;
        }
        auto *tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);

        if (!tech_pvt) return SWITCH_STATUS_FALSE;

        std::shared_ptr<AudioStreamer> streamer;

        switch_mutex_lock(tech_pvt->mutex);

        if (tech_pvt->pAudioStreamer) {
            auto sp_wrap = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            if (sp_wrap && *sp_wrap) {
                streamer = *sp_wrap; // copy shared_ptr
            }
        }

        switch_mutex_unlock(tech_pvt->mutex);

        if (streamer) {
            streamer->writeText(text);
            return SWITCH_STATUS_SUCCESS;
        }

        return SWITCH_STATUS_FALSE;
    }

    switch_status_t stream_session_pauseresume(switch_core_session_t *session, int pause) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "stream_session_pauseresume failed because no bug\n");
            return SWITCH_STATUS_FALSE;
        }
        auto *tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);

        if (!tech_pvt) return SWITCH_STATUS_FALSE;

        switch_core_media_bug_flush(bug);
        tech_pvt->audio_paused = pause;
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_init(switch_core_session_t *session,
                                        responseHandler_t responseHandler,
                                        uint32_t samples_per_second,
                                        char *wsUri,
                                        int sampling,
                                        int channels,
                                        char* metadata,
                                        void **ppUserData)
    {
        int deflate, heart_beat;
        bool suppressLog = false;
        const char* buffer_size;
        const char* extra_headers;
        int rtp_packets = 1; //20ms burst
        const char* tls_cafile = NULL;;
        const char* tls_keyfile = NULL;;
        const char* tls_certfile = NULL;;
        bool tls_disable_hostname_validation = false;

        switch_channel_t *channel = switch_core_session_get_channel(session);

        if (switch_channel_var_true(channel, "STREAM_MESSAGE_DEFLATE")) {
            deflate = 1;
        }

        if (switch_channel_var_true(channel, "STREAM_SUPPRESS_LOG")) {
            suppressLog = true;
        }

        tls_cafile = switch_channel_get_variable(channel, "STREAM_TLS_CA_FILE");
        tls_keyfile = switch_channel_get_variable(channel, "STREAM_TLS_KEY_FILE");
        tls_certfile = switch_channel_get_variable(channel, "STREAM_TLS_CERT_FILE");

        if (switch_channel_var_true(channel, "STREAM_TLS_DISABLE_HOSTNAME_VALIDATION")) {
            tls_disable_hostname_validation = true;
        }

        const char* heartBeat = switch_channel_get_variable(channel, "STREAM_HEART_BEAT");
        if (heartBeat) {
            char *endptr;
            long value = strtol(heartBeat, &endptr, 10);
            if (*endptr == '\0' && value <= INT_MAX && value >= INT_MIN) {
                heart_beat = (int) value;
            }
        }

        if ((buffer_size = switch_channel_get_variable(channel, "STREAM_BUFFER_SIZE"))) {
            int bSize = atoi(buffer_size);
            if(bSize % 20 != 0) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "%s: Buffer size of %s is not a multiple of 20ms. Using default 20ms.\n",
                                  switch_channel_get_name(channel), buffer_size);
            } else if(bSize >= 20){
                rtp_packets = bSize/20;
            }
        }

        extra_headers = switch_channel_get_variable(channel, "STREAM_EXTRA_HEADERS");

        // allocate per-session tech_pvt
        auto* tech_pvt = (private_t *) switch_core_session_alloc(session, sizeof(private_t));

        if (!tech_pvt) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "error allocating memory!\n");
            return SWITCH_STATUS_FALSE;
        }
        if (SWITCH_STATUS_SUCCESS != stream_data_init(tech_pvt, session, wsUri, samples_per_second, sampling, channels,
                                                        metadata, responseHandler, deflate, heart_beat, suppressLog, rtp_packets,
                                                        extra_headers, tls_cafile, tls_keyfile, tls_certfile,
                                                        tls_disable_hostname_validation)) {
            destroy_tech_pvt(tech_pvt);
            return SWITCH_STATUS_FALSE;
        }

        *ppUserData = tech_pvt;

        return SWITCH_STATUS_SUCCESS;
    }

    /* Uplink: read caller audio from the media bug and stream it to the
     * WebSocket as binary L16 frames. */
    switch_bool_t stream_frame(switch_media_bug_t *bug) {
        auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt) return SWITCH_TRUE;
        if (tech_pvt->audio_paused || tech_pvt->cleanup_started) return SWITCH_TRUE;

        std::shared_ptr<AudioStreamer> streamer;
        std::vector<std::vector<uint8_t>> pending_send;

        if (switch_mutex_trylock(tech_pvt->mutex) != SWITCH_STATUS_SUCCESS) {
            return SWITCH_TRUE;
        }

        if (!tech_pvt->pAudioStreamer) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_TRUE;
        }

        auto sp_ptr = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
        if (!sp_ptr || !(*sp_ptr)) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_TRUE;
        }

        streamer = *sp_ptr;

        auto *resampler = tech_pvt->resampler;
        const int channels = tech_pvt->channels;
        const int rtp_packets = tech_pvt->rtp_packets;

        if (nullptr == resampler) {

            uint8_t data_buf[SWITCH_RECOMMENDED_BUFFER_SIZE];
            switch_frame_t frame = {};
            frame.data = data_buf;
            frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;

            while (switch_core_media_bug_read(bug, &frame, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS) {
                if (!frame.datalen) {
                    continue;
                }

                if (rtp_packets == 1) {
                    pending_send.emplace_back((uint8_t*)frame.data, (uint8_t*)frame.data + frame.datalen);
                    continue;
                }

                size_t freespace = switch_buffer_freespace(tech_pvt->sbuffer);

                if (freespace >= frame.datalen) {
                    switch_buffer_write(tech_pvt->sbuffer, static_cast<uint8_t *>(frame.data), frame.datalen);
                }

                if (switch_buffer_freespace(tech_pvt->sbuffer) == 0) {
                    switch_size_t inuse = switch_buffer_inuse(tech_pvt->sbuffer);
                    if (inuse > 0) {
                        std::vector<uint8_t> tmp(inuse);
                        switch_buffer_read(tech_pvt->sbuffer, tmp.data(), inuse);
                        switch_buffer_zero(tech_pvt->sbuffer);
                        pending_send.emplace_back(std::move(tmp));
                    }
                }
            }

        } else {

            uint8_t data[SWITCH_RECOMMENDED_BUFFER_SIZE];
            switch_frame_t frame = {};
            frame.data = data;
            frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;

            while (switch_core_media_bug_read(bug, &frame, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS) {
                if(!frame.datalen) {
                    continue;
                }

                const size_t freespace = switch_buffer_freespace(tech_pvt->sbuffer);
                spx_uint32_t in_len = frame.samples;
                spx_uint32_t out_len = (freespace / (tech_pvt->channels * sizeof(spx_int16_t)));

                if(out_len == 0) {
                    if(freespace == 0) {
                        switch_size_t inuse = switch_buffer_inuse(tech_pvt->sbuffer);
                        if (inuse > 0) {
                            std::vector<uint8_t> tmp(inuse);
                            switch_buffer_read(tech_pvt->sbuffer, tmp.data(), inuse);
                            switch_buffer_zero(tech_pvt->sbuffer);
                            pending_send.emplace_back(std::move(tmp));
                        }
                    }
                    continue;
                }

                std::vector<spx_int16_t> out;
                out.resize((size_t)out_len * (size_t)channels);

                if(channels == 1) {
                    speex_resampler_process_int(resampler,
                                    0,
                                    (const spx_int16_t *)frame.data,
                                    &in_len,
                                    out.data(),
                                    &out_len);
                } else {
                    speex_resampler_process_interleaved_int(resampler,
                                    (const spx_int16_t *)frame.data,
                                    &in_len,
                                    out.data(),
                                    &out_len);
                }

                if(out_len > 0) {
                    const size_t bytes_written = (size_t)out_len * (size_t)channels * sizeof(spx_int16_t);

                    if (rtp_packets == 1) { //20ms packet
                        const uint8_t* p = (const uint8_t*)out.data();
                        pending_send.emplace_back(p, p + bytes_written);
                        continue;
                    }

                    if (bytes_written <= switch_buffer_freespace(tech_pvt->sbuffer)) {
                        switch_buffer_write(tech_pvt->sbuffer, (const uint8_t *)out.data(), bytes_written);
                    }
                }

                if (switch_buffer_freespace(tech_pvt->sbuffer) == 0) {
                    switch_size_t inuse = switch_buffer_inuse(tech_pvt->sbuffer);
                    if (inuse > 0) {
                        std::vector<uint8_t> tmp(inuse);
                        switch_buffer_read(tech_pvt->sbuffer, tmp.data(), inuse);
                        switch_buffer_zero(tech_pvt->sbuffer);
                        pending_send.emplace_back(std::move(tmp));
                    }
                }
            }
        }

        switch_mutex_unlock(tech_pvt->mutex);

        if (!streamer || !streamer->isConnected()) return SWITCH_TRUE;

        for (auto &chunk : pending_send) {
            if (!chunk.empty()) {
                streamer->writeBinary(chunk.data(), chunk.size());
            }
        }

        return SWITCH_TRUE;
    }

    /* Downlink: replace the caller-bound write frame with queued playback
     * audio. Runs on the FreeSWITCH media thread (SMBF_WRITE_REPLACE). On
     * underrun the original frame passes through untouched. */
    switch_bool_t stream_playback_frame(switch_media_bug_t *bug) {
        auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt) return SWITCH_TRUE;
        if (tech_pvt->cleanup_started) return SWITCH_TRUE;

        switch_frame_t *frame = switch_core_media_bug_get_write_replace_frame(bug);
        if (!frame || !frame->datalen || !frame->rate) return SWITCH_TRUE;

        const size_t need = frame->datalen;
        const int channels = frame->channels > 0 ? frame->channels : 1;

        switch_mutex_lock(tech_pvt->play_mutex);

        /* (Re)initialize the downlink resampler when the channel write rate
         * is learned or changes. */
        if (tech_pvt->dl_rate != (int)frame->rate) {
            if (tech_pvt->dl_resampler) {
                speex_resampler_destroy(tech_pvt->dl_resampler);
                tech_pvt->dl_resampler = nullptr;
            }
            tech_pvt->dl_rate = (int)frame->rate;
            switch_buffer_zero(tech_pvt->dl_stash);

            if ((int)frame->rate != tech_pvt->sampling) {
                int err = 0;
                tech_pvt->dl_resampler = speex_resampler_init(channels, tech_pvt->sampling,
                                                              frame->rate, SWITCH_RESAMPLE_QUALITY, &err);
                if (err != 0) {
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
                                      "(%s) downlink resampler init failed: %s\n",
                                      tech_pvt->sessionId, speex_resampler_strerror(err));
                    tech_pvt->dl_resampler = nullptr;
                }
            }
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
                              "(%s) downlink path: %d Hz -> %d Hz, %d ch\n",
                              tech_pvt->sessionId, tech_pvt->sampling, tech_pvt->dl_rate, channels);
        }

        /* Top up the stash with audio resampled to the channel rate until we
         * have a full frame. */
        while (switch_buffer_inuse(tech_pvt->dl_stash) < need) {
            switch_size_t inuse = switch_buffer_inuse(tech_pvt->play_buffer);
            if (inuse == 0) break;

            uint8_t in[4096];
            switch_size_t got = switch_buffer_read(tech_pvt->play_buffer, in,
                                                   std::min((switch_size_t)sizeof(in), inuse));
            if (got == 0) break;

            if (tech_pvt->dl_resampler) {
                spx_uint32_t in_len = got / (channels * (spx_uint32_t)sizeof(spx_int16_t));
                spx_uint32_t out_len = (spx_uint32_t)(((double)in_len * frame->rate) / tech_pvt->sampling) + 64;
                std::vector<spx_int16_t> out((size_t)out_len * channels);

                if (channels == 1) {
                    speex_resampler_process_int(tech_pvt->dl_resampler, 0,
                                                (const spx_int16_t *)in, &in_len, out.data(), &out_len);
                } else {
                    speex_resampler_process_interleaved_int(tech_pvt->dl_resampler,
                                                            (const spx_int16_t *)in, &in_len, out.data(), &out_len);
                }

                if (out_len > 0) {
                    const size_t out_bytes = (size_t)out_len * channels * sizeof(spx_int16_t);
                    if (out_bytes <= switch_buffer_freespace(tech_pvt->dl_stash)) {
                        switch_buffer_write(tech_pvt->dl_stash, out.data(), out_bytes);
                    }
                }
            } else {
                if (got <= switch_buffer_freespace(tech_pvt->dl_stash)) {
                    switch_buffer_write(tech_pvt->dl_stash, in, got);
                } else {
                    /* Stash full; put nothing back, oldest wins. */
                    switch_buffer_write(tech_pvt->dl_stash, in,
                                        switch_buffer_freespace(tech_pvt->dl_stash));
                }
            }

            if (got < sizeof(in)) break; /* play_buffer drained */
        }

        if (switch_buffer_inuse(tech_pvt->dl_stash) >= need) {
            switch_buffer_read(tech_pvt->dl_stash, frame->data, need);
            tech_pvt->replaced++;
        } else {
            /* Underrun: pass the original frame through (silence/MOH). */
            tech_pvt->underruns++;
        }

        switch_mutex_unlock(tech_pvt->play_mutex);

        switch_core_media_bug_set_write_replace_frame(bug, frame);
        return SWITCH_TRUE;
    }

    switch_status_t stream_session_cleanup(switch_core_session_t *session, char* text, int channelIsClosing) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if(bug)
        {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            char sessionId[MAX_SESSION_ID];
            strcpy(sessionId, tech_pvt->sessionId);

            std::shared_ptr<AudioStreamer>* sp_wrap = nullptr;
            std::shared_ptr<AudioStreamer> streamer;

            switch_mutex_lock(tech_pvt->mutex);

            if (tech_pvt->cleanup_started) {
                switch_mutex_unlock(tech_pvt->mutex);
                return SWITCH_STATUS_SUCCESS;
            }
            tech_pvt->cleanup_started = 1;

            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) stream_session_cleanup\n", sessionId);

            switch_channel_set_private(channel, MY_BUG_NAME, nullptr);

            sp_wrap = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            tech_pvt->pAudioStreamer = nullptr;

            if (sp_wrap && *sp_wrap) {
                streamer = *sp_wrap;
            }

            switch_mutex_unlock(tech_pvt->mutex);

            if (!channelIsClosing) {
                switch_core_media_bug_remove(session, &bug);
            }

            if (sp_wrap) {
                delete sp_wrap;
                sp_wrap = nullptr;
            }

            if(streamer) {
                if (text) streamer->writeText(text);

                streamer->markCleanedUp();
                streamer->disconnect();
            }

            destroy_tech_pvt(tech_pvt);

            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO, "(%s) stream_session_cleanup: connection closed\n", sessionId);
            return SWITCH_STATUS_SUCCESS;
        }

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "stream_session_cleanup: no bug - websocket connection already closed\n");
        return SWITCH_STATUS_FALSE;
    }
}
