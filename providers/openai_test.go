package providers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fs_websocket_stream/pipeline"
	"github.com/coder/websocket"
)

func TestOpenAITTS(t *testing.T) {
	// 100ms of 24kHz silence = 2400 samples = 4800 bytes.
	pcm24k := make([]byte, 4800)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/audio/speech" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("missing/invalid auth header")
		}
		var req speechRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("bad request body: %v", err)
		}
		if req.ResponseFormat != "pcm" || req.Model != "gpt-4o-mini-tts" || req.Voice != "alloy" {
			t.Errorf("unexpected request: %+v", req)
		}
		w.Write(pcm24k)
	}))
	defer srv.Close()

	tts := OpenAITTS{APIKey: "test-key", BaseURL: srv.URL, SampleRate: 16000}
	chunks, err := tts.Synthesize(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Synthesize: %v", err)
	}
	var got int
	for chunk := range chunks {
		got += len(chunk)
	}
	// 24k -> 16k: 4800 bytes -> 3200 bytes.
	if got != 3200 {
		t.Fatalf("want 3200 resampled bytes, got %d", got)
	}
}

func TestOpenAITTSErrors(t *testing.T) {
	if _, err := (OpenAITTS{}).Synthesize(context.Background(), "hi"); err == nil {
		t.Fatal("want error without API key")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusUnauthorized)
	}))
	defer srv.Close()
	tts := OpenAITTS{APIKey: "k", BaseURL: srv.URL}
	if _, err := tts.Synthesize(context.Background(), "hi"); err == nil {
		t.Fatal("want error on HTTP 401")
	}
}

// fakeTranscriptionServer speaks the OpenAI Realtime transcription protocol.
func fakeTranscriptionServer(t *testing.T, gotAppend chan<- int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("missing/invalid auth header")
		}
		if !strings.Contains(r.URL.RawQuery, "intent=transcription") {
			t.Errorf("missing intent=transcription in %s", r.URL.RawQuery)
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		ctx := context.Background()

		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var ev struct {
				Type  string `json:"type"`
				Audio string `json:"audio"`
			}
			if json.Unmarshal(data, &ev) != nil {
				continue
			}
			switch ev.Type {
			case "session.update":
				// Handshake done: emit speech-start, a delta, and a final.
				for _, msg := range []string{
					`{"type":"input_audio_buffer.speech_started"}`,
					`{"type":"conversation.item.input_audio_transcription.delta","delta":"hel"}`,
					`{"type":"conversation.item.input_audio_transcription.completed","transcript":"hello"}`,
				} {
					if err := conn.Write(ctx, websocket.MessageText, []byte(msg)); err != nil {
						return
					}
				}
			case "input_audio_buffer.append":
				raw, err := base64.StdEncoding.DecodeString(ev.Audio)
				if err != nil {
					t.Errorf("bad base64 audio: %v", err)
					continue
				}
				gotAppend <- len(raw)
			}
		}
	}))
}

func TestOpenAIASR(t *testing.T) {
	gotAppend := make(chan int, 1)
	srv := fakeTranscriptionServer(t, gotAppend)
	defer srv.Close()

	asr, err := NewOpenAIASR(context.Background(), OpenAIASRConfig{
		APIKey:  "test-key",
		BaseURL: srv.URL,
	}, 16000)
	if err != nil {
		t.Fatalf("NewOpenAIASR: %v", err)
	}
	defer asr.Close()

	// Expect: speech-start, delta (partial), completed (final).
	want := []pipeline.ASREvent{
		{Type: pipeline.EventSpeechStarted},
		{Type: pipeline.EventTranscript, Text: "hel"},
		{Type: pipeline.EventTranscript, Text: "hello", Final: true},
	}
	for i, w := range want {
		select {
		case ev := <-asr.Events():
			if ev != w {
				t.Fatalf("event %d: want %+v, got %+v", i, w, ev)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}

	// 100ms of 16kHz audio = 3200 bytes -> resampled to 24k = 4800 bytes.
	if err := asr.Write(make([]byte, 3200)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	select {
	case n := <-gotAppend:
		if n != 4800 {
			t.Fatalf("want 4800 appended bytes, got %d", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for append")
	}
}
