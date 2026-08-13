package bridge

import (
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newTestSession builds a minimal session for recorder tests.
func newTestSession(uuid string) *Session {
	return &Session{
		id:         newSessionID(),
		sampleRate: 16000,
		mixType:    "mono",
		fsUUID:     uuid,
		writeQueue: make(chan writeRequest, 8),
		done:       make(chan struct{}),
	}
}

func TestRecorderBundle(t *testing.T) {
	dir := t.TempDir()
	bus := NewEventBus()
	rec, err := NewRecorder(dir)
	if err != nil {
		t.Fatal(err)
	}
	rec.Bus = bus

	s := newTestSession("call-abc")
	rec.Begin(s)

	// 100ms of caller audio, 50ms of agent audio (20ms frames @16k = 640B).
	frame := make([]byte, 640)
	for i := range frame {
		frame[i] = byte(i % 251)
	}
	for i := 0; i < 5; i++ {
		rec.Uplink(s, frame)
	}
	for i := 0; i < 2; i++ {
		rec.Downlink(s, frame)
	}

	bus.Publish(Event{Name: "transcript", UUID: "call-abc", Data: map[string]any{"text": "hi", "final": true}})
	bus.Publish(Event{Name: "transcript", UUID: "other-call"}) // filtered out

	rec.Finish(s)

	bundle := filepath.Join(dir, "call-abc")

	// audio.wav: stereo, data = 2x caller bytes.
	wav, err := os.ReadFile(filepath.Join(bundle, "audio.wav"))
	if err != nil {
		t.Fatalf("audio.wav: %v", err)
	}
	if string(wav[0:4]) != "RIFF" || string(wav[8:12]) != "WAVE" {
		t.Fatal("bad WAV magic")
	}
	if got := binary.LittleEndian.Uint32(wav[24:]); got != 16000 {
		t.Errorf("wav rate = %d", got)
	}
	if got := binary.LittleEndian.Uint16(wav[22:]); got != 2 {
		t.Errorf("wav channels = %d", got)
	}
	dataLen := binary.LittleEndian.Uint32(wav[40:])
	if want := uint32(5 * 640 * 2); dataLen != want {
		t.Errorf("wav data len = %d, want %d", dataLen, want)
	}
	if uint32(len(wav)) != 44+dataLen {
		t.Errorf("wav file len = %d", len(wav))
	}

	// Interleave check: first caller sample (bytes 0,1 of frame) lands at
	// data[0:2], first agent sample at data[2:4].
	if wav[44] != frame[0] || wav[45] != frame[1] || wav[46] != frame[0] || wav[47] != frame[1] {
		t.Error("interleave wrong at start")
	}
	// Past the agent track (2 frames = 1280 samples), right channel is silence.
	off := 44 + 1280*4 // after 1280 stereo frames
	if wav[off] != frame[0] || wav[off+2] != 0 || wav[off+3] != 0 {
		t.Error("agent track not silence-padded")
	}

	// meta.json
	metaRaw, err := os.ReadFile(filepath.Join(bundle, "meta.json"))
	if err != nil {
		t.Fatalf("meta.json: %v", err)
	}
	var meta recordingMeta
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		t.Fatal(err)
	}
	if meta.UUID != "call-abc" || meta.Rate != 16000 || meta.DurationMs != 100 {
		t.Errorf("meta = %+v", meta)
	}

	// events.jsonl: only this call's events.
	eventsRaw, err := os.ReadFile(filepath.Join(bundle, "events.jsonl"))
	if err != nil {
		t.Fatalf("events.jsonl: %v", err)
	}
	if want := `"text":"hi"`; !strings.Contains(string(eventsRaw), want) {
		t.Errorf("events.jsonl missing %q: %s", want, eventsRaw)
	}
	if strings.Contains(string(eventsRaw), "other-call") {
		t.Error("events.jsonl leaked another call's event")
	}

	// raw sidecars removed after finalize.
	if _, err := os.Stat(filepath.Join(bundle, "caller.raw")); !os.IsNotExist(err) {
		t.Error("caller.raw should be removed after finalize")
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }

func TestRecorderHTTPAPI(t *testing.T) {
	dir := t.TempDir()
	rec, err := NewRecorder(dir)
	if err != nil {
		t.Fatal(err)
	}

	s := newTestSession("call-http")
	rec.Begin(s)
	rec.Uplink(s, make([]byte, 640))
	rec.Finish(s)

	mux := http.NewServeMux()
	rec.RegisterRoutes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// List.
	resp, err := http.Get(srv.URL + "/recordings")
	if err != nil {
		t.Fatal(err)
	}
	var list []recordingMeta
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(list) != 1 || list[0].UUID != "call-http" {
		t.Fatalf("list = %+v", list)
	}

	// File fetch.
	resp, err = http.Get(srv.URL + "/recordings/call-http/audio.wav")
	if err != nil {
		t.Fatal(err)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "audio/wav" {
		t.Errorf("content-type = %q", ct)
	}
	resp.Body.Close()

	// Unknown file rejected.
	resp, err = http.Get(srv.URL + "/recordings/call-http/caller.raw")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 404 {
		t.Errorf("caller.raw fetch = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// Path traversal rejected by sanitizeName.
	resp, err = http.Get(srv.URL + "/recordings/..%2F..%2Fetc/meta.json")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 404 && resp.StatusCode != 400 {
		t.Errorf("traversal fetch = %d, want 400 or 404", resp.StatusCode)
	}
	resp.Body.Close()

	// Delete.
	req, _ := http.NewRequest("DELETE", srv.URL+"/recordings/call-http", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Errorf("delete = %d", resp.StatusCode)
	}
	if _, err := os.Stat(filepath.Join(dir, "call-http")); !os.IsNotExist(err) {
		t.Error("bundle not deleted")
	}
}

func TestRecorderSweep(t *testing.T) {
	dir := t.TempDir()
	rec, err := NewRecorder(dir)
	if err != nil {
		t.Fatal(err)
	}
	rec.Retention = time.Hour

	// An old bundle and a fresh one.
	old := filepath.Join(dir, "old-call")
	fresh := filepath.Join(dir, "fresh-call")
	os.MkdirAll(old, 0o755)
	os.MkdirAll(fresh, 0o755)
	os.WriteFile(filepath.Join(old, "meta.json"), []byte("{}"), 0o644)
	os.WriteFile(filepath.Join(fresh, "meta.json"), []byte("{}"), 0o644)
	past := time.Now().Add(-2 * time.Hour)
	os.Chtimes(filepath.Join(old, "meta.json"), past, past)

	rec.sweep()

	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Error("old bundle not swept")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Error("fresh bundle swept")
	}
}

func TestRecordingCompleteEvent(t *testing.T) {
	dir := t.TempDir()
	bus := NewEventBus()
	rec, _ := NewRecorder(dir)
	rec.Bus = bus

	events, unsub := bus.Subscribe(8)
	defer unsub()

	s := newTestSession("call-ev")
	rec.Begin(s)
	rec.Uplink(s, make([]byte, 640))
	rec.Finish(s)

	// Drain until recording.complete.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-events:
			if ev.Name == "recording.complete" {
				if ev.UUID != "call-ev" || ev.Data["path"] == nil {
					t.Fatalf("event = %+v", ev)
				}
				return
			}
		case <-deadline:
			t.Fatal("no recording.complete event")
		}
	}
}
