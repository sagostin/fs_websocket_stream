package pipeline

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"fs_websocket_stream/bridge"
	"github.com/coder/websocket"
)

// runCascade spins up a bridge server with a mock cascade and returns a
// connected client.
func runCascade(t *testing.T, transcript string, toneMs int) (*websocket.Conn, context.CancelFunc) {
	t.Helper()

	cascade := &Cascade{
		NewASR: MockASRFactory(transcript),
		LLM:    MockLLM{},
		NewTTS: MockTTSFactory(toneMs),
	}
	srv := bridge.NewServer(cascade, nil)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = srv.ListenAndServe(ctx, "127.0.0.1:18099") }()
	t.Cleanup(cancel)

	// Wait for the listener.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, _, err := websocket.Dial(context.Background(), "ws://127.0.0.1:18099/stream?rate=16000&mix=mono", nil)
		if err == nil {
			t.Cleanup(func() { conn.Close(websocket.StatusNormalClosure, "") })
			return conn, cancel
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("server did not start")
	return nil, nil
}

func toneFrame(samples int) []byte {
	buf := make([]byte, samples*2)
	for i := 0; i < samples; i++ {
		binary.LittleEndian.PutUint16(buf[2*i:], uint16(1000))
	}
	return buf
}

func TestCascadeMockTurn(t *testing.T) {
	conn, _ := runCascade(t, "hello world", 500)

	// Feed 1s of audio (50 x 20ms frames at 16kHz): past MockASR's
	// speech + final thresholds, triggering one full mock turn.
	frame := toneFrame(320)
	for i := 0; i < 50; i++ {
		if err := conn.Write(context.Background(), websocket.MessageBinary, frame); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	// Expect downlink tone frames from the mock TTS (500ms = 25 frames).
	got := 0
	deadline := time.After(5 * time.Second)
	for got < 25 {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for reply audio, got %d frames", got)
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		mt, data, err := conn.Read(ctx)
		cancel()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if mt == websocket.MessageBinary {
			if len(data) != 640 {
				t.Fatalf("reply frame = %d bytes, want 640 (20ms @16k)", len(data))
			}
			got++
		}
	}
}

func TestCascadeBargeInSendsClear(t *testing.T) {
	conn, _ := runCascade(t, "hello world", 5000) // long reply

	frame := toneFrame(320)
	// Trigger a turn.
	for i := 0; i < 50; i++ {
		if err := conn.Write(context.Background(), websocket.MessageBinary, frame); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	// Wait for reply audio to start, then send more audio to trip the
	// speech-started threshold again -> barge-in -> clear control frame.
	sawAudio := false
	sawClear := false
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && !(sawAudio && sawClear) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		mt, data, err := conn.Read(ctx)
		cancel()
		if err != nil {
			// On read timeout, keep feeding audio to trigger barge-in.
			if sawAudio {
				_ = conn.Write(context.Background(), websocket.MessageBinary, frame)
			}
			continue
		}
		switch mt {
		case websocket.MessageBinary:
			sawAudio = true
		case websocket.MessageText:
			if bytes.Contains(data, []byte(`"clear"`)) {
				sawClear = true
			}
		}
		if sawAudio && !sawClear {
			// Keep talking to force the mock ASR over the speech threshold.
			for i := 0; i < 20; i++ {
				_ = conn.Write(context.Background(), websocket.MessageBinary, frame)
			}
		}
	}
	if !sawAudio {
		t.Fatal("no reply audio received")
	}
	if !sawClear {
		t.Fatal("no clear control frame received on barge-in")
	}
}
