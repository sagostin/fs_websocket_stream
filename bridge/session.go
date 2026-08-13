package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/coder/websocket"
)

// ErrSessionClosed is returned by send operations on a closed session.
var ErrSessionClosed = errors.New("bridge: session closed")

// writeRequest is a queued outbound frame.
type writeRequest struct {
	mt   websocket.MessageType
	data []byte
}

// Session represents a single call's WebSocket connection from FreeSWITCH.
// It is safe for concurrent use: audio and control frames may be queued from
// any goroutine while the handler callbacks run on the read pump.
type Session struct {
	id         string
	conn       *websocket.Conn
	sampleRate int
	mixType    string
	fsUUID     string // FreeSWITCH call UUID (from the module's query param)

	writeQueue chan writeRequest
	done       chan struct{}
	closeOnce  sync.Once

	// downlinkTap, when set, observes each downlink PCM frame before it is
	// written to the WebSocket. Used by the recorder.
	downlinkTap func([]byte)

	mu       sync.RWMutex
	metadata json.RawMessage
}

// ID returns the server-assigned session identifier.
func (s *Session) ID() string { return s.id }

// SampleRate returns the negotiated PCM sample rate in Hz (e.g. 16000).
func (s *Session) SampleRate() int { return s.sampleRate }

// MixType returns the requested mix type: "mono", "mixed" or "stereo".
func (s *Session) MixType() string { return s.mixType }

// FSUUID returns the FreeSWITCH call UUID for this audio session, or ""
// if the module did not supply one. Used to correlate the audio plane with
// the call-control/event plane.
func (s *Session) FSUUID() string { return s.fsUUID }

// Metadata returns the most recent JSON text frame sent by the module,
// typically the call metadata passed to `uuid_ws_bridge ... start`.
// Returns nil if no metadata frame has arrived yet.
func (s *Session) Metadata() json.RawMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.metadata
}

func (s *Session) setMetadata(m json.RawMessage) {
	s.mu.Lock()
	s.metadata = m
	s.mu.Unlock()
}

// SendAudio queues raw L16 PCM for downlink playback. It never blocks: if
// the playback queue is full the oldest frame is dropped, bounding latency
// under slow-consumer or backpressure conditions. The pcm slice is copied.
func (s *Session) SendAudio(pcm []byte) error {
	if len(pcm) == 0 {
		return nil
	}
	buf := make([]byte, len(pcm))
	copy(buf, pcm)
	return s.enqueue(writeRequest{mt: websocket.MessageBinary, data: buf})
}

// SendControl queues a JSON control message (e.g. ControlClear for barge-in).
func (s *Session) SendControl(msg ControlMessage) error {
	return s.enqueue(writeRequest{mt: websocket.MessageText, data: msg.Marshal()})
}

// ClearPlayback sends a clear control message, flushing the module's
// playback buffer. Used for barge-in when the caller starts speaking.
func (s *Session) ClearPlayback() error {
	return s.SendControl(ControlMessage{Type: ControlClear})
}

func (s *Session) enqueue(req writeRequest) error {
	select {
	case <-s.done:
		return ErrSessionClosed
	default:
	}
	select {
	case s.writeQueue <- req:
		return nil
	default:
	}
	// Queue full: drop the oldest frame to bound playback latency,
	// then make one more attempt.
	select {
	case <-s.writeQueue:
	default:
	}
	select {
	case s.writeQueue <- req:
		return nil
	case <-s.done:
		return ErrSessionClosed
	}
}

// Close terminates the session, closing the WebSocket connection.
func (s *Session) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.conn.Close(websocket.StatusNormalClosure, "session closed")
	})
}

// writePump drains the write queue to the WebSocket until the session ends.
func (s *Session) writePump(ctx context.Context) {
	for {
		select {
		case <-s.done:
			return
		case req := <-s.writeQueue:
			if req.mt == websocket.MessageBinary && s.downlinkTap != nil {
				s.downlinkTap(req.data)
			}
			if err := s.conn.Write(ctx, req.mt, req.data); err != nil {
				s.Close()
				return
			}
		}
	}
}
