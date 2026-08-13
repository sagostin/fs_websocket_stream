package bridge

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Recorder captures every audio session to a per-call bundle on disk:
//
//	<dir>/<call-uuid>/
//	  caller.raw    caller PCM (written live, removed after finalize)
//	  agent.raw     AI PCM      (written live, removed after finalize)
//	  audio.wav     stereo 16-bit WAV: caller = left, AI = right
//	  meta.json     call metadata (uuid, times, rate, caller, ...)
//	  events.jsonl  this call's bus events (transcripts, speech, call state)
//
// Both tracks advance at the session rate from session start, so the
// shorter (agent) track is silence-padded to the caller track's length and
// interleaved at finalize time. Recording assumes mono sessions.
type Recorder struct {
	// Dir is the root recording directory. Required.
	Dir string
	// Bus, when set, is subscribed per call to capture events.jsonl.
	Bus *EventBus
	// Logger for recorder logs. Default slog.Default().
	Logger *slog.Logger
	// Retention is the maximum age of a finished bundle. Default 24h.
	Retention time.Duration
	// MaxBytes caps total disk usage; oldest bundles are deleted first.
	// Default 1 GiB.
	MaxBytes int64
	// SweepInterval between retention passes. Default 60s.
	SweepInterval time.Duration

	mu       sync.Map // *Session -> *callRecording
	stop     chan struct{}
	stopOnce sync.Once
}

type callRecording struct {
	uuid      string
	sessionID string
	rate      int
	mix       string
	started   time.Time
	dir       string

	mu          sync.Mutex
	callerFile  *os.File
	agentFile   *os.File
	eventsFile  *os.File
	callerBytes int64
	agentBytes  int64
	metadata    json.RawMessage

	unsub      func()
	events     <-chan Event
	eventsDone chan struct{}
}

// NewRecorder creates a Recorder rooted at dir (created if missing).
func NewRecorder(dir string) (*Recorder, error) {
	if dir == "" {
		return nil, fmt.Errorf("recorder: dir required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Recorder{
		Dir:           dir,
		Logger:        slog.Default(),
		Retention:     24 * time.Hour,
		MaxBytes:      1 << 30,
		SweepInterval: time.Minute,
		stop:          make(chan struct{}),
	}, nil
}

// Start launches the retention sweeper.
func (r *Recorder) Start() {
	go r.sweepLoop()
}

// Close stops the sweeper.
func (r *Recorder) Close() {
	r.stopOnce.Do(func() { close(r.stop) })
}

// Begin starts recording a session. Called by Server on session start.
func (r *Recorder) Begin(s *Session) {
	uuid := sanitizeName(s.FSUUID())
	if uuid == "" {
		uuid = "session-" + s.ID()
	}
	dir := filepath.Join(r.Dir, uuid)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		r.Logger.Error("recorder mkdir failed", "dir", dir, "err", err)
		return
	}

	caller, err := os.Create(filepath.Join(dir, "caller.raw"))
	if err != nil {
		r.Logger.Error("recorder create failed", "err", err)
		return
	}
	agent, err := os.Create(filepath.Join(dir, "agent.raw"))
	if err != nil {
		caller.Close()
		r.Logger.Error("recorder create failed", "err", err)
		return
	}

	cr := &callRecording{
		uuid:       uuid,
		sessionID:  s.ID(),
		rate:       s.SampleRate(),
		mix:        s.MixType(),
		started:    time.Now(),
		dir:        dir,
		callerFile: caller,
		agentFile:  agent,
		eventsDone: make(chan struct{}),
	}

	if r.Bus != nil {
		cr.events, cr.unsub = r.Bus.Subscribe(128)
		ef, err := os.Create(filepath.Join(dir, "events.jsonl"))
		if err == nil {
			cr.eventsFile = ef
			go cr.captureEvents()
		}
	}

	r.mu.Store(s, cr)
}

// Uplink appends caller audio. Called by Server's read pump.
func (r *Recorder) Uplink(s *Session, pcm []byte) {
	v, ok := r.mu.Load(s)
	if !ok {
		return
	}
	cr := v.(*callRecording)
	cr.mu.Lock()
	n, _ := cr.callerFile.Write(pcm)
	cr.callerBytes += int64(n)
	if cr.metadata == nil {
		if m := s.Metadata(); len(m) > 0 {
			cr.metadata = append(json.RawMessage(nil), m...)
		}
	}
	cr.mu.Unlock()
}

// Downlink appends AI audio. Called by the session write pump.
func (r *Recorder) Downlink(s *Session, pcm []byte) {
	v, ok := r.mu.Load(s)
	if !ok {
		return
	}
	cr := v.(*callRecording)
	cr.mu.Lock()
	n, _ := cr.agentFile.Write(pcm)
	cr.agentBytes += int64(n)
	cr.mu.Unlock()
}

// Finish finalizes the bundle: builds audio.wav, writes meta.json, closes
// sidecars, and publishes a recording.complete event.
func (r *Recorder) Finish(s *Session) {
	v, ok := r.mu.LoadAndDelete(s)
	if !ok {
		return
	}
	cr := v.(*callRecording)
	ended := time.Now()

	cr.mu.Lock()
	cr.callerFile.Close()
	cr.agentFile.Close()
	meta := cr.metadata
	callerBytes, agentBytes := cr.callerBytes, cr.agentBytes
	cr.mu.Unlock()

	if cr.unsub != nil {
		cr.unsub() // closes cr.events; captureEvents drains it and signals eventsDone
		<-cr.eventsDone
	}
	if cr.eventsFile != nil {
		cr.eventsFile.Close()
	}

	if err := cr.finalizeWAV(callerBytes, agentBytes); err != nil {
		r.Logger.Error("recorder wav finalize failed", "uuid", cr.uuid, "err", err)
	}
	os.Remove(filepath.Join(cr.dir, "caller.raw"))
	os.Remove(filepath.Join(cr.dir, "agent.raw"))

	durationMs := int64(0)
	if cr.rate > 0 {
		durationMs = callerBytes / 2 * 1000 / int64(cr.rate)
	}
	writeMeta(cr, meta, ended, callerBytes, agentBytes, durationMs)

	if r.Bus != nil {
		r.Bus.Publish(Event{Name: "recording.complete", UUID: s.FSUUID(), Data: map[string]any{
			"path":        cr.dir,
			"session":     cr.sessionID,
			"duration_ms": durationMs,
		}})
	}
	r.Logger.Info("recording complete", "uuid", cr.uuid, "duration_ms", durationMs)
}

// captureEvents writes this call's bus events to events.jsonl. The bus
// closes the events channel on unsubscribe; ranging drains any buffered
// events before we signal completion.
func (cr *callRecording) captureEvents() {
	defer close(cr.eventsDone)
	enc := json.NewEncoder(cr.eventsFile)
	for ev := range cr.events {
		if ev.UUID == cr.uuid || ev.Data["session"] == cr.sessionID {
			_ = enc.Encode(ev)
		}
	}
}

// finalizeWAV interleaves caller.raw (left) and agent.raw (right) into
// audio.wav, padding the agent track with silence to the caller's length.
func (cr *callRecording) finalizeWAV(callerBytes, agentBytes int64) error {
	caller, err := os.Open(filepath.Join(cr.dir, "caller.raw"))
	if err != nil {
		return err
	}
	defer caller.Close()
	agent, err := os.Open(filepath.Join(cr.dir, "agent.raw"))
	if err != nil {
		return err
	}
	defer agent.Close()
	out, err := os.Create(filepath.Join(cr.dir, "audio.wav"))
	if err != nil {
		return err
	}
	defer out.Close()

	// Interleaved stereo data is exactly 2x the caller track length.
	dataLen := uint32(callerBytes * 2)
	if err := writeWAVHeader(out, dataLen, uint32(cr.rate)); err != nil {
		return err
	}

	cbuf := make([]byte, 64*1024)
	abuf := make([]byte, 64*1024)
	ibuf := make([]byte, 2*64*1024)
	remaining := callerBytes
	for remaining > 0 {
		n := int64(len(cbuf))
		if remaining < n {
			n = remaining
		}
		// PCM frames are even-sized; keep it so.
		n &^= 1
		if n <= 0 {
			break
		}
		if _, err := io.ReadFull(caller, cbuf[:n]); err != nil {
			return err
		}
		an, _ := io.ReadFull(agent, abuf[:n]) // short read: rest stays zero (silence)
		_ = an
		for i := int64(0); i < n; i += 2 {
			ibuf[2*i] = cbuf[i]
			ibuf[2*i+1] = cbuf[i+1]
			ibuf[2*i+2] = abuf[i]
			ibuf[2*i+3] = abuf[i+1]
		}
		if _, err := out.Write(ibuf[:2*n]); err != nil {
			return err
		}
		remaining -= n
	}
	return nil
}

// writeWAVHeader writes a 44-byte PCM WAV header for stereo 16-bit audio.
func writeWAVHeader(w io.Writer, dataLen uint32, sampleRate uint32) error {
	const channels = 2
	const bitsPerSample = 16
	byteRate := sampleRate * channels * bitsPerSample / 8
	blockAlign := uint16(channels * bitsPerSample / 8)

	h := make([]byte, 44)
	copy(h[0:], "RIFF")
	putU32(h[4:], 36+dataLen)
	copy(h[8:], "WAVE")
	copy(h[12:], "fmt ")
	putU32(h[16:], 16)         // fmt chunk size
	putU16(h[20:], 1)          // PCM
	putU16(h[22:], channels)   //
	putU32(h[24:], sampleRate) //
	putU32(h[28:], byteRate)   //
	putU16(h[32:], blockAlign) //
	putU16(h[34:], bitsPerSample)
	copy(h[36:], "data")
	putU32(h[40:], dataLen)
	_, err := w.Write(h)
	return err
}

func putU16(b []byte, v uint16) { b[0], b[1] = byte(v), byte(v>>8) }
func putU32(b []byte, v uint32) {
	b[0], b[1], b[2], b[3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
}

type recordingMeta struct {
	UUID       string          `json:"uuid"`
	Session    string          `json:"session"`
	Rate       int             `json:"rate"`
	Mix        string          `json:"mix"`
	Started    time.Time       `json:"started"`
	Ended      time.Time       `json:"ended"`
	DurationMs int64           `json:"duration_ms"`
	CallerPCM  int64           `json:"caller_bytes"`
	AgentPCM   int64           `json:"agent_bytes"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
}

func writeMeta(cr *callRecording, metadata json.RawMessage, ended time.Time, callerBytes, agentBytes, durationMs int64) {
	m := recordingMeta{
		UUID: cr.uuid, Session: cr.sessionID, Rate: cr.rate, Mix: cr.mix,
		Started: cr.started, Ended: ended, DurationMs: durationMs,
		CallerPCM: callerBytes, AgentPCM: agentBytes, Metadata: metadata,
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(cr.dir, "meta.json"), data, 0o644)
}

// sanitizeName restricts bundle dir names to a safe character set.
func sanitizeName(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			return r
		}
		return -1
	}, s)
}

// sweepLoop periodically applies retention policy.
func (r *Recorder) sweepLoop() {
	t := time.NewTicker(r.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
			r.sweep()
		}
	}
}

func (r *Recorder) sweep() {
	type bundle struct {
		dir  string
		mod  time.Time
		size int64
	}
	entries, err := os.ReadDir(r.Dir)
	if err != nil {
		return
	}
	var bundles []bundle
	var total int64
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(r.Dir, e.Name())
		var size int64
		var mod time.Time
		filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				size += info.Size()
				if info.ModTime().After(mod) {
					mod = info.ModTime()
				}
			}
			return nil
		})
		bundles = append(bundles, bundle{dir: dir, mod: mod, size: size})
		total += size
	}
	sort.Slice(bundles, func(i, j int) bool { return bundles[i].mod.Before(bundles[j].mod) })

	cutoff := time.Now().Add(-r.Retention)
	for _, b := range bundles {
		age := b.mod.Before(cutoff)
		over := r.MaxBytes > 0 && total > r.MaxBytes
		if !age && !over {
			continue
		}
		if err := os.RemoveAll(b.dir); err != nil {
			r.Logger.Warn("recorder sweep delete failed", "dir", b.dir, "err", err)
			continue
		}
		total -= b.size
		r.Logger.Info("recorder swept bundle", "dir", b.dir, "aged_out", age)
	}
}
