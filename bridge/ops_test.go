package bridge

import (
	"bufio"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeLoki accepts pushes and inspects them.
type fakeLoki struct {
	mu      sync.Mutex
	streams []map[string]any
	body    string
	status  int
	hits    int32
}

func newFakeLoki(t *testing.T) (*fakeLoki, *httptest.Server) {
	t.Helper()
	f := &fakeLoki{status: 204}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.hits, 1)
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.body = string(body)
		var doc struct {
			Streams []map[string]any `json:"streams"`
		}
		if err := json.Unmarshal(body, &doc); err == nil {
			f.streams = append(f.streams, doc.Streams...)
		}
		f.mu.Unlock()
		w.WriteHeader(f.status)
	}))
	t.Cleanup(srv.Close)
	return f, srv
}

func TestLokiShipper(t *testing.T) {
	f, srv := newFakeLoki(t)

	loki, err := NewLokiClient(LokiConfig{
		URL:           srv.URL,
		Job:           "fsbridge-test",
		Host:          "h",
		BatchSize:     2,
		FlushInterval: 50 * time.Millisecond,
		QueueSize:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer loki.Close()

	loki.Send("info", `{"msg":"hello"}`)
	loki.Send("warn", `{"msg":"careful"}`)
	loki.Send("error", `{"msg":"oh no"}`)

	// Wait for at least two push requests (BatchSize=2, plus the trailing
	// partial flushes via FlushInterval).
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&f.hits) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&f.hits) == 0 {
		t.Fatal("no Loki push observed")
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.streams) == 0 {
		t.Fatal("no batch captured")
	}
	// We should have at least one stream per distinct level.
	got := map[string]bool{}
	for _, s := range f.streams {
		streamAny, hasStream := s["stream"]
		if !hasStream {
			continue
		}
		stream, ok := streamAny.(map[string]any)
		if !ok {
			continue
		}
		levelAny, hasLevel := stream["level"]
		if !hasLevel {
			continue
		}
		lvl, ok := levelAny.(string)
		if !ok {
			continue
		}
		got[lvl] = true
	}
	for _, want := range []string{"info", "warn", "error"} {
		if !got[want] {
			t.Errorf("missing level %q in streamed batches", want)
		}
	}
}

func TestLokiSlogHandler(t *testing.T) {
	f, srv := newFakeLoki(t)

	loki, _ := NewLokiClient(LokiConfig{
		URL: srv.URL, Job: "fsbridge-test", Host: "h",
		BatchSize: 1, FlushInterval: 20 * time.Millisecond,
	})

	var stderr strings.Builder
	inner := slog.NewTextHandler(&stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(NewLokiHandler(inner, loki))
	logger.Info("hello world", "k1", "v1")

	// Allow batcher to push.
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&f.hits) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	loki.Close() // ensures drain before we check stderr

	if !strings.Contains(stderr.String(), "hello world") {
		t.Errorf("inner handler didn't see record: %q", stderr.String())
	}
	if atomic.LoadInt32(&f.hits) == 0 {
		t.Fatal("Loki never received the log")
	}
}

func TestLokiDropsOnOverflow(t *testing.T) {
	f, srv := newFakeLoki(t)
	f.status = 500 // force Loki push to fail every time → batcher busy
	loki, _ := NewLokiClient(LokiConfig{
		URL: srv.URL, Job: "fsbridge-test", Host: "h",
		BatchSize: 1, FlushInterval: 10 * time.Millisecond, QueueSize: 4,
	})
	defer loki.Close()

	for i := 0; i < 1000; i++ {
		loki.Send("info", `{"msg":"drop"}`)
	}
	// Just confirm Send is non-blocking under saturation; we don't
	// assert on hits (server is busy) — only that the call returns.
}

func TestAuthMiddleware(t *testing.T) {
	mw := AuthMiddleware("secret", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv := httptest.NewServer(mw)
	defer srv.Close()

	// No token → 401.
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 401 {
		t.Errorf("no token = %d, want 401", resp.StatusCode)
	}
	resp.Body.Close()

	// Query token.
	resp, _ = http.Get(srv.URL + "?token=secret")
	if resp.StatusCode != 200 {
		t.Errorf("query token = %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Header token.
	req, _ := http.NewRequest("GET", srv.URL, nil)
	req.Header.Set("Authorization", "Bearer secret")
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != 200 {
		t.Errorf("bearer = %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Wrong token.
	req, _ = http.NewRequest("GET", srv.URL, nil)
	req.Header.Set("Authorization", "Bearer wrong")
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != 401 {
		t.Errorf("wrong bearer = %d, want 401", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestAuthMiddlewareNoTokenAllows(t *testing.T) {
	mw := AuthMiddleware("", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv := httptest.NewServer(mw)
	defer srv.Close()
	resp, _ := http.Get(srv.URL)
	if resp.StatusCode != 200 {
		t.Errorf("empty token config = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
}

// sanity: ReadAll on response body to satisfy linter and confirm drain
var _ = bufio.NewReader
