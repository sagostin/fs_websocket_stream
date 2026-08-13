package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

// LokiConfig configures the optional Loki push shipper.
type LokiConfig struct {
	// URL is the Loki base URL, e.g. http://loki:3100 or
	// https://logs-prod-eu-west-0.grafana.net for Grafana Cloud.
	URL string
	// TenantID is sent as the X-Scope-OrgID header (Grafana Cloud).
	TenantID string
	// Job is the static `job` label. Default "fsbridge".
	Job string
	// Host is the static `host` label. Default os.Hostname().
	Host string
	// Username/Password use HTTP Basic auth (for Loki behind a proxy).
	Username string
	Password string
	// BatchSize: flush when this many entries queued. Default 100.
	BatchSize int
	// FlushInterval: flush at least this often. Default 5s.
	FlushInterval time.Duration
	// QueueSize: drop entries beyond this rather than block logs.
	// Default 10000.
	QueueSize int
}

// LokiClient ships structured logs to a Loki push endpoint. It is a
// best-effort, drop-on-overflow shipper — it never blocks log emission.
type LokiClient struct {
	cfg     LokiConfig
	http    *http.Client
	queue   chan lokiEntry
	stop    chan struct{}
	stopOne sync.Once
	done    chan struct{}
}

type lokiEntry struct {
	ts    time.Time
	level string
	line  string // pre-rendered JSON of the slog record
}

// NewLokiClient validates config and starts the background batcher.
func NewLokiClient(cfg LokiConfig) (*LokiClient, error) {
	if cfg.URL == "" {
		return nil, errors.New("loki: URL required")
	}
	if cfg.Job == "" {
		cfg.Job = "fsbridge"
	}
	if cfg.Host == "" {
		if h, err := os.Hostname(); err == nil {
			cfg.Host = h
		}
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 10000
	}
	c := &LokiClient{
		cfg:  cfg,
		http: &http.Client{Timeout: 15 * time.Second},
		// Buffered at QueueSize; non-blocking sends to drop under pressure.
		queue: make(chan lokiEntry, cfg.QueueSize),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	go c.run()
	return c, nil
}

// Close flushes any pending entries and stops the batcher.
func (c *LokiClient) Close() error {
	c.stopOne.Do(func() { close(c.stop) })
	<-c.done
	return nil
}

// Send enqueues a pre-rendered JSON log line.
func (c *LokiClient) Send(level, jsonLine string) {
	e := lokiEntry{ts: time.Now(), level: level, line: jsonLine}
	select {
	case c.queue <- e:
	default:
		// Drop; never block log emission.
	}
}

// LokiHandler is a slog.Handler that forwards to an inner handler (e.g. a
// JSON stderr handler) and also ships records to Loki.
type LokiHandler struct {
	inner slog.Handler
	loki  *LokiClient
}

// NewLokiHandler wraps inner; all records are emitted to inner and sent to
// Loki asynchronously.
func NewLokiHandler(inner slog.Handler, loki *LokiClient) *LokiHandler {
	return &LokiHandler{inner: inner, loki: loki}
}

func (h *LokiHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *LokiHandler) Handle(ctx context.Context, r slog.Record) error {
	if err := h.inner.Handle(ctx, r); err != nil {
		return err
	}
	var buf bytes.Buffer
	attrs := map[string]any{}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	if err := json.NewEncoder(&buf).Encode(map[string]any{
		"time":  r.Time,
		"level": r.Level.String(),
		"msg":   r.Message,
		"attrs": attrs,
	}); err == nil {
		h.loki.Send(r.Level.String(), buf.String())
	}
	return nil
}

func (h *LokiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &LokiHandler{inner: h.inner.WithAttrs(attrs), loki: h.loki}
}

func (h *LokiHandler) WithGroup(name string) slog.Handler {
	return &LokiHandler{inner: h.inner.WithGroup(name), loki: h.loki}
}

// run batches entries and pushes them to Loki. Flushes on BatchSize or
// FlushInterval, whichever comes first.
func (c *LokiClient) run() {
	defer close(c.done)
	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()
	batch := make([]lokiEntry, 0, c.cfg.BatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := c.push(batch); err != nil {
			// Best-effort: don't retry; next flush will retry fresh entries.
			fmt.Fprintf(io.Discard, "loki: %v\n", err)
		}
		batch = batch[:0]
	}
	for {
		select {
		case <-c.stop:
			// Drain.
			for {
				select {
				case e := <-c.queue:
					batch = append(batch, e)
				default:
					flush()
					return
				}
			}
		case e := <-c.queue:
			batch = append(batch, e)
			if len(batch) >= c.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// push POSTs the entries as a single streams batch to Loki.
func (c *LokiClient) push(entries []lokiEntry) error {
	// Group by level so each level becomes its own stream (Loki quirk:
	// labels must be constant per stream).
	by := map[string][][2]string{}
	for _, e := range entries {
		ns := fmt.Sprintf("%d", e.ts.UnixNano())
		by[e.level] = append(by[e.level], [2]string{ns, e.line})
	}
	streams := make([]map[string]any, 0, len(by))
	for level, vals := range by {
		streams = append(streams, map[string]any{
			"stream": map[string]string{
				"job":   c.cfg.Job,
				"host":  c.cfg.Host,
				"level": level,
			},
			"values": vals,
		})
	}
	body, _ := json.Marshal(map[string]any{"streams": streams})

	req, err := http.NewRequest(http.MethodPost, c.cfg.URL+"/loki/api/v1/push", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.TenantID != "" {
		req.Header.Set("X-Scope-OrgID", c.cfg.TenantID)
	}
	if c.cfg.Username != "" {
		req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("loki push: HTTP %d", resp.StatusCode)
	}
	return nil
}
