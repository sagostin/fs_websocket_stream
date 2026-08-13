// Command s3uploader is an example of post-call processing driven by the
// fsbridge event stream: it subscribes to the /control endpoint, and on
// every `recording.complete` event uploads the call's recording bundle
// (audio.wav, meta.json, events.jsonl) to S3.
//
// This is the integration seam for AI-agent debugging/eval pipelines: the
// same hook can fan out to an eval runner, a data lake, or a support UI.
//
// Usage:
//
//	export AWS_REGION=us-east-1            # + standard AWS credentials
//	s3uploader -control ws://localhost:8090/control -bucket my-call-recordings [-prefix prod/]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/coder/websocket"
)

// event mirrors bridge.Event (kept local: this example is a separate module).
type event struct {
	Name string         `json:"event"`
	UUID string         `json:"uuid,omitempty"`
	Data map[string]any `json:"data,omitempty"`
}

func main() {
	control := flag.String("control", "ws://localhost:8090/control", "fsbridge control endpoint")
	bucket := flag.String("bucket", "", "S3 bucket for recording bundles")
	prefix := flag.String("prefix", "recordings/", "S3 key prefix")
	flag.Parse()

	if *bucket == "" {
		slog.Error("-bucket required")
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Error("aws config", "err", err)
		os.Exit(1)
	}
	s3c := s3.NewFromConfig(cfg)

	for {
		if err := run(ctx, *control, *bucket, *prefix, s3c, logger); err != nil {
			logger.Warn("control connection lost", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func run(ctx context.Context, controlURL, bucket, prefix string, s3c *s3.Client, logger *slog.Logger) error {
	conn, _, err := websocket.Dial(ctx, controlURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	logger.Info("connected", "control", controlURL)

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		var ev event
		if json.Unmarshal(data, &ev) != nil || ev.Name != "recording.complete" {
			continue
		}
		path, _ := ev.Data["path"].(string)
		if path == "" {
			continue
		}
		go uploadBundle(ctx, s3c, bucket, prefix, ev.UUID, path, logger)
	}
}

// uploadBundle pushes every file in the bundle dir to
// s3://<bucket>/<prefix><uuid>/<file>.
func uploadBundle(ctx context.Context, s3c *s3.Client, bucket, prefix, uuid, dir string, logger *slog.Logger) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		logger.Warn("bundle unreadable", "dir", dir, "err", err)
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		f, err := os.Open(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		key := prefix + uuid + "/" + e.Name()
		_, err = s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   f,
		})
		f.Close()
		if err != nil {
			logger.Warn("upload failed", "key", key, "err", err)
			return
		}
	}
	logger.Info("bundle uploaded", "uuid", uuid, "dest", "s3://"+bucket+"/"+prefix+uuid+"/")
}
