package events

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

// HTTPSinkConfig holds configuration for the HTTP webhook sink.
type HTTPSinkConfig struct {
	Endpoint string
	Secret   string // HMAC-SHA256 secret for X-Webhook-Signature header (optional)
	Format   Format
}

// HTTPSink publishes events to an HTTP endpoint via POST requests.
// Each batch is sent as a single POST with a JSON array or protobuf body.
// Events are sent one at a time to allow the receiver to process them individually.
type HTTPSink struct {
	client   *http.Client
	endpoint string
	secret   string
	format   Format
}

// NewHTTPSink creates a new HTTP webhook sink.
func NewHTTPSink(cfg HTTPSinkConfig) (*HTTPSink, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("HTTP sink endpoint is required")
	}

	return &HTTPSink{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		endpoint: cfg.Endpoint,
		secret:   cfg.Secret,
		format:   cfg.Format,
	}, nil
}

func (s *HTTPSink) Publish(ctx context.Context, events []*eventspb.Event) error {
	for _, event := range events {
		data, err := SerializeEvent(event, s.format)
		if err != nil {
			return fmt.Errorf("serializing event seq=%d: %w", event.GetLogSequence(), err)
		}

		if err := s.post(ctx, event, data); err != nil {
			return fmt.Errorf("posting event seq=%d: %w", event.GetLogSequence(), err)
		}
	}

	return nil
}

func (s *HTTPSink) Close() error {
	s.client.CloseIdleConnections()

	return nil
}

func (s *HTTPSink) post(ctx context.Context, event *eventspb.Event, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	contentType := "application/json"
	if s.format == FormatProto {
		contentType = "application/protobuf"
	}

	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-Event-Type", strings.ToLower(event.GetType().String()))
	req.Header.Set("X-Ledger", event.GetLedger())
	req.Header.Set("X-Log-Sequence", strconv.FormatUint(event.GetLogSequence(), 10))

	if s.secret != "" {
		mac := hmac.New(sha256.New, []byte(s.secret))
		mac.Write(body)
		req.Header.Set("X-Webhook-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
}
