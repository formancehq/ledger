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
// Events are sent one at a time to allow the receiver to process them individually.
type HTTPSink struct {
	errors   sinkErrorSanitizer
	client   *http.Client
	endpoint string
	secret   string
	format   Format
}

// NewHTTPSink creates a new HTTP webhook sink.
func NewHTTPSink(cfg HTTPSinkConfig) (result *HTTPSink, retErr error) {
	sanitizer := newSinkErrorSanitizer([]string{cfg.Endpoint}, cfg.Secret)
	defer sanitizer.finish(&retErr)
	if cfg.Endpoint == "" {
		return nil, errors.New("HTTP sink endpoint is required")
	}

	return &HTTPSink{
		errors: sanitizer,
		client: &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				// A bodyless GET cannot acknowledge the event POST. Return the
				// redirect response so post reports its non-2xx status as a failure.
				if req.Method != http.MethodPost {
					return http.ErrUseLastResponse
				}
				// Installing a policy replaces net/http's default redirect limit.
				if len(via) >= 10 {
					return errors.New("stopped after 10 redirects")
				}

				return nil
			},
		},
		endpoint: cfg.Endpoint,
		secret:   cfg.Secret,
		format:   cfg.Format,
	}, nil
}

func (s *HTTPSink) Publish(ctx context.Context, events []*eventspb.Event) (retErr error) {
	defer s.errors.finish(&retErr)
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

func (s *HTTPSink) Close() (retErr error) {
	defer s.errors.finish(&retErr)
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
