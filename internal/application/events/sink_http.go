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
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/ledger/v3/internal/pkg/httptransport"
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
// Only a 2xx response from the configured endpoint acknowledges an event. The
// sink never follows redirects: any 3xx is a delivery failure, so the body, its
// signature and the endpoint URL never reach an unconfigured destination.
type HTTPSink struct {
	errors sinkErrorSanitizer
	client *http.Client
	// ownsTransport is false when the client shares an uncloneable
	// http.DefaultTransport, whose idle connections Close must leave alone.
	ownsTransport bool
	endpoint      string
	secret        string
	format        Format
}

// NewHTTPSink creates a new HTTP webhook sink.
func NewHTTPSink(cfg HTTPSinkConfig) (_ *HTTPSink, retErr error) {
	sanitizer := newSinkErrorSanitizer([]string{cfg.Endpoint}, cfg.Secret)
	defer sanitizer.sanitizeReturned(&retErr)
	if cfg.Endpoint == "" {
		return nil, errors.New("HTTP sink endpoint is required")
	}

	// Own the transport when possible: Close must not drop connections of
	// other sinks or of any other client sharing http.DefaultTransport.
	transport, ownsTransport := httptransport.CloneDefault()

	return &HTTPSink{
		errors:        sanitizer,
		ownsTransport: ownsTransport,
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
			// Return every redirect response to post, which reports it as a failure.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		endpoint: cfg.Endpoint,
		secret:   cfg.Secret,
		format:   cfg.Format,
	}, nil
}

func (s *HTTPSink) Publish(ctx context.Context, events []*eventspb.Event) (retErr error) {
	defer s.errors.sanitizeReturned(&retErr)
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
	defer s.errors.sanitizeReturned(&retErr)
	if s.ownsTransport {
		s.client.CloseIdleConnections()
	}

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
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return fmt.Errorf("endpoint %s returned redirect %s to %s; configure the final URL",
			s.endpoint, resp.Status, redirectTarget(resp))
	}

	return fmt.Errorf("unexpected status code %s from %s", resp.Status, s.endpoint)
}

// redirectTarget reduces the remote Location, resolved against the request URL,
// to scheme, host and path. Userinfo, query and fragment can carry credentials
// and are not reported.
func redirectTarget(resp *http.Response) string {
	parsed, err := resp.Location()
	if errors.Is(err, http.ErrNoLocation) {
		return "(no Location header)"
	}
	if err != nil {
		return "(invalid Location header)"
	}
	safe := url.URL{Scheme: parsed.Scheme, Host: parsed.Host, Path: parsed.Path}

	return strconv.Quote(safe.String())
}
