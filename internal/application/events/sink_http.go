package events

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
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
	Batch    bool
}

// HTTPSink publishes events to an HTTP endpoint via POST requests.
// Events are sent one at a time by default; Batch enables versioned requests
// containing multiple events for receivers that support the batch contract.
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
	batch         bool
}

// NewHTTPSink creates a new HTTP webhook sink.
func NewHTTPSink(cfg HTTPSinkConfig) (*HTTPSink, error) {
	return newHTTPSink(cfg, http.DefaultTransport)
}

// newHTTPSink builds the sink on a transport derived from defaultTransport.
func newHTTPSink(cfg HTTPSinkConfig, defaultTransport http.RoundTripper) (_ *HTTPSink, retErr error) {
	sanitizer := newSinkErrorSanitizer([]string{cfg.Endpoint}, cfg.Secret)
	defer sanitizer.sanitizeReturned(&retErr)
	if cfg.Endpoint == "" {
		return nil, errors.New("HTTP sink endpoint is required")
	}

	// Own the transport when possible: Close must not drop connections of
	// other sinks or of any other client sharing the default transport.
	transport, ownsTransport := httptransport.Clone(defaultTransport)

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
		batch:    cfg.Batch,
	}, nil
}

func (s *HTTPSink) Publish(ctx context.Context, events []*eventspb.Event) (retErr error) {
	defer s.errors.sanitizeReturned(&retErr)
	if s.batch {
		return s.publishBatch(ctx, events)
	}
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

// HTTP batch v1 uses a JSON array or consecutive uint32 big-endian length
// prefixed protobuf Events. This is a packing target: an event larger than
// the target is sent alone so it cannot permanently stall the shared sink.
// A failed POST leaves the entire Publish unacknowledged.
const maxHTTPBatchBytes = 1 << 20

func (s *HTTPSink) publishBatch(ctx context.Context, events []*eventspb.Event) error {
	if len(events) == 0 {
		return nil
	}
	var body []byte
	flush := func(first, last *eventspb.Event, count int) error {
		if s.format == FormatJSON {
			body = append(body, ']')
		}
		if err := s.postBatch(ctx, first, last, count, body); err != nil {
			return fmt.Errorf("posting batch seq=%d..%d: %w", first.GetLogSequence(), last.GetLogSequence(), err)
		}
		body = nil
		return nil
	}
	var first, last *eventspb.Event
	count := 0
	for _, event := range events {
		data, err := SerializeEvent(event, s.format)
		if err != nil {
			return fmt.Errorf("serializing event seq=%d: %w", event.GetLogSequence(), err)
		}
		overhead := 4
		if s.format == FormatJSON {
			overhead = 2 // brackets or comma and closing bracket
		}
		if count > 0 && len(body)+len(data)+overhead > maxHTTPBatchBytes {
			if err := flush(first, last, count); err != nil {
				return err
			}
			count = 0
		}
		if count == 0 {
			first = event
			if s.format == FormatJSON {
				body = append(body, '[')
			}
		} else if s.format == FormatJSON {
			body = append(body, ',')
		}
		if s.format == FormatProto {
			body = binary.BigEndian.AppendUint32(body, uint32(len(data)))
		}
		body = append(body, data...)
		last = event
		count++
	}
	return flush(first, last, count)
}

func (s *HTTPSink) postBatch(ctx context.Context, first, last *eventspb.Event, count int, body []byte) error {
	return s.send(ctx, body, func(req *http.Request) {
		req.Header.Set("X-Ledger-Batch-Version", "1")
		req.Header.Set("X-Log-Sequence-First", strconv.FormatUint(first.GetLogSequence(), 10))
		req.Header.Set("X-Log-Sequence-Last", strconv.FormatUint(last.GetLogSequence(), 10))
		req.Header.Set("X-Event-Count", strconv.Itoa(count))
	})
}

func (s *HTTPSink) Close() (retErr error) {
	defer s.errors.sanitizeReturned(&retErr)
	if s.ownsTransport {
		s.client.CloseIdleConnections()
	}

	return nil
}

func (s *HTTPSink) post(ctx context.Context, event *eventspb.Event, body []byte) error {
	return s.send(ctx, body, func(req *http.Request) {
		req.Header.Set("X-Event-Type", strings.ToLower(event.GetType().String()))
		req.Header.Set("X-Ledger", event.GetLedger())
		req.Header.Set("X-Log-Sequence", strconv.FormatUint(event.GetLogSequence(), 10))
	})
}

func (s *HTTPSink) send(ctx context.Context, body []byte, headers func(*http.Request)) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	contentType := "application/json"
	if s.format == FormatProto {
		contentType = "application/protobuf"
	}

	req.Header.Set("Content-Type", contentType)
	headers(req)

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
