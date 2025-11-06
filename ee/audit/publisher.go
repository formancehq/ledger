//go:build ee

package audit

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/v3/publish"
	goLibsAudit "github.com/formancehq/go-libs/v3/audit"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Client wraps the existing publisher to send audit logs
// This avoids creating a separate Kafka/NATS connection
type Client struct {
	publisher        message.Publisher
	topic            string // Audit topic (auto-detected from wildcard mapping)
	appName          string
	maxBodySize      int64
	excludedPaths    []string
	sensitiveHeaders []string
	logger           *zap.Logger
	bufPool          *sync.Pool
}

// NewClientWithPublisher creates an audit client using existing publisher
func NewClientWithPublisher(
	publisher message.Publisher,
	topic string,
	appName string,
	maxBodySize int64,
	excludedPaths []string,
	sensitiveHeaders []string,
	logger *zap.Logger,
) *Client {
	return &Client{
		publisher:        publisher,
		topic:            topic,
		appName:          appName,
		maxBodySize:      maxBodySize,
		excludedPaths:    excludedPaths,
		sensitiveHeaders: sensitiveHeaders,
		logger:           logger,
		bufPool: &sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
	}
}

// AuditHTTPRequest audits an HTTP request/response
func (c *Client) AuditHTTPRequest(w http.ResponseWriter, r *http.Request, next http.Handler) {
	// Check if path is excluded
	for _, excludedPath := range c.excludedPaths {
		if r.URL.Path == excludedPath {
			next.ServeHTTP(w, r)
			return
		}
	}

	// Capture request
	request := goLibsAudit.HTTPRequest{
		Method: r.Method,
		Path:   r.URL.Path,
		Host:   r.Host,
		Header: r.Header,
		Body:   "",
	}

	// Read body with size limit
	var body []byte
	var err error
	if c.maxBodySize > 0 {
		limitedReader := io.LimitReader(r.Body, c.maxBodySize)
		body, err = io.ReadAll(limitedReader)
	} else {
		body, err = io.ReadAll(r.Body)
	}

	if err != nil && err != io.EOF {
		c.logger.Error("failed to read request body", zap.Error(err))
	}

	if len(body) > 0 {
		request.Body = string(body)
		_ = r.Body.Close()
		r.Body = io.NopCloser(bytes.NewBuffer(body))
	}

	// Capture response
	buf := c.bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.bufPool.Put(buf)

	rww := NewResponseWriterWrapper(w, buf)
	next.ServeHTTP(rww, r)

	response := goLibsAudit.HTTPResponse{
		StatusCode: *rww.StatusCode,
		Headers:    rww.Header(),
		Body:       rww.Body.String(),
	}

	// Publish audit event
	c.publishAuditEvent(r.Context(), request, response)
}

func (c *Client) publishAuditEvent(ctx context.Context, req goLibsAudit.HTTPRequest, resp goLibsAudit.HTTPResponse) {
	// Extract identity from context (set by auth middleware)
	identity := goLibsAudit.ExtractIdentity(ctx, c.logger)

	// Sanitize headers
	if req.Header != nil {
		req.Header = goLibsAudit.SanitizeHeaders(req.Header, c.sensitiveHeaders)
	}

	// Create payload
	payload := struct {
		ID       string                  `json:"id"`
		Identity string                  `json:"identity"`
		Request  goLibsAudit.HTTPRequest `json:"request"`
		Response goLibsAudit.HTTPResponse `json:"response"`
	}{
		ID:       uuid.New().String(),
		Identity: identity,
		Request:  req,
		Response: resp,
	}

	// Create event message (using same format as ledger events)
	eventMessage := publish.EventMessage{
		Date:    time.Now().UTC(),
		App:     c.appName,
		Version: "v1",
		Type:    "AUDIT",
		Payload: payload,
	}

	msg := publish.NewMessage(ctx, eventMessage)

	// Publish to audit topic (auto-detected from wildcard mapping as {stack}.audit)
	if err := c.publisher.Publish(c.topic, msg); err != nil {
		c.logger.Error("failed to publish audit message",
			zap.Error(err),
			zap.String("method", req.Method),
			zap.String("path", req.Path),
			zap.Int("status", resp.StatusCode),
		)
	}
}

// Close is a no-op since we don't own the publisher
func (c *Client) Close() error {
	return nil
}
