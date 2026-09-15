package sdk

import (
	"context"
	"errors"
	"io"
	"sync"
)

// ErrNoRequestHandler is returned when a MemoryHost has no request behavior.
var ErrNoRequestHandler = errors.New("memory host request handler is not configured")

// ErrNoWaitHandler is returned when a MemoryHost has no controlled wait
// behavior. MemoryHost never sleeps implicitly.
var ErrNoWaitHandler = errors.New("memory host wait handler is not configured")

// RequestHandler provides product responses for a MemoryHost request.
type RequestHandler func(context.Context, Request) (Responses, error)

// WaitHandler provides deterministic wait completion for a MemoryHost.
type WaitHandler func(context.Context, WaitRequest) error

// MemoryHostOption configures one deterministic test-host surface.
type MemoryHostOption func(*MemoryHost)

// WithWaitHandler configures controlled wait completion. The handler owns no
// timer semantics unless the test supplies them explicitly.
func WithWaitHandler(handler WaitHandler) MemoryHostOption {
	return func(host *MemoryHost) {
		host.waitHandler = handler
	}
}

// MemoryHost is a deterministic, concurrency-safe Host for plugin-core tests.
type MemoryHost struct {
	handler     RequestHandler
	waitHandler WaitHandler

	mu       sync.Mutex
	requests []Request
	waits    []WaitRequest
	events   []Event
	logs     []LogEntry
}

// NewMemoryHost constructs a host backed by handler.
func NewMemoryHost(handler RequestHandler, options ...MemoryHostOption) *MemoryHost {
	host := &MemoryHost{handler: handler}
	for _, option := range options {
		if option != nil {
			option(host)
		}
	}
	return host
}

// Wait records a relative wait request and delegates completion to the
// controlled handler. It never treats elapsed time as product success.
func (h *MemoryHost) Wait(ctx context.Context, request WaitRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	h.waits = append(h.waits, request)
	h.mu.Unlock()
	if h.waitHandler == nil {
		return ErrNoWaitHandler
	}
	return h.waitHandler(ctx, request)
}

// NewResponseStream returns an in-memory stream that ends with io.EOF.
func NewResponseStream(responses ...Response) Responses {
	clones := make([]Response, len(responses))
	for index, response := range responses {
		clones[index] = cloneResponse(response)
	}
	return &memoryResponses{responses: clones}
}

// Request records the request and delegates it to the configured handler.
func (h *MemoryHost) Request(ctx context.Context, request Request) (Responses, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	request = cloneRequest(request)
	h.mu.Lock()
	h.requests = append(h.requests, request)
	h.mu.Unlock()

	if h.handler == nil {
		return nil, ErrNoRequestHandler
	}
	responses, err := h.handler(ctx, cloneRequest(request))
	if err != nil {
		return nil, err
	}
	if responses == nil {
		return nil, errors.New("memory host request handler returned nil responses")
	}
	return &contextResponses{ctx: ctx, responses: responses}, nil
}

// Emit records an output event.
func (h *MemoryHost) Emit(event Event) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, cloneEvent(event))
	return nil
}

// Log records a diagnostic entry.
func (h *MemoryHost) Log(level Level, message string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.logs = append(h.logs, LogEntry{Level: level, Message: message})
}

// Requests returns a snapshot of recorded requests.
func (h *MemoryHost) Requests() []Request {
	h.mu.Lock()
	defer h.mu.Unlock()
	requests := make([]Request, len(h.requests))
	for index, request := range h.requests {
		requests[index] = cloneRequest(request)
	}
	return requests
}

// Waits returns a snapshot of recorded relative wait requests.
func (h *MemoryHost) Waits() []WaitRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]WaitRequest(nil), h.waits...)
}

// Events returns a snapshot of recorded events.
func (h *MemoryHost) Events() []Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	events := make([]Event, len(h.events))
	for index, event := range h.events {
		events[index] = cloneEvent(event)
	}
	return events
}

// Logs returns a snapshot of recorded logs.
func (h *MemoryHost) Logs() []LogEntry {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]LogEntry(nil), h.logs...)
}

type contextResponses struct {
	ctx       context.Context
	responses Responses
}

type memoryResponses struct {
	mu        sync.Mutex
	responses []Response
	index     int
}

func (r *memoryResponses) Recv() (Response, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.index == len(r.responses) {
		return Response{}, io.EOF
	}
	response := cloneResponse(r.responses[r.index])
	r.index++
	return response, nil
}

func (r *contextResponses) Recv() (Response, error) {
	if err := r.ctx.Err(); err != nil {
		return Response{}, err
	}
	response, err := r.responses.Recv()
	if err != nil {
		return Response{}, err
	}
	if err := r.ctx.Err(); err != nil {
		return Response{}, err
	}
	return cloneResponse(response), nil
}

func (r *contextResponses) ResponseStreamMetadata() ResponseStreamMetadata {
	return ResponseStreamMetadataOf(r.responses)
}

func cloneRequest(request Request) Request {
	clone := request
	if request.HTTP != nil {
		http := *request.HTTP
		http.Body = append([]byte(nil), request.HTTP.Body...)
		if request.HTTP.Query != nil {
			http.Query = make(map[string][]string, len(request.HTTP.Query))
			for key, values := range request.HTTP.Query {
				http.Query[key] = append([]string(nil), values...)
			}
		}
		if request.HTTP.Headers != nil {
			http.Headers = make(map[string][]string, len(request.HTTP.Headers))
			for key, values := range request.HTTP.Headers {
				http.Headers[key] = append([]string(nil), values...)
			}
		}
		clone.HTTP = &http
	}
	if request.GRPC != nil {
		grpc := *request.GRPC
		grpc.Message = append([]byte(nil), request.GRPC.Message...)
		clone.GRPC = &grpc
	}
	return clone
}

func cloneResponse(response Response) Response {
	response.Body = append([]byte(nil), response.Body...)
	return response
}

func cloneEvent(event Event) Event {
	event.Payload = append([]byte(nil), event.Payload...)
	return event
}
