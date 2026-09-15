package portable

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

// pumpHost is the sdk.Host the author's code sees. Every blocking operation
// buffers the frames produced so far, hands them to the runtime, and waits for
// the response that settles it.
type pumpHost struct {
	pump         *Pump
	correlations CorrelationSource
	// executionContext is the only cancellation signal the suspended author
	// has. Every blocking handover must select on it, or a cancelled execution
	// would leave this goroutine parked forever.
	executionContext context.Context
	mu               sync.Mutex
	buffer           []Frame
}

func (h *pumpHost) take() []Frame {
	h.mu.Lock()
	defer h.mu.Unlock()
	frames := append([]Frame(nil), h.buffer...)
	h.buffer = nil
	return frames
}

func (h *pumpHost) appendFrame(frame Frame) {
	h.mu.Lock()
	h.buffer = append(h.buffer, frame)
	h.mu.Unlock()
}

func (h *pumpHost) replaceBuffer(frame Frame) {
	h.mu.Lock()
	h.buffer = []Frame{frame}
	h.mu.Unlock()
}

// Request opens one host operation and returns a stream whose Recv suspends the
// author. Deferring suspension until Recv permits several requests in flight.
func (h *pumpHost) Request(ctx context.Context, request sdk.Request) (sdk.Responses, error) {
	request = cloneRequest(request)
	if requestPayloadBytes(request) > MaxLogicalPayloadBytes {
		h.pump.markFatal(sdk.FailureBudgetExhausted)
		return nil, sdk.Failure{Code: string(sdk.FailureBudgetExhausted), Message: "portable logical request payload exceeds its byte limit"}
	}
	operation, err := h.open(Frame{Kind: FrameHostRequest, Request: request})
	if err != nil {
		return nil, err
	}
	return &pumpResponses{host: h, context: ctx, operation: operation}, nil
}

// Wait suspends the author until the runtime reports the admitted delay
// elapsed. Completion carries no product status.
func (h *pumpHost) Wait(ctx context.Context, request sdk.WaitRequest) error {
	operation, err := h.open(Frame{Kind: FrameWaitRequest, Wait: request})
	if err != nil {
		return err
	}
	response, err := h.await(ctx, operation)
	if err != nil {
		return err
	}
	if response.Err != nil {
		return response.Err
	}
	if !response.Complete {
		return errors.New("portable pump: a wait completion must settle the wait")
	}
	return nil
}

func (h *pumpHost) ReadInput(ctx context.Context, opaqueHandle string) (sdk.InputArtifactChunk, error) {
	if opaqueHandle == "" || len(opaqueHandle) > sdk.MaxInputArtifactHandleBytes {
		return sdk.InputArtifactChunk{}, sdk.Failure{Code: string(sdk.FailureOperationNotPermitted), Message: "operation not permitted"}
	}
	operation, err := h.open(Frame{Kind: FrameInputRead, InputHandle: opaqueHandle})
	if err != nil {
		return sdk.InputArtifactChunk{}, err
	}
	response, err := h.await(ctx, operation)
	if err != nil {
		return sdk.InputArtifactChunk{}, err
	}
	if response.Err != nil {
		return sdk.InputArtifactChunk{}, response.Err
	}
	if !response.Complete {
		return sdk.InputArtifactChunk{}, errors.New("portable pump: an input read must settle")
	}
	return response.Input, nil
}

func (h *pumpHost) Emit(event sdk.Event) error {
	h.appendFrame(Frame{Kind: FrameEvent, Event: event})
	return nil
}

func (h *pumpHost) Log(level sdk.Level, message string) {
	h.appendFrame(Frame{Kind: FrameLog, Level: level, Message: message})
}

// open mints one correlation identifier and records the request. Request does
// not suspend here: returning the response stream lets an author open several
// operations before blocking in Recv, as RFC 0012 requires.
func (h *pumpHost) open(frame Frame) (*pendingOperation, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	correlation, err := h.correlations.NextCorrelationID()
	if err != nil {
		return nil, sdk.Failure{Code: string(sdk.FailureInternal), Message: "no correlation identifier available"}
	}
	operation, err := h.pump.register(correlation, frame.Kind)
	if err != nil {
		return nil, err
	}
	frame.CorrelationID = correlation
	h.buffer = append(h.buffer, frame)
	return operation, nil
}

// await hands every buffered frame to the runtime and blocks for the response
// correlated with this operation. If an earlier out-of-order resume already
// buffered the response, it is consumed without another handover.
func (h *pumpHost) await(ctx context.Context, operation *pendingOperation) (Response, error) {
	h.pump.setWaiting(operation, true)
	defer h.pump.setWaiting(operation, false)
	select {
	case response := <-operation.deliver:
		return response, nil
	default:
	}
	// Both handovers select on the execution context as well as the caller's.
	// An author may legitimately pass a detached context to a host call, and
	// this pump exists to survive arbitrary author code, so the execution
	// context is the only cancellation signal that can always reach here. An
	// earlier revision watched only the caller's context, which left the
	// goroutine parked forever after a cancel and deadlocked Close.
	select {
	case h.pump.yield <- h.take():
	case <-h.executionContext.Done():
		return Response{}, h.executionContext.Err()
	case <-ctx.Done():
		return Response{}, ctx.Err()
	}
	select {
	case response := <-operation.deliver:
		return response, nil
	case <-h.executionContext.Done():
		return Response{}, h.executionContext.Err()
	case <-ctx.Done():
		return Response{}, ctx.Err()
	}
}

// pumpResponses turns a suspended host request into the author's blocking
// stream. Each Recv after the first suspends again until the runtime delivers
// the next response.
type pumpResponses struct {
	host      *pumpHost
	context   context.Context
	operation *pendingOperation
	complete  bool
	metadata  sdk.ResponseStreamMetadata
}

func (r *pumpResponses) Recv() (sdk.Response, error) {
	if r.complete {
		return sdk.Response{}, io.EOF
	}
	response, err := r.host.await(r.context, r.operation)
	if err != nil {
		return sdk.Response{}, err
	}
	if response.Err != nil {
		r.complete = true
		return sdk.Response{}, response.Err
	}
	r.complete = response.Complete
	if response.StreamTerminal != nil {
		r.metadata = *response.StreamTerminal
		return sdk.Response{}, io.EOF
	}
	return response.Response, nil
}

// ResponseStreamMetadata exposes the continuation only after Recv consumes
// the explicit stream terminal.
func (r *pumpResponses) ResponseStreamMetadata() sdk.ResponseStreamMetadata {
	return r.metadata
}

func requestPayloadBytes(request sdk.Request) int {
	if request.HTTP != nil && len(request.HTTP.Body) > MaxLogicalPayloadBytes {
		return len(request.HTTP.Body)
	}
	if request.GRPC != nil {
		return len(request.GRPC.Message)
	}
	if request.HTTP != nil {
		return len(request.HTTP.Body)
	}
	return 0
}

func cloneRequest(request sdk.Request) sdk.Request {
	clone := request
	if request.HTTP != nil {
		http := *request.HTTP
		http.Body = append([]byte(nil), request.HTTP.Body...)
		http.Query = cloneStringSlices(request.HTTP.Query)
		http.Headers = cloneStringSlices(request.HTTP.Headers)
		clone.HTTP = &http
	}
	if request.GRPC != nil {
		grpc := *request.GRPC
		grpc.Message = append([]byte(nil), request.GRPC.Message...)
		clone.GRPC = &grpc
	}
	return clone
}

func cloneStringSlices(values map[string][]string) map[string][]string {
	if values == nil {
		return nil
	}
	clone := make(map[string][]string, len(values))
	for key, entries := range values {
		clone[key] = append([]string(nil), entries...)
	}
	return clone
}

// authHost shares pumpHost's handover; it never receives product network access.
type authHost struct {
	host           *pumpHost
	mu             sync.Mutex
	binding        sdk.ServiceBinding
	bindingStarted bool
}

func (h *authHost) call(ctx context.Context, request AuthRequest) (AuthResponse, error) {
	h.mu.Lock()
	binding := request.Operation == AuthBind || request.Operation == AuthBindCredential
	if h.bindingStarted {
		h.mu.Unlock()
		h.host.pump.markFatal(sdk.FailureProtocolError)
		return AuthResponse{}, sdk.Failure{Code: string(sdk.FailureProtocolError)}
	}
	if binding {
		h.bindingStarted = true
	}
	h.mu.Unlock()
	request.Slot.Scopes = append([]string{}, request.Slot.Scopes...)
	request.Scopes = append([]string{}, request.Scopes...)
	request.Operations = append([]string(nil), request.Operations...)
	operation, err := h.host.open(Frame{Kind: FrameAuthRequest, Auth: request})
	if err != nil {
		return AuthResponse{}, err
	}
	response, err := h.host.await(ctx, operation)
	if err != nil {
		return AuthResponse{}, err
	}
	if response.Err != nil {
		return AuthResponse{}, response.Err
	}
	if !response.Complete {
		h.host.pump.markFatal(sdk.FailureProtocolError)
		return AuthResponse{}, sdk.Failure{Code: string(sdk.FailureProtocolError)}
	}
	if binding {
		h.mu.Lock()
		h.binding = response.Auth.Binding
		h.mu.Unlock()
	}
	return response.Auth, nil
}

func (h *authHost) Load(ctx context.Context, slot sdk.CredentialSlot) (sdk.CredentialHandle, sdk.LeaseMetadata, sdk.StoreState, error) {
	r, e := h.call(ctx, AuthRequest{Operation: AuthLoad, Slot: slot})
	return r.Handle, r.Lease, r.State, e
}
func (h *authHost) AuthorizeOIDC(ctx context.Context, slot sdk.CredentialSlot, intent sdk.OIDCIntent) (sdk.CredentialHandle, sdk.LeaseMetadata, error) {
	r, e := h.call(ctx, AuthRequest{Operation: AuthAuthorizeOIDC, Slot: slot, Issuer: intent.Issuer, Scopes: intent.Scopes})
	return r.Handle, r.Lease, e
}
func (h *authHost) Refresh(ctx context.Context, slot sdk.CredentialSlot, handle sdk.CredentialHandle, intent sdk.RefreshIntent) (sdk.CredentialHandle, sdk.LeaseMetadata, error) {
	r, e := h.call(ctx, AuthRequest{Operation: AuthRefresh, Slot: slot, Handle: handle, Scopes: intent.Scopes})
	return r.Handle, r.Lease, e
}
func (h *authHost) Exchange(ctx context.Context, slot sdk.CredentialSlot, handle sdk.CredentialHandle, intent sdk.ExchangeIntent) (sdk.CredentialHandle, sdk.LeaseMetadata, error) {
	r, e := h.call(ctx, AuthRequest{Operation: AuthExchange, Slot: slot, Handle: handle, Resource: intent.Resource, Scopes: intent.Scopes})
	return r.Handle, r.Lease, e
}
func (h *authHost) Invalidate(ctx context.Context, selector sdk.CredentialSelector) error {
	_, e := h.call(ctx, AuthRequest{Operation: AuthInvalidate, Slot: selector.Slot, Descendants: selector.Descendants})
	return e
}
func (h *authHost) Bind(ctx context.Context, handle sdk.CredentialHandle, spec sdk.BindingSpec) (sdk.ServiceBinding, error) {
	r, e := h.call(ctx, AuthRequest{Operation: AuthBind, Handle: handle, Service: spec.Service, Operations: spec.Operations})
	return r.Binding, e
}
func (h *authHost) AuthorizeClientCredentials(ctx context.Context, request sdk.AuthorizeClientCredentialsRequest) (sdk.AuthorizeClientCredentialsResponse, error) {
	r, e := h.call(ctx, AuthRequest{Operation: AuthAuthorizeClientCredentials, Slot: request.Slot, Service: request.Intent.Service, Scopes: request.Intent.Scopes})
	return sdk.AuthorizeClientCredentialsResponse{CredentialHandle: r.Handle, Lease: r.Lease}, e
}
func (h *authHost) BindCredential(ctx context.Context, request sdk.BindCredentialRequest) (sdk.BindCredentialResponse, error) {
	r, e := h.call(ctx, AuthRequest{Operation: AuthBindCredential, Slot: request.Slot, Handle: request.CredentialHandle, Operations: request.Operations})
	return sdk.BindCredentialResponse{Binding: sdk.CredentialBinding(r.Binding), Lease: r.Lease}, e
}
