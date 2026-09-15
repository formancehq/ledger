// Package portable runs the blocking plugin authoring model on a
// start/resume runtime.
//
// RFC 0012 requires the existing `Plugin.Execute(ctx, request, host)` model to
// survive a lifecycle whose only calls are start, resume, cancel, and close.
// This pump is that adapter: the author's code blocks while receiving a host
// response, while every opened host operation becomes a frame the runtime
// returns and a later resume unblocks the matching receiver.
//
// The plugin never learns it is being suspended. Nothing here decodes or
// encodes an envelope; frames stay in the SDK's own vocabulary so the pump has
// no dependency on the unfrozen candidate contract.
//
// Synchronisation is by channel handover only. No elapsed time decides an
// outcome, which is what keeps the pump deterministic under load.
package portable

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"unicode/utf8"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

// MaxLogicalPayloadBytes is the portable v1 ceiling for one HTTP body or gRPC
// message. It is a logical payload limit, separate from the encoded lifecycle
// frame budget owned by the runtime.
const MaxLogicalPayloadBytes = 4 << 20

var (
	// ErrUnconfigured reports a missing plugin, correlation source, or context.
	ErrUnconfigured = errors.New("portable pump is not configured")
	// ErrNotOutstanding reports a resume that no suspended operation awaits,
	// including a resume after the terminal outcome.
	ErrNotOutstanding = errors.New("portable pump has no such outstanding operation")
)

// FrameKind is what one pump frame asks of the host.
type FrameKind uint8

const (
	FrameUnspecified FrameKind = iota
	FrameHostRequest
	FrameWaitRequest
	FrameEvent
	FrameLog
	FrameTerminal
	FrameAuthRequest
	FrameTargetResult
	FrameInputRead
)

// Frame is one unit of guest output in the SDK's vocabulary.
type Frame struct {
	Kind          FrameKind
	CorrelationID string
	Request       sdk.Request
	Wait          sdk.WaitRequest
	Event         sdk.Event
	Level         sdk.Level
	Message       string
	FailureCode   sdk.FailureCode
	Auth          AuthRequest
	Target        sdk.TargetDiscoveryResult
	InputHandle   string
}

// AuthOperation identifies a typed, opaque credential broker operation.
type AuthOperation uint8

const (
	AuthLoad AuthOperation = iota + 1
	AuthAuthorizeOIDC
	AuthRefresh
	AuthExchange
	AuthInvalidate
	AuthBind
	AuthAuthorizeClientCredentials
	AuthBindCredential
)

// AuthRequest carries only public SDK broker values, never credential bytes.
type AuthRequest struct {
	Operation   AuthOperation
	Slot        sdk.CredentialSlot
	Handle      sdk.CredentialHandle
	Scopes      []string
	Issuer      string
	Resource    string
	Descendants bool
	Service     sdk.Service
	Operations  []string
}

// AuthResponse is an opaque broker result, validated by the lifecycle codec.
type AuthResponse struct {
	Handle  sdk.CredentialHandle
	Lease   sdk.LeaseMetadata
	State   sdk.StoreState
	Binding sdk.ServiceBinding
}

// Response is one validated host answer for a suspended operation. Complete
// settles a single product response; an incomplete product response continues
// a stream. StreamTerminal closes a stream without a product response.
type Response struct {
	CorrelationID string
	Response      sdk.Response
	Complete      bool
	Err           error
	Auth          AuthResponse
	Input         sdk.InputArtifactChunk
	// StreamTerminal closes a server stream without delivering a product
	// message, including when the stream has no messages. The runtime adapter
	// validates operation cardinality and continuation bounds before Resume.
	StreamTerminal *sdk.ResponseStreamMetadata
}

// CorrelationSource mints the identifier for each new host operation. The pump
// never invents one, and a source that runs out fails the execution rather
// than reusing an identifier.
type CorrelationSource interface {
	NextCorrelationID() (string, error)
}

// Pump owns the suspended execution.
//
// One caller at a time. The lifecycle serializes start, resume, cancel and
// close for a single execution, and the pump relies on that. Internal state is
// synchronized because plugin author code may open requests from goroutines,
// but concurrent lifecycle calls remain unsupported.
//
// A Resume whose caller context expires while collecting leaves the author
// parked with no handover pending. That state is unrecoverable by design except
// through Cancel or Close, both of which release the author.
type Pump struct {
	yield  chan []Frame
	done   chan struct{}
	cancel context.CancelFunc

	mu       sync.Mutex
	pending  map[string]*pendingOperation
	used     map[string]struct{}
	fatal    sdk.FailureCode
	terminal bool
}

type pendingOperation struct {
	kind    FrameKind
	deliver chan Response
	waiting bool
}

// Start runs the plugin until it either finishes or blocks receiving a host
// response, and returns the frames produced up to that point.
func Start(
	ctx context.Context,
	plugin sdk.Plugin,
	request sdk.ExecuteRequest,
	correlations CorrelationSource,
) (*Pump, []Frame, error) {
	if ctx == nil || plugin == nil || correlations == nil {
		return nil, nil, ErrUnconfigured
	}
	return start(ctx, correlations, func(ctx context.Context, host *pumpHost) error {
		return plugin.Execute(ctx, request, host)
	})
}

// StartAuth runs a provider through the same suspension and cancellation pump.
func StartAuth(ctx context.Context, provider sdk.AuthProvider, request sdk.AuthRequest, correlations CorrelationSource) (*Pump, []Frame, error) {
	if ctx == nil || provider == nil || correlations == nil {
		return nil, nil, ErrUnconfigured
	}
	request = sdk.CloneAuthRequest(request)
	return start(ctx, correlations, func(ctx context.Context, host *pumpHost) error {
		broker := &authHost{host: host}
		binding, err := provider.Resolve(ctx, request, broker)
		if err != nil {
			return err
		}
		broker.mu.Lock()
		defer broker.mu.Unlock()
		if binding == "" || binding != broker.binding {
			return sdk.Failure{Code: string(sdk.FailureProtocolError)}
		}
		return nil
	})
}

// StartTarget returns the discovery result only after all requests settle.
func StartTarget(ctx context.Context, provider sdk.TargetProvider, request sdk.TargetDiscoveryRequest, correlations CorrelationSource) (*Pump, []Frame, error) {
	if ctx == nil || provider == nil || correlations == nil {
		return nil, nil, ErrUnconfigured
	}
	if request.Parent != nil {
		parent := *request.Parent
		request.Parent = &parent
	}
	return start(ctx, correlations, func(ctx context.Context, host *pumpHost) error {
		result, err := provider.Discover(ctx, request, targetHost{host})
		if err != nil {
			return err
		}
		result.Targets = append([]sdk.TargetDescriptor(nil), result.Targets...)
		host.appendFrame(Frame{Kind: FrameTargetResult, Target: result})
		return nil
	})
}

// targetHost exposes only the methods in the public discovery contract.
type targetHost struct{ host *pumpHost }

func (h targetHost) Request(ctx context.Context, request sdk.Request) (sdk.Responses, error) {
	return h.host.Request(ctx, request)
}
func (h targetHost) Log(level sdk.Level, message string) { h.host.Log(level, message) }

func start(ctx context.Context, correlations CorrelationSource, execute func(context.Context, *pumpHost) error) (*Pump, []Frame, error) {
	executionContext, cancel := context.WithCancel(ctx)
	pump := &Pump{
		yield:   make(chan []Frame),
		done:    make(chan struct{}),
		cancel:  cancel,
		pending: make(map[string]*pendingOperation),
		used:    make(map[string]struct{}),
	}
	host := &pumpHost{pump: pump, correlations: correlations, executionContext: executionContext}
	go func() {
		defer close(pump.done)
		err := execute(executionContext, host)
		code, replace := pump.terminalOutcome(terminalCode(err))
		if replace {
			// A fatal host outcome or a guest terminal with an outstanding
			// operation discards actionable frames, so no partial success or
			// post-terminal dispatch can cross the boundary.
			host.replaceBuffer(Frame{Kind: FrameTerminal, FailureCode: code})
		} else {
			host.appendFrame(Frame{Kind: FrameTerminal, FailureCode: code})
		}
		select {
		case pump.yield <- host.take():
		case <-executionContext.Done():
		}
	}()
	frames, err := pump.collect(executionContext)
	if err != nil {
		return nil, nil, err
	}
	return pump, frames, nil
}

// Resume settles or continues one suspended operation and returns the frames
// the author produced next.
func (p *Pump) Resume(ctx context.Context, response Response) ([]Frame, error) {
	if p == nil || ctx == nil {
		return nil, ErrUnconfigured
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	operation, outstanding := p.pending[response.CorrelationID]
	if p.terminal || !outstanding {
		p.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrNotOutstanding, response.CorrelationID)
	}
	response, fatal := admittedResponse(response)
	if fatal && p.fatal == "" {
		p.fatal = sdk.FailureBudgetExhausted
	}
	settled := response.Complete || response.Err != nil
	if settled {
		delete(p.pending, response.CorrelationID)
	}
	waiting := operation.waiting
	p.mu.Unlock()
	select {
	case operation.deliver <- response:
	case <-p.done:
		return nil, ErrNotOutstanding
	case <-ctx.Done():
		if settled {
			p.mu.Lock()
			if !p.terminal {
				p.pending[response.CorrelationID] = operation
			}
			p.mu.Unlock()
		}
		return nil, ctx.Err()
	}
	if !waiting {
		return []Frame{}, nil
	}
	return p.collect(ctx)
}

// Cancel ends a quiescent execution and reports the canceled outcome. It is
// callable only between calls, which is the same rule the lifecycle applies to
// its cancel export.
func (p *Pump) Cancel(ctx context.Context) ([]Frame, error) {
	if p == nil || ctx == nil {
		return nil, ErrUnconfigured
	}
	p.mu.Lock()
	if p.terminal {
		p.mu.Unlock()
		return nil, ErrNotOutstanding
	}
	p.mu.Unlock()
	p.cancel()
	select {
	case <-p.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	p.mu.Lock()
	p.terminal = true
	clear(p.pending)
	p.mu.Unlock()
	return []Frame{{Kind: FrameTerminal, FailureCode: sdk.FailureCanceled}}, nil
}

// Close releases the suspended goroutine. It is safe to call more than once,
// so a deferred close beside an explicit one cannot deadlock.
func (p *Pump) Close() {
	if p == nil {
		return
	}
	p.cancel()
	<-p.done
}

// collect waits for the author's next handover and records what it implies.
func (p *Pump) collect(ctx context.Context) ([]Frame, error) {
	select {
	case frames := <-p.yield:
		p.mu.Lock()
		for _, frame := range frames {
			if frame.Kind == FrameTerminal {
				p.terminal = true
			}
		}
		p.mu.Unlock()
		return frames, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func terminalCode(err error) sdk.FailureCode {
	if err == nil {
		return ""
	}
	if failure, ok := typedFailure(err); ok && sdk.FailureCode(failure.Code).Valid() {
		return sdk.FailureCode(failure.Code)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return sdk.FailureCanceled
	}
	// An unclassified author error must not leak its text across the boundary.
	return sdk.FailureInternal
}

func admittedResponse(response Response) (Response, bool) {
	if len(response.Input.Bytes) > sdk.MaxInputArtifactChunkBytes {
		response.Input = sdk.InputArtifactChunk{}
		response.Err = sdk.Failure{Code: string(sdk.FailureBudgetExhausted), Message: "portable input chunk exceeds its byte limit"}
		response.Complete = true
		return response, true
	}
	response.Input.Bytes = append([]byte(nil), response.Input.Bytes...)
	if len(response.Response.Body) > MaxLogicalPayloadBytes {
		response.Response = sdk.Response{}
		response.Err = sdk.Failure{Code: string(sdk.FailureBudgetExhausted), Message: "portable logical response payload exceeds its byte limit"}
		response.Complete = true
		return response, true
	}
	response.Response.Body = append([]byte(nil), response.Response.Body...)
	if response.StreamTerminal != nil {
		terminal := *response.StreamTerminal
		response.StreamTerminal = &terminal
		response.Complete = true
	}
	if response.Err == nil {
		return response, false
	}
	if errors.Is(response.Err, context.Canceled) || errors.Is(response.Err, context.DeadlineExceeded) {
		response.Err = sdk.Failure{Code: string(sdk.FailureCanceled), Message: "host request canceled"}
		response.Complete = true
		return response, false
	}
	failure, ok := typedFailure(response.Err)
	if !ok || !sdk.FailureCode(failure.Code).Valid() {
		response.Err = sdk.Failure{Code: string(sdk.FailureInternal), Message: "host request failed"}
		response.Complete = true
		return response, false
	}
	failure.Details = append([]byte(nil), failure.Details...)
	response.Err = failure
	response.Complete = true
	return response, false
}

func typedFailure(err error) (sdk.Failure, bool) {
	var value sdk.Failure
	if errors.As(err, &value) {
		return value, true
	}
	var pointer *sdk.Failure
	if errors.As(err, &pointer) && pointer != nil {
		return *pointer, true
	}
	return sdk.Failure{}, false
}

func (p *Pump) register(correlation string, kind FrameKind) (*pendingOperation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fatal != "" {
		return nil, sdk.Failure{Code: string(p.fatal), Message: "portable execution has already exhausted its budget"}
	}
	if correlation == "" || len(correlation) > 128 || !utf8.ValidString(correlation) {
		return nil, sdk.Failure{Code: string(sdk.FailureInternal), Message: "invalid correlation identifier"}
	}
	if _, duplicate := p.used[correlation]; duplicate {
		return nil, sdk.Failure{Code: string(sdk.FailureInternal), Message: "duplicate correlation identifier"}
	}
	for _, pending := range p.pending {
		if kind == FrameWaitRequest || pending.kind == FrameWaitRequest {
			return nil, sdk.Failure{Code: string(sdk.FailureProtocolError), Message: "a wait cannot overlap another host operation"}
		}
	}
	if len(p.pending) >= 4 {
		p.fatal = sdk.FailureBudgetExhausted
		return nil, sdk.Failure{Code: string(sdk.FailureBudgetExhausted)}
	}
	operation := &pendingOperation{
		kind:    kind,
		deliver: make(chan Response, int(sdk.GeneratedClientMaxResponseMessages)+1),
	}
	p.pending[correlation] = operation
	p.used[correlation] = struct{}{}
	return operation, nil
}

func (p *Pump) setWaiting(operation *pendingOperation, waiting bool) {
	p.mu.Lock()
	operation.waiting = waiting
	p.mu.Unlock()
}

func (p *Pump) markFatal(code sdk.FailureCode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fatal == "" {
		p.fatal = code
	}
}

func (p *Pump) terminalOutcome(code sdk.FailureCode) (sdk.FailureCode, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fatal != "" {
		clear(p.pending)
		return p.fatal, true
	}
	if len(p.pending) != 0 {
		clear(p.pending)
		return sdk.FailureProtocolError, true
	}
	return code, false
}
