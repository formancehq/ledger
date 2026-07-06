package internal

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
)

// NewGRPCConn creates a gRPC connection to the ledger service with retry on
// UNAVAILABLE and round-robin load balancing across all provided addresses.
// LEDGER_GRPC_ADDR accepts a comma-separated list (e.g. "ledger-0:8888,ledger-1:8888,ledger-2:8888").
func NewGRPCConn() (*grpc.ClientConn, error) {
	target := os.Getenv("LEDGER_GRPC_ADDR")
	if target == "" {
		target = "localhost:15100"
	}

	// LEDGER_NO_RETRY disables the automatic UNAVAILABLE retry entirely (both the
	// service-config policy and the interceptors). Useful for isolating whether a
	// divergence is caused by retried (and thus possibly double-applied, when the
	// request is non-idempotent) Apply calls.
	retryDisabled := os.Getenv("LEDGER_NO_RETRY") != ""

	// LEDGER_RETRY_FOREVER raises the retry budget to ~infinite (off by default —
	// master keeps MaxAttempts 50, which grpc-go silently caps to 5 because no
	// WithMaxCallAttempts is set). The model-based driver sets it so the
	// "ambiguous commit" class of errors becomes eventually-definitive: combined
	// with the idempotency key on every Request, a retry that lands after the
	// cluster recovers hits the server's idempotency cache and returns the cached
	// log reference, so the validator never has to model "may have committed".
	// MaxBackoff caps each interval at 2s, so 1M attempts is ~23 days of budget.
	retryForever := os.Getenv("LEDGER_RETRY_FOREVER") != ""

	maxAttempts := 50
	if retryForever {
		maxAttempts = 1000000
	}

	// The retry interceptors retry the transient set (IsTransient) to a definitive
	// outcome; their loop budget is the bounded default unless retry-forever lifts
	// it.
	interceptorAttempts := retryMaxAttempts
	if retryForever {
		interceptorAttempts = maxAttempts
	}

	// Service-config retry covers only the raw UNAVAILABLE code; the interceptors
	// add ReadIndexNotCaughtUp, which is reason-specific and so cannot be matched
	// here (it would over-retry permanent FailedPrecondition business errors).
	methodConfig := ""
	if !retryDisabled {
		methodConfig = fmt.Sprintf(`,
		"methodConfig": [{
			"name": [{}],
			"retryPolicy": {
				"MaxAttempts": %d,
				"InitialBackoff": "0.2s",
				"MaxBackoff": "2s",
				"BackoffMultiplier": 1.5,
				"RetryableStatusCodes": ["UNAVAILABLE"]
			}
		}]`, maxAttempts)
	}
	// Round-robin load balancing always applies; only the retry policy is toggled.
	serviceConfig := `{"loadBalancingConfig": [{"round_robin": {}}]` + methodConfig + `}`

	addrs := strings.Split(target, ",")
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(serviceConfig),
	}

	if !retryDisabled {
		// MUST use the Chain* dial options: WithUnaryInterceptor (singular)
		// stores ONE interceptor per dial and a second call overwrites the
		// first silently. Order inside the chain matters: gRPC applies
		// interceptors left-to-right (retry runs first, classify wraps the
		// final post-retry error). The classify interceptor stays here even
		// in retry-on mode because it asserts the workload's classification
		// map is complete.
		opts = append(opts,
			grpc.WithChainUnaryInterceptor(
				retryUnaryInterceptor(interceptorAttempts),
				classifyUnaryInterceptor(),
			),
			grpc.WithChainStreamInterceptor(
				retryStreamInterceptor(interceptorAttempts),
				classifyStreamInterceptor(),
			),
		)

		if retryForever {
			// grpc-go's WithMaxCallAttempts default is 5 — the service-config
			// MaxAttempts is silently capped to that otherwise. Only lifted in
			// retry-forever mode so the default path matches master exactly.
			opts = append(opts, grpc.WithMaxCallAttempts(maxAttempts))
		}
	} else {
		// Retry disabled (LEDGER_NO_RETRY) — classify stays on; same Chain*
		// option for consistency, even with a single member.
		opts = append(opts,
			grpc.WithChainUnaryInterceptor(classifyUnaryInterceptor()),
			grpc.WithChainStreamInterceptor(classifyStreamInterceptor()),
		)
	}

	// When multiple addresses are provided, use a manual resolver so gRPC
	// round-robins across all nodes and survives individual node failures.
	if len(addrs) > 1 {
		r := manual.NewBuilderWithScheme("ledger")
		var resolverAddrs []resolver.Address
		for _, addr := range addrs {
			resolverAddrs = append(resolverAddrs, resolver.Address{Addr: strings.TrimSpace(addr)})
		}
		r.InitialState(resolver.State{Addresses: resolverAddrs})
		opts = append(opts, grpc.WithResolvers(r))
		target = r.Scheme() + ":///"
	}

	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

const (
	retryMaxAttempts = 10
	retryBaseDelay   = 500 * time.Millisecond
	retryMaxDelay    = 5 * time.Second
)

func retryDelay(attempt int) time.Duration {
	d := retryBaseDelay
	for range attempt {
		d = d * 2
		if d > retryMaxDelay {
			return retryMaxDelay
		}
	}
	return d
}

// retryUnaryInterceptor retries unary RPCs on the transient set (IsTransient)
// to a definitive outcome — each code either clears (Unavailable: no leader
// → elected; ReadIndexNotCaughtUp: lagging read catches up; ExternalServiceError:
// external service recovers) or is an ambiguous commit (DeadlineExceeded — see
// IsAmbiguousCommit) that a retry resolves via the idempotency cache. None is
// a permanent business answer, so retrying is safe and cannot loop forever.
// maxAttempts bounds the loop (~infinite in retry-forever mode); ctx
// cancellation (shutdown / MODEL_MAX_SECONDS) ends it regardless.
//
// Aborted is NOT retried (see IsAborted): the gRPC spec allows business
// "please retry" semantics there, and we want to see it surface in the
// classify interceptor rather than silently spinning the retry loop.
func retryUnaryInterceptor(maxAttempts int) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		var err error
		for attempt := range maxAttempts {
			err = invoker(ctx, method, req, reply, cc, opts...)
			if !IsTransient(err) {
				return err
			}
			select {
			case <-ctx.Done():
				return err
			case <-time.After(retryDelay(attempt)):
			}
		}
		return err
	}
}

// classifyUnaryInterceptor asserts that every error escaping an RPC is
// recognized by IsClassified. An unrecognized code (Internal panic from the
// server, ResourceExhausted from a future rate limiter, a brand-new gRPC
// status …) flips an Unreachable — the workload's classification map is
// incomplete and silent code paths exist somewhere downstream. The Details
// pin the RPC method and the actual code so triage finds the orphan quickly.
//
// Single global assertion name on purpose: one alarm whose Details vary, not
// one alarm per RPC method (which would explode the Antithesis triage UI).
func classifyUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		assert.Always(IsClassified(err),
			"every RPC error must be classified (workload predicate set complete)",
			map[string]any{
				"method": method,
				"code":   status.Code(err).String(),
				"err":    fmt.Sprintf("%v", err),
			})
		return err
	}
}

// classifyStreamInterceptor mirrors classifyUnaryInterceptor for the
// stream-creation error. Mid-stream Recv() errors are NOT covered here —
// each driver classifies its own stream loop (see the convention in
// tests/antithesis/README.md).
func classifyStreamInterceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		assert.Always(IsClassified(err),
			"every RPC error must be classified (workload predicate set complete)",
			map[string]any{
				"method": method,
				"code":   status.Code(err).String(),
				"err":    fmt.Sprintf("%v", err),
			})
		return stream, err
	}
}

// retryStreamInterceptor retries stream creation on the same transient set as
// retryUnaryInterceptor (IsTransient).
func retryStreamInterceptor(maxAttempts int) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		var (
			stream grpc.ClientStream
			err    error
		)
		for attempt := range maxAttempts {
			stream, err = streamer(ctx, desc, cc, method, opts...)
			if !IsTransient(err) {
				return stream, err
			}
			select {
			case <-ctx.Done():
				return nil, err
			case <-time.After(retryDelay(attempt)):
			}
		}
		return stream, err
	}
}

// IsUnavailable returns true if the error is a gRPC Unavailable status
// (cluster unhealthy, no leader, etc.). Kept narrow on purpose: it is
// used by the retry interceptors above, where broadening would change
// retry behaviour for every RPC. For test-level "is this a fault-window
// transient I should skip" classification, use IsTransient instead.
func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable
}

// IsDeadlineExceeded returns true if the error is a gRPC DeadlineExceeded
// status. The server's errorConversionInterceptor maps internal
// context.DeadlineExceeded to Unavailable, so seeing DeadlineExceeded at
// the client is always a wire-level/availability transient (server
// unreachable, hub blackholing under a clog fault, etc.).
func IsDeadlineExceeded(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.DeadlineExceeded
}

// IsAborted returns true if the error is a gRPC Aborted status.
//
// Deliberately NOT part of IsTransient: the gRPC spec allows business-level
// concurrency errors to map to Aborted ("Concurrency issue, please retry"),
// and we have no audited guarantee that no current or future server path
// uses Aborted as a business answer. Surfacing it loud (via the classify
// interceptor's Unreachable) is intentional — if Aborted shows up in a
// real chaos run, that is a finding worth triaging, not silently
// retried-then-skipped.
func IsAborted(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Aborted
}

// IsCanceled returns true if the error is a gRPC Canceled status. Emitted
// when the local ctx is dead — the parent driver is shutting down (global
// deadline reached, composer kill propagated). Not retry-safe (the next
// retry would see ctx.Done() immediately) and not a finding: the driver
// just exits.
func IsCanceled(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Canceled
}

// IsAmbiguousCommit returns true if the error indicates the request may have
// committed despite the error code — the retry resolves the ambiguity via
// the idempotency cache. Today: DeadlineExceeded only (Unavailable surfaces
// before the server sees the request, ReadIndexNotCaughtUp is a read-only
// answer, ExternalServiceError happens before the audit ack).
//
// IsAmbiguousCommit is a STRICT SUBSET of IsTransient — every member is
// already retried by the interceptors. The separation exists so drivers
// asserting on post-commit state can decide whether to verify
// read-after-write even on the "error" branch.
func IsAmbiguousCommit(err error) bool {
	return IsDeadlineExceeded(err)
}

// IsReadIndexNotCaughtUp returns true if the error is the server's
// FailedPrecondition response carrying the READ_INDEX_NOT_CAUGHT_UP
// reason. Emitted when a linearizable read targets an index the local
// read-side store has not yet caught up to — always transient (the read
// store will eventually catch up).
func IsReadIndexNotCaughtUp(err error) bool {
	return HasErrorReason(err, "READ_INDEX_NOT_CAUGHT_UP")
}

// HasErrorReason returns true if the error is a gRPC status with an
// ErrorInfo detail matching the given reason.
func HasErrorReason(err error, reason string) bool {
	if err == nil {
		return false
	}

	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			if info.GetReason() == reason {
				return true
			}
		}
	}

	return false
}

// ErrorReason returns the ErrorInfo reason carried by a gRPC status error, or
// "" if the error is nil or carries no ErrorInfo detail.
func ErrorReason(err error) string {
	if err == nil {
		return ""
	}

	st, ok := status.FromError(err)
	if !ok {
		return ""
	}

	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}

	return ""
}

// IsLedgerDeleted returns true if the error indicates a soft-deleted ledger.
func IsLedgerDeleted(err error) bool {
	return HasErrorReason(err, domain.ErrReasonLedgerDeleted)
}

// IsExternalServiceError returns true if the error indicates an external
// service failure (e.g. S3 bucket not found, credentials error).
func IsExternalServiceError(err error) bool {
	return HasErrorReason(err, "EXTERNAL_SERVICE_ERROR")
}

// IsAlreadyExists returns true if the error is a gRPC AlreadyExists status.
// In the Antithesis workload context, this happens when two concurrent driver
// instances create the same ledger name or account type (name collision).
func IsAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.AlreadyExists
}

// IsNotFound returns true if the error is a gRPC NotFound status. Every NotFound
// the server returns is a permanent business answer — the requested entity does
// not exist (ACCOUNT_TYPE_NOT_FOUND, METADATA_NOT_FOUND, TRANSACTION_NOT_FOUND,
// …) — so it is deliberately NOT part of IsTransient. Callers that want to
// tolerate a specific not-found (e.g. a concurrently-deleted ledger) check it
// explicitly.
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.NotFound
}

// IsLedgerNotFound is a backward-compatible alias for IsNotFound.
//
// Deprecated: the implementation has always matched any NotFound, not
// just ledger-not-found. New code should call IsNotFound directly. Kept
// only so existing callers keep compiling.
func IsLedgerNotFound(err error) bool { return IsNotFound(err) }

// IsTransient returns true for a retry-safe infrastructure error — not a
// definitive business answer, not a local-lifecycle event. Retrying reaches a
// definitive outcome: the condition clears (no leader → elected, lagging read
// → caught up) or — since DeadlineExceeded can follow a commit — the retry
// resolves the ambiguity via the idempotency cache. The retry interceptors
// retry exactly this set. Covers:
//   - Unavailable (cluster unhealthy / no leader / Raft transients)
//   - DeadlineExceeded (wire-level timeout, also see IsAmbiguousCommit)
//   - FailedPrecondition + READ_INDEX_NOT_CAUGHT_UP (read store catching up)
//   - ExternalServiceError (S3 / NATS down, etc.)
//
// NOT in IsTransient:
//   - Aborted (see IsAborted comment — surfaced loud, not retried)
//   - Canceled (see IsCanceled — local lifecycle, not a server transient)
//   - All business outcomes (NotFound, AlreadyExists, LedgerDeleted, generic
//     FailedPrecondition) — definitive, validated rather than skipped.
func IsTransient(err error) bool {
	return IsUnavailable(err) ||
		IsDeadlineExceeded(err) ||
		IsReadIndexNotCaughtUp(err) ||
		IsExternalServiceError(err)
}

// IsTolerated returns true for any error the workload should NOT surface as
// a finding: nil, retry-safe transient, or local-lifecycle Canceled. This is
// the predicate the Sometimes() probes use (`assert.Sometimes(IsTolerated(err),
// ...)`) so that a context cancellation late in the run doesn't flip a
// per-driver Sometimes signal to "never true".
func IsTolerated(err error) bool {
	return err == nil || IsTransient(err) || IsCanceled(err)
}

// isBusinessError returns true for a definitive business answer the server
// returns when the request was syntactically valid but the requested action
// cannot apply (NotFound, AlreadyExists, InvalidArgument, FailedPrecondition
// minus the two reasons that IsTransient already covers).
//
// Unexported because it is only used by IsClassified — drivers that need to
// validate a specific business outcome check the precise reason via
// HasErrorReason (e.g. domain.ErrReasonInsufficientFunds), never this coarse
// "is it a known business code at all" predicate.
func isBusinessError(err error) bool {
	if err == nil {
		return false
	}
	if IsReadIndexNotCaughtUp(err) || IsExternalServiceError(err) {
		// These are FailedPrecondition codes but already classified as transient.
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.NotFound,
		codes.AlreadyExists,
		codes.InvalidArgument,
		codes.FailedPrecondition:
		return true
	}
	return false
}

// IsClassified returns true if the error is nil, retry-safe, locally
// canceled, or a known business code. The classify interceptor uses this
// to flag any OTHER code (Internal panic, ResourceExhausted from a future
// rate-limiter, an unhandled new gRPC code, Aborted from an unaudited
// server path, …) as an Unreachable assertion. A finding here means the
// workload's classification map is incomplete and silent paths exist —
// expand the predicates above (or, for Aborted specifically, audit the
// server path that produced it).
//
// Aborted is DELIBERATELY NOT classified: the gRPC spec allows business
// "please retry" semantics there, and we have no audited guarantee the
// server keeps Aborted as a pure transport transient. Surfacing it loud
// via the classify interceptor is the entire point — see IsAborted.
func IsClassified(err error) bool {
	return err == nil ||
		IsTransient(err) ||
		IsCanceled(err) ||
		isBusinessError(err)
}

// NewClient creates a BucketServiceClient connected to the ledger service.
func NewClient() (servicepb.BucketServiceClient, *grpc.ClientConn, error) {
	conn, err := NewGRPCConn()
	if err != nil {
		return nil, nil, err
	}
	return servicepb.NewBucketServiceClient(conn), conn, nil
}
