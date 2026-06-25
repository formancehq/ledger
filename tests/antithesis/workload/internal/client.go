package internal

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

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
		opts = append(opts,
			grpc.WithUnaryInterceptor(retryUnaryInterceptor(interceptorAttempts)),
			grpc.WithStreamInterceptor(retryStreamInterceptor(interceptorAttempts)),
		)

		if retryForever {
			// grpc-go's WithMaxCallAttempts default is 5 — the service-config
			// MaxAttempts is silently capped to that otherwise. Only lifted in
			// retry-forever mode so the default path matches master exactly.
			opts = append(opts, grpc.WithMaxCallAttempts(maxAttempts))
		}
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

// retryUnaryInterceptor retries unary RPCs on the transient set (IsTransient) to
// a definitive outcome — each code either clears (no leader → elected, lagging
// read → caught up) or is an ambiguous commit (Unavailable / DeadlineExceeded /
// Aborted / ExternalServiceError can follow a commit) that a retry resolves via
// the idempotency cache. None is a permanent business answer, so retrying is safe
// and cannot loop forever. The retried set and the set the processor drops are
// the same predicate, so they cannot drift. maxAttempts bounds the loop
// (~infinite in retry-forever mode); ctx cancellation (shutdown /
// MODEL_MAX_SECONDS) ends it regardless.
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

// IsAborted returns true if the error is a gRPC Aborted status. The
// codebase's own retry classifier
// (internal/application/ctrl/snapshot_fetcher.go) treats Aborted on the
// same footing as Unavailable / DeadlineExceeded, so the workload follows
// the same convention.
func IsAborted(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Aborted
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

// IsTransient returns true for a genuinely transient infrastructure error — not a
// definitive business answer. Retrying it reaches a definitive outcome: the
// condition clears (no leader → elected, lagging read → caught up), or — since
// Unavailable / DeadlineExceeded / Aborted / ExternalServiceError can follow a
// commit — an ambiguous commit a retry resolves via the idempotency cache. The
// retry interceptors retry exactly this set, and the model processor drops
// exactly this set as "did not happen" (sound because retry resolves it first, so
// the processor only sees one on shutdown); keeping them one predicate is why
// they cannot drift. Covers:
//   - Unavailable (cluster unhealthy / no leader / Raft transients)
//   - DeadlineExceeded (server unreachable, fault window)
//   - Aborted (no domain error maps to it; only Raft / transport transients)
//   - FailedPrecondition + READ_INDEX_NOT_CAUGHT_UP (read store catching up)
//   - ExternalServiceError (S3 down etc.)
//
// Business outcomes are deliberately excluded — definitive, never-clearing, and
// validated rather than retried or dropped: NotFound and LedgerDeleted.
// Deletable-ledger prefixes are restricted from the shared pool (see
// restrictedPrefixes), so only the drivers that delete their own ledgers ever
// legitimately see LedgerDeleted; for anything drawn from the pool it's a finding.
func IsTransient(err error) bool {
	return IsUnavailable(err) ||
		IsDeadlineExceeded(err) ||
		IsAborted(err) ||
		IsReadIndexNotCaughtUp(err) ||
		IsExternalServiceError(err)
}

// NewClient creates a BucketServiceClient connected to the ledger service.
func NewClient() (servicepb.BucketServiceClient, *grpc.ClientConn, error) {
	conn, err := NewGRPCConn()
	if err != nil {
		return nil, nil, err
	}
	return servicepb.NewBucketServiceClient(conn), conn, nil
}
