package internal

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"os"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// NewGRPCConn creates a gRPC connection to the ledger service with retry on UNAVAILABLE.
func NewGRPCConn() (*grpc.ClientConn, error) {
	target := os.Getenv("LEDGER_GRPC_ADDR")
	if target == "" {
		target = "localhost:15100"
	}

	retryPolicy := `{
		"methodConfig": [{
			"name": [{}],
			"retryPolicy": {
				"MaxAttempts": 50,
				"InitialBackoff": "0.2s",
				"MaxBackoff": "2s",
				"BackoffMultiplier": 1.5,
				"RetryableStatusCodes": ["UNAVAILABLE"]
			}
		}]
	}`

	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(injectIdempotencyKeys),
		grpc.WithDefaultServiceConfig(retryPolicy),
	)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// injectIdempotencyKeys ensures every Apply request element carries a stable
// idempotency key, so a gRPC retry of an Apply that already landed on the
// server is deduplicated by the FSM rather than re-applied as a new
// transaction. The interceptor runs once before the first attempt; retries
// reuse the same wire bytes and therefore the same keys.
func injectIdempotencyKeys(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if applyReq, ok := req.(*servicepb.ApplyRequest); ok {
		for _, r := range applyReq.GetRequests() {
			if r.GetIdempotencyKey() == "" {
				r.IdempotencyKey = newIdempotencyKey()
			}
		}
	}

	return invoker(ctx, method, req, reply, cc, opts...)
}

// newIdempotencyKey draws 128 bits from the Antithesis-deterministic RNG and
// formats them as URL-safe base64. Determinism matters so multiverse replay
// produces the same keys on every branch.
func newIdempotencyKey() string {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], Rand().Uint64())
	binary.LittleEndian.PutUint64(b[8:16], Rand().Uint64())

	return base64.RawURLEncoding.EncodeToString(b[:])
}

// IsUnavailable returns true if the error is a gRPC Unavailable status
// (cluster unhealthy, no leader, etc.). These are transient errors that
// should not trigger failure assertions in Antithesis tests.
func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable
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

// IsLedgerNotFound returns true if the error is a gRPC NotFound status.
// In the Antithesis workload context, this happens when a ledger picked from
// ListLedgers is deleted by another driver before we finish using it.
func IsLedgerNotFound(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.NotFound
}

// IsTransient returns true if the error is transient and should not
// trigger failure assertions (Unavailable, ledger deleted/not found, or external service error).
func IsTransient(err error) bool {
	return IsUnavailable(err) || IsLedgerDeleted(err) || IsLedgerNotFound(err) || IsExternalServiceError(err)
}

// NewClient creates a BucketServiceClient connected to the ledger service.
func NewClient() (servicepb.BucketServiceClient, *grpc.ClientConn, error) {
	conn, err := NewGRPCConn()
	if err != nil {
		return nil, nil, err
	}
	return servicepb.NewBucketServiceClient(conn), conn, nil
}
