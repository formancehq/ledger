package internal

import (
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
		grpc.WithDefaultServiceConfig(retryPolicy),
	)
	if err != nil {
		return nil, err
	}
	return conn, nil
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

// IsLedgerDeleted returns true if the error is a gRPC FailedPrecondition
// with reason LEDGER_DELETED. This means the ledger was soft-deleted by a
// concurrent driver — not a bug, just a race.
func IsLedgerDeleted(err error) bool {
	if err == nil {
		return false
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return false
	}

	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info.GetReason() == domain.ErrReasonLedgerDeleted
		}
	}

	return false
}

// IsTransient returns true if the error is transient and should not
// trigger failure assertions (Unavailable or ledger deleted by concurrent driver).
func IsTransient(err error) bool {
	return IsUnavailable(err) || IsLedgerDeleted(err)
}

// NewClient creates a BucketServiceClient connected to the ledger service.
func NewClient() (servicepb.BucketServiceClient, *grpc.ClientConn, error) {
	conn, err := NewGRPCConn()
	if err != nil {
		return nil, nil, err
	}
	return servicepb.NewBucketServiceClient(conn), conn, nil
}
