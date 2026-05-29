package internal

import (
	"context"
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

	serviceConfig := `{
		"loadBalancingConfig": [{"round_robin": {}}],
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

	addrs := strings.Split(target, ",")
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(serviceConfig),
		grpc.WithUnaryInterceptor(retryUnaryInterceptor),
		grpc.WithStreamInterceptor(retryStreamInterceptor),
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

// retryUnaryInterceptor retries unary RPCs on UNAVAILABLE errors (including
// DNS resolution failures) that the gRPC service-config retry policy misses.
func retryUnaryInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	var err error
	for attempt := range retryMaxAttempts {
		err = invoker(ctx, method, req, reply, cc, opts...)
		if !IsUnavailable(err) {
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

// retryStreamInterceptor retries stream creation on UNAVAILABLE errors.
func retryStreamInterceptor(
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
	for attempt := range retryMaxAttempts {
		stream, err = streamer(ctx, desc, cc, method, opts...)
		if !IsUnavailable(err) {
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
