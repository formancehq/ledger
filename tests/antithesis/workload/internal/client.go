package internal

import (
	"fmt"
	"os"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewGRPCConn creates a gRPC connection to the ledger service with retry on UNAVAILABLE.
func NewGRPCConn() (*grpc.ClientConn, error) {
	target := os.Getenv("LEDGER_GRPC_ADDR")
	if target == "" {
		target = "localhost:15100"
	}

	retryPolicy := fmt.Sprintf(`{
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
	}`)

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

// NewClient creates a BucketServiceClient connected to the ledger service.
func NewClient() (servicepb.BucketServiceClient, *grpc.ClientConn, error) {
	conn, err := NewGRPCConn()
	if err != nil {
		return nil, nil, err
	}
	return servicepb.NewBucketServiceClient(conn), conn, nil
}
