package cmdutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	DefaultTimeout  = 10 * time.Second
	DefaultPageSize = 10
)

// GRPCRetryPolicy defines the retry policy for gRPC clients when no leader is available.
var GRPCRetryPolicy = `{
	"methodConfig": [{
		"name": [{}],
		"retryPolicy": {
			"MaxAttempts": 50,
			"InitialBackoff": "0.2s",
			"MaxBackoff": "0.2s",
			"BackoffMultiplier": 1.0,
			"RetryableStatusCodes": ["UNAVAILABLE"]
		}
	}]
}`

// GetClientTransportCredentials returns the transport credentials based on CLI flags.
// If --insecure is set, returns insecure credentials.
// Otherwise, returns TLS credentials, optionally using a custom CA from --tls-ca-cert.
func GetClientTransportCredentials(cmd *cobra.Command) (credentials.TransportCredentials, error) {
	insecureMode, _ := cmd.Flags().GetBool("insecure")
	if insecureMode {
		return insecure.NewCredentials(), nil
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	caCertPath, _ := cmd.Flags().GetString("tls-ca-cert")
	if caCertPath != "" {
		caPEM, err := os.ReadFile(caCertPath)
		if err != nil {
			return nil, fmt.Errorf("reading CA cert: %w", err)
		}
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("failed to parse CA certificate from %s", caCertPath)
		}
		tlsConfig.RootCAs = certPool
	}

	return credentials.NewTLS(tlsConfig), nil
}

// GetClient creates a gRPC client connection and returns the client.
// The caller is responsible for closing the connection.
func GetClient(cmd *cobra.Command) (servicepb.BucketServiceClient, *grpc.ClientConn, error) {
	serverAddr, _ := cmd.Flags().GetString("server")

	creds, err := GetClientTransportCredentials(cmd)
	if err != nil {
		return nil, nil, err
	}

	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultServiceConfig(GRPCRetryPolicy), // Retry on UNAVAILABLE (no leader) up to 50 times with 200ms delay (10s max)
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	return servicepb.NewBucketServiceClient(conn), conn, nil
}

// GetClusterClient creates a gRPC client connection for cluster operations.
func GetClusterClient(cmd *cobra.Command) (clusterpb.ClusterServiceClient, *grpc.ClientConn, error) {
	serverAddr, _ := cmd.Flags().GetString("server")

	creds, err := GetClientTransportCredentials(cmd)
	if err != nil {
		return nil, nil, err
	}

	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	return clusterpb.NewClusterServiceClient(conn), conn, nil
}

// GetContext returns a context with the configured timeout.
func GetContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	timeout, _ := cmd.Flags().GetDuration("timeout")
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	return context.WithTimeout(cmd.Context(), timeout)
}

// ParseKeyValue parses a string in the format "key=value" and returns the key and value.
func ParseKeyValue(s string) (string, string, error) {
	parts := strings.SplitN(s, "=", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("expected key=value format")
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}
