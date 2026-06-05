package cmdutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

const (
	DefaultTimeout  = 10 * time.Second
	DefaultPageSize = 10
	MaxRecvMsgSize  = 64 * 1024 * 1024 // 64 MB
)

// GetClientTransportCredentials returns the transport credentials based on CLI flags.
// If --insecure is set, returns insecure credentials.
// Otherwise, returns TLS credentials, optionally using a custom CA from --tls-ca-cert.
//
// Setting --insecure together with --tls-ca-cert is rejected: those flags
// encode conflicting intent (no TLS vs. verify with this CA), and silently
// preferring one of them — as the older code did — masks env-var leakage like
// a stray INSECURE=true in a container image (the original cause of the
// "error reading server preface: EOF" production incident).
func GetClientTransportCredentials(cmd *cobra.Command) (credentials.TransportCredentials, error) {
	insecureMode, _ := cmd.Flags().GetBool("insecure")
	caCertPath, _ := cmd.Flags().GetString("tls-ca-cert")

	if insecureMode && caCertPath != "" {
		return nil, errors.New("--insecure and --tls-ca-cert are mutually exclusive (--insecure may also come from the INSECURE env var; unset it to use TLS)")
	}

	if insecureMode {
		return insecure.NewCredentials(), nil
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

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
		grpc.WithDefaultServiceConfig(actions.GRPCRetryPolicy), // Retry on UNAVAILABLE (no leader) up to 50 times with 200ms delay (10s max)
		grpc.WithUnaryInterceptor(TracingUnaryInterceptor()),
		grpc.WithStreamInterceptor(TracingStreamInterceptor()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxRecvMsgSize)),
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
		grpc.WithUnaryInterceptor(TracingUnaryInterceptor()),
		grpc.WithStreamInterceptor(TracingStreamInterceptor()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxRecvMsgSize)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	return clusterpb.NewClusterServiceClient(conn), conn, nil
}

// GetContext returns a context with the configured timeout.
// If --consistency is set, appends x-consistency metadata to the outgoing gRPC context.
// If --auth-token is set, appends the Authorization: Bearer header.
func GetContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	timeout, _ := cmd.Flags().GetDuration("timeout")
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	ctx := cmd.Context()
	if consistency, _ := cmd.Flags().GetString("consistency"); consistency != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-consistency", consistency)
	}

	if token := resolveAuthToken(cmd); token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}

	return context.WithTimeout(ctx, timeout)
}

// resolveAuthToken resolves the bearer token using the following priority:
//  1. --auth-token flag (or AUTH_TOKEN env via bindEnvToFlag)
//  2. OS keychain (keyed by --server address)
//  3. No authentication (empty string)
func resolveAuthToken(cmd *cobra.Command) string {
	token, _ := cmd.Flags().GetString("auth-token")
	if token != "" {
		if strings.HasPrefix(token, "@") {
			data, err := os.ReadFile(token[1:])
			if err != nil {
				return ""
			}

			return strings.TrimSpace(string(data))
		}

		return token
	}

	// Fall back to OS keychain.
	server, _ := cmd.Flags().GetString("server")
	if t, err := GetKeyring(cmd).Get(server); err == nil {
		return t
	}

	return ""
}

// ResolveTokenSource returns a human-readable description of where the auth token comes from.
func ResolveTokenSource(cmd *cobra.Command) (source string, token string) {
	if t, _ := cmd.Flags().GetString("auth-token"); t != "" {
		if strings.HasPrefix(t, "@") {
			data, err := os.ReadFile(t[1:])
			if err != nil {
				return "file (error)", ""
			}

			return "file (" + t[1:] + ")", strings.TrimSpace(string(data))
		}

		if _, ok := os.LookupEnv("AUTH_TOKEN"); ok && !cmd.Flags().Changed("auth-token") {
			return "environment (AUTH_TOKEN)", t
		}

		return "flag (--auth-token)", t
	}

	server, _ := cmd.Flags().GetString("server")
	if t, err := GetKeyring(cmd).Get(server); err == nil {
		return "keychain", t
	}

	return "none", ""
}

// ParseKeyValue parses a string in the format "key=value" and returns the key and value.
func ParseKeyValue(s string) (string, string, error) {
	parts := strings.SplitN(s, "=", 2)
	if len(parts) != 2 {
		return "", "", errors.New("expected key=value format")
	}

	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}
