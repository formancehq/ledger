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
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
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
// Otherwise, returns TLS credentials, optionally using a custom CA from --tls-ca-cert
// and a custom verification hostname from --tls-server-name.
//
// Setting --insecure together with --tls-ca-cert or --tls-server-name is
// rejected: those flags encode conflicting intent (no TLS vs. verify with this
// CA / against this hostname), and silently preferring one of them — as the
// older code did — masks env-var leakage like a stray LEDGERCTL_INSECURE=true
// in a container image (the original cause of the "error reading server
// preface: EOF" production incident).
//
// --tls-server-name overrides the hostname the certificate is verified against
// (SNI + SAN match), decoupling it from the dial address in --server. This is
// what lets a client dial by IP (e.g. 127.0.0.1:8888 from inside a pod, or a
// LoadBalancer VIP) while still validating an operator-issued certificate whose
// SANs only cover the in-cluster DNS names (e.g.
// <pod>.<cluster>-headless.<ns>.svc.cluster.local), never localhost/127.0.0.1.
func GetClientTransportCredentials(cmd *cobra.Command) (credentials.TransportCredentials, error) {
	insecureMode, _ := cmd.Flags().GetBool("insecure")
	caCertPath, _ := cmd.Flags().GetString("tls-ca-cert")
	serverName, _ := cmd.Flags().GetString("tls-server-name")

	if err := ValidateTLSFlags(insecureMode, caCertPath, serverName); err != nil {
		return nil, err
	}

	if insecureMode {
		return insecure.NewCredentials(), nil
	}

	tlsConfig, err := buildClientTLSConfig(caCertPath, serverName)
	if err != nil {
		return nil, err
	}

	return credentials.NewTLS(tlsConfig), nil
}

// ValidateTLSFlags rejects contradictory TLS flag combinations: --insecure
// (no TLS at all) cannot be paired with --tls-ca-cert or --tls-server-name,
// which only make sense when verifying a TLS connection.
//
// It is the single source of truth for that rule, shared by the connection path
// (GetClientTransportCredentials) and the profile-persistence paths (profile
// create, auth login). Persisting the conflict would store a profile every
// subsequent TLS-aware command immediately rejects; validating here lets those
// commands fail fast at write time instead. Mentioning LEDGERCTL_INSECURE keeps
// the message actionable when --insecure was injected via the environment (a
// stray container-image default was the original "server preface: EOF" cause).
func ValidateTLSFlags(insecureMode bool, caCertPath, serverName string) error {
	if insecureMode && caCertPath != "" {
		return errors.New("--insecure and --tls-ca-cert are mutually exclusive (--insecure may also come from the LEDGERCTL_INSECURE env var; unset it to use TLS)")
	}

	if insecureMode && serverName != "" {
		return errors.New("--insecure and --tls-server-name are mutually exclusive (--insecure may also come from the LEDGERCTL_INSECURE env var; unset it to use TLS)")
	}

	return nil
}

// buildClientTLSConfig assembles the *tls.Config for a verifying (non-insecure)
// client connection. Split out from GetClientTransportCredentials so the
// resulting config is directly assertable in tests: credentials.NewTLS wraps it
// in an opaque type whose only exported accessor (ProtocolInfo.ServerName) is
// deprecated, so the config is the sole reliable place to verify ServerName and
// RootCAs were applied.
func buildClientTLSConfig(caCertPath, serverName string) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	// Override the name verified against the server certificate's SANs, so the
	// dial address (--server) and the verification identity can differ.
	if serverName != "" {
		tlsConfig.ServerName = serverName
	}

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

	return tlsConfig, nil
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
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),     // Emit a client span per RPC and propagate W3C trace context.
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
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()), // Emit a client span per RPC and propagate W3C trace context.
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

		if _, ok := os.LookupEnv("LEDGERCTL_AUTH_TOKEN"); ok && !cmd.Flags().Changed("auth-token") {
			return "environment (LEDGERCTL_AUTH_TOKEN)", t
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
