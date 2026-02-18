package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultTimeout = 10 * time.Second

// grpcRetryPolicy defines the retry policy for gRPC clients when no leader is available
var grpcRetryPolicy = `{
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

// ErrNoLedgers is returned when no ledgers exist.
var ErrNoLedgers = fmt.Errorf("no ledgers found")

// selectLedger selects a ledger interactively or automatically.
// If ledgerFlag is set, it returns that value.
// If only one ledger exists, it returns that ledger's name automatically.
// If multiple ledgers exist, it prompts the user to select one.
// If no ledgers exist, it returns an error with a hint to create one.
func selectLedger(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerFlag string) (string, error) {
	// If a ledger was specified via flag, use it directly
	if ledgerFlag != "" {
		return ledgerFlag, nil
	}

	// Get context for the API call
	ctx, cancel := getContext(cmd)
	defer cancel()

	// List available ledgers
	ledgers, err := getAllLedgersInfo(ctx, client)
	if err != nil {
		return "", fmt.Errorf("failed to list ledgers: %w", err)
	}

	// Convert map to sorted slice for consistent ordering
	var ledgerNames []string
	for name := range ledgers {
		ledgerNames = append(ledgerNames, name)
	}

	// Sort for consistent ordering
	sortStrings(ledgerNames)

	// No ledgers exist
	if len(ledgerNames) == 0 {
		pterm.Println("No ledgers found.")
		pterm.Println(pterm.Gray("Hint: Create a ledger first using:"))
		pterm.FgCyan.Println("  ledgerctl ledgers create --name <ledger-name>")
		return "", ErrNoLedgers
	}

	// Only one ledger exists, use it automatically
	if len(ledgerNames) == 1 {
		pterm.Info.Printfln("Using ledger: %s", pterm.Cyan(ledgerNames[0]))
		return ledgerNames[0], nil
	}

	// Multiple ledgers exist, prompt for selection using interactive select
	selectedLedger, err := pterm.DefaultInteractiveSelect.
		WithOptions(ledgerNames).
		WithDefaultText("Select a ledger").
		Show()
	if err != nil {
		return "", fmt.Errorf("failed to select ledger: %w", err)
	}

	return selectedLedger, nil
}

// sortStrings sorts a slice of strings in place.
func sortStrings(s []string) {
	for i := 0; i < len(s)-1; i++ {
		for j := i + 1; j < len(s); j++ {
			if s[i] > s[j] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

// getClient creates a gRPC client connection and returns the client.
// The caller is responsible for closing the connection.
func getClient(cmd *cobra.Command) (servicepb.BucketServiceClient, *grpc.ClientConn, error) {
	serverAddr, _ := cmd.Flags().GetString("server")
	insecureMode, _ := cmd.Flags().GetBool("insecure")

	var creds credentials.TransportCredentials
	if insecureMode {
		creds = insecure.NewCredentials()
	} else {
		// Use TLS by default (for port 443 or any secure endpoint)
		creds = credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})
	}

	// If connecting to port 443, ensure we don't include the port in the address
	// as some gRPC implementations may have issues with explicit :443
	if strings.HasSuffix(serverAddr, ":443") {
		serverAddr = strings.TrimSuffix(serverAddr, ":443") + ":443"
	}

	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultServiceConfig(grpcRetryPolicy), // Retry on UNAVAILABLE (no leader) up to 50 times with 200ms delay (10s max)
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	return servicepb.NewBucketServiceClient(conn), conn, nil
}

// getContext returns a context with the configured timeout.
func getContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	timeout, _ := cmd.Flags().GetDuration("timeout")
	if timeout == 0 {
		timeout = defaultTimeout
	}
	return context.WithTimeout(cmd.Context(), timeout)
}

// getAllLedgersInfo collects all ledgers from the streaming RPC into a map
func getAllLedgersInfo(ctx context.Context, client servicepb.BucketServiceClient) (map[string]*commonpb.LedgerInfo, error) {
	stream, err := client.GetAllLedgersInfo(ctx, &servicepb.GetAllLedgersRequest{})
	if err != nil {
		return nil, err
	}

	ledgers := make(map[string]*commonpb.LedgerInfo)
	for {
		ledger, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ledgers[ledger.Name] = ledger
	}

	return ledgers, nil
}

// parseKeyValue parses a string in the format "key=value" and returns the key and value.
func parseKeyValue(s string) (string, string, error) {
	parts := strings.SplitN(s, "=", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("expected key=value format")
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

// loadSigningKey loads the signing key and key ID from command flags.
// Returns empty values if no signing key is configured.
func loadSigningKey(cmd *cobra.Command) (string, ed25519.PrivateKey, error) {
	keyPath, _ := cmd.Flags().GetString("signing-key")
	if keyPath == "" {
		return "", nil, nil
	}

	keyID, _ := cmd.Flags().GetString("signing-key-id")
	if keyID == "" {
		keyID = "default"
	}

	// Read the seed file
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to read signing key file: %w", err)
	}

	// Try to interpret as hex-encoded seed
	seed := data
	trimmed := strings.TrimSpace(string(data))
	if decoded, err := hex.DecodeString(trimmed); err == nil && len(decoded) == ed25519.SeedSize {
		seed = decoded
	}

	if len(seed) != ed25519.SeedSize {
		return "", nil, fmt.Errorf("signing key seed must be %d bytes, got %d", ed25519.SeedSize, len(seed))
	}

	return keyID, ed25519.NewKeyFromSeed(seed), nil
}

// signRequests signs each request using the signing key from command flags.
// If no signing key is configured, this is a no-op.
func signRequests(cmd *cobra.Command, requests []*servicepb.Request) error {
	keyID, privKey, err := loadSigningKey(cmd)
	if err != nil {
		return err
	}
	if privKey == nil {
		return nil
	}

	for _, req := range requests {
		if err := signing.Sign(req, keyID, privKey); err != nil {
			return fmt.Errorf("failed to sign request: %w", err)
		}
	}
	return nil
}

// formatBytes formats a byte count as a human-readable string.
func formatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/TB)
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
