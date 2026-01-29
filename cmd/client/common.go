package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultTimeout = 10 * time.Second

// ErrNoLedgers is returned when no ledgers exist.
var ErrNoLedgers = fmt.Errorf("no ledgers found")

// selectLedger selects a ledger interactively or automatically.
// If ledgerFlag is set, it returns that value.
// If only one ledger exists, it returns that ledger's name automatically.
// If multiple ledgers exist, it prompts the user to select one.
// If no ledgers exist, it returns an error with a hint to create one.
func selectLedger(cmd *cobra.Command, client servicepb.LedgerServiceClient, reader *bufio.Reader, ledgerFlag string) (string, error) {
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
		fmt.Println("\nNo ledgers found.")
		fmt.Println("Hint: Create a ledger first using:")
		fmt.Println("  ledgerctl ledgers create --name <ledger-name>")
		return "", ErrNoLedgers
	}

	// Only one ledger exists, use it automatically
	if len(ledgerNames) == 1 {
		fmt.Printf("Using ledger: %s\n", ledgerNames[0])
		return ledgerNames[0], nil
	}

	// Multiple ledgers exist, prompt for selection
	return promptLedgerSelection(reader, ledgerNames)
}

// promptLedgerSelection prompts the user to select a ledger from a list.
func promptLedgerSelection(reader *bufio.Reader, ledgerNames []string) (string, error) {
	fmt.Println("\nAvailable ledgers:")
	for i, name := range ledgerNames {
		fmt.Printf("  [%d] %s\n", i+1, name)
	}

	for {
		fmt.Printf("\nSelect a ledger (1-%d): ", len(ledgerNames))
		input, err := reader.ReadString('\n')
		if err != nil {
			return "", fmt.Errorf("failed to read input: %w", err)
		}

		input = strings.TrimSpace(input)

		// Try to parse as a number
		num, err := strconv.Atoi(input)
		if err == nil && num >= 1 && num <= len(ledgerNames) {
			return ledgerNames[num-1], nil
		}

		// Try to match by name
		for _, name := range ledgerNames {
			if name == input {
				return name, nil
			}
		}

		fmt.Printf("Invalid selection. Please enter a number between 1 and %d, or a ledger name.\n", len(ledgerNames))
	}
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
func getClient(cmd *cobra.Command) (servicepb.LedgerServiceClient, *grpc.ClientConn, error) {
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
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	return servicepb.NewLedgerServiceClient(conn), conn, nil
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
func getAllLedgersInfo(ctx context.Context, client servicepb.LedgerServiceClient) (map[string]*commonpb.LedgerInfo, error) {
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
