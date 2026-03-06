package cmdutil

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// ErrNoLedgers is returned when no ledgers exist.
var ErrNoLedgers = errors.New("no ledgers found")

// SelectLedger selects a ledger interactively or automatically.
// If ledgerFlag is set, it returns that value.
// If only one ledger exists, it returns that ledger's name automatically.
// If multiple ledgers exist, it prompts the user to select one.
// If no ledgers exist, it returns an error with a hint to create one.
func SelectLedger(cmd *cobra.Command, client servicepb.BucketServiceClient, ledgerFlag string) (string, error) {
	// If a ledger was specified via flag, use it directly
	if ledgerFlag != "" {
		return ledgerFlag, nil
	}

	// Get context for the API call
	ctx, cancel := GetContext(cmd)
	defer cancel()

	// List available ledgers
	ledgers, err := GetAllLedgersInfo(ctx, client)
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
	for i := range len(s) - 1 {
		for j := i + 1; j < len(s); j++ {
			if s[i] > s[j] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

// GetAllLedgersInfo collects all ledgers from the streaming RPC into a map.
func GetAllLedgersInfo(ctx context.Context, client servicepb.BucketServiceClient) (map[string]*commonpb.LedgerInfo, error) {
	stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{})
	if err != nil {
		return nil, err
	}

	ledgers := make(map[string]*commonpb.LedgerInfo)

	for {
		ledger, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		ledgers[ledger.GetName()] = ledger
	}

	return ledgers, nil
}
