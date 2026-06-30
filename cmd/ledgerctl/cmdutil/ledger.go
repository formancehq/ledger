package cmdutil

import (
	"context"
	"errors"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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

// CompleteLedgerNames is a cobra shell-completion function that suggests the
// ledgers available on the connected server. It is wired to every --ledger
// flag so pressing TAB lists the existing ledger names.
//
// Completion runs in the user's interactive shell, so any failure (server
// unreachable, auth missing, slow network) returns no suggestions rather than
// surfacing an error: a broken connection must never disrupt tab completion.
func CompleteLedgerNames(cmd *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	// cobra does not run the root PersistentPreRunE during `__complete`, so the
	// connection flags still hold their defaults here. Resolve --profile/env
	// ourselves or we would list ledgers from the default server instead of the
	// one the active profile points at.
	if err := ResolveConnectionFlags(cmd); err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	client, conn, err := GetClient(cmd)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := GetContext(cmd)
	defer cancel()

	ledgers, err := GetAllLedgersInfo(ctx, client)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	names := make([]string, 0, len(ledgers))
	for name := range ledgers {
		names = append(names, name)
	}

	sortStrings(names)

	return names, cobra.ShellCompDirectiveNoFileComp
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

// GetAllLedgersInfo collects every ledger from the streaming RPC, following
// the x-next-cursor trailer chain so clusters with more ledgers than the
// server's default page still surface them all.
func GetAllLedgersInfo(ctx context.Context, client servicepb.BucketServiceClient, checkpointID ...uint64) (map[string]*commonpb.LedgerInfo, error) {
	var read *commonpb.ReadOptions
	if len(checkpointID) > 0 && checkpointID[0] > 0 {
		read = &commonpb.ReadOptions{CheckpointId: checkpointID[0]}
	}

	all, err := DrainAllPages("", func(cur string) ([]*commonpb.LedgerInfo, metadata.MD, error) {
		stream, streamErr := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{
			Options: &commonpb.ListOptions{Read: read, Cursor: cur},
		})
		if streamErr != nil {
			return nil, nil, streamErr
		}

		items, recvErr := CollectStream(stream)
		if recvErr != nil {
			return nil, nil, recvErr
		}

		return items, stream.Trailer(), nil
	})
	if err != nil {
		return nil, err
	}

	ledgers := make(map[string]*commonpb.LedgerInfo, len(all))
	for _, l := range all {
		ledgers[l.GetName()] = l
	}

	return ledgers, nil
}
