package ledgers

import (
	"fmt"
	"sort"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewListIndexesCommand creates the ledgers list-indexes command.
func NewListIndexesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list-indexes [flags]",
		Aliases: []string{"li", "indexes"},
		Short:   "List indexes on a ledger",
		Long: `List all configured indexes on a ledger, including their build status.

Examples:
  ledgerctl ledgers list-indexes --ledger my-ledger`,
		Args: cobra.NoArgs,
		RunE: runListIndexes,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runListIndexes(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ledgerFlag, _ := cmd.Flags().GetString("ledger")
	ledgerName, err := cmdutil.SelectLedger(cmd, client, ledgerFlag)
	if err != nil {
		return err
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching indexes for %s...", ledgerName))

	ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{
		Ledger: ledgerName,
	})
	if err != nil {
		spinner.Fail("Failed to get ledger")
		return cmdutil.FormatGRPCError("failed to get ledger", err)
	}

	_ = spinner.Stop()

	pterm.Println()
	pterm.Printf("Indexes for ledger: %s\n", pterm.Cyan(ledgerName))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	table := pterm.TableData{
		{"TYPE", "TARGET", "KEY", "STATUS"},
	}

	// Address indexes
	if ac := ledger.AddressIndexes; ac != nil {
		if ac.Address {
			table = append(table, []string{"address", "-", "-", indexBuildStatusString(ac.AddressStatus)})
		}
		if ac.Source {
			table = append(table, []string{"source-address", "-", "-", indexBuildStatusString(ac.SourceStatus)})
		}
		if ac.Destination {
			table = append(table, []string{"dest-address", "-", "-", indexBuildStatusString(ac.DestinationStatus)})
		}
	}

	// Metadata indexes
	if schema := ledger.MetadataSchema; schema != nil {
		addMetadataIndexRows(&table, "account", schema.AccountFields)
		addMetadataIndexRows(&table, "transaction", schema.TransactionFields)
	}

	if len(table) == 1 {
		pterm.Println("No indexes configured.")
		pterm.Println(pterm.Gray("Hint: Create an index using:"))
		pterm.FgCyan.Println("  ledgerctl ledgers create-index --ledger " + ledgerName + " --type address")
		return nil
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()

	return nil
}

// addMetadataIndexRows adds rows for indexed metadata fields to the table.
func addMetadataIndexRows(table *pterm.TableData, targetName string, fields map[string]*commonpb.MetadataFieldSchema) {
	keys := make([]string, 0, len(fields))
	for k, f := range fields {
		if f.Indexed {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	for _, key := range keys {
		*table = append(*table, []string{
			"metadata",
			targetName,
			key,
			indexBuildStatusString(fields[key].IndexBuildStatus),
		})
	}
}

// indexBuildStatusString returns a user-friendly string for IndexBuildStatus.
func indexBuildStatusString(status commonpb.IndexBuildStatus) string {
	switch status {
	case commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING:
		return pterm.Yellow("BUILDING")
	case commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY:
		return pterm.Green("READY")
	default:
		return pterm.Gray("UNKNOWN")
	}
}
