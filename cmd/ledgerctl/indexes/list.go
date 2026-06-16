package indexes

import (
	"fmt"
	"sort"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewListCommand creates the indexes list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list [flags]",
		Aliases: cmdutil.ListAliases,
		Short:   "List indexes on a ledger",
		Long: `List all configured indexes on a ledger, including their build status.

Indexes are embedded in the ledger configuration and naturally bounded in size;
this endpoint is intentionally not paginated.

Examples:
  ledgerctl indexes list --ledger my-ledger`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runListIndexes,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmdutil.AddOutputFlags(cmd)
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
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to get ledger", err)
	}

	hasBuilding := false
	for _, idx := range ledger.GetIndexes() {
		if idx.GetBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			hasBuilding = true

			break
		}
	}

	var (
		cursorByID map[string]uint64
		lastLogSeq uint64
	)

	if hasBuilding {
		idxStatus, statusErr := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: ledgerName})
		if statusErr != nil {
			spinner.Fail(fmt.Sprintf("Failed to fetch index status: %v", statusErr))
		} else {
			lastLogSeq = idxStatus.GetLastLogSequence()
			cursorByID = make(map[string]uint64)

			for _, e := range idxStatus.GetIndexes() {
				if e.GetLedger() != ledgerName {
					continue
				}

				cursorByID[indexes.Canonical(e.GetIndex().GetId())] = e.GetCursor()
			}
		}
	}

	_ = spinner.Stop()

	if handled, err := cmdutil.EncodeStructured(cmd, ledger.GetIndexes()); handled || err != nil {
		return err
	}

	pterm.Println()
	pterm.Printf("Indexes for ledger: %s\n", pterm.Cyan(ledgerName))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	table := pterm.TableData{
		{"TYPE", "TARGET", "KEY", "STATUS"},
	}

	// Sort indexes by canonical id for stable output.
	sorted := append([]*commonpb.Index(nil), ledger.GetIndexes()...)
	sort.Slice(sorted, func(i, j int) bool {
		return indexes.Canonical(sorted[i].GetId()) < indexes.Canonical(sorted[j].GetId())
	})

	for _, idx := range sorted {
		typeName, target, key := describeIndex(idx.GetId())
		table = append(table, []string{
			typeName,
			target,
			key,
			indexStatusWithProgress(idx.GetBuildStatus(), cursorByID, lastLogSeq, indexes.Canonical(idx.GetId())),
		})
	}

	if len(table) == 1 {
		pterm.Println("No indexes configured.")
		pterm.Println(pterm.Gray("Hint: Create an index using:"))
		pterm.FgCyan.Println("  ledgerctl indexes create --ledger " + ledgerName + " --type address")

		return nil
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()

	return nil
}

// describeIndex returns the CLI-facing tuple (type, target, key) for an IndexID.
func describeIndex(id *commonpb.IndexID) (typeName, target, key string) {
	switch k := id.GetKind().(type) {
	case *commonpb.IndexID_TxBuiltin:
		switch k.TxBuiltin {
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE:
			return "reference", "-", "-"
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
			return "timestamp", "-", "-"
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS:
			return "address", "-", "-"
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS:
			return "source-address", "-", "-"
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS:
			return "dest-address", "-", "-"
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
			return "inserted-at", "-", "-"
		}

		return "tx-builtin", "-", k.TxBuiltin.String()
	case *commonpb.IndexID_LogBuiltin:
		return "log-" + k.LogBuiltin.String(), "-", "-"
	case *commonpb.IndexID_AccountBuiltin:
		return "account-builtin", "-", k.AccountBuiltin.String()
	case *commonpb.IndexID_Metadata:
		return "metadata", targetName(k.Metadata.GetTarget()), k.Metadata.GetKey()
	}

	return "unknown", "-", "-"
}

func targetName(t commonpb.TargetType) string {
	switch t {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		return "account"
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		return "transaction"
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		return "ledger"
	}

	return "-"
}

// indexStatusWithProgress returns the status string, appending progress percentage for BUILDING indexes.
func indexStatusWithProgress(status commonpb.IndexBuildStatus, cursorByID map[string]uint64, lastLogSeq uint64, canonical string) string {
	if status != commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
		return indexBuildStatusString(status)
	}

	if cursorByID == nil || lastLogSeq == 0 {
		return indexBuildStatusString(status)
	}

	cursor, ok := cursorByID[canonical]
	if !ok {
		return pterm.Yellow("BUILDING (starting...)")
	}

	pct := cursor * 100 / lastLogSeq

	return pterm.Yellow(fmt.Sprintf("BUILDING (%d%%)", pct))
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
