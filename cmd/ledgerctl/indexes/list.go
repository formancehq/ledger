package indexes

import (
	"fmt"
	"sort"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewListCommand creates the indexes list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list [flags]",
		Aliases: []string{"ls"},
		Short:   "List indexes on a ledger",
		Long: `List all configured indexes on a ledger, including their build status.

Examples:
  ledgerctl indexes list --ledger my-ledger`,
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
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to get ledger", err)
	}

	// Check if any index is in BUILDING state before fetching progress.
	hasBuilding := hasBuildingIndexes(ledger)

	var (
		progressMap map[string]uint64
		lastLogSeq  uint64
	)

	if hasBuilding {
		idxStatus, err := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{})
		if err != nil {
			spinner.Fail(fmt.Sprintf("Failed to fetch index status: %v", err))
			// Continue without progress info — indexes will show as BUILDING without percentage.
		} else {
			lastLogSeq = idxStatus.GetLastLogSequence()
			progressMap = buildProgressMap(ledgerName, idxStatus.GetBackfillProgress())
		}
	}

	_ = spinner.Stop()

	pterm.Println()
	pterm.Printf("Indexes for ledger: %s\n", pterm.Cyan(ledgerName))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	table := pterm.TableData{
		{"TYPE", "TARGET", "KEY", "STATUS"},
	}

	// Builtin indexes (includes address indexes)
	if bi := ledger.GetBuiltinIndexes(); bi != nil {
		if bi.GetReference() {
			table = append(table, []string{"reference", "-", "-",
				indexStatusWithProgress(bi.GetReferenceStatus(), progressMap, lastLogSeq, "b:0")})
		}

		if bi.GetTimestamp() {
			table = append(table, []string{"timestamp", "-", "-",
				indexStatusWithProgress(bi.GetTimestampStatus(), progressMap, lastLogSeq, "b:1")})
		}

		if bi.GetAddress() {
			table = append(table, []string{"address", "-", "-",
				indexStatusWithProgress(bi.GetAddressStatus(), progressMap, lastLogSeq, "b:3")})
		}

		if bi.GetSourceAddress() {
			table = append(table, []string{"source-address", "-", "-",
				indexStatusWithProgress(bi.GetSourceAddressStatus(), progressMap, lastLogSeq, "b:4")})
		}

		if bi.GetDestAddress() {
			table = append(table, []string{"dest-address", "-", "-",
				indexStatusWithProgress(bi.GetDestAddressStatus(), progressMap, lastLogSeq, "b:5")})
		}
	}

	// Metadata indexes
	if schema := ledger.GetMetadataSchema(); schema != nil {
		addMetadataIndexRowsWithProgress(&table, "account", schema.GetAccountFields(), progressMap, lastLogSeq, commonpb.TargetType_TARGET_TYPE_ACCOUNT)
		addMetadataIndexRowsWithProgress(&table, "transaction", schema.GetTransactionFields(), progressMap, lastLogSeq, commonpb.TargetType_TARGET_TYPE_TRANSACTION)
	}

	// The per-ledger log index is always-on (no row to display).
	// Opt-in log builtin indexes (e.g. date) would be listed here if added.

	if len(table) == 1 {
		pterm.Println("No indexes configured.")
		pterm.Println(pterm.Gray("Hint: Create an index using:"))
		pterm.FgCyan.Println("  ledgerctl indexes create --ledger " + ledgerName + " --type address")

		return nil
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(table).Render()

	return nil
}

// hasBuildingIndexes returns true if any index on the ledger has BUILDING status.
func hasBuildingIndexes(ledger *commonpb.LedgerInfo) bool {
	if bi := ledger.GetBuiltinIndexes(); bi != nil {
		if bi.GetReference() && bi.GetReferenceStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			return true
		}

		if bi.GetTimestamp() && bi.GetTimestampStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			return true
		}

		if bi.GetAddress() && bi.GetAddressStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			return true
		}

		if bi.GetSourceAddress() && bi.GetSourceAddressStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			return true
		}

		if bi.GetDestAddress() && bi.GetDestAddressStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			return true
		}
	}

	if schema := ledger.GetMetadataSchema(); schema != nil {
		for _, f := range schema.GetAccountFields() {
			if f.GetIndexed() && f.GetIndexBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
				return true
			}
		}

		for _, f := range schema.GetTransactionFields() {
			if f.GetIndexed() && f.GetIndexBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
				return true
			}
		}
	}

	// LogBuiltinIndexConfig.date is not exposed via the CLI yet; when it is,
	// add the BUILDING check here.

	return false
}

// buildProgressMap creates a lookup map from backfill progress entries.
// Keys use the format "b:<builtin_int>" for transaction builtin indexes,
// "tm:<key>" for transaction metadata, "am:<key>" for account metadata,
// or "l:<builtin_int>" for log builtin indexes.
func buildProgressMap(ledgerName string, entries []*servicepb.IndexBackfillProgress) map[string]uint64 {
	m := make(map[string]uint64, len(entries))
	for _, e := range entries {
		if e.GetLedger() != ledgerName {
			continue
		}

		switch idx := e.GetIndex().(type) {
		case *servicepb.IndexBackfillProgress_Transaction:
			switch txIdx := idx.Transaction.GetKind().(type) {
			case *commonpb.TransactionIndex_Builtin:
				m[fmt.Sprintf("b:%d", txIdx.Builtin)] = e.GetCursor()
			case *commonpb.TransactionIndex_MetadataKey:
				m["tm:"+txIdx.MetadataKey] = e.GetCursor()
			}
		case *servicepb.IndexBackfillProgress_Account:
			switch acctIdx := idx.Account.GetKind().(type) {
			case *commonpb.AccountIndex_Builtin:
				m[fmt.Sprintf("ab:%d", acctIdx.Builtin)] = e.GetCursor()
			case *commonpb.AccountIndex_MetadataKey:
				m["am:"+acctIdx.MetadataKey] = e.GetCursor()
			}
		case *servicepb.IndexBackfillProgress_LogBuiltin:
			m[fmt.Sprintf("l:%d", idx.LogBuiltin)] = e.GetCursor()
		}
	}

	return m
}

// indexStatusWithProgress returns the status string, appending progress percentage for BUILDING indexes.
func indexStatusWithProgress(status commonpb.IndexBuildStatus, progressMap map[string]uint64, lastLogSeq uint64, key string) string {
	if status != commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
		return indexBuildStatusString(status)
	}

	if progressMap == nil || lastLogSeq == 0 {
		return indexBuildStatusString(status)
	}

	cursor, ok := progressMap[key]
	if !ok {
		// Backfill task exists but hasn't written its first batch yet
		// (initial log catch-up is still running).
		return pterm.Yellow("BUILDING (starting...)")
	}

	pct := cursor * 100 / lastLogSeq

	return pterm.Yellow(fmt.Sprintf("BUILDING (%d%%)", pct))
}

// addMetadataIndexRowsWithProgress adds rows for indexed metadata fields with progress info.
func addMetadataIndexRowsWithProgress(table *pterm.TableData, targetName string, fields map[string]*commonpb.MetadataFieldSchema, progressMap map[string]uint64, lastLogSeq uint64, targetType commonpb.TargetType) {
	keys := make([]string, 0, len(fields))
	for k, f := range fields {
		if f.GetIndexed() {
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)

	var progressPrefix string

	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		progressPrefix = "am:"
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		progressPrefix = "tm:"
	}

	for _, key := range keys {
		progressKey := progressPrefix + key
		*table = append(*table, []string{
			"metadata",
			targetName,
			key,
			indexStatusWithProgress(fields[key].GetIndexBuildStatus(), progressMap, lastLogSeq, progressKey),
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
