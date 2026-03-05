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
		_ = spinner.Stop()
		return cmdutil.FormatGRPCError("failed to get ledger", err)
	}

	// Check if any index is in BUILDING state before fetching progress.
	hasBuilding := hasBuildingIndexes(ledger)
	var progressMap map[string]uint64
	var lastLogSeq uint64

	if hasBuilding {
		idxStatus, err := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{})
		if err != nil {
			spinner.Fail(fmt.Sprintf("Failed to fetch index status: %v", err))
			// Continue without progress info — indexes will show as BUILDING without percentage.
		} else {
			lastLogSeq = idxStatus.LastLogSequence
			progressMap = buildProgressMap(ledgerName, idxStatus.BackfillProgress)
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
	if bi := ledger.BuiltinIndexes; bi != nil {
		if bi.Reference {
			table = append(table, []string{"reference", "-", "-",
				indexStatusWithProgress(bi.ReferenceStatus, progressMap, lastLogSeq, "b:0")})
		}
		if bi.Timestamp {
			table = append(table, []string{"timestamp", "-", "-",
				indexStatusWithProgress(bi.TimestampStatus, progressMap, lastLogSeq, "b:1")})
		}
		if bi.Address {
			table = append(table, []string{"address", "-", "-",
				indexStatusWithProgress(bi.AddressStatus, progressMap, lastLogSeq, "b:3")})
		}
		if bi.SourceAddress {
			table = append(table, []string{"source-address", "-", "-",
				indexStatusWithProgress(bi.SourceAddressStatus, progressMap, lastLogSeq, "b:4")})
		}
		if bi.DestAddress {
			table = append(table, []string{"dest-address", "-", "-",
				indexStatusWithProgress(bi.DestAddressStatus, progressMap, lastLogSeq, "b:5")})
		}
	}

	// Metadata indexes
	if schema := ledger.MetadataSchema; schema != nil {
		addMetadataIndexRowsWithProgress(&table, "account", schema.AccountFields, progressMap, lastLogSeq, commonpb.TargetType_TARGET_TYPE_ACCOUNT)
		addMetadataIndexRowsWithProgress(&table, "transaction", schema.TransactionFields, progressMap, lastLogSeq, commonpb.TargetType_TARGET_TYPE_TRANSACTION)
	}

	// Log builtin indexes
	if li := ledger.LogBuiltinIndexes; li != nil {
		if li.Ledger {
			table = append(table, []string{"log-ledger", "-", "-",
				indexStatusWithProgress(li.LedgerStatus, progressMap, lastLogSeq, "l:0")})
		}
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

// hasBuildingIndexes returns true if any index on the ledger has BUILDING status.
func hasBuildingIndexes(ledger *commonpb.LedgerInfo) bool {
	if bi := ledger.BuiltinIndexes; bi != nil {
		if bi.Reference && bi.ReferenceStatus == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			return true
		}
		if bi.Timestamp && bi.TimestampStatus == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			return true
		}
		if bi.Address && bi.AddressStatus == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			return true
		}
		if bi.SourceAddress && bi.SourceAddressStatus == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			return true
		}
		if bi.DestAddress && bi.DestAddressStatus == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			return true
		}
	}
	if schema := ledger.MetadataSchema; schema != nil {
		for _, f := range schema.AccountFields {
			if f.Indexed && f.IndexBuildStatus == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
				return true
			}
		}
		for _, f := range schema.TransactionFields {
			if f.Indexed && f.IndexBuildStatus == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
				return true
			}
		}
	}
	if li := ledger.LogBuiltinIndexes; li != nil {
		if li.Ledger && li.LedgerStatus == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			return true
		}
	}
	return false
}

// buildProgressMap creates a lookup map from backfill progress entries.
// Keys use the format "b:<builtin_int>" for transaction builtin indexes,
// "tm:<key>" for transaction metadata, "am:<key>" for account metadata,
// or "l:<builtin_int>" for log builtin indexes.
func buildProgressMap(ledgerName string, entries []*servicepb.IndexBackfillProgress) map[string]uint64 {
	m := make(map[string]uint64, len(entries))
	for _, e := range entries {
		if e.Ledger != ledgerName {
			continue
		}
		switch idx := e.Index.(type) {
		case *servicepb.IndexBackfillProgress_Transaction:
			switch txIdx := idx.Transaction.Kind.(type) {
			case *commonpb.TransactionIndex_Builtin:
				m[fmt.Sprintf("b:%d", txIdx.Builtin)] = e.Cursor
			case *commonpb.TransactionIndex_MetadataKey:
				m[fmt.Sprintf("tm:%s", txIdx.MetadataKey)] = e.Cursor
			}
		case *servicepb.IndexBackfillProgress_Account:
			switch acctIdx := idx.Account.Kind.(type) {
			case *commonpb.AccountIndex_Builtin:
				m[fmt.Sprintf("ab:%d", acctIdx.Builtin)] = e.Cursor
			case *commonpb.AccountIndex_MetadataKey:
				m[fmt.Sprintf("am:%s", acctIdx.MetadataKey)] = e.Cursor
			}
		case *servicepb.IndexBackfillProgress_LogBuiltin:
			m[fmt.Sprintf("l:%d", idx.LogBuiltin)] = e.Cursor
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
		if f.Indexed {
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
			indexStatusWithProgress(fields[key].IndexBuildStatus, progressMap, lastLogSeq, progressKey),
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
