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
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("creation-key-prefix", "", "Filter by audited singleton creation key prefix (attribution only, not authorization; uses the idempotency-key audit index)")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

// RenderList writes indexes in ledgerctl's native format. A nil status means
// readiness could not be loaded; it renders UNKNOWN rather than BUILDING.
// The caller owns attribution filtering and fetching the replica status.
func RenderList(cmd *cobra.Command, ledgerName string, entries []*commonpb.Index, idxStatus *servicepb.GetIndexStatusResponse) error {
	text := pterm.DefaultBasicText.WithWriter(cmd.OutOrStdout())

	var (
		statusOK           bool
		cursorByID         map[string]uint64
		currentVersionByID map[string]uint32
		pendingVersionByID map[string]uint32
		lastLogSeq         uint64
	)

	if idxStatus != nil {
		statusOK = true
		lastLogSeq = idxStatus.GetLastLogSequence()
		cursorByID = make(map[string]uint64)
		currentVersionByID = make(map[string]uint32)
		pendingVersionByID = make(map[string]uint32)

		for _, e := range idxStatus.GetIndexes() {
			if e.GetLedger() != ledgerName {
				continue
			}

			canonical := indexes.Canonical(e.GetIndex().GetId())
			cursorByID[canonical] = e.GetCursor()
			currentVersionByID[canonical] = e.GetCurrentVersion()
			pendingVersionByID[canonical] = e.GetPendingVersion()
		}
	}

	if handled, err := cmdutil.EncodeStructured(cmd, entries); handled || err != nil {
		return err
	}

	text.Println()
	text.Printf("Indexes for ledger: %s\n", pterm.Cyan(ledgerName))
	text.Println(pterm.Gray("─────────────────────────────────"))

	table := pterm.TableData{
		{"TYPE", "TARGET", "KEY", "STATUS"},
	}

	// Sort indexes by canonical id for stable output.
	sorted := append([]*commonpb.Index(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool {
		return indexes.Canonical(sorted[i].GetId()) < indexes.Canonical(sorted[j].GetId())
	})

	for _, idx := range sorted {
		typeName, target, key := describeIndex(idx.GetId())
		canonical := indexes.Canonical(idx.GetId())
		table = append(table, []string{
			typeName,
			target,
			key,
			indexStatusWithProgress(statusOK, currentVersionByID[canonical], pendingVersionByID[canonical], cursorByID, lastLogSeq, canonical),
		})
	}

	if len(table) == 1 {
		text.Println("No indexes configured.")
		text.Println(pterm.Gray("Hint: Create an index using:"))
		text.Println(pterm.FgCyan.Sprint("  ledgerctl indexes create --ledger " + ledgerName + " --type address"))

		return nil
	}

	return pterm.DefaultTable.WithWriter(cmd.OutOrStdout()).WithHasHeader().WithData(table).Render()
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
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS:
			return "destination-address", "-", "-"
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
			return "inserted-at", "-", "-"
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT:
			return "reverted-at", "-", "-"
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

// indexStatusWithProgress derives the per-replica status from
// IndexVersionState (current_version > 0 = READY locally, pending_version
// != 0 = rewrite in flight). When the IndexStatus RPC failed entirely
// (statusOK=false) we surface UNKNOWN rather than the misleading
// BUILDING the old code returned — pre-fix an RPC outage looked
// identical to a fresh-CreateIndex still-priming case.
func indexStatusWithProgress(statusOK bool, currentVersion, pendingVersion uint32, cursorByID map[string]uint64, lastLogSeq uint64, canonical string) string {
	if !statusOK {
		// GetIndexStatus failed — we have no signal at all. Don't
		// pretend BUILDING; that would let an operator misread an
		// RPC outage as a healthy still-priming index.
		return pterm.Gray("UNKNOWN")
	}

	switch {
	case currentVersion == 0 && pendingVersion == 0:
		// IndexStatus returned but no IndexVersionState entry for this
		// index — the local backfill hasn't primed it yet.
		return pterm.Yellow("BUILDING")
	case currentVersion == 0 && pendingVersion != 0:
		// Initial backfill in flight on this replica.
		if cursorByID == nil || lastLogSeq == 0 {
			return pterm.Yellow("BUILDING")
		}

		cursor, ok := cursorByID[canonical]
		if !ok {
			return pterm.Yellow("BUILDING (starting...)")
		}

		pct := cursor * 100 / lastLogSeq

		return pterm.Yellow(fmt.Sprintf("BUILDING (%d%%)", pct))
	case currentVersion != 0 && pendingVersion != 0:
		// Rewrite in flight; v_current keeps serving queries.
		return pterm.Cyan(fmt.Sprintf("READY (v%d, rewriting → v%d)", currentVersion, pendingVersion))
	default:
		return pterm.Green(fmt.Sprintf("READY (v%d)", currentVersion))
	}
}
