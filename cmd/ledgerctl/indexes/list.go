package indexes

import (
	"errors"
	"fmt"
	"io"
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

	stream, err := client.ListIndexes(ctx, &servicepb.ListIndexesRequest{
		Scope:  servicepb.ListIndexesRequest_SCOPE_LEDGER,
		Ledger: ledgerName,
	})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to list indexes", err)
	}

	var entries []*commonpb.Index

	for {
		idx, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}

		if recvErr != nil {
			_ = spinner.Stop()

			return cmdutil.FormatGRPCError("streaming ListIndexes", recvErr)
		}

		entries = append(entries, idx)
	}

	// EN-1323: BuildStatus is informational only; the per-replica
	// readiness signal lives in IndexEntry.current_version (>0 ⇒ local
	// atomic switch has fired). Fetch IndexStatus unconditionally so
	// the status column reflects the local replica's view rather than
	// the never-updated FSM-side BUILDING flag.
	//
	// statusOK distinguishes "RPC succeeded, got real data" from "RPC
	// failed, no version info available" — the renderer uses it to
	// show UNKNOWN instead of falsely reporting BUILDING when we
	// genuinely have no signal.
	var (
		statusOK           bool
		cursorByID         map[string]uint64
		currentVersionByID map[string]uint32
		pendingVersionByID map[string]uint32
		lastLogSeq         uint64
	)

	idxStatus, statusErr := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: ledgerName})
	if statusErr != nil {
		spinner.Fail(fmt.Sprintf("Failed to fetch index status: %v", statusErr))
	} else {
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

	_ = spinner.Stop()

	if handled, err := cmdutil.EncodeStructured(cmd, entries); handled || err != nil {
		return err
	}

	pterm.Println()
	pterm.Printf("Indexes for ledger: %s\n", pterm.Cyan(ledgerName))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

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
