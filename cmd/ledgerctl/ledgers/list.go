package ledgers

import (
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewListCommand creates the ledgers list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "list",
		Aliases:           cmdutil.ListAliases,
		Short:             "List all ledgers",
		Long:              "List all ledgers in the cluster via gRPC streaming. Ledgers are bounded per cluster; --page-size and --cursor give finer-grained server-side pagination when needed.",
		RunE:              runList,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmdutil.AddPaginationFlags(cmd, cmdutil.PaginationOptions{
		// Most clusters have a handful of ledgers; default to the server-side
		// max page (100) so `ledgers list` keeps showing everything by default.
		DefaultPageSize: 100,
		SupportsReverse: true,
	})
	cmdutil.AddConsistencyFlags(cmd)
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runList(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	pgn := cmdutil.GetPaginationFlags(cmd)
	cns := cmdutil.GetConsistencyFlags(cmd)

	spinner, _ := pterm.DefaultSpinner.Start("Fetching ledgers...")

	all, nextCursor, err := cmdutil.FetchSinglePageOrAll(cmd, pgn.Cursor, func(cur string) ([]*commonpb.LedgerInfo, metadata.MD, error) {
		page := pgn
		page.Cursor = cur

		stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{
			Options: cmdutil.BuildListOptions(page, cns, nil),
		})
		if err != nil {
			return nil, nil, cmdutil.FormatGRPCError("failed to list ledgers", err)
		}

		items, recvErr := cmdutil.CollectStream(stream)
		if recvErr != nil {
			return nil, nil, cmdutil.FormatGRPCError("failed to receive ledger", recvErr)
		}

		return items, stream.Trailer(), nil
	})
	if err != nil {
		_ = spinner.Stop()

		return err
	}

	_ = spinner.Stop()

	ledgers := make(map[string]*commonpb.LedgerInfo, len(all))
	for _, l := range all {
		ledgers[l.GetName()] = l
	}

	if handled, err := cmdutil.EncodeStructured(cmd, ledgers); handled || err != nil {
		cmdutil.EmitNextCursorHint(cmd, nextCursor)

		return err
	}

	// Server is the source of truth for ordering: the raft-state scan is
	// already in name order (forward) or reversed, then clipped on the
	// last-sent key. Re-sorting locally undoes the clip in reverse mode by
	// silently dropping "items that look adjacent past the cursor". Iterate
	// `all` in wire order instead.

	if len(all) == 0 {
		pterm.Info.Println("No ledgers found.")
		pterm.Println(pterm.Gray("Create one with: ledgerctl ledgers create --name <name>"))

		return nil
	}

	tableData := pterm.TableData{
		{"NAME", "CREATED AT"},
	}

	for _, ledger := range all {
		createdAt := "-"
		if ledger.GetCreatedAt() != 0 {
			createdAt = ledger.CreatedAtTs().AsTime().Format(time.RFC3339)
		}

		tableData = append(tableData, []string{
			ledger.GetName(),
			createdAt,
		})
	}

	pterm.Println()

	if err := pterm.DefaultTable.WithHasHeader().WithData(tableData).Render(); err != nil {
		return err
	}

	pterm.Println()

	cmdutil.EmitNextCursorHint(cmd, nextCursor)

	return nil
}
