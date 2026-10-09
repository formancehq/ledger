package ledgers

import (
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// NewListCommand creates the ledgers list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: cmdutil.ListAliases,
		Short:   "List all ledgers",
		Long:    "List all ledgers in the cluster via gRPC streaming. Ledgers are bounded per cluster; --page-size and --cursor give finer-grained server-side pagination when needed.",

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

// RenderList writes ledgers in wire order and keeps cursor hints on stderr.
func RenderList(cmd *cobra.Command, all []*commonpb.LedgerInfo, cursors cmdutil.PageCursors) error {
	ledgers := make(map[string]*commonpb.LedgerInfo, len(all))
	for _, l := range all {
		ledgers[l.GetName()] = l
	}

	if handled, err := cmdutil.EncodeStructured(cmd, ledgers); handled || err != nil {
		cmdutil.EmitCursorHints(cmd, cursors)

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
		cmdutil.EmitCursorHints(cmd, cursors)

		return nil
	}

	tableData := pterm.TableData{
		{"NAME", "CREATED AT"},
	}

	for _, ledger := range all {
		createdAt := "-"
		if ledger.GetCreatedAt() != nil {
			createdAt = ledger.GetCreatedAt().AsTime().Format(time.RFC3339)
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

	cmdutil.EmitCursorHints(cmd, cursors)

	return nil
}
