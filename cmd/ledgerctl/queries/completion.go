package queries

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// completeQueryNames fetches prepared query names from the server for shell
// autocompletion. If --ledger is not set, it auto-detects when exactly one
// ledger exists (same logic as SelectLedger).
func completeQueryNames(cmd *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
	if len(args) > 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	// cobra does not run the root PersistentPreRunE during `__complete`, so the
	// connection flags still hold their defaults here. Resolve --profile/env
	// ourselves or we would query the default server instead of the one the
	// active profile points at.
	if err := cmdutil.ResolveConnectionFlags(cmd); err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	defer func() { _ = conn.Close() }()

	ledgerName, _ := cmd.Flags().GetString("ledger")
	if ledgerName == "" {
		ctx, cancel := cmdutil.GetContext(cmd)
		defer cancel()

		ledgers, listErr := cmdutil.GetAllLedgersInfo(ctx, client)
		if listErr != nil || len(ledgers) != 1 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}

		for name := range ledgers {
			ledgerName = name
		}
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	resp, err := client.ListPreparedQueries(ctx, &servicepb.ListPreparedQueriesRequest{
		Ledger: ledgerName,
	})
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	names := make([]string, 0, len(resp.GetQueries()))
	for _, q := range resp.GetQueries() {
		names = append(names, q.GetName())
	}

	return names, cobra.ShellCompDirectiveNoFileComp
}
