package queries

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// completeQueryNames fetches prepared query names from the server for shell
// autocompletion. Query names are per-ledger, so nothing is suggested until
// --ledger is set.
func completeQueryNames(cmd *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
	if len(args) > 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	ledgerName, _ := cmd.Flags().GetString(cmdutil.LedgerFlagName)
	if ledgerName == "" {
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
