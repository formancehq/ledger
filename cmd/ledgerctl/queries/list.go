package queries

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewListCommand creates the queries list command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: cmdutil.ListAliases,
		Short:   "List prepared queries for a ledger",
		Long: `List prepared queries configured on a ledger.

Prepared queries are stored in raft state per ledger and naturally bounded
in size; this endpoint is intentionally not paginated.`,
		RunE: runList,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	cmdutil.AddOutputFlags(cmd)

	return cmd
}

func runList(cmd *cobra.Command, _ []string) error {
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

	resp, err := client.ListPreparedQueries(ctx, &servicepb.ListPreparedQueriesRequest{
		Ledger: ledgerName,
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to list prepared queries", err)
	}

	if handled, err := cmdutil.EncodeStructured(cmd, resp.GetQueries()); handled || err != nil {
		return err
	}

	if len(resp.GetQueries()) == 0 {
		pterm.Info.Println("No prepared queries found.")

		return nil
	}

	tableData := pterm.TableData{
		{"NAME", "TARGET", "FILTER"},
	}

	for _, q := range resp.GetQueries() {
		tableData = append(tableData, []string{
			q.GetName(),
			formatTarget(q.GetTarget()),
			filterexpr.Format(q.GetFilter()),
		})
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

	return nil
}

func formatTarget(t commonpb.QueryTarget) string {
	switch t {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		return "accounts"
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		return "transactions"
	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		return "logs"
	default:
		return t.String()
	}
}
