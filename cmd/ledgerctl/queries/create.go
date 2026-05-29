package queries

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewCreateCommand creates the queries create command.
func NewCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a prepared query",
		Long: `Create a named prepared query for a ledger.

Examples:
  ledgerctl queries create active-users --ledger my-ledger --target accounts --filter "metadata[active] == true"
  ledgerctl queries create big-txns --ledger my-ledger --target transactions --filter "amount > 1000"`,
		Args: cobra.ExactArgs(1),
		RunE: runCreate,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("target", "accounts", "Query target: accounts, transactions, or logs")
	cmd.Flags().String("filter", "", "Filter expression")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runCreate(cmd *cobra.Command, args []string) error {
	name := args[0]

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

	targetStr, _ := cmd.Flags().GetString("target")
	filterExpr, _ := cmd.Flags().GetString("filter")

	target, err := parseTarget(targetStr)
	if err != nil {
		return err
	}

	var filter *commonpb.QueryFilter
	if filterExpr != "" {
		filter, err = filterexpr.Parse(filterExpr)
		if err != nil {
			return fmt.Errorf("invalid filter expression: %w", err)
		}
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	_, err = client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
		Query: &commonpb.PreparedQuery{
			Name:   name,
			Ledger: ledgerName,
			Filter: filter,
			Target: target,
		},
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to create prepared query", err)
	}

	pterm.Success.Printfln("Prepared query %q created for ledger %q", name, ledgerName)

	return nil
}

func parseTarget(s string) (commonpb.QueryTarget, error) {
	switch s {
	case "accounts", "account":
		return commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil
	case "transactions", "transaction", "txn":
		return commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, nil
	case "logs", "log":
		return commonpb.QueryTarget_QUERY_TARGET_LOGS, nil
	default:
		return 0, fmt.Errorf("unknown target %q (use accounts, transactions, or logs)", s)
	}
}
