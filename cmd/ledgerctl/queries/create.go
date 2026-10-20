package queries

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

// NewCreateCommand creates the queries create command.
func NewCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a prepared query",
		Long: `Create a named prepared query for a ledger.

--filter accepts the textual grammar only (not the JSON DSL), so JSON-only
fields (transaction reference, reverted, id, insertedAt, revertedAt; log logId)
cannot be used here; "queries update" accepts both formats. Valid fields per
--target:
  accounts:      metadata[key], address, has asset BASE[/PRECISION]
  transactions:  metadata[key], address, source, destination, timestamp
  logs:          date, ledger
Transaction amounts are not filterable.

Examples:
  ledgerctl queries create active-users --ledger my-ledger --target accounts --filter "metadata[active] == true"
  ledgerctl queries create pending-payouts --ledger my-ledger --target transactions --filter 'source ^= "payouts:" and metadata[status] == pending'
  ledgerctl queries create recent-logs --ledger my-ledger --target logs --filter 'date >= "2026-01-01T00:00:00Z"'`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runCreate,
	}

	cmdutil.AddLedgerFlag(cmd)
	cmd.Flags().String("target", "accounts", "Query target: accounts, transactions, or logs")
	cmdutil.RegisterEnumCompletion(cmd, "target", "accounts", "transactions", "logs")
	cmd.Flags().String("filter", "", `Filter expression, textual grammar only (e.g. "metadata[k] == v")`)
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

	ledgerName, _ := cmd.Flags().GetString(cmdutil.LedgerFlagName)

	targetStr, _ := cmd.Flags().GetString("target")
	filterExpr, _ := cmd.Flags().GetString("filter")

	target, err := parseTarget(targetStr)
	if err != nil {
		return err
	}

	var filter *commonpb.QueryFilter
	if filterExpr != "" {
		filter, err = filterexpr.Parse(filterExpr, target)
		if err != nil {
			return fmt.Errorf("invalid filter expression: %w", err)
		}
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	applyReq, err := cmdutil.BuildApplyRequest(cmd, actions.CreatePreparedQueryAction(name, ledgerName, target, filter))
	if err != nil {
		return cmdutil.Displayed(err)
	}

	resp, err := client.Apply(ctx, applyReq)
	if err != nil {
		return cmdutil.FormatGRPCError("failed to create prepared query", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		return fmt.Errorf("response signature verification failed: %w", err)
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
