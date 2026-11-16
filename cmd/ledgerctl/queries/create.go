package queries

import (
	"errors"
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

--filter accepts the textual grammar or the JSON QueryFilter DSL; both forms
cover the same fields. Valid fields per --target:
  accounts:      metadata[key], address, has asset BASE[/PRECISION]
  transactions:  metadata[key], address, source, destination, reference,
                 reverted, id, timestamp, insertedAt, revertedAt
  logs:          id, date, ledger (legacy logId ranges remain accepted)
Transaction amounts are not filterable.

Examples:
  ledgerctl queries create active-users --ledger my-ledger --target accounts --filter "metadata[active] == true"
  ledgerctl queries create pending-payouts --ledger my-ledger --target transactions --filter 'source ^= "payouts:" and metadata[status] == pending'
  ledgerctl queries create by-ref --ledger my-ledger --target transactions --filter 'reference == $ref'
  ledgerctl queries create by-ref-json --ledger my-ledger --target transactions --filter '{"$match":{"reference":{"$param":"ref"}}}'
  ledgerctl queries create recent-logs --ledger my-ledger --target logs --filter 'date >= "2026-01-01T00:00:00Z"'`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runCreate,
	}

	cmdutil.AddLedgerFlag(cmd)
	cmd.Flags().String("target", "accounts", "Query target: accounts, transactions, or logs")
	cmdutil.RegisterEnumCompletion(cmd, "target", "accounts", "transactions", "logs")
	cmd.Flags().String("filter", "", `Filter expression: textual grammar ("metadata[k] == v") or JSON DSL ('{"$match":{"metadata[k]":"v"}}')`)
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

	filter, err := parseCreateFilter(filterExpr, cmd.Flags().Changed("filter"), target)
	if err != nil {
		return err
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

// parseCreateFilter decodes the --filter value in either form (textual grammar
// or JSON DSL), resolving bare fields against the query's target. Omitting
// --filter creates an unfiltered query; a --filter that is set but decodes to
// no condition (empty or whitespace-only) is refused rather than silently
// widened to match-all, mirroring `queries update`. Like the list commands, it
// leaves the per-target validity gate to the server, which applies it on
// create.
func parseCreateFilter(expr string, set bool, target commonpb.QueryTarget) (*commonpb.QueryFilter, error) {
	if !set {
		return nil, nil
	}

	filter, err := filterexpr.DecodeDualFormatStructuralOnly([]byte(expr), target)
	if err != nil {
		return nil, fmt.Errorf("invalid filter expression: %w", err)
	}

	if filter == nil {
		return nil, errors.New("--filter must contain at least one condition; omit the flag to create an unfiltered query")
	}

	return filter, nil
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
