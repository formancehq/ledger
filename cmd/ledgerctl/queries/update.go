package queries

import (
	"errors"
	"fmt"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

// NewUpdateCommand creates the queries update command.
func NewUpdateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <name>",
		Short: "Update a prepared query target and filter",
		Long: `Replace the target and filter of an existing prepared query. Omit --filter to clear
the existing filter and make the query match all entities in the selected target.

--filter accepts the textual grammar or the JSON DSL; both forms cover the same
fields. The fields valid for the query's target are listed in
"ledgerctl queries create --help".

Examples:
  ledgerctl queries update active-users --ledger my-ledger --target accounts --filter "metadata[active] == true and metadata[tier] == gold"
  ledgerctl queries update order-lookup --ledger my-ledger --target transactions --filter 'reference == "order-42"'
  ledgerctl queries update log-lookup --ledger my-ledger --target logs --filter 'id == $wanted'
  ledgerctl queries update active-users --ledger my-ledger --target accounts`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeQueryNames,
		RunE:              runUpdate,
	}

	cmdutil.AddLedgerFlag(cmd)
	cmd.Flags().String("target", "", "Query target: accounts, transactions, or logs (required)")
	cmdutil.RegisterEnumCompletion(cmd, "target", "accounts", "transactions", "logs")
	cmd.Flags().String("filter", "", `New filter expression: textual grammar ("metadata[k] == v") or JSON DSL ('{"$match":{"metadata[k]":"v"}}')`)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runUpdate(cmd *cobra.Command, args []string) error {
	name := args[0]
	targetStr, _ := cmd.Flags().GetString("target")
	if targetStr == "" {
		return errors.New("--target is required")
	}
	target, err := parseTarget(targetStr)
	if err != nil {
		return err
	}
	filterExpr, _ := cmd.Flags().GetString("filter")
	filterSet := cmd.Flags().Changed("filter")
	if filterSet && strings.TrimSpace(filterExpr) == "" {
		return errors.New("--filter must contain at least one condition; omit the flag to clear the filter")
	}

	var filter *commonpb.QueryFilter
	if filterSet {
		filter, err = filterexpr.DecodeDualFormatStructuralOnly([]byte(filterExpr), target)
		if err != nil {
			return fmt.Errorf("invalid filter expression: %w", err)
		}

		if filter == nil {
			return errors.New("--filter must contain at least one condition")
		}
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ledgerName, _ := cmd.Flags().GetString(cmdutil.LedgerFlagName)

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()
	applyReq, err := cmdutil.BuildApplyRequest(cmd, actions.UpdatePreparedQueryForTargetAction(ledgerName, name, target, filter))
	if err != nil {
		return cmdutil.Displayed(err)
	}

	resp, err := client.Apply(ctx, applyReq)
	if err != nil {
		return cmdutil.FormatGRPCError("failed to update prepared query", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		return fmt.Errorf("response signature verification failed: %w", err)
	}

	pterm.Success.Printfln("Prepared query %q updated", name)

	return nil
}
