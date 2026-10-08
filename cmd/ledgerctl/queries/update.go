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
		Short: "Update a prepared query filter",
		Long: `Update the filter of an existing prepared query. Omit --filter to clear
the existing filter and make the query match all entities in its target.

Examples:
	  ledgerctl queries update active-users --ledger my-ledger --filter "metadata[active] == true and metadata[tier] == gold"
	  ledgerctl queries update active-users --ledger my-ledger`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeQueryNames,
		RunE:              runUpdate,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("filter", "", "New filter expression")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runUpdate(cmd *cobra.Command, args []string) error {
	name := args[0]
	filterExpr, _ := cmd.Flags().GetString("filter")
	filterSet := cmd.Flags().Changed("filter")
	if filterSet && strings.TrimSpace(filterExpr) == "" {
		return errors.New("--filter must contain at least one condition; omit the flag to clear the filter")
	}

	var filter *commonpb.QueryFilter
	var err error
	if filterSet {
		filter, err = filterexpr.DecodeDualFormatStructuralOnly([]byte(filterExpr), commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
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

	ledgerFlag, _ := cmd.Flags().GetString("ledger")

	ledgerName, err := cmdutil.SelectLedger(cmd, client, ledgerFlag)
	if err != nil {
		return err
	}

	// The update carries only the new filter, not the target — the target is
	// immutable and lives on the stored prepared query (the FSM re-validates the
	// filter against it). DecodeDualFormatStructuralOnly is the shared "target not
	// known here" entry point: it resolves bare fields with a non-audit target
	// (prepared queries are never audit) and defers the per-target validity gate
	// to the server (EN-1549).
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	applyReq, err := cmdutil.BuildApplyRequest(cmd, actions.UpdatePreparedQueryAction(ledgerName, name, filter))
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
