package queries

import (
	"errors"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewUpdateCommand creates the queries update command.
func NewUpdateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <name>",
		Short: "Update a prepared query filter",
		Long: `Update the filter of an existing prepared query.

Examples:
  ledgerctl queries update active-users --ledger my-ledger --filter "metadata[active] == true and metadata[tier] == gold"`,
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

	filterExpr, _ := cmd.Flags().GetString("filter")

	// An update replaces the stored filter, so an empty --filter would decode to a
	// nil filter and silently erase the prepared query's filter on the server
	// (ValidateFilterForTarget accepts nil). Require a non-empty filter, matching
	// the HTTP update handler's guard (handlers_update_prepared_query.go).
	if filterExpr == "" {
		return errors.New("--filter is required")
	}

	// The update carries only the new filter, not the target — the target is
	// immutable and lives on the stored prepared query (the FSM re-validates the
	// filter against it). DecodeDualFormatStructuralOnly is the shared "target not
	// known here" entry point: it resolves bare fields with a non-audit target
	// (prepared queries are never audit) and defers the per-target validity gate
	// to the server (EN-1549).
	filter, err := filterexpr.DecodeDualFormatStructuralOnly([]byte(filterExpr), commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
	if err != nil {
		return fmt.Errorf("invalid filter expression: %w", err)
	}

	// A structurally-valid but conditionless filter (e.g. `""` quoted, whitespace)
	// also decodes to nil; reject it for the same reason.
	if filter == nil {
		return errors.New("--filter must contain at least one condition")
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	_, err = client.UpdatePreparedQuery(ctx, &servicepb.UpdatePreparedQueryRequest{
		Ledger: ledgerName,
		Name:   name,
		Filter: filter,
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to update prepared query", err)
	}

	pterm.Success.Printfln("Prepared query %q updated", name)

	return nil
}
