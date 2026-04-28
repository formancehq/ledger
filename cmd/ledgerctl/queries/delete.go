package queries

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewDeleteCommand creates the queries delete command.
func NewDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "delete <name>",
		Aliases:           []string{"rm"},
		Short:             "Delete a prepared query",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeQueryNames,
		RunE:              runDelete,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runDelete(cmd *cobra.Command, args []string) error {
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

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	_, err = client.DeletePreparedQuery(ctx, &servicepb.DeletePreparedQueryRequest{
		Ledger: ledgerName,
		Name:   name,
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to delete prepared query", err)
	}

	pterm.Success.Printfln("Prepared query %q deleted", name)

	return nil
}
