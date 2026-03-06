package numscripts

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewDeleteCommand creates the numscripts delete command.
func NewDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "delete <name>",
		Aliases: []string{"rm", "remove"},
		Short:   "Delete a numscript from the library",
		Long: `Delete a numscript from the global library.

This removes the latest version pointer. Historical versions are preserved.

Examples:
  ledgerctl numscripts delete transfer`,
		Args: cobra.ExactArgs(1),
		RunE: runDelete,
	}

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

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Deleting numscript %s...", name))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_DeleteNumscript{
				DeleteNumscript: &servicepb.DeleteNumscriptRequest{
					Name: name,
				},
			},
		},
	}

	if err := cmdutil.SignRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	_, err = client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to delete numscript", err)
	}

	spinner.Success("Deleted")

	pterm.Println()
	pterm.Printf("Numscript: %s (deleted)\n", pterm.Gray(name))

	return nil
}
