package accounttypes

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewRemoveCommand creates the account-types remove command.
func NewRemoveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove <name>",
		Aliases: []string{"rm", "delete"},
		Short:   "Remove an account type",
		Long: `Remove an account type from a ledger.

The account type must be in DEPRECATED status or have no matching accounts.

Examples:
  ledgerctl account-types remove old-type --ledger my-ledger
  ledgerctl at rm deprecated-type --ledger my-ledger`,
		Args: cobra.ExactArgs(1),
		RunE: runRemove,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger (required)")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	_ = cmd.MarkFlagRequired("ledger")

	return cmd
}

func runRemove(cmd *cobra.Command, args []string) error {
	typeName := args[0]
	ledgerName, _ := cmd.Flags().GetString("ledger")

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Removing account type %s...", typeName))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_RemoveAccountType{
				RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{
					Ledger: ledgerName,
					Name:   typeName,
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

		return cmdutil.FormatGRPCError("failed to remove account type", err)
	}

	spinner.Success("Removed")

	pterm.Println()
	pterm.Printf("Account type: %s (removed)\n", pterm.Gray(typeName))

	return nil
}
