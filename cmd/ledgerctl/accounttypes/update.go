package accounttypes

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewUpdateCommand creates the account-types update command.
func NewUpdateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <name>",
		Short: "Update an account type's enforcement mode",
		Long: `Update the enforcement mode of an existing account type.

Examples:
  ledgerctl account-types update user-checking --ledger my-ledger --enforcement-mode AUDIT
  ledgerctl at update bank-main --ledger my-ledger --enforcement-mode STRICT`,
		Args: cobra.ExactArgs(1),
		RunE: runUpdate,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger (required)")
	cmd.Flags().String("enforcement-mode", "", "New enforcement mode: STRICT or AUDIT (required)")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	_ = cmd.MarkFlagRequired("ledger")
	_ = cmd.MarkFlagRequired("enforcement-mode")

	return cmd
}

func runUpdate(cmd *cobra.Command, args []string) error {
	typeName := args[0]

	ledgerName, _ := cmd.Flags().GetString("ledger")
	modeStr, _ := cmd.Flags().GetString("enforcement-mode")

	mode, err := parseEnforcementMode(modeStr)
	if err != nil {
		return err
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Updating account type %s...", typeName))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_UpdateAccountType{
				UpdateAccountType: &servicepb.UpdateAccountTypeLedgerRequest{
					Ledger:          ledgerName,
					Name:            typeName,
					EnforcementMode: mode,
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

		return cmdutil.FormatGRPCError("failed to update account type", err)
	}

	spinner.Success("Updated")

	pterm.Println()
	pterm.Printf("Name:        %s\n", pterm.Cyan(typeName))
	pterm.Printf("Enforcement: %s\n", modeStr)

	return nil
}
