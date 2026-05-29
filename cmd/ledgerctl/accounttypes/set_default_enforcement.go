package accounttypes

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewSetDefaultEnforcementCommand creates the account-types set-default-enforcement command.
func NewSetDefaultEnforcementCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-default-enforcement",
		Short: "Set the default enforcement mode for unmatched accounts",
		Long: `Set the ledger-level default enforcement mode that controls what happens
when an account address doesn't match any defined account type.

STRICT (default): reject transactions with unmatched accounts.
AUDIT: allow transactions but log a warning.

Examples:
  ledgerctl account-types set-default-enforcement --ledger my-ledger --mode STRICT
  ledgerctl at set-default-enforcement --ledger my-ledger --mode AUDIT`,
		Args: cobra.NoArgs,
		RunE: runSetDefaultEnforcement,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger (required)")
	cmd.Flags().String("mode", "", "Enforcement mode: STRICT or AUDIT (required)")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	_ = cmd.MarkFlagRequired("ledger")
	_ = cmd.MarkFlagRequired("mode")

	return cmd
}

func runSetDefaultEnforcement(cmd *cobra.Command, _ []string) error {
	ledgerName, _ := cmd.Flags().GetString("ledger")
	modeStr, _ := cmd.Flags().GetString("mode")

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

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Setting default enforcement mode to %s...", modeStr))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_SetDefaultEnforcementMode{
				SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{
					Ledger:          ledgerName,
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

		return cmdutil.FormatGRPCError("failed to set default enforcement mode", err)
	}

	spinner.Success("Updated")

	pterm.Println()
	pterm.Printf("Ledger:      %s\n", pterm.Gray(ledgerName))
	pterm.Printf("Default enforcement mode: %s\n", pterm.Cyan(modeStr))

	return nil
}
