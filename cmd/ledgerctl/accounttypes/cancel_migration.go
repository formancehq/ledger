package accounttypes

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewCancelMigrationCommand creates the account-types cancel-migration command.
func NewCancelMigrationCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cancel-migration <source-type>",
		Short: "Cancel an in-progress migration",
		Long: `Cancel an in-progress migration for a source account type.

The source type will transition back from MIGRATING to ACTIVE status.

Examples:
  ledgerctl account-types cancel-migration old-checking --ledger my-ledger
  ledgerctl at cancel-migration old-checking --ledger my-ledger`,
		Args: cobra.ExactArgs(1),
		RunE: runCancelMigration,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger (required)")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	_ = cmd.MarkFlagRequired("ledger")

	return cmd
}

func runCancelMigration(cmd *cobra.Command, args []string) error {
	sourceType := args[0]
	ledgerName, _ := cmd.Flags().GetString("ledger")

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Cancelling migration for %s...", sourceType))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_CancelMigration{
				CancelMigration: &servicepb.CancelMigrationLedgerRequest{
					Ledger:     ledgerName,
					SourceType: sourceType,
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

		return cmdutil.FormatGRPCError("failed to cancel migration", err)
	}

	spinner.Success("Migration cancelled")

	pterm.Println()
	pterm.Printf("Source type: %s (reverted to ACTIVE)\n", pterm.Cyan(sourceType))

	return nil
}
