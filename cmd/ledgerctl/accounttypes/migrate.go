package accounttypes

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewMigrateCommand creates the account-types migrate command.
func NewMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate <name> <target-pattern>",
		Short: "Migrate an account type to a new pattern",
		Long: `Start a background migration of an account type to a new pattern.

During migration, only the new pattern is accepted for validation.
The old pattern is treated as deprecated until migration completes.

Examples:
  ledgerctl account-types migrate user-checking "usr:{id}:checking" --ledger my-ledger
  ledgerctl at migrate bank-main "b:{iban}:main" --ledger my-ledger`,
		Args: cobra.ExactArgs(2),
		RunE: runMigrate,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger (required)")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	_ = cmd.MarkFlagRequired("ledger")

	return cmd
}

func runMigrate(cmd *cobra.Command, args []string) error {
	name := args[0]
	targetPattern := args[1]

	ledgerName, _ := cmd.Flags().GetString("ledger")

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Starting migration for %s...", name))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_MigrateAccountType{
				MigrateAccountType: &servicepb.MigrateAccountTypeLedgerRequest{
					Ledger:        ledgerName,
					Name:          name,
					TargetPattern: targetPattern,
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

		return cmdutil.FormatGRPCError("failed to start migration", err)
	}

	spinner.Success("Migration started")

	pterm.Println()
	pterm.Printf("Name:           %s\n", pterm.Cyan(name))
	pterm.Printf("Target Pattern: %s\n", targetPattern)
	pterm.Printf("Ledger:         %s\n", pterm.Gray(ledgerName))

	return nil
}
