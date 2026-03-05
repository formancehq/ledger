package accounttypes

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewMigrateCommand creates the account-types migrate command.
func NewMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate <source-type> <target-type>",
		Short: "Start migrating accounts from one type to another",
		Long: `Start a migration from a source account type to a target account type.

The source type will transition to MIGRATING status and a background worker
will rewrite account keys from the old pattern to the new pattern.

Use --dry-run to preview the migration without applying changes.

Examples:
  ledgerctl account-types migrate old-checking new-checking --ledger my-ledger
  ledgerctl at migrate old-checking new-checking --ledger my-ledger --dry-run`,
		Args: cobra.ExactArgs(2),
		RunE: runMigrate,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger (required)")
	cmd.Flags().Bool("dry-run", false, "Preview the migration without applying changes")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	_ = cmd.MarkFlagRequired("ledger")

	return cmd
}

func runMigrate(cmd *cobra.Command, args []string) error {
	sourceType := args[0]
	targetType := args[1]

	ledgerName, _ := cmd.Flags().GetString("ledger")
	dryRun, _ := cmd.Flags().GetBool("dry-run")

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	action := "Starting migration"
	if dryRun {
		action = "Dry-run migration"
	}
	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("%s from %s to %s...", action, sourceType, targetType))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_MigrateAccountType{
				MigrateAccountType: &servicepb.MigrateAccountTypeLedgerRequest{
					Ledger:     ledgerName,
					SourceType: sourceType,
					TargetType: targetType,
					DryRun:     dryRun,
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

	if dryRun {
		spinner.Success("Dry-run completed (no changes applied)")
	} else {
		spinner.Success("Migration started")
	}

	pterm.Println()
	pterm.Printf("Source: %s\n", pterm.Cyan(sourceType))
	pterm.Printf("Target: %s\n", pterm.Cyan(targetType))
	pterm.Printf("Ledger: %s\n", pterm.Gray(ledgerName))

	if !dryRun {
		pterm.Println()
		pterm.Println(pterm.Gray("The migration is running in the background."))
		pterm.Println(pterm.Gray("Use 'ledgerctl account-types get' to check progress."))
	}

	return nil
}
