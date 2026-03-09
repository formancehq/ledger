package accounttypes

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewGetCommand creates the account-types get command.
func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Get details of an account type",
		Long: `Get detailed information about a specific account type.

If --ledger is not provided and only one ledger exists, it will be used automatically.

Examples:
  ledgerctl account-types get user-checking --ledger my-ledger
  ledgerctl at get bank-main`,
		Args: cobra.ExactArgs(1),
		RunE: runGet,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runGet(cmd *cobra.Command, args []string) error {
	typeName := args[0]

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

	ledgers, err := cmdutil.GetAllLedgersInfo(ctx, client)
	if err != nil {
		return cmdutil.FormatGRPCError("failed to list ledgers", err)
	}

	info, ok := ledgers[ledgerName]
	if !ok {
		return fmt.Errorf("ledger %q not found", ledgerName)
	}

	at, exists := info.AccountTypes[typeName]
	if !exists {
		return fmt.Errorf("account type %q not found on ledger %q", typeName, ledgerName)
	}

	pterm.Printf("Name:        %s\n", pterm.Cyan(at.Name))
	pterm.Printf("Pattern:     %s\n", at.Pattern)
	pterm.Printf("Status:      %s\n", formatStatus(at.Status))
	pterm.Printf("Enforcement: %s\n", formatEnforcementMode(at.EnforcementMode))

	if at.SupersededBy != "" {
		pterm.Printf("Superseded:  %s\n", at.SupersededBy)
	}

	if at.MigrationProgress != nil {
		pterm.Println()
		pterm.DefaultSection.Println("Migration Progress")
		pterm.Printf("Total:    %d\n", at.MigrationProgress.TotalAccounts)
		pterm.Printf("Migrated: %d\n", at.MigrationProgress.MigratedAccounts)
		if at.MigrationProgress.StartedAt != nil {
			pterm.Printf("Started:  %s\n", at.MigrationProgress.StartedAt.AsTime().Format("2006-01-02T15:04:05Z07:00"))
		}
		if at.MigrationProgress.CompletedAt != nil {
			pterm.Printf("Completed: %s\n", at.MigrationProgress.CompletedAt.AsTime().Format("2006-01-02T15:04:05Z07:00"))
		}
	}

	return nil
}
