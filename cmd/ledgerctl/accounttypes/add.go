package accounttypes

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewAddCommand creates the account-types add command.
func NewAddCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add <name> <pattern>",
		Short: "Add a new account type",
		Long: `Add a new account type with a pattern to a ledger.

Patterns use colon-separated segments with optional variables:
  users:{id}:checking     — matches "users:alice:checking"
  banks:{iban:^[A-Z]{2}[0-9]{14}$}:main — regex-constrained variable

Examples:
  ledgerctl account-types add user-checking "users:{id}:checking" --ledger my-ledger
  ledgerctl at add bank-main "banks:{iban}:main" --ledger my-ledger`,
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runAdd,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger (required)")
	cmd.Flags().String("persistence", "normal", "Volume persistence mode: normal, ephemeral (purge on zero), transient (never persisted, must be zero at batch end)")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	_ = cmd.MarkFlagRequired("ledger")

	return cmd
}

func runAdd(cmd *cobra.Command, args []string) error {
	name := args[0]
	pattern := args[1]

	ledgerName, _ := cmd.Flags().GetString("ledger")
	persistence, _ := cmd.Flags().GetString("persistence")

	persistenceEnum, err := ParsePersistence(persistence)
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

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Adding account type %s...", name))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_AddAccountType{
				AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
					Ledger: ledgerName,
					AccountType: &commonpb.AccountType{
						Name:        name,
						Pattern:     pattern,
						Persistence: persistenceEnum,
					},
				},
			},
		},
	}

	applyReq, err := cmdutil.BuildApplyRequest(cmd, requests...)
	if err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	_, err = client.Apply(ctx, applyReq)
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to add account type", err)
	}

	spinner.Success("Added")

	pterm.Println()
	pterm.Printf("Name:      %s\n", pterm.Cyan(name))
	pterm.Printf("Pattern:   %s\n", pattern)
	pterm.Printf("Persistence: %s\n", FormatPersistence(persistenceEnum))
	pterm.Printf("Ledger:    %s\n", pterm.Gray(ledgerName))

	return nil
}
