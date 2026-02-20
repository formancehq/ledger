package accounts

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewDeleteMetadataCommand creates the accounts delete-metadata command.
func NewDeleteMetadataCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "delete-metadata [address] [key]",
		Aliases: []string{"del-meta", "dm", "rm-meta"},
		Short:   "Delete metadata from an account",
		Long: `Delete a metadata key from an account via gRPC.

If --ledger is not provided and only one ledger exists, it will be used automatically.

Examples:
  ledgerctl accounts delete-metadata bank type --ledger my-ledger
  ledgerctl accounts delete-metadata users:alice role
  ledgerctl acc dm bank type`,
		Args: cobra.MaximumNArgs(2),
		RunE: runDeleteMetadata,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runDeleteMetadata(cmd *cobra.Command, args []string) error {
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

	var address string
	if len(args) > 0 {
		address = args[0]
	} else {
		address, err = pterm.DefaultInteractiveTextInput.
			WithDefaultText("Enter account address").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}
	}

	if address == "" {
		pterm.Error.Println("Account address is required")
		return fmt.Errorf("account address is required")
	}

	var key string
	if len(args) > 1 {
		key = args[1]
	} else {
		key, err = pterm.DefaultInteractiveTextInput.
			WithDefaultText("Enter metadata key to delete").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}
	}

	if key == "" {
		pterm.Error.Println("Metadata key is required")
		return fmt.Errorf("metadata key is required")
	}

	yes, _ := cmd.Flags().GetBool("yes")
	if !yes {
		pterm.Println()
		pterm.Warning.Printfln("You are about to delete metadata key %q from account %q", key, address)

		confirmed, err := pterm.DefaultInteractiveConfirm.
			WithDefaultText("Are you sure?").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read confirmation: %w", err)
		}
		if !confirmed {
			pterm.Info.Println("Deletion cancelled")
			return nil
		}
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Deleting metadata key %q from account %s...", key, address))

	req := &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{
			{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledgerName,
						Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
							DeleteMetadata: &commonpb.DeleteMetadataCommand{
								Target: &commonpb.Target{
									Target: &commonpb.Target_Account{
										Account: &commonpb.TargetAccount{Addr: address},
									},
								},
								Key: key,
							},
						},
					},
				},
			},
		},
	}

	if err := cmdutil.SignRequests(cmd, req.Requests); err != nil {
		spinner.Fail("Failed to sign request")
		return err
	}

	_, err = client.Apply(ctx, req)
	if err != nil {
		spinner.Fail("Failed to delete metadata")
		return cmdutil.FormatGRPCError("failed to delete metadata", err)
	}

	spinner.Success("Deleted")

	pterm.Println()
	pterm.Printf("Deleted key \"%s\" from account %s\n", pterm.Yellow(key), pterm.Cyan(address))

	return nil
}
