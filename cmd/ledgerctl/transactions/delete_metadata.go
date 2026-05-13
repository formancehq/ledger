package transactions

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewDeleteMetadataCommand creates the transactions delete-metadata command.
func NewDeleteMetadataCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "delete-metadata [transaction-id] [key]",
		Aliases: []string{"del-meta", "dm", "rm-meta"},
		Short:   "Delete metadata from a transaction",
		Long: `Delete a metadata key from a transaction via gRPC.

If --ledger is not provided and only one ledger exists, it will be used automatically.

Examples:
  ledgerctl transactions delete-metadata 42 status --ledger my-ledger
  ledgerctl transactions delete-metadata 42 reason
  ledgerctl tx dm 42 status`,
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

	// Get ledger name (from flag or interactive selection)
	ledgerFlag, _ := cmd.Flags().GetString("ledger")

	ledgerName, err := cmdutil.SelectLedger(cmd, client, ledgerFlag)
	if err != nil {
		return err
	}

	// Get transaction ID
	var txID uint64

	if len(args) > 0 {
		var err error

		txID, err = strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			pterm.Error.Printfln("Invalid transaction ID: %v", err)

			return cmdutil.Displayed(fmt.Errorf("invalid transaction ID: %w", err))
		}
	} else {
		input, err := pterm.DefaultInteractiveTextInput.
			WithDefaultText("Enter transaction ID").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		txID, err = strconv.ParseUint(input, 10, 64)
		if err != nil {
			pterm.Error.Printfln("Invalid transaction ID: %v", err)

			return cmdutil.Displayed(fmt.Errorf("invalid transaction ID: %w", err))
		}
	}

	// Get metadata key
	var key string
	if len(args) > 1 {
		key = args[1]
	} else {
		var err error

		key, err = pterm.DefaultInteractiveTextInput.
			WithDefaultText("Enter metadata key to delete").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}
	}

	if key == "" {
		pterm.Error.Println("Metadata key is required")

		return cmdutil.Displayed(errors.New("metadata key is required"))
	}

	// Confirmation prompt
	yes, _ := cmd.Flags().GetBool("yes")
	if !yes {
		pterm.Println()
		pterm.Warning.Printfln("You are about to delete metadata key %q from transaction #%d", key, txID)

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

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Deleting metadata key %q from transaction #%d...", key, txID))

	// Build request
	req := &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{
			{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledgerName,
						Action: &servicepb.LedgerAction{
							Data: &servicepb.LedgerAction_DeleteMetadata{
								DeleteMetadata: &commonpb.DeleteMetadataCommand{
									Target: &commonpb.Target{
										Target: &commonpb.Target_Transaction{
											Transaction: &commonpb.TargetTransaction{Id: txID},
										},
									},
									Key: key,
								},
							},
						},
					},
				},
			},
		},
	}

	if err := cmdutil.SignRequests(cmd, req.GetRequests()); err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	_, err = client.Apply(ctx, req)
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to delete metadata", err)
	}

	spinner.Success("Deleted")

	pterm.Println()
	pterm.Printf("Deleted key \"%s\" from transaction #%d\n", pterm.Yellow(key), txID)

	return nil
}
