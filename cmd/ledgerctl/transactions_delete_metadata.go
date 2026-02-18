package main

import (
	"fmt"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newTransactionsDeleteMetadataCommand creates the transactions delete-metadata command.
func newTransactionsDeleteMetadataCommand() *cobra.Command {
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
		RunE: runTransactionsDeleteMetadata,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runTransactionsDeleteMetadata(cmd *cobra.Command, args []string) error {
	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	// Get ledger name (from flag or interactive selection)
	ledgerFlag, _ := cmd.Flags().GetString("ledger")
	ledgerName, err := selectLedger(cmd, client, ledgerFlag)
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
			return fmt.Errorf("invalid transaction ID: %w", err)
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
			return fmt.Errorf("invalid transaction ID: %w", err)
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
		return fmt.Errorf("metadata key is required")
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

	ctx, cancel := getContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Deleting metadata key %q from transaction #%d...", key, txID))

	// Build request
	req := &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{
			{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledgerName,
						Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
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
	}

	if err := signRequests(cmd, req.Requests); err != nil {
		spinner.Fail("Failed to sign request")
		return err
	}

	_, err = client.Apply(ctx, req)
	if err != nil {
		spinner.Fail("Failed to delete metadata")
		return formatGRPCError("failed to delete metadata", err)
	}

	spinner.Success("Deleted")

	pterm.Println()
	pterm.Printf("Deleted key \"%s\" from transaction #%d\n", pterm.Yellow(key), txID)

	return nil
}
