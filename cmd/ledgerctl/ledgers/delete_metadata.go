package ledgers

import (
	"errors"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewDeleteMetadataCommand creates the ledgers delete-metadata command.
func NewDeleteMetadataCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "delete-metadata [key]",
		Aliases: []string{"del-meta", "dm", "rm-meta"},
		Short:   "Delete metadata from a ledger",
		Long: `Delete a metadata key from a ledger via gRPC.

If --ledger is not provided and only one ledger exists, it will be used automatically.

Examples:
  ledgerctl ledgers delete-metadata region --ledger my-ledger
  ledgerctl ledgers dm environment --ledger my-ledger -y
  ledgerctl ledgers delete-metadata  # Interactive mode`,
		Args: cobra.MaximumNArgs(1),
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

	var key string
	if len(args) > 0 {
		key = args[0]
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

		return cmdutil.Displayed(errors.New("metadata key is required"))
	}

	yes, _ := cmd.Flags().GetBool("yes")
	if !yes {
		pterm.Println()
		pterm.Warning.Printfln("You are about to delete metadata key %q from ledger %q", key, ledgerName)

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

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Deleting metadata key %q from ledger %s...", key, ledgerName))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_DeleteLedgerMetadata{
				DeleteLedgerMetadata: &servicepb.DeleteLedgerMetadataRequest{
					Ledger: ledgerName,
					Key:    key,
				},
			},
		},
	}

	envelopes, err := cmdutil.BuildEnvelopes(cmd, requests)
	if err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{Envelopes: envelopes})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to delete ledger metadata", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		spinner.Fail("Response signature verification failed")

		return cmdutil.Displayed(fmt.Errorf("response signature verification failed: %w", err))
	}

	spinner.Success("Deleted")

	pterm.Println()
	pterm.Printf("Deleted key \"%s\" from ledger %s\n", pterm.Yellow(key), pterm.Cyan(ledgerName))

	return nil
}
