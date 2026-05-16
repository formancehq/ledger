package ledgers

import (
	"errors"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewRemoveMetadataTypeCommand creates the ledgers remove-metadata-type command.
func NewRemoveMetadataTypeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove-metadata-type [flags]",
		Aliases: []string{"rm-type", "rmt"},
		Short:   "Remove a metadata field type from a ledger",
		Long: `Remove a typed metadata field declaration from a ledger.

After removal, the key will accept values of any type again.

Examples:
  ledgerctl ledgers remove-metadata-type --ledger my-ledger --target account --key age
  ledgerctl ledgers rmt --ledger my-ledger --target transaction --key priority -y
  ledgerctl ledgers rmt --ledger my-ledger --target ledger --key env -y
  ledgerctl ledgers remove-metadata-type  # Interactive mode`,
		Args: cobra.NoArgs,
		RunE: runRemoveMetadataType,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("target", "", "Target type: account, transaction, or ledger")
	cmd.Flags().String("key", "", "Metadata key name")
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runRemoveMetadataType(cmd *cobra.Command, _ []string) error {
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

	targetStr, _ := cmd.Flags().GetString("target")
	if targetStr == "" {
		result, err := pterm.DefaultInteractiveSelect.
			WithOptions(cmdutil.TargetTypeOptions()).
			WithDefaultText("Select target type").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		targetStr = result
	}

	targetType, err := cmdutil.ParseTargetType(targetStr)
	if err != nil {
		return err
	}

	key, _ := cmd.Flags().GetString("key")
	if key == "" {
		result, err := pterm.DefaultInteractiveTextInput.
			WithDefaultText("Enter metadata key name to remove").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		key = result
		if key == "" {
			return errors.New("metadata key is required")
		}
	}

	yes, _ := cmd.Flags().GetBool("yes")
	if !yes {
		pterm.Warning.Printfln("You are about to remove type declaration for %s.%s on ledger %s",
			cmdutil.TargetTypeString(targetType), key, ledgerName)

		confirmed, err := pterm.DefaultInteractiveConfirm.
			WithDefaultText(fmt.Sprintf("Remove type for '%s.%s'?", cmdutil.TargetTypeString(targetType), key)).
			WithDefaultValue(false).
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		if !confirmed {
			pterm.Info.Println("Removal cancelled.")

			return nil
		}
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Removing metadata type %s.%s from %s...",
		cmdutil.TargetTypeString(targetType), key, ledgerName))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_RemoveMetadataFieldType{
				RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{
					Ledger:     ledgerName,
					TargetType: targetType,
					Key:        key,
				},
			},
		},
	}

	if err := cmdutil.SignRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to remove metadata type", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		spinner.Fail("Response signature verification failed")

		return cmdutil.Displayed(fmt.Errorf("response signature verification failed: %w", err))
	}

	spinner.Success(fmt.Sprintf("Removed type for %s.%s on ledger %s",
		cmdutil.TargetTypeString(targetType), key, ledgerName))

	return nil
}
