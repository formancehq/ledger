package ledgers

import (
	"errors"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewSetMetadataTypeCommand creates the ledgers set-metadata-type command.
func NewSetMetadataTypeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "set-metadata-type [flags]",
		Aliases: []string{"set-type", "smt"},
		Short:   "Set a metadata field type on a ledger",
		Long: `Declare a typed metadata field on a ledger.

Once set, all new metadata values for this key must conform to the declared type.
Existing untyped values will be converted in the background.

Examples:
  ledgerctl ledgers set-metadata-type --ledger my-ledger --target account --key age --type int64
  ledgerctl ledgers smt --ledger my-ledger --target transaction --key priority --type uint64
  ledgerctl ledgers smt --ledger my-ledger --target ledger --key env --type string
  ledgerctl ledgers set-metadata-type  # Interactive mode`,
		Args: cobra.NoArgs,
		RunE: runSetMetadataType,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("target", "", "Target type: account, transaction, or ledger")
	cmd.Flags().String("key", "", "Metadata key name")
	cmd.Flags().String("type", "", "Metadata type: string, int64, bool, uint64, int8, int16, int32, uint8, uint16, uint32")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runSetMetadataType(cmd *cobra.Command, _ []string) error {
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
			WithDefaultText("Enter metadata key name").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		key = result
		if key == "" {
			return errors.New("metadata key is required")
		}
	}

	typeStr, _ := cmd.Flags().GetString("type")
	if typeStr == "" {
		result, err := pterm.DefaultInteractiveSelect.
			WithOptions(cmdutil.MetadataTypeOptions()).
			WithDefaultText("Select metadata type").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		typeStr = result
	}

	mdType, err := cmdutil.ParseMetadataType(typeStr)
	if err != nil {
		return err
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Setting metadata type %s.%s = %s on %s...",
		cmdutil.TargetTypeString(targetType), key, cmdutil.MetadataTypeString(mdType), ledgerName))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_SetMetadataFieldType{
				SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
					Ledger:     ledgerName,
					TargetType: targetType,
					Key:        key,
					Type:       mdType,
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

		return cmdutil.FormatGRPCError("failed to set metadata type", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		spinner.Fail("Response signature verification failed")

		return cmdutil.Displayed(fmt.Errorf("response signature verification failed: %w", err))
	}

	spinner.Success(fmt.Sprintf("Set %s.%s = %s on ledger %s",
		cmdutil.TargetTypeString(targetType), key, cmdutil.MetadataTypeString(mdType), ledgerName))

	return nil
}
