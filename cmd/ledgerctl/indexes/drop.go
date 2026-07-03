package indexes

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewDropCommand creates the indexes drop command.
func NewDropCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "drop [flags]",
		Aliases: []string{"d"},
		Short:   "Drop an index from a ledger",
		Long: `Drop an opt-in index from a ledger. This stops the index from being updated
and frees the associated storage.

Index types:
  address              Account→transaction mapping (any role)
  source-address       Source account→transaction mapping
  destination-address  Destination account→transaction mapping
  metadata             Metadata field index (requires --target and --key)
  account-asset        Account asset-presence index (the 'has asset' account filter)

Examples:
  ledgerctl indexes drop --ledger my-ledger --type address
  ledgerctl indexes drop --ledger my-ledger --type metadata --target account --key category
  ledgerctl indexes drop --ledger my-ledger --type account-asset`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runDropIndex,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("type", "", "Index type: address, source-address, destination-address, metadata")
	cmdutil.RegisterEnumCompletion(cmd, "type", indexTypeOptions...)
	cmd.Flags().String("target", "", "Target type for metadata index: account, transaction, or ledger")
	cmdutil.RegisterEnumCompletion(cmd, "target", cmdutil.TargetTypeOptions()...)
	cmd.Flags().String("key", "", "Metadata key name (for metadata index)")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runDropIndex(cmd *cobra.Command, _ []string) error {
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

	indexType, _ := cmd.Flags().GetString("type")
	if indexType == "" {
		result, err := pterm.DefaultInteractiveSelect.
			WithOptions(indexTypeOptions).
			WithDefaultText("Select index type to drop").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		indexType = result
	}

	if err := rejectMetadataOnlyFlags(cmd, indexType); err != nil {
		return err
	}

	req := &servicepb.DropIndexRequest{
		Ledger: ledgerName,
	}

	var indexDesc string

	switch indexType {
	case "address":
		req.Id = txBuiltinIndexID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS)
		indexDesc = "address (any role)"
	case "source-address":
		req.Id = txBuiltinIndexID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS)
		indexDesc = "source-address"
	case "destination-address":
		req.Id = txBuiltinIndexID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS)
		indexDesc = "destination-address"
	case "metadata":
		target, key, err := resolveMetadataIndexFlags(cmd)
		if err != nil {
			return err
		}

		req.Id = metadataIndexID(target, key)
		indexDesc = fmt.Sprintf("metadata %s.%s", cmdutil.TargetTypeString(target), key)
	case "reference":
		req.Id = txBuiltinIndexID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
		indexDesc = "reference"
	case "timestamp":
		req.Id = txBuiltinIndexID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
		indexDesc = "timestamp"
	case "inserted-at":
		req.Id = txBuiltinIndexID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT)
		indexDesc = "inserted-at"
	case "account-asset":
		req.Id = accountBuiltinIndexID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET)
		indexDesc = "account has-asset"
	default:
		return fmt.Errorf("invalid index type %q: must be address, source-address, destination-address, metadata, reference, timestamp, inserted-at, or account-asset", indexType)
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Dropping index %s on %s...", indexDesc, ledgerName))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_DropIndex{
				DropIndex: req,
			},
		},
	}

	applyReq, err := cmdutil.BuildApplyRequest(cmd, requests...)
	if err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	resp, err := client.Apply(ctx, applyReq)
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to drop index", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		spinner.Fail("Response signature verification failed")

		return cmdutil.Displayed(fmt.Errorf("response signature verification failed: %w", err))
	}

	spinner.Success(fmt.Sprintf("Dropped index %s from ledger %s", indexDesc, ledgerName))

	return nil
}
