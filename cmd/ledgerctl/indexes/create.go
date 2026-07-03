package indexes

import (
	"errors"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewCreateCommand creates the indexes create command.
func NewCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "create [flags]",
		Aliases: []string{"c"},
		Short:   "Create an index on a ledger",
		Long: `Create an opt-in index on a ledger.

Index types:
  address              Account→transaction mapping (any role)
  source-address       Source account→transaction mapping
  dest-address         Destination account→transaction mapping
  metadata             Metadata field index (requires --target and --key)
  reference            Transaction reference index (exact-match filter)
  timestamp            Transaction timestamp index (range filter)
  inserted-at          Transaction inserted_at (creation date) index (range filter)
  log-ledger           Per-ledger log index (enables filtered log listing)
  account-asset        Account asset-presence index (enables the 'has asset' account filter)

Examples:
  ledgerctl indexes create --ledger my-ledger --type address
  ledgerctl indexes create --ledger my-ledger --type source-address
  ledgerctl indexes create --ledger my-ledger --type metadata --target account --key category
  ledgerctl indexes create --ledger my-ledger --type reference
  ledgerctl indexes create --ledger my-ledger --type timestamp
  ledgerctl indexes create --ledger my-ledger --type inserted-at
  ledgerctl indexes create --ledger my-ledger --type log-ledger
  ledgerctl indexes create --ledger my-ledger --type account-asset`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runCreateIndex,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("type", "", "Index type: address, source-address, dest-address, metadata")
	cmdutil.RegisterEnumCompletion(cmd, "type", indexTypeOptions...)
	cmd.Flags().String("target", "", "Target type for metadata index: account, transaction, or ledger")
	cmdutil.RegisterEnumCompletion(cmd, "target", cmdutil.TargetTypeOptions()...)
	cmd.Flags().String("key", "", "Metadata key name (for metadata index)")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runCreateIndex(cmd *cobra.Command, _ []string) error {
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
			WithDefaultText("Select index type").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		indexType = result
	}

	req := &servicepb.CreateIndexRequest{
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
	case "dest-address":
		req.Id = txBuiltinIndexID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS)
		indexDesc = "dest-address"
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
		return fmt.Errorf("invalid index type %q: must be address, source-address, dest-address, metadata, reference, timestamp, inserted-at, log-ledger, or account-asset", indexType)
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Creating index %s on %s...", indexDesc, ledgerName))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_CreateIndex{
				CreateIndex: req,
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

		return cmdutil.FormatGRPCError("failed to create index", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		spinner.Fail("Response signature verification failed")

		return cmdutil.Displayed(fmt.Errorf("response signature verification failed: %w", err))
	}

	spinner.Success(fmt.Sprintf("Created index %s on ledger %s", indexDesc, ledgerName))

	return nil
}

// resolveMetadataIndexFlags resolves the target and key for a metadata index.
func resolveMetadataIndexFlags(cmd *cobra.Command) (commonpb.TargetType, string, error) {
	targetStr, _ := cmd.Flags().GetString("target")
	if targetStr == "" {
		result, err := pterm.DefaultInteractiveSelect.
			WithOptions(cmdutil.TargetTypeOptions()).
			WithDefaultText("Select target type").
			Show()
		if err != nil {
			return 0, "", fmt.Errorf("failed to read input: %w", err)
		}

		targetStr = result
	}

	target, err := cmdutil.ParseTargetType(targetStr)
	if err != nil {
		return 0, "", err
	}

	key, _ := cmd.Flags().GetString("key")
	if key == "" {
		result, err := pterm.DefaultInteractiveTextInput.
			WithDefaultText("Enter metadata key name").
			Show()
		if err != nil {
			return 0, "", fmt.Errorf("failed to read input: %w", err)
		}

		key = result
		if key == "" {
			return 0, "", errors.New("metadata key is required")
		}
	}

	return target, key, nil
}
