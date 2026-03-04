package ledgers

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewCreateIndexCommand creates the ledgers create-index command.
func NewCreateIndexCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "create-index [flags]",
		Aliases: []string{"ci"},
		Short:   "Create an index on a ledger",
		Long: `Create an opt-in index on a ledger.

Index types:
  address              Account→transaction mapping (any role)
  source-address       Source account→transaction mapping
  dest-address         Destination account→transaction mapping
  metadata             Metadata field index (requires --target and --key)

Examples:
  ledgerctl ledgers create-index --ledger my-ledger --type address
  ledgerctl ledgers create-index --ledger my-ledger --type source-address
  ledgerctl ledgers create-index --ledger my-ledger --type metadata --target account --key category`,
		Args: cobra.NoArgs,
		RunE: runCreateIndex,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("type", "", "Index type: address, source-address, dest-address, metadata")
	cmd.Flags().String("target", "", "Target type for metadata index: account or transaction")
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
			WithOptions([]string{"address", "source-address", "dest-address", "metadata"}).
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
		req.Index = &servicepb.CreateIndexRequest_AddressRole{
			AddressRole: commonpb.AddressRole_ADDRESS_ROLE_ANY,
		}
		indexDesc = "address (any role)"
	case "source-address":
		req.Index = &servicepb.CreateIndexRequest_AddressRole{
			AddressRole: commonpb.AddressRole_ADDRESS_ROLE_SOURCE,
		}
		indexDesc = "source-address"
	case "dest-address":
		req.Index = &servicepb.CreateIndexRequest_AddressRole{
			AddressRole: commonpb.AddressRole_ADDRESS_ROLE_DESTINATION,
		}
		indexDesc = "dest-address"
	case "metadata":
		target, key, err := resolveMetadataIndexFlags(cmd)
		if err != nil {
			return err
		}
		req.Index = &servicepb.CreateIndexRequest_Metadata{
			Metadata: &commonpb.MetadataIndexTarget{
				Target: target,
				Key:    key,
			},
		}
		indexDesc = fmt.Sprintf("metadata %s.%s", cmdutil.TargetTypeString(target), key)
	default:
		return fmt.Errorf("invalid index type %q: must be address, source-address, dest-address, or metadata", indexType)
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

	if err := cmdutil.SignRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")
		return cmdutil.Displayed(err)
	}

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		_ = spinner.Stop()
		return cmdutil.FormatGRPCError("failed to create index", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.Logs); err != nil {
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
			return 0, "", fmt.Errorf("metadata key is required")
		}
	}

	return target, key, nil
}
