package ledgers

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewSetMetadataCommand creates the ledgers set-metadata command.
func NewSetMetadataCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "set-metadata [flags]",
		Aliases: []string{"set-meta", "sm"},
		Short:   "Set metadata on a ledger",
		Long: `Set metadata key-value pairs on a ledger via gRPC.

Metadata is provided as key=value pairs using the --metadata flag.
Multiple metadata entries can be set at once.

If --ledger is not provided and only one ledger exists, it will be used automatically.

Examples:
  ledgerctl ledgers set-metadata --ledger my-ledger -m environment=production -m team=payments
  ledgerctl ledgers sm --ledger my-ledger -m region=eu-west-1
  ledgerctl ledgers set-metadata  # Interactive mode`,
		Args:              cobra.NoArgs,
		RunE:              runSetMetadata,
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().StringArrayP("metadata", "m", nil, "Metadata key=value pairs (can be repeated)")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runSetMetadata(cmd *cobra.Command, _ []string) error {
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

	metadataFlags, _ := cmd.Flags().GetStringArray("metadata")

	if len(metadataFlags) == 0 {
		pterm.Println()
		pterm.Println("Enter metadata (key=value format, empty line to finish):")

		for {
			input, err := pterm.DefaultInteractiveTextInput.
				WithDefaultText("Metadata (or empty to finish)").
				Show()
			if err != nil {
				return fmt.Errorf("failed to read input: %w", err)
			}

			if input == "" {
				break
			}

			metadataFlags = append(metadataFlags, input)
		}
	}

	if len(metadataFlags) == 0 {
		pterm.Warning.Println("No metadata provided")

		return nil
	}

	metadata := make(map[string]string)

	for _, m := range metadataFlags {
		key, value, err := cmdutil.ParseKeyValue(m)
		if err != nil {
			pterm.Error.Printfln("Invalid metadata format: %s", m)

			return cmdutil.Displayed(fmt.Errorf("invalid metadata format %q: %w", m, err))
		}

		metadata[key] = value
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Setting metadata on ledger %s...", ledgerName))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_SaveLedgerMetadata{
				SaveLedgerMetadata: &servicepb.SaveLedgerMetadataRequest{
					Ledger:   ledgerName,
					Metadata: commonpb.MetadataFromGoMap(metadata),
				},
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

		return cmdutil.FormatGRPCError("failed to set ledger metadata", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		spinner.Fail("Response signature verification failed")

		return cmdutil.Displayed(fmt.Errorf("response signature verification failed: %w", err))
	}

	spinner.Success("Metadata set")

	if handled, err := cmdutil.EncodeStructured(cmd, map[string]any{
		"ledger":   ledgerName,
		"metadata": metadata,
	}); handled || err != nil {
		return err
	}

	pterm.Println()
	pterm.Printf("Ledger: %s\n", pterm.Cyan(ledgerName))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	metadataTable := pterm.TableData{
		{"KEY", "VALUE"},
	}
	for key, value := range metadata {
		metadataTable = append(metadataTable, []string{key, value})
	}

	return pterm.DefaultTable.WithHasHeader().WithData(metadataTable).Render()
}
