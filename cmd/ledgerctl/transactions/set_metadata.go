package transactions

import (
	"fmt"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewSetMetadataCommand creates the transactions set-metadata command.
func NewSetMetadataCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "set-metadata [transaction-id]",
		Aliases: []string{"set-meta", "sm"},
		Short:   "Set metadata on a transaction",
		Long: `Set metadata on a transaction via gRPC.

Metadata is provided as key=value pairs using the --metadata flag.
Multiple metadata entries can be set at once.

If --ledger is not provided and only one ledger exists, it will be used automatically.

Examples:
  ledgerctl transactions set-metadata 42 --ledger my-ledger --metadata status=processed
  ledgerctl transactions set-metadata 42 --metadata reason="refund" --metadata ticket=JIRA-123
  ledgerctl tx sm 42 -m status=processed`,
		Args: cobra.MaximumNArgs(1),
		RunE: runSetMetadata,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().StringArrayP("metadata", "m", nil, "Metadata key=value pairs (can be repeated)")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runSetMetadata(cmd *cobra.Command, args []string) error {
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

	// Get transaction ID (from args or prompt)
	var txID uint64
	if len(args) > 0 {
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

	// Get metadata from flags
	metadataFlags, _ := cmd.Flags().GetStringArray("metadata")

	// If no metadata provided via flags, prompt for it
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

	// Parse metadata
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

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Setting metadata on transaction #%d...", txID))

	// Build request
	req := &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{
			{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledgerName,
						Data: &servicepb.LedgerApplyRequest_AddMetadata{
							AddMetadata: &commonpb.SaveMetadataCommand{
								Target: &commonpb.Target{
									Target: &commonpb.Target_Transaction{
										Transaction: &commonpb.TargetTransaction{Id: txID},
									},
								},
								Metadata: commonpb.MetadataSetFromMap(metadata),
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

		return cmdutil.FormatGRPCError("failed to set metadata", err)
	}

	spinner.Success("Metadata set")

	if handled, err := cmdutil.EncodeStructured(cmd, map[string]any{
		"transactionId": txID,
		"metadata":      metadata,
	}); handled || err != nil {
		return err
	}

	// Display what was set
	pterm.Println()
	pterm.Printf("Transaction #%d\n", txID)
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	metadataTable := pterm.TableData{
		{"KEY", "VALUE"},
	}
	for key, value := range metadata {
		metadataTable = append(metadataTable, []string{key, value})
	}

	return pterm.DefaultTable.WithHasHeader().WithData(metadataTable).Render()
}
