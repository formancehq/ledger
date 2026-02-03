package main

import (
	"fmt"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newTransactionsSetMetadataCommand creates the transactions set-metadata command.
func newTransactionsSetMetadataCommand() *cobra.Command {
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
		RunE: runTransactionsSetMetadata,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().StringArrayP("metadata", "m", nil, "Metadata key=value pairs (can be repeated)")
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runTransactionsSetMetadata(cmd *cobra.Command, args []string) error {
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

	// Get transaction ID (from args or prompt)
	var txID uint64
	if len(args) > 0 {
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
		key, value, err := parseKeyValue(m)
		if err != nil {
			pterm.Error.Printfln("Invalid metadata format: %s", m)
			return fmt.Errorf("invalid metadata format %q: %w", m, err)
		}
		metadata[key] = value
	}

	ctx, cancel := getContext(cmd)
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

	_, err = client.Apply(ctx, req)
	if err != nil {
		spinner.Fail("Failed to set metadata")
		return fmt.Errorf("failed to set metadata: %w", err)
	}

	spinner.Success("Metadata set")

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
