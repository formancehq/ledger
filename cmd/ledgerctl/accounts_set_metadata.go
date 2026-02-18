package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newAccountsSetMetadataCommand creates the accounts set-metadata command.
func newAccountsSetMetadataCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "set-metadata [address]",
		Aliases: []string{"set-meta", "sm"},
		Short:   "Set metadata on an account",
		Long: `Set metadata on an account via gRPC.

Metadata is provided as key=value pairs using the --metadata flag.
Multiple metadata entries can be set at once.

If --ledger is not provided and only one ledger exists, it will be used automatically.

Examples:
  ledgerctl accounts set-metadata bank --ledger my-ledger --metadata type=asset
  ledgerctl accounts set-metadata users:alice --metadata role=admin --metadata tier=premium
  ledgerctl acc sm bank -m type=asset -m label="Main Bank"`,
		Args: cobra.MaximumNArgs(1),
		RunE: runAccountsSetMetadata,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().StringArrayP("metadata", "m", nil, "Metadata key=value pairs (can be repeated)")
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runAccountsSetMetadata(cmd *cobra.Command, args []string) error {
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

	// Get account address (from args or prompt)
	var address string
	if len(args) > 0 {
		address = args[0]
	} else {
		address, err = pterm.DefaultInteractiveTextInput.
			WithDefaultText("Enter account address").
			Show()
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}
	}

	if address == "" {
		pterm.Error.Println("Account address is required")
		return fmt.Errorf("account address is required")
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

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Setting metadata on account %s...", address))

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
									Target: &commonpb.Target_Account{
										Account: &commonpb.TargetAccount{Addr: address},
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

	if err := signRequests(cmd, req.Requests); err != nil {
		spinner.Fail("Failed to sign request")
		return err
	}

	_, err = client.Apply(ctx, req)
	if err != nil {
		spinner.Fail("Failed to set metadata")
		return formatGRPCError("failed to set metadata", err)
	}

	spinner.Success("Metadata set")

	// Display what was set
	pterm.Println()
	pterm.Printf("Account: %s\n", pterm.Cyan(address))
	pterm.Println(pterm.Gray("─────────────────────────────────"))

	metadataTable := pterm.TableData{
		{"KEY", "VALUE"},
	}
	for key, value := range metadata {
		metadataTable = append(metadataTable, []string{key, value})
	}

	return pterm.DefaultTable.WithHasHeader().WithData(metadataTable).Render()
}
