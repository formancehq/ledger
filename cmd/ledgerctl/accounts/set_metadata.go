package accounts

import (
	"errors"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewSetMetadataCommand creates the accounts set-metadata command.
func NewSetMetadataCommand() *cobra.Command {
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

	ledgerFlag, _ := cmd.Flags().GetString("ledger")

	ledgerName, err := cmdutil.SelectLedger(cmd, client, ledgerFlag)
	if err != nil {
		return err
	}

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

		return cmdutil.Displayed(errors.New("account address is required"))
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

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Setting metadata on account %s...", address))

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
								Metadata: commonpb.MetadataFromGoMap(metadata),
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
		"address":  address,
		"metadata": metadata,
	}); handled || err != nil {
		return err
	}

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
