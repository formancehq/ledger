package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// createLedgerOptions holds all the flags for the create ledger command
type createLedgerOptions struct {
	name     string
	driver   string
	metadata map[string]string
}

var ledgersCreateCmd = &cobra.Command{
	Use:          "create",
	Short:        "Create a new ledger",
	Long:         "Creates a new ledger with the specified storage driver",
	RunE:         runCreateLedger,
	SilenceUsage: true,
}

func init() {
	ledgersCreateCmd.Flags().String("name", "", "Ledger name")
	ledgersCreateCmd.Flags().String("driver", "sqlite", "Storage driver (sqlite, clickhouse)")
	ledgersCreateCmd.Flags().String("metadata", "{}", "Metadata as JSON (default: {})")
	// Name is no longer required - wizard will prompt if not provided

	// Register completions
	ledgersCreateCmd.RegisterFlagCompletionFunc("name", completeLedgerNames())
	ledgersCreateCmd.RegisterFlagCompletionFunc("driver", completeDriverNames())
}

func runCreateLedger(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Extract options from flags
	opts := &createLedgerOptions{}
	opts.name, _ = cmd.Flags().GetString("name")
	opts.driver, _ = cmd.Flags().GetString("driver")
	metadataStr, _ := cmd.Flags().GetString("metadata")

	// Parse metadata JSON
	var metadata map[string]string
	if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
		return fmt.Errorf("invalid metadata JSON: %w", err)
	}
	opts.metadata = metadata

	// Create SDK instance once
	sdk := newSDKClient()

	// Run wizard if name not provided
	if opts.name == "" {
		if err := runCreateLedgerWizard(ctx, sdk, opts); err != nil {
			return err
		}
	}

	// Validate required fields after wizard
	if opts.name == "" {
		return fmt.Errorf("ledger name is required")
	}
	if opts.driver == "" {
		opts.driver = "sqlite" // Default driver
	}

	// Create ledger request
	driverEnum := components.CreateLedgerRequestDriver(opts.driver)
	req := operations.CreateLedgerRequest{
		LedgerName: opts.name,
		CreateLedgerRequest: components.CreateLedgerRequest{
			Driver:   &driverEnum,
			Metadata: opts.metadata,
		},
	}

	// Show spinner while creating
	spinner, _ := pterm.DefaultSpinner.Start("Creating ledger...")

	// Call the create ledger endpoint
	res, err := sdk.Ledgers.CreateLedger(ctx, req)
	if err != nil {
		spinner.Fail("Failed to create ledger")
		return fmt.Errorf("failed to create ledger: %w", err)
	}

	// Extract response data
	ledgerResponse := res.GetCreateLedgerResponse()
	if ledgerResponse == nil {
		spinner.Success("Ledger created successfully")
		return nil
	}

	data := ledgerResponse.Data
	spinner.Success("Ledger created successfully")
	pterm.Println()

	// Create info panel
	panelData := ""
	panelData += fmt.Sprintf("Name: %s\n", data.Name)
	panelData += fmt.Sprintf("Driver: %s\n", string(data.Driver))
	if len(data.Metadata) > 0 {
		panelData += "Metadata:\n"
		for k, v := range data.Metadata {
			panelData += fmt.Sprintf("  %s: %s\n", k, v)
		}
	}

	pterm.DefaultBox.WithTitle("Ledger Information").WithBoxStyle(pterm.NewStyle(pterm.FgLightGreen)).Println(panelData)

	return nil
}

// runCreateLedgerWizard runs an interactive wizard to collect ledger creation parameters
func runCreateLedgerWizard(ctx context.Context, sdk *client.Formance, opts *createLedgerOptions) error {
	pterm.DefaultHeader.WithFullWidth().Println("Ledger Creation Wizard")
	pterm.Println()

	// Step 1: Get ledger name if not provided
	if opts.name == "" {
		pterm.Info.Println("Ledger Name")
		pterm.Println("Enter a unique name for the ledger.")
		pterm.Println()

		name, err := pterm.DefaultInteractiveTextInput.
			Show("Ledger name")
		if err != nil {
			return fmt.Errorf("failed to get ledger name: %w", err)
		}
		if name == "" {
			return fmt.Errorf("ledger name cannot be empty")
		}
		opts.name = name
		pterm.Success.Printf("Ledger name: %s\n", opts.name)
		pterm.Println()
	}

	// Step 2: Get driver if not provided
	if opts.driver == "" {
		driverOptions := []string{"sqlite", "clickhouse"}
		selectedDriver, err := pterm.DefaultInteractiveSelect.
			WithOptions(driverOptions).
			WithDefaultOption("sqlite").
			Show("Select storage driver")
		if err != nil {
			return fmt.Errorf("failed to select driver: %w", err)
		}
		opts.driver = selectedDriver
		pterm.Success.Printf("Selected driver: %s\n", opts.driver)
		pterm.Println()
	}

	// Step 3: Get metadata if not provided or empty
	if len(opts.metadata) == 0 {
		pterm.Info.Println("Metadata (Optional)")
		pterm.Println("Enter metadata as JSON object, or press Enter to skip.")
		pterm.Println("Example: {\"environment\":\"production\",\"region\":\"us-east-1\"}")
		pterm.Println()

		metadataStr, err := pterm.DefaultInteractiveTextInput.
			WithDefaultText("{}").
			Show("Metadata (JSON)")
		if err != nil {
			return fmt.Errorf("failed to get metadata: %w", err)
		}

		if metadataStr != "" && metadataStr != "{}" {
			var metadata map[string]string
			if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
				return fmt.Errorf("invalid metadata JSON: %w", err)
			}
			opts.metadata = metadata
		}
	}

	pterm.Println()
	pterm.Success.Println("Configuration collected successfully!")
	pterm.Println()

	return nil
}
