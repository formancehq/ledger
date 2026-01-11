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
	name        string
	storeDriver string
	metadata    map[string]string
}

var ledgersCreateCmd = &cobra.Command{
	Use:          "create",
	Short:        "Create a new ledger",
	Long:         "Creates a new ledger with the specified store driver",
	RunE:         runCreateLedger,
	SilenceUsage: true,
}

func init() {
	ledgersCreateCmd.Flags().String("name", "", "Ledger name")
	ledgersCreateCmd.Flags().String("store-driver", "sqlite-mattn", "Store driver (sqlite-mattn, sqlite-modern, pebble)")
	ledgersCreateCmd.Flags().String("metadata", "{}", "Metadata as JSON (default: {})")
	ledgersCreateCmd.Flags().Bool("no-metadata", false, "Skip metadata prompt in wizard")
	// Name is no longer required - wizard will prompt if not provided

	// Register completions
	if err := ledgersCreateCmd.RegisterFlagCompletionFunc("name", completeLedgerNames()); err != nil {
		panic(err)
	}
	if err := ledgersCreateCmd.RegisterFlagCompletionFunc("store-driver", completeDriverNames()); err != nil {
		panic(err)
	}
}

func runCreateLedger(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Extract options from flags
	opts := &createLedgerOptions{}
	opts.name, _ = cmd.Flags().GetString("name")
	opts.storeDriver, _ = cmd.Flags().GetString("store-driver")
	metadataStr, _ := cmd.Flags().GetString("metadata")

	// Parse metadata JSON
	var metadata map[string]string
	if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
		return fmt.Errorf("invalid metadata JSON: %w", err)
	}
	opts.metadata = metadata

	// Create SDK instance once
	sdk := newSDKClient()

	// Check which flags were explicitly provided
	nameProvided := cmd.Flags().Changed("name")
	storeDriverProvided := cmd.Flags().Changed("store-driver")
	metadataProvided := cmd.Flags().Changed("metadata")
	noMetadata, _ := cmd.Flags().GetBool("no-metadata")

	// Run wizard only if name is not provided (name is the only required field)
	if !nameProvided {
		if err := runCreateLedgerWizard(ctx, sdk, opts, nameProvided, storeDriverProvided, metadataProvided, noMetadata); err != nil {
			return err
		}
	}

	// Validate required fields after wizard
	if opts.name == "" {
		return fmt.Errorf("ledger name is required")
	}
	if opts.storeDriver == "" {
		opts.storeDriver = "sqlite-mattn" // Default store driver
	}

	// Convert driver strings to enums
	storeDriverEnum := components.CreateLedgerRequestStoreDriver(opts.storeDriver)

	// Create ledger request
	req := operations.CreateLedgerRequest{
		LedgerName: opts.name,
		CreateLedgerRequest: components.CreateLedgerRequest{
			StoreDriver: storeDriverEnum,
			Metadata:    opts.metadata,
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
	panelData += fmt.Sprintf("Store Driver: %s\n", string(data.StoreDriver))
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
// It only prompts for fields that were not explicitly provided via flags
func runCreateLedgerWizard(ctx context.Context, sdk *client.Formance, opts *createLedgerOptions, nameProvided, storeDriverProvided, metadataProvided, noMetadata bool) error {
	pterm.DefaultHeader.WithFullWidth().Println("Ledger Creation Wizard")
	pterm.Println()

	// Step 1: Get ledger name if not provided via flag
	if !nameProvided {
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
	} else {
		pterm.Success.Printf("Ledger name: %s (from flag)\n", opts.name)
		pterm.Println()
	}

	// Step 2: Get store driver if not provided via flag
	if !storeDriverProvided {
		driverOptions := []string{"sqlite-mattn", "sqlite-modern", "pebble"}
		selectedDriver, err := pterm.DefaultInteractiveSelect.
			WithOptions(driverOptions).
			WithDefaultOption("sqlite-mattn").
			Show("Select store driver")
		if err != nil {
			return fmt.Errorf("failed to select store driver: %w", err)
		}
		opts.storeDriver = selectedDriver
		pterm.Success.Printf("Selected store driver: %s\n", opts.storeDriver)
		pterm.Println()
	} else {
		pterm.Success.Printf("Store driver: %s (from flag)\n", opts.storeDriver)
		pterm.Println()
	}

	// Step 3: Get metadata if not provided via flag and --no-metadata is not set
	if !metadataProvided && !noMetadata {
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
	} else if metadataProvided && len(opts.metadata) > 0 {
		pterm.Success.Printf("Metadata: provided via flag\n")
		pterm.Println()
	} else if noMetadata {
		pterm.Success.Printf("Metadata: skipped (--no-metadata flag)\n")
		pterm.Println()
	}

	pterm.Println()
	pterm.Success.Println("Configuration collected successfully!")
	pterm.Println()

	return nil
}
