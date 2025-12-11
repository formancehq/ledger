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
	bucket   string
	name     string
	metadata map[string]string
}

var ledgersCreateCmd = &cobra.Command{
	Use:          "create",
	Short:        "Create a new ledger",
	Long:         "Creates a new ledger in the specified bucket",
	RunE:         runCreateLedger,
	SilenceUsage: true,
}

func init() {
	ledgersCreateCmd.Flags().String("bucket", "", "Bucket name")
	ledgersCreateCmd.Flags().String("name", "", "Ledger name")
	ledgersCreateCmd.Flags().String("metadata", "{}", "Metadata as JSON (default: {})")
	// Bucket and name are no longer required - wizard will prompt if not provided

	// Register completions
	ledgersCreateCmd.RegisterFlagCompletionFunc("bucket", completeBucketNames())
	ledgersCreateCmd.RegisterFlagCompletionFunc("name", completeLedgerNames())
}

func runCreateLedger(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Extract options from flags
	opts := &createLedgerOptions{}
	opts.bucket, _ = cmd.Flags().GetString("bucket")
	opts.name, _ = cmd.Flags().GetString("name")
	metadataStr, _ := cmd.Flags().GetString("metadata")

	// Parse metadata JSON
	var metadata map[string]string
	if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
		return fmt.Errorf("invalid metadata JSON: %w", err)
	}
	opts.metadata = metadata

	// Create SDK instance once
	sdk := newSDKClient()

	// Run wizard if bucket or name not provided
	if opts.bucket == "" || opts.name == "" {
		if err := runCreateLedgerWizard(ctx, sdk, opts); err != nil {
			return err
		}
	}

	// Validate required fields after wizard
	if opts.bucket == "" {
		return fmt.Errorf("bucket name is required")
	}
	if opts.name == "" {
		return fmt.Errorf("ledger name is required")
	}

	// Create ledger request
	req := operations.CreateLedgerRequest{
		LedgerName: opts.name,
		CreateLedgerRequest: components.CreateLedgerRequest{
			Bucket:   opts.bucket,
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
	if ledgerResponse == nil || ledgerResponse.Data == nil {
		spinner.Success("Ledger created successfully")
		return nil
	}

	data := ledgerResponse.Data
	spinner.Success("Ledger created successfully")
	pterm.Println()

	// Create info panel
	panelData := ""
	if data.Name != nil {
		panelData += fmt.Sprintf("Name: %s\n", *data.Name)
	}
	if data.Bucket != nil {
		panelData += fmt.Sprintf("Bucket: %s\n", *data.Bucket)
	}
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

	// Step 1: Select bucket if not provided
	if opts.bucket == "" {
		// Fetch available buckets
		res, err := sdk.Buckets.ListBuckets(ctx)
		if err != nil {
			return fmt.Errorf("failed to list buckets: %w", err)
		}

		bucketsResponse := res.GetListBucketsResponse()
		if bucketsResponse == nil || bucketsResponse.Data == nil || len(bucketsResponse.Data) == 0 {
			return fmt.Errorf("no buckets available. Please create a bucket first using 'buckets create'")
		}

		buckets := bucketsResponse.Data

		// Build options list for selection
		options := make([]string, 0, len(buckets))
		bucketMap := make(map[string]string) // Maps display string to bucket name

		for _, bucket := range buckets {
			bucketName := "N/A"
			if bucket.Name != nil {
				bucketName = *bucket.Name
			}
			driver := "N/A"
			if bucket.Driver != nil {
				driver = *bucket.Driver
			}
			displayName := fmt.Sprintf("%s (%s)", bucketName, driver)
			options = append(options, displayName)
			bucketMap[displayName] = bucketName
		}

		selectedOption, err := pterm.DefaultInteractiveSelect.
			WithOptions(options).
			Show("Select a bucket")
		if err != nil {
			return fmt.Errorf("failed to select bucket: %w", err)
		}

		opts.bucket = bucketMap[selectedOption]
		if opts.bucket == "" {
			return fmt.Errorf("failed to parse bucket from selection")
		}
		pterm.Success.Printf("Selected bucket: %s\n", opts.bucket)
		pterm.Println()
	}

	// Step 2: Get ledger name if not provided
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
