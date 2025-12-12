package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// createBucketOptions holds all the flags for the create bucket command
type createBucketOptions struct {
	name   string
	driver string
	config interface{} // Will be one of: SQLiteConfig, PostgresConfig
}

var bucketsCreateCmd = &cobra.Command{
	Use:          "create",
	Short:        "Create a new bucket",
	Long:         "Creates a new bucket with the specified name, driver, and configuration",
	RunE:         runCreateBucket,
	SilenceUsage: true,
}

func init() {
	bucketsCreateCmd.Flags().String("name", "", "Bucket name")
	bucketsCreateCmd.Flags().String("driver", "", "Driver name (sqlite, postgres)")
	bucketsCreateCmd.Flags().String("postgres-dsn", "", "PostgreSQL connection string (required for postgres driver)")
	// Name, driver and config are no longer required - wizard will prompt if not provided
	// Note: SQLite and File drivers don't require config - paths are automatically generated

	// Register completions
	bucketsCreateCmd.RegisterFlagCompletionFunc("driver", completeDriverNames())
}

func runCreateBucket(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Extract options from flags
	opts := &createBucketOptions{}
	opts.name, _ = cmd.Flags().GetString("name")
	opts.driver, _ = cmd.Flags().GetString("driver")

	// Extract driver-specific config flags
	postgresDSN, _ := cmd.Flags().GetString("postgres-dsn")

	// Build config struct from flags
	switch opts.driver {
	case "sqlite":
		// SQLite doesn't require config - DSN is automatically generated
		opts.config = service.SQLiteConfig{}
	case "postgres":
		if postgresDSN != "" {
			opts.config = service.PostgresConfig{DSN: postgresDSN}
		}
	}

	// Run wizard if name, driver or config not provided
	// For SQLite and File, config can be nil (will be set to empty config)
	needsWizard := opts.name == "" || opts.driver == "" || (opts.config == nil && opts.driver != "sqlite" && opts.driver != "file")
	if needsWizard {
		if err := runCreateBucketWizard(opts); err != nil {
			return err
		}
	}

	// Ensure SQLite and File have empty config if not set
	if opts.driver == "sqlite" && opts.config == nil {
		opts.config = service.SQLiteConfig{}
	}

	// Validate required fields after wizard
	if opts.name == "" {
		return fmt.Errorf("bucket name is required")
	}

	// Convert config struct to JSON for API request
	configJSON, err := json.Marshal(opts.config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Unmarshal into map for API compatibility
	var configMap map[string]interface{}
	if err := json.Unmarshal(configJSON, &configMap); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Create SDK instance
	sdk := newSDKClient()

	// Create bucket request
	req := operations.CreateBucketRequest{
		BucketName: opts.name,
		CreateBucketRequest: components.CreateBucketRequest{
			Driver: opts.driver,
			Config: configMap,
		},
	}

	// Show spinner while creating
	spinner, _ := pterm.DefaultSpinner.Start("Creating bucket...")

	// Call the create bucket endpoint
	res, err := sdk.Buckets.CreateBucket(ctx, req)
	if err != nil {
		spinner.Fail("Failed to create bucket")
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	// Extract response data
	bucketResponse := res.GetCreateBucketResponse()
	if bucketResponse == nil || bucketResponse.Data == nil {
		spinner.Success("Bucket created successfully")
		return nil
	}

	data := bucketResponse.Data
	spinner.Success("Bucket created successfully")
	pterm.Println()

	// Create info panel
	panelData := ""
	if data.ID != nil {
		panelData += fmt.Sprintf("ID: %d\n", *data.ID)
	}
	if data.Name != nil {
		panelData += fmt.Sprintf("Name: %s\n", *data.Name)
	}
	if data.Driver != nil {
		panelData += fmt.Sprintf("Driver: %s\n", *data.Driver)
	}

	// Display storage-specific information
	if data.Driver != nil && data.Config != nil {
		driver := *data.Driver
		switch driver {
		case "sqlite":
			// SQLite DSN is auto-generated, show a note
			panelData += "Storage: SQLite (auto-generated database file)\n"
		case "postgres":
			if dsn, ok := data.Config["dsn"].(string); ok {
				panelData += fmt.Sprintf("DSN: %s\n", dsn)
			}
		}
	}

	pterm.DefaultBox.WithTitle("Bucket Information").WithBoxStyle(pterm.NewStyle(pterm.FgLightCyan)).Println(panelData)

	return nil
}

// runCreateBucketWizard runs an interactive wizard to collect bucket creation parameters
func runCreateBucketWizard(opts *createBucketOptions) error {
	pterm.DefaultHeader.WithFullWidth().Println("Bucket Creation Wizard")
	pterm.Println()

	// Step 1: Get bucket name if not provided
	if opts.name == "" {
		pterm.Info.Println("Bucket Name")
		pterm.Println("Enter a unique name for the bucket.")
		pterm.Println()

		name, err := pterm.DefaultInteractiveTextInput.
			Show("Bucket name")
		if err != nil {
			return fmt.Errorf("failed to get bucket name: %w", err)
		}
		if name == "" {
			return fmt.Errorf("bucket name cannot be empty")
		}
		opts.name = name
		pterm.Success.Printf("Bucket name: %s\n", opts.name)
		pterm.Println()
	}

	// Step 2: Select driver if not provided
	if opts.driver == "" {
		options := []string{
			"sqlite    - SQLite database (file-based, good for development)",
			"postgres  - PostgreSQL database (production-ready)",
		}

		selectedOption, err := pterm.DefaultInteractiveSelect.
			WithOptions(options).
			Show("Select a storage driver")
		if err != nil {
			return fmt.Errorf("failed to select driver: %w", err)
		}

		// Extract driver name from selected option (format: "driver    - description")
		driverMap := map[string]string{
			"sqlite    - SQLite database (file-based, good for development)": "sqlite",
			"postgres  - PostgreSQL database (production-ready)":             "postgres",
		}
		opts.driver = driverMap[selectedOption]
		if opts.driver == "" {
			// Fallback: extract from first word
			parts := strings.Fields(selectedOption)
			if len(parts) > 0 {
				opts.driver = parts[0]
			} else {
				return fmt.Errorf("failed to parse driver from selection")
			}
		}
		pterm.Success.Printf("Selected driver: %s\n", opts.driver)
		pterm.Println()
	}

	// Step 3: Collect driver-specific configuration
	switch opts.driver {
	case "sqlite":
		// SQLite doesn't require config - DSN is automatically generated based on bucket ID
		if opts.config == nil {
			pterm.Info.Println("SQLite Configuration")
			pterm.Println("SQLite database will be automatically created in the extra-data-dir.")
			pterm.Println("The database file will be named: bucket-{id}.db")
			pterm.Println()
			opts.config = service.SQLiteConfig{}
		}

	case "postgres":
		if opts.config == nil {
			pterm.Info.Println("PostgreSQL Configuration")
			pterm.Println("Enter the PostgreSQL connection string.")
			pterm.Println("Example: postgres://user:password@localhost:5432/ledger?sslmode=disable")
			pterm.Println()

			dsn, err := pterm.DefaultInteractiveTextInput.
				WithDefaultText("postgres://user:password@localhost:5432/ledger?sslmode=disable").
				Show("PostgreSQL DSN")
			if err != nil {
				return fmt.Errorf("failed to get PostgreSQL DSN: %w", err)
			}
			opts.config = service.PostgresConfig{DSN: dsn}
		}
	}

	pterm.Println()
	pterm.Success.Println("Configuration collected successfully!")
	pterm.Println()

	return nil
}
