package main

import (
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
	name              string
	driver            string
	config            interface{} // Will be one of: SQLiteConfig, PostgresConfig
	snapshotThreshold *uint64     // Optional snapshot threshold
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
	bucketsCreateCmd.Flags().Uint64("snapshot-threshold", 0, "Number of logs before triggering a snapshot (optional, uses global config if not set)")
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

	// Extract snapshot threshold flag
	if snapshotThreshold, _ := cmd.Flags().GetUint64("snapshot-threshold"); snapshotThreshold > 0 {
		opts.snapshotThreshold = &snapshotThreshold
	}

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

	// Create SDK instance
	sdk := newSDKClient()

	// Convert driver string to SDK type
	var driver components.CreateBucketRequestDriver
	switch opts.driver {
	case "sqlite":
		driver = components.CreateBucketRequestDriverSqlite
	case "postgres":
		driver = components.CreateBucketRequestDriverPostgres
	default:
		return fmt.Errorf("unsupported driver: %s", opts.driver)
	}

	// Convert config struct to SDK type
	var config *components.CreateBucketRequestConfig
	switch cfg := opts.config.(type) {
	case service.SQLiteConfig:
		cfgSDK := components.CreateCreateBucketRequestConfigSQLiteConfig(components.SQLiteConfig{})
		config = &cfgSDK
	case service.PostgresConfig:
		cfgSDK := components.CreateCreateBucketRequestConfigPostgresConfig(components.PostgresConfig{
			Dsn: cfg.DSN,
		})
		config = &cfgSDK
	case nil:
		// For SQLite, use empty config
		if opts.driver == "sqlite" {
			cfgSDK := components.CreateCreateBucketRequestConfigSQLiteConfig(components.SQLiteConfig{})
			config = &cfgSDK
		}
	}

	// Create bucket request
	createReq := components.CreateBucketRequest{
		Driver: driver,
		Config: config,
	}
	// Add snapshot threshold if provided
	if opts.snapshotThreshold != nil && *opts.snapshotThreshold > 0 {
		threshold := int64(*opts.snapshotThreshold)
		createReq.SnapshotThreshold = &threshold
	}

	req := operations.CreateBucketRequest{
		BucketName:          opts.name,
		CreateBucketRequest: createReq,
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
	if bucketResponse == nil {
		spinner.Success("Bucket created successfully")
		return nil
	}

	data := bucketResponse.Data
	spinner.Success("Bucket created successfully")
	pterm.Println()

	// Create info panel
	panelData := ""
	panelData += fmt.Sprintf("ID: %d\n", data.ID)
	panelData += fmt.Sprintf("Name: %s\n", data.Name)
	panelData += fmt.Sprintf("Driver: %s\n", string(data.Driver))

	// Display storage-specific information
	switch data.Driver {
	case components.DriverSqlite:
		// SQLite DSN is auto-generated, show a note
		panelData += "Storage: SQLite (auto-generated database file)\n"
	case components.DriverPostgres:
		if data.Config.Type == components.ConfigTypePostgresConfig && data.Config.PostgresConfig != nil {
			panelData += fmt.Sprintf("DSN: %s\n", data.Config.PostgresConfig.Dsn)
		}
	}

	// Display snapshot threshold if set
	if data.SnapshotThreshold != nil && *data.SnapshotThreshold > 0 {
		panelData += fmt.Sprintf("Snapshot Threshold: %d\n", *data.SnapshotThreshold)
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

	// Step 4: Ask for snapshot threshold (optional)
	if opts.snapshotThreshold == nil {
		pterm.Info.Println("Snapshot Threshold (Optional)")
		pterm.Println("Enter the number of logs before triggering a snapshot.")
		pterm.Println("Leave empty to use the global configuration.")
		pterm.Println()

		thresholdStr, err := pterm.DefaultInteractiveTextInput.
			WithDefaultText("").
			Show("Snapshot threshold (press Enter to skip)")
		if err != nil {
			return fmt.Errorf("failed to get snapshot threshold: %w", err)
		}
		if thresholdStr != "" {
			var threshold uint64
			if _, err := fmt.Sscanf(thresholdStr, "%d", &threshold); err != nil {
				return fmt.Errorf("invalid snapshot threshold: %w", err)
			}
			if threshold > 0 {
				opts.snapshotThreshold = &threshold
				pterm.Success.Printf("Snapshot threshold: %d\n", threshold)
			}
		} else {
			pterm.Info.Println("Using global snapshot threshold configuration")
		}
		pterm.Println()
	}

	pterm.Println()
	pterm.Success.Println("Configuration collected successfully!")
	pterm.Println()

	return nil
}
