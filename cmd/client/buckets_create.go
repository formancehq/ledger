package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var (
	bucketName          string
	bucketDriver        string
	bucketSQLiteDSN     string
	bucketPostgresDSN   string
	bucketClickHouseDSN string
	bucketFilePath      string
)

var bucketsCreateCmd = &cobra.Command{
	Use:          "create",
	Short:        "Create a new bucket",
	Long:         "Creates a new bucket with the specified name, driver, and configuration",
	RunE:         runCreateBucket,
	SilenceUsage: true,
}

func init() {
	bucketsCreateCmd.Flags().StringVar(&bucketName, "name", "", "Bucket name (required)")
	bucketsCreateCmd.Flags().StringVar(&bucketDriver, "driver", "", "Driver name (required: sqlite, postgres, clickhouse, file)")
	bucketsCreateCmd.Flags().StringVar(&bucketSQLiteDSN, "sqlite-dsn", "", "SQLite connection address (required for sqlite driver)")
	bucketsCreateCmd.Flags().StringVar(&bucketPostgresDSN, "postgres-dsn", "", "PostgreSQL connection string (required for postgres driver)")
	bucketsCreateCmd.Flags().StringVar(&bucketClickHouseDSN, "clickhouse-dsn", "", "ClickHouse connection string (required for clickhouse driver)")
	bucketsCreateCmd.Flags().StringVar(&bucketFilePath, "file-path", "", "Directory path for file storage (required for file driver)")
	if err := bucketsCreateCmd.MarkFlagRequired("name"); err != nil {
		panic(err)
	}
	if err := bucketsCreateCmd.MarkFlagRequired("driver"); err != nil {
		panic(err)
	}
}

func runCreateBucket(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Validate required flags
	if bucketName == "" {
		return fmt.Errorf("bucket name is required")
	}
	if bucketDriver == "" {
		return fmt.Errorf("driver is required")
	}

	// Build config from driver-specific flags
	config := make(map[string]interface{})

	switch bucketDriver {
	case "sqlite":
		if bucketSQLiteDSN == "" {
			return fmt.Errorf("--sqlite-dsn is required for sqlite driver")
		}
		config["dsn"] = bucketSQLiteDSN
	case "postgres":
		if bucketPostgresDSN == "" {
			return fmt.Errorf("--postgres-dsn is required for postgres driver")
		}
		config["dsn"] = bucketPostgresDSN
	case "clickhouse":
		if bucketClickHouseDSN == "" {
			return fmt.Errorf("--clickhouse-dsn is required for clickhouse driver")
		}
		config["dsn"] = bucketClickHouseDSN
	case "file":
		if bucketFilePath == "" {
			return fmt.Errorf("--file-path is required for file driver")
		}
		config["path"] = bucketFilePath
	default:
		return fmt.Errorf("unsupported driver: %s (supported drivers: sqlite, postgres, clickhouse, file)", bucketDriver)
	}

	// Create SDK instance
	sdk := newSDKClient()

	// Create bucket request
	req := operations.CreateBucketRequest{
		BucketName: bucketName,
		CreateBucketRequest: components.CreateBucketRequest{
			Driver: bucketDriver,
			Config: config,
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
		case "sqlite", "postgres", "clickhouse":
			if dsn, ok := data.Config["dsn"].(string); ok {
				panelData += fmt.Sprintf("DSN: %s\n", dsn)
			}
		case "file":
			if path, ok := data.Config["path"].(string); ok {
				panelData += fmt.Sprintf("Directory: %s\n", path)
			}
		}
	}

	pterm.DefaultBox.WithTitle("Bucket Information").WithBoxStyle(pterm.NewStyle(pterm.FgLightCyan)).Println(panelData)

	return nil
}
