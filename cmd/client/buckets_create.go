package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/spf13/cobra"
)

var (
	bucketName      string
	bucketDriver    string
	bucketSQLiteDSN string
	bucketFilePath  string
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
	bucketsCreateCmd.Flags().StringVar(&bucketDriver, "driver", "", "Driver name (required: sqlite, file)")
	bucketsCreateCmd.Flags().StringVar(&bucketSQLiteDSN, "sqlite-dsn", "", "SQLite connection address (required for sqlite driver)")
	bucketsCreateCmd.Flags().StringVar(&bucketFilePath, "file-path", "", "Directory path for file storage (required for file driver)")
	bucketsCreateCmd.MarkFlagRequired("name")
	bucketsCreateCmd.MarkFlagRequired("driver")
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
	case "file":
		if bucketFilePath == "" {
			return fmt.Errorf("--file-path is required for file driver")
		}
		config["path"] = bucketFilePath
	default:
		return fmt.Errorf("unsupported driver: %s (supported drivers: sqlite, file)", bucketDriver)
	}

	// Create SDK instance with custom server URL
	sdk := newSDKClient()

	// Create bucket request
	req := operations.CreateBucketRequest{
		BucketName: bucketName,
		CreateBucketRequest: components.CreateBucketRequest{
			Driver: bucketDriver,
			Config: config,
		},
	}

	// Call the create bucket endpoint
	res, err := sdk.Buckets.CreateBucket(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	// Extract response data
	bucketResponse := res.GetCreateBucketResponse()
	if bucketResponse == nil || bucketResponse.Data == nil {
		fmt.Println("Bucket created successfully")
		return nil
	}

	data := bucketResponse.Data
	fmt.Println("Bucket created successfully")
	fmt.Println()

	if data.Name != nil {
		fmt.Printf("Name: %s\n", *data.Name)
	}
	if data.Driver != nil {
		fmt.Printf("Driver: %s\n", *data.Driver)
	}

	// Display storage-specific information
	if data.Driver != nil && data.Config != nil {
		driver := *data.Driver
		fmt.Println()
		fmt.Println("Storage configuration:")

		switch driver {
		case "sqlite":
			if dsn, ok := data.Config["dsn"].(string); ok {
				fmt.Printf("  Database: %s\n", dsn)
			} else {
				fmt.Printf("  Config: %v\n", data.Config)
			}
		case "file":
			if path, ok := data.Config["path"].(string); ok {
				fmt.Printf("  Directory: %s\n", path)
			} else {
				fmt.Printf("  Config: %v\n", data.Config)
			}
		default:
			fmt.Printf("  Config: %v\n", data.Config)
		}
	}

	if data.ID != nil {
		fmt.Printf("ID: %d\n", *data.ID)
	}

	return nil
}

