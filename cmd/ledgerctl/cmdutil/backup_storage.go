package cmdutil

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// AddBackupStorageFlags registers the shared backup storage flags (driver +
// per-provider credentials) on cmd. Commands that target a backup backend
// (backup, incremental backup, restore download, bootstrap) all use the same
// set, so they share this registration and BackupStorageFromFlags /
// BackupStorageConfigFromFlags to read it back.
func AddBackupStorageFlags(cmd *cobra.Command) {
	cmd.Flags().String("driver", "s3", "Backup storage driver: s3 or azure")
	RegisterEnumCompletion(cmd, "driver", "s3", "azure")
	// S3 flags
	cmd.Flags().String("s3-bucket", "", "S3 bucket name (required when driver=s3)")
	cmd.Flags().String("s3-region", "", "AWS region for S3 bucket")
	cmd.Flags().String("s3-endpoint", "", "Custom S3 endpoint (for MinIO)")
	cmd.Flags().String("s3-access-key-id", "", "Static AWS access key ID (default: use default credential chain)")
	cmd.Flags().String("s3-secret-access-key", "", "Static AWS secret access key (default: use default credential chain)")
	// Azure flags
	cmd.Flags().String("azure-account-name", "", "Azure storage account name (required when driver=azure)")
	cmd.Flags().String("azure-account-key", "", "Azure storage account key (omit to use DefaultAzureCredential)")
	cmd.Flags().String("azure-container", "", "Azure Blob Storage container name (required when driver=azure)")
	cmd.Flags().String("azure-endpoint", "",
		"Custom Azure Blob service URL. For Azurite, include the account path "+
			"(e.g. http://127.0.0.1:10000/devstoreaccount1). Leave empty to use the "+
			"production https://<account>.blob.core.windows.net/ endpoint.")
}

// backupStorageFlags is the parsed form of AddBackupStorageFlags' flag set.
// Reading the flags once and projecting to either StorageConfig or the proto
// keeps the two public helpers DRY and makes required-field validation share
// a single source of truth.
type backupStorageFlags struct {
	driver string

	s3Bucket          string
	s3Region          string
	s3Endpoint        string
	s3AccessKeyID     string
	s3SecretAccessKey string

	azureAccountName string
	azureAccountKey  string
	azureContainer   string
	azureEndpoint    string
}

// readBackupStorageFlags collects all backup-storage flag values and rejects
// missing driver-specific required fields up front so callers get a clear,
// CLI-level error instead of a late failure from backup.NewStorage.
func readBackupStorageFlags(cmd *cobra.Command) (backupStorageFlags, error) {
	f := backupStorageFlags{}
	f.driver, _ = cmd.Flags().GetString("driver")
	f.s3Bucket, _ = cmd.Flags().GetString("s3-bucket")
	f.s3Region, _ = cmd.Flags().GetString("s3-region")
	f.s3Endpoint, _ = cmd.Flags().GetString("s3-endpoint")
	f.s3AccessKeyID, _ = cmd.Flags().GetString("s3-access-key-id")
	f.s3SecretAccessKey, _ = cmd.Flags().GetString("s3-secret-access-key")
	f.azureAccountName, _ = cmd.Flags().GetString("azure-account-name")
	f.azureAccountKey, _ = cmd.Flags().GetString("azure-account-key")
	f.azureContainer, _ = cmd.Flags().GetString("azure-container")
	f.azureEndpoint, _ = cmd.Flags().GetString("azure-endpoint")

	switch f.driver {
	case "s3":
		if f.s3Bucket == "" {
			return backupStorageFlags{}, errors.New("--s3-bucket is required when --driver=s3")
		}
	case "azure":
		var missing []string

		if f.azureAccountName == "" {
			missing = append(missing, "--azure-account-name")
		}

		if f.azureContainer == "" {
			missing = append(missing, "--azure-container")
		}

		if len(missing) > 0 {
			return backupStorageFlags{}, fmt.Errorf("%v is required when --driver=azure", missing)
		}
	default:
		return backupStorageFlags{}, fmt.Errorf("unsupported backup driver %q (supported: s3, azure)", f.driver)
	}

	return f, nil
}

// BackupStorageFromFlags builds the BackupStorage oneof from the flags
// registered by AddBackupStorageFlags. The driver flag selects which provider
// oneof case is populated; an unknown driver or missing driver-specific
// required field is rejected with a clear error before any RPC is issued.
func BackupStorageFromFlags(cmd *cobra.Command) (*commonpb.BackupStorage, error) {
	f, err := readBackupStorageFlags(cmd)
	if err != nil {
		return nil, err
	}

	switch f.driver {
	case "s3":
		return &commonpb.BackupStorage{
			Provider: &commonpb.BackupStorage_S3{
				S3: &commonpb.S3StorageConfig{
					Bucket:          f.s3Bucket,
					Region:          f.s3Region,
					Endpoint:        f.s3Endpoint,
					AccessKeyId:     f.s3AccessKeyID,
					SecretAccessKey: f.s3SecretAccessKey,
				},
			},
		}, nil
	case "azure":
		return &commonpb.BackupStorage{
			Provider: &commonpb.BackupStorage_Azure{
				Azure: &commonpb.AzureStorageConfig{
					AccountName: f.azureAccountName,
					AccountKey:  f.azureAccountKey,
					Container:   f.azureContainer,
					Endpoint:    f.azureEndpoint,
				},
			},
		}, nil
	default:
		// readBackupStorageFlags already rejected unknown drivers; this is
		// defensive to keep the switch exhaustive.
		return nil, fmt.Errorf("unsupported backup driver %q (supported: s3, azure)", f.driver)
	}
}

// BackupStorageConfigFromFlags builds a backup.StorageConfig directly from the
// flags registered by AddBackupStorageFlags. This is the offline counterpart of
// BackupStorageFromFlags: gRPC commands send the BackupStorage proto to the
// server, while offline commands (e.g. bootstrap) instantiate Storage locally.
func BackupStorageConfigFromFlags(cmd *cobra.Command) (backup.StorageConfig, error) {
	f, err := readBackupStorageFlags(cmd)
	if err != nil {
		return backup.StorageConfig{}, err
	}

	switch f.driver {
	case "s3":
		return backup.StorageConfig{
			Driver:            "s3",
			S3Bucket:          f.s3Bucket,
			S3Region:          f.s3Region,
			S3Endpoint:        f.s3Endpoint,
			S3AccessKeyID:     f.s3AccessKeyID,
			S3SecretAccessKey: f.s3SecretAccessKey,
		}, nil
	case "azure":
		return backup.StorageConfig{
			Driver:           "azure",
			AzureAccountName: f.azureAccountName,
			AzureAccountKey:  f.azureAccountKey,
			AzureContainer:   f.azureContainer,
			AzureEndpoint:    f.azureEndpoint,
		}, nil
	default:
		return backup.StorageConfig{}, fmt.Errorf("unsupported backup driver %q (supported: s3, azure)", f.driver)
	}
}
