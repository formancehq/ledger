package cmdutil_test

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func newBackupStorageCmd(args ...string) (*cobra.Command, error) {
	cmd := &cobra.Command{Use: "test"}
	cmdutil.AddBackupStorageFlags(cmd)
	cmd.SetArgs(args)
	cmd.RunE = func(*cobra.Command, []string) error { return nil }

	if err := cmd.Execute(); err != nil {
		return nil, err
	}

	return cmd, nil
}

func TestBackupStorageFromFlags_S3(t *testing.T) {
	t.Parallel()

	cmd, err := newBackupStorageCmd(
		"--driver", "s3",
		"--s3-bucket", "my-bucket",
		"--s3-region", "us-east-1",
		"--s3-endpoint", "https://minio.local",
		"--s3-access-key-id", "AKID",
		"--s3-secret-access-key", "SECRET",
	)
	require.NoError(t, err)

	storage, err := cmdutil.BackupStorageFromFlags(cmd)
	require.NoError(t, err)

	s3 := storage.GetS3()
	require.NotNil(t, s3, "S3 provider must be populated for driver=s3")
	require.Equal(t, "my-bucket", s3.GetBucket())
	require.Equal(t, "us-east-1", s3.GetRegion())
	require.Equal(t, "https://minio.local", s3.GetEndpoint())
	require.Equal(t, "AKID", s3.GetAccessKeyId())
	require.Equal(t, "SECRET", s3.GetSecretAccessKey())
	require.Nil(t, storage.GetAzure(), "Azure provider must remain unset for driver=s3")
}

func TestBackupStorageFromFlags_Azure(t *testing.T) {
	t.Parallel()

	cmd, err := newBackupStorageCmd(
		"--driver", "azure",
		"--azure-account-name", "myaccount",
		"--azure-account-key", "AZKEY",
		"--azure-container", "backups",
		"--azure-endpoint", "http://127.0.0.1:10000/devstoreaccount1",
	)
	require.NoError(t, err)

	storage, err := cmdutil.BackupStorageFromFlags(cmd)
	require.NoError(t, err)

	azure := storage.GetAzure()
	require.NotNil(t, azure, "Azure provider must be populated for driver=azure")
	require.Equal(t, "myaccount", azure.GetAccountName())
	require.Equal(t, "AZKEY", azure.GetAccountKey())
	require.Equal(t, "backups", azure.GetContainer())
	require.Equal(t, "http://127.0.0.1:10000/devstoreaccount1", azure.GetEndpoint())
	require.Nil(t, storage.GetS3(), "S3 provider must remain unset for driver=azure")
}

func TestBackupStorageFromFlags_UnknownDriver(t *testing.T) {
	t.Parallel()

	cmd, err := newBackupStorageCmd("--driver", "gcs")
	require.NoError(t, err)

	storage, err := cmdutil.BackupStorageFromFlags(cmd)
	require.Error(t, err)
	require.Nil(t, storage)
	require.Contains(t, err.Error(), "unsupported backup driver")
}

func TestBackupStorageFromFlags_DefaultDriverIsS3(t *testing.T) {
	t.Parallel()

	// No --driver supplied: the default ("s3") must apply so users can rely on
	// the documented default without always passing the flag explicitly.
	cmd, err := newBackupStorageCmd("--s3-bucket", "my-bucket")
	require.NoError(t, err)

	storage, err := cmdutil.BackupStorageFromFlags(cmd)
	require.NoError(t, err)

	_, ok := storage.GetProvider().(*commonpb.BackupStorage_S3)
	require.True(t, ok, "default driver must populate the S3 provider oneof")
	require.Equal(t, "my-bucket", storage.GetS3().GetBucket())
}

func TestBackupStorageConfigFromFlags_S3(t *testing.T) {
	t.Parallel()

	cmd, err := newBackupStorageCmd(
		"--driver", "s3",
		"--s3-bucket", "my-bucket",
		"--s3-region", "us-east-1",
		"--s3-endpoint", "https://minio.local",
		"--s3-access-key-id", "AKID",
		"--s3-secret-access-key", "SECRET",
	)
	require.NoError(t, err)

	cfg, err := cmdutil.BackupStorageConfigFromFlags(cmd)
	require.NoError(t, err)
	require.Equal(t, "s3", cfg.Driver)
	require.Equal(t, "my-bucket", cfg.S3Bucket)
	require.Equal(t, "us-east-1", cfg.S3Region)
	require.Equal(t, "https://minio.local", cfg.S3Endpoint)
	require.Equal(t, "AKID", cfg.S3AccessKeyID)
	require.Equal(t, "SECRET", cfg.S3SecretAccessKey)
	require.Empty(t, cfg.AzureAccountName, "Azure fields must remain empty for driver=s3")
}

func TestBackupStorageConfigFromFlags_Azure(t *testing.T) {
	t.Parallel()

	cmd, err := newBackupStorageCmd(
		"--driver", "azure",
		"--azure-account-name", "myaccount",
		"--azure-account-key", "AZKEY",
		"--azure-container", "backups",
		"--azure-endpoint", "http://127.0.0.1:10000/devstoreaccount1",
	)
	require.NoError(t, err)

	cfg, err := cmdutil.BackupStorageConfigFromFlags(cmd)
	require.NoError(t, err)
	require.Equal(t, "azure", cfg.Driver)
	require.Equal(t, "myaccount", cfg.AzureAccountName)
	require.Equal(t, "AZKEY", cfg.AzureAccountKey)
	require.Equal(t, "backups", cfg.AzureContainer)
	require.Equal(t, "http://127.0.0.1:10000/devstoreaccount1", cfg.AzureEndpoint)
	require.Empty(t, cfg.S3Bucket, "S3 fields must remain empty for driver=azure")
}

func TestBackupStorageConfigFromFlags_UnknownDriver(t *testing.T) {
	t.Parallel()

	cmd, err := newBackupStorageCmd("--driver", "gcs")
	require.NoError(t, err)

	cfg, err := cmdutil.BackupStorageConfigFromFlags(cmd)
	require.Error(t, err)
	require.Empty(t, cfg.Driver)
	require.Contains(t, err.Error(), "unsupported backup driver")
}

func TestBackupStorageFromFlags_MissingS3Bucket(t *testing.T) {
	t.Parallel()

	cmd, err := newBackupStorageCmd("--driver", "s3")
	require.NoError(t, err)

	storage, err := cmdutil.BackupStorageFromFlags(cmd)
	require.Error(t, err)
	require.Nil(t, storage)
	require.Contains(t, err.Error(), "--s3-bucket is required")
}

func TestBackupStorageFromFlags_MissingAzureRequired(t *testing.T) {
	t.Parallel()

	cmd, err := newBackupStorageCmd("--driver", "azure")
	require.NoError(t, err)

	storage, err := cmdutil.BackupStorageFromFlags(cmd)
	require.Error(t, err)
	require.Nil(t, storage)
	require.Contains(t, err.Error(), "--azure-account-name")
	require.Contains(t, err.Error(), "--azure-container")
}

func TestBackupStorageConfigFromFlags_MissingS3Bucket(t *testing.T) {
	t.Parallel()

	cmd, err := newBackupStorageCmd("--driver", "s3")
	require.NoError(t, err)

	cfg, err := cmdutil.BackupStorageConfigFromFlags(cmd)
	require.Error(t, err)
	require.Empty(t, cfg.Driver)
	require.Contains(t, err.Error(), "--s3-bucket is required")
}
