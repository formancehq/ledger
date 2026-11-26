package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestStorageConfigFromProto_S3 maps the S3 oneof into the backup
// package's StorageConfig. Every field on the proto must land on the
// matching StorageConfig field; no defaulting allowed here — that lives
// on the inner backup.NewStorage layer.
func TestStorageConfigFromProto_S3(t *testing.T) {
	t.Parallel()

	in := &commonpb.BackupStorage{
		Provider: &commonpb.BackupStorage_S3{
			S3: &commonpb.S3StorageConfig{
				Bucket:          "my-bucket",
				Region:          "eu-west-1",
				Endpoint:        "https://s3.example.com",
				AccessKeyId:     "AKIA...",
				SecretAccessKey: "secret",
			},
		},
	}

	cfg, err := storageConfigFromProto(in)
	require.NoError(t, err)
	require.Equal(t, "s3", cfg.Driver)
	require.Equal(t, "my-bucket", cfg.S3Bucket)
	require.Equal(t, "eu-west-1", cfg.S3Region)
	require.Equal(t, "https://s3.example.com", cfg.S3Endpoint)
	require.Equal(t, "AKIA...", cfg.S3AccessKeyID)
	require.Equal(t, "secret", cfg.S3SecretAccessKey)
}

// TestStorageConfigFromProto_Azure mirrors the S3 test for the Azure
// oneof. Same expectation: every field crosses cleanly.
func TestStorageConfigFromProto_Azure(t *testing.T) {
	t.Parallel()

	in := &commonpb.BackupStorage{
		Provider: &commonpb.BackupStorage_Azure{
			Azure: &commonpb.AzureStorageConfig{
				AccountName: "myaccount",
				AccountKey:  "key",
				Container:   "backups",
				Endpoint:    "https://myaccount.blob.core.windows.net",
			},
		},
	}

	cfg, err := storageConfigFromProto(in)
	require.NoError(t, err)
	require.Equal(t, "azure", cfg.Driver)
	require.Equal(t, "myaccount", cfg.AzureAccountName)
	require.Equal(t, "key", cfg.AzureAccountKey)
	require.Equal(t, "backups", cfg.AzureContainer)
	require.Equal(t, "https://myaccount.blob.core.windows.net", cfg.AzureEndpoint)
}

// TestStorageConfigFromProto_UnsetProviderInvalidArgument pins the
// contract that an unset provider oneof is a client error, not a
// silent default. Surfaces as codes.InvalidArgument so the gRPC
// client can distinguish bad input from server-side failures.
func TestStorageConfigFromProto_UnsetProviderInvalidArgument(t *testing.T) {
	t.Parallel()

	_, err := storageConfigFromProto(&commonpb.BackupStorage{})
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok, "must be a gRPC status error")
	require.Equal(t, codes.InvalidArgument, st.Code())
}
