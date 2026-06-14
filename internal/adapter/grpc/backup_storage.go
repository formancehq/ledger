package grpc

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// storageConfigFromProto maps the BackupStorage oneof to the backup package's
// StorageConfig. The provider oneof is the single source of truth for the
// driver, so an unset provider is an InvalidArgument error.
func storageConfigFromProto(s *commonpb.BackupStorage) (backup.StorageConfig, error) {
	switch p := s.GetProvider().(type) {
	case *commonpb.BackupStorage_S3:
		return backup.StorageConfig{
			Driver:            "s3",
			S3Bucket:          p.S3.GetBucket(),
			S3Region:          p.S3.GetRegion(),
			S3Endpoint:        p.S3.GetEndpoint(),
			S3AccessKeyID:     p.S3.GetAccessKeyId(),
			S3SecretAccessKey: p.S3.GetSecretAccessKey(),
		}, nil
	case *commonpb.BackupStorage_Azure:
		return backup.StorageConfig{
			Driver:           "azure",
			AzureAccountName: p.Azure.GetAccountName(),
			AzureAccountKey:  p.Azure.GetAccountKey(),
			AzureContainer:   p.Azure.GetContainer(),
			AzureEndpoint:    p.Azure.GetEndpoint(),
		}, nil
	default:
		return backup.StorageConfig{}, status.Error(codes.InvalidArgument, "backup storage provider is required (s3 or azure)")
	}
}
