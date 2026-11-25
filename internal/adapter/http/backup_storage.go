package http

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// backupStorageBody selects exactly one backup storage backend.
type backupStorageBody struct {
	S3    *s3StorageBody    `json:"s3"`
	Azure *azureStorageBody `json:"azure"`
}

type s3StorageBody struct {
	Bucket          string `json:"bucket"`
	Region          string `json:"region"`
	Endpoint        string `json:"endpoint"`
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
}

type azureStorageBody struct {
	AccountName string `json:"accountName"`
	AccountKey  string `json:"accountKey"`
	Container   string `json:"container"`
	Endpoint    string `json:"endpoint"`
}

var errBackupStorageProvider = errors.New("storage must set exactly one of s3 or azure")

func (b *backupStorageBody) toProto() (*commonpb.BackupStorage, error) {
	switch {
	case b == nil || (b.S3 == nil) == (b.Azure == nil):
		return nil, errBackupStorageProvider
	case b.S3 != nil:
		if b.S3.Bucket == "" {
			return nil, errors.New("storage.s3.bucket is required")
		}

		return &commonpb.BackupStorage{Provider: &commonpb.BackupStorage_S3{S3: &commonpb.S3StorageConfig{
			Bucket:          b.S3.Bucket,
			Region:          b.S3.Region,
			Endpoint:        b.S3.Endpoint,
			AccessKeyId:     b.S3.AccessKeyID,
			SecretAccessKey: b.S3.SecretAccessKey,
		}}}, nil
	default:
		if b.Azure.AccountName == "" || b.Azure.Container == "" {
			return nil, errors.New("storage.azure.accountName and storage.azure.container are required")
		}

		return &commonpb.BackupStorage{Provider: &commonpb.BackupStorage_Azure{Azure: &commonpb.AzureStorageConfig{
			AccountName: b.Azure.AccountName,
			AccountKey:  b.Azure.AccountKey,
			Container:   b.Azure.Container,
			Endpoint:    b.Azure.Endpoint,
		}}}, nil
	}
}

// requireEndpointCredentials refuses a custom endpoint without explicit keys:
// the storage client would otherwise authenticate to that endpoint with the
// process's ambient cloud identity.
func (b *backupStorageBody) requireEndpointCredentials() error {
	switch {
	case b.S3 != nil && b.S3.Endpoint != "" && (b.S3.AccessKeyID == "" || b.S3.SecretAccessKey == ""):
		return errors.New("storage.s3.endpoint requires storage.s3.accessKeyId and storage.s3.secretAccessKey")
	case b.Azure != nil && b.Azure.Endpoint != "" && b.Azure.AccountKey == "":
		return errors.New("storage.azure.endpoint requires storage.azure.accountKey")
	default:
		return nil
	}
}
