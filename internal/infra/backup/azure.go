//go:build azure

package backup

import (
	"context"
	"fmt"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

// AzureStorage implements Storage using Azure Blob Storage.
type AzureStorage struct {
	client    *azblob.Client
	container string
}

// NewAzureStorage creates a new AzureStorage backed by the given Azure Blob client and container.
func NewAzureStorage(client *azblob.Client, container string) *AzureStorage {
	return &AzureStorage{
		client:    client,
		container: container,
	}
}

// azureUploadBlockSize is the per-block buffer used by UploadStream. 8 MiB
// keeps memory bounded while staying well under the 4000-block-per-blob and
// 4 GiB-per-block limits, so it works for everything from small manifests to
// multi-GB SST files without per-call tuning.
const azureUploadBlockSize = 8 * 1024 * 1024

// azureUploadConcurrency caps in-flight block uploads. Five matches the S3 SDK's
// default and balances throughput against per-upload memory (concurrency × block size).
const azureUploadConcurrency = 5

func (s *AzureStorage) PutFile(ctx context.Context, key string, data io.Reader, _ int64) error {
	_, err := s.client.UploadStream(ctx, s.container, key, data, &azblob.UploadStreamOptions{
		BlockSize:   azureUploadBlockSize,
		Concurrency: azureUploadConcurrency,
	})
	if err != nil {
		return fmt.Errorf("azure upload %s: %w", key, err)
	}

	return nil
}

func (s *AzureStorage) GetFile(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := s.client.DownloadStream(ctx, s.container, key, nil)
	if err != nil {
		return nil, mapDownloadErr(key, err)
	}

	return resp.Body, nil
}

// mapDownloadErr wraps a DownloadStream error, translating Azure's
// blob/container not-found codes into the storage-agnostic ErrFileNotFound so
// callers can use errors.Is without importing Azure types.
func mapDownloadErr(key string, err error) error {
	if bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
		return fmt.Errorf("azure download %s: %w", key, ErrFileNotFound)
	}

	return fmt.Errorf("azure download %s: %w", key, err)
}

func (s *AzureStorage) DeleteFile(ctx context.Context, key string) error {
	_, err := s.client.DeleteBlob(ctx, s.container, key, nil)
	if err != nil {
		return fmt.Errorf("azure delete %s: %w", key, err)
	}

	return nil
}

func (s *AzureStorage) ListFiles(ctx context.Context, prefix string) ([]string, error) {
	var keys []string

	pager := s.client.NewListBlobsFlatPager(s.container, &azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("azure list %s: %w", prefix, err)
		}

		for _, blob := range page.Segment.BlobItems {
			if blob.Name != nil {
				keys = append(keys, *blob.Name)
			}
		}
	}

	return keys, nil
}

var _ Storage = (*AzureStorage)(nil)

// NewAzureBackupStorage creates a Storage backed by Azure Blob Storage.
// If accountKey is empty, DefaultAzureCredential is used (managed identity, env vars, etc.).
// If endpoint is non-empty it overrides the default service URL, which is useful for Azurite.
func NewAzureBackupStorage(accountName, accountKey, container, endpoint string) (Storage, error) {
	serviceURL := endpoint
	if serviceURL == "" {
		serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	}

	var (
		client *azblob.Client
		err    error
	)

	if accountKey != "" {
		cred, credErr := azblob.NewSharedKeyCredential(accountName, accountKey)
		if credErr != nil {
			return nil, fmt.Errorf("azure shared key credential: %w", credErr)
		}

		client, err = azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	} else {
		cred, credErr := azidentity.NewDefaultAzureCredential(nil)
		if credErr != nil {
			return nil, fmt.Errorf("azure default credential: %w", credErr)
		}

		client, err = azblob.NewClient(serviceURL, cred, nil)
	}

	if err != nil {
		return nil, fmt.Errorf("creating azure client: %w", err)
	}

	return NewAzureStorage(client, container), nil
}
