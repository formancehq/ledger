//go:build e2e && azure

package business

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

const (
	// Azurite ships a single well-known account whose name and key are fixed.
	azuriteAccountName = "devstoreaccount1"
	azuriteAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

	backupAzureContainer = "backup-e2e"
	azureBackupHTTPPort  = 16300
	azureBackupGRPCPort  = 16400
	azureManifestKey     = "test-cluster/backups/manifest.json"
	azureBackupDataPfx   = "test-cluster/backups/data/"
)

// readAzureBackupManifest fetches and parses the backup manifest from Azure Blob.
func readAzureBackupManifest(ctx context.Context, client *azblob.Client) (*backup.Manifest, error) {
	resp, err := client.DownloadStream(ctx, backupAzureContainer, azureManifestKey, nil)
	if err != nil {
		return nil, err
	}

	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var manifest backup.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

// azureBlobExists checks whether a blob exists in the backup container.
func azureBlobExists(ctx context.Context, client *azblob.Client, key string) bool {
	_, err := client.DownloadStream(ctx, backupAzureContainer, key, nil)

	return err == nil
}

var _ = Describe("Azure Blob Backup", Ordered, func() {
	var (
		ctx           context.Context
		client        servicepb.BucketServiceClient
		clusterClient clusterpb.ClusterServiceClient
		azureClient   *azblob.Client
		azureEndpoint string
	)

	BeforeAll(func() {
		// Start Azurite (the Azure Storage emulator). --blobHost 0.0.0.0 is
		// required so the mapped port is reachable from the host process.
		container, err := testcontainers.Run(context.Background(), "mcr.microsoft.com/azure-storage/azurite:latest",
			testcontainers.WithCmd("azurite-blob", "--blobHost", "0.0.0.0"),
			testcontainers.WithExposedPorts("10000/tcp"),
			testcontainers.WithWaitStrategy(
				wait.ForListeningPort("10000/tcp").WithStartupTimeout(30*time.Second),
			),
		)
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = container.Terminate(context.Background()) })

		host, err := container.Host(context.Background())
		Expect(err).To(Succeed())

		mappedPort, err := container.MappedPort(context.Background(), "10000/tcp")
		Expect(err).To(Succeed())

		// Azurite uses path-style URLs that embed the account name.
		azureEndpoint = fmt.Sprintf("http://%s:%s/%s", host, mappedPort.Port(), azuriteAccountName)

		cred, err := azblob.NewSharedKeyCredential(azuriteAccountName, azuriteAccountKey)
		Expect(err).To(Succeed())

		azureClient, err = azblob.NewClientWithSharedKeyCredential(azureEndpoint, cred, nil)
		Expect(err).To(Succeed())

		_, err = azureClient.CreateContainer(context.Background(), backupAzureContainer, nil)
		Expect(err).To(Succeed())

		// Start single-node ledger server (backend config travels in the request).
		ctx, client, clusterClient = testutil.SetupSingleNode(azureBackupHTTPPort, azureBackupGRPCPort)
	})

	azureStorage := func() *commonpb.BackupStorage {
		return testutil.AzureBackupStorage(&commonpb.AzureStorageConfig{
			AccountName: azuriteAccountName,
			AccountKey:  azuriteAccountKey,
			Container:   backupAzureContainer,
			Endpoint:    azureEndpoint,
		})
	}

	It("should create a full backup on Azure with checkpoint manifest", func() {
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateLedgerAction("azure-backup-test", nil),
			},
		})
		Expect(err).To(Succeed())

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateForceTransactionAction("azure-backup-test",
					[]*commonpb.Posting{
						actions.NewPosting("world", "users:alice", big.NewInt(1000), "USD"),
					},
					nil,
				),
			},
		})
		Expect(err).To(Succeed())

		resp, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{
			Storage:  azureStorage(),
			BucketId: "test-cluster",
		})
		Expect(err).To(Succeed())
		Expect(resp.GetTotalFiles()).To(BeNumerically(">", 0))
		Expect(resp.GetFilesUploaded()).To(BeNumerically(">", 0))

		manifest, err := readAzureBackupManifest(ctx, azureClient)
		Expect(err).To(Succeed())
		Expect(manifest.Checkpoint).NotTo(BeNil())
		Expect(manifest.Checkpoint.Files).NotTo(BeEmpty())
		Expect(manifest.Exports).To(BeEmpty())

		for filename := range manifest.Checkpoint.Files {
			key := azureBackupDataPfx + filename
			Expect(azureBlobExists(ctx, azureClient, key)).To(BeTrue(),
				"Azure blob %s should exist", key)
		}
	})

	It("should export new entries incrementally after a full backup", func() {
		fullResp, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{
			Storage:  azureStorage(),
			BucketId: "test-cluster",
		})
		Expect(err).To(Succeed())
		checkpointLogSeq := fullResp.GetLastLogSequence()

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateForceTransactionAction("azure-backup-test",
					[]*commonpb.Posting{
						actions.NewPosting("world", "users:charlie", big.NewInt(500), "GBP"),
					},
					nil,
				),
			},
		})
		Expect(err).To(Succeed())

		incrResp, err := clusterClient.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{
			Storage:  azureStorage(),
			BucketId: "test-cluster",
		})
		Expect(err).To(Succeed())
		Expect(incrResp.GetLogEntriesExported()).To(BeNumerically(">", 0))
		Expect(incrResp.GetSegmentsUploaded()).To(BeNumerically(">", 0))
		Expect(incrResp.GetLastLogSequence()).To(BeNumerically(">", checkpointLogSeq))

		manifest, err := readAzureBackupManifest(ctx, azureClient)
		Expect(err).To(Succeed())
		Expect(manifest.Exports).NotTo(BeEmpty())

		for _, seg := range manifest.Exports {
			Expect(azureBlobExists(ctx, azureClient, seg.Key)).To(BeTrue(),
				"export segment %s should exist", seg.Key)
		}
	})
})
