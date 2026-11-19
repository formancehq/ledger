//go:build e2e && s3

package business

import (
	"context"
	"encoding/json"
	"io"
	"math/big"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	backupMinioAccessKey = "minioadmin"
	backupMinioSecretKey = "minioadmin"
	backupS3Bucket       = "backup-e2e"
	backupS3Region       = "us-east-1"
	backupManifestKey    = "test-cluster/manifest.json"
	backupS3DataPrefix   = "test-cluster/data/"
)

// readS3BackupManifest fetches and parses the backup manifest from S3.
func readS3BackupManifest(ctx context.Context, client *s3.Client) (*backup.Manifest, error) {
	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(backupS3Bucket),
		Key:    aws.String(backupManifestKey),
	})
	if err != nil {
		return nil, err
	}

	defer func() { _ = output.Body.Close() }()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, err
	}

	var manifest backup.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

// backupS3ObjectExists checks if an object exists in S3.
func backupS3ObjectExists(ctx context.Context, client *s3.Client, key string) bool {
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(backupS3Bucket),
		Key:    aws.String(key),
	})

	return err == nil
}

var _ = Describe("S3 Backup", Ordered, func() {
	var (
		ctx           context.Context
		client        servicepb.BucketServiceClient
		clusterClient clusterpb.ClusterServiceClient
		s3Client      *s3.Client
		minioEndpoint string
	)

	BeforeAll(func() {
		// Start MinIO container
		container, err := testcontainers.Run(context.Background(), testutil.MinIOImage,
			testcontainers.WithEnv(map[string]string{
				"MINIO_ROOT_USER":     backupMinioAccessKey,
				"MINIO_ROOT_PASSWORD": backupMinioSecretKey,
			}),
			testcontainers.WithCmd("server", "/data"),
			testcontainers.WithExposedPorts("9000/tcp"),
			testcontainers.WithWaitStrategy(
				wait.ForHTTP("/minio/health/live").WithPort("9000/tcp").WithStartupTimeout(30*time.Second),
			),
		)
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = container.Terminate(context.Background()) })

		minioEndpoint, err = container.Endpoint(context.Background(), "http")
		Expect(err).To(Succeed())

		// Create S3 client
		cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(backupS3Region),
			awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				backupMinioAccessKey, backupMinioSecretKey, "",
			)),
		)
		Expect(err).To(Succeed())

		s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(minioEndpoint)
			o.UsePathStyle = true
		})

		// Create the backup bucket
		_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: aws.String(backupS3Bucket),
		})
		Expect(err).To(Succeed())

		// Set AWS credentials for the ledger server process
		GinkgoT().Setenv("AWS_ACCESS_KEY_ID", backupMinioAccessKey)
		GinkgoT().Setenv("AWS_SECRET_ACCESS_KEY", backupMinioSecretKey)

		// Start single-node ledger server (no backup config needed on server)
		var node *testutil.ServiceWithClient
		ctx, node = testutil.SetupSingleNode()
		client = node.Client
		clusterClient = node.ClusterClient
	})

	s3Storage := func() *commonpb.BackupStorage {
		return testutil.S3BackupStorage(&commonpb.S3StorageConfig{
			Bucket:   backupS3Bucket,
			Region:   backupS3Region,
			Endpoint: minioEndpoint,
		})
	}

	backupRequest := func() *clusterpb.BackupRequest {
		return &clusterpb.BackupRequest{
			Storage:  s3Storage(),
			BucketId: "test-cluster",
		}
	}

	incrementalBackupRequest := func() *clusterpb.IncrementalBackupRequest {
		return &clusterpb.IncrementalBackupRequest{
			Storage:  s3Storage(),
			BucketId: "test-cluster",
		}
	}

	It("should create a full backup on S3 with checkpoint manifest", func() {
		// Create a ledger with data
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("s3-backup-test", nil)))
		Expect(err).To(Succeed())

		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction("s3-backup-test",
			[]*commonpb.Posting{
				actions.NewPosting("world", "users:alice", big.NewInt(1000), "USD"),
			},
			nil,
		)))
		Expect(err).To(Succeed())

		// Trigger full backup via gRPC
		resp, err := clusterClient.Backup(ctx, backupRequest())
		Expect(err).To(Succeed())
		Expect(resp.GetTotalFiles()).To(BeNumerically(">", 0))
		Expect(resp.GetFilesUploaded()).To(BeNumerically(">", 0))
		Expect(resp.GetLastLogSequence()).To(BeNumerically(">", 0))
		Expect(resp.GetLastAuditSequence()).To(BeNumerically(">", 0))
		Expect(resp.GetLastAppliedIndex()).To(BeNumerically(">", 0))

		// Verify manifest exists and has checkpoint structure
		manifest, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		Expect(manifest.Checkpoint).NotTo(BeNil())
		Expect(manifest.Checkpoint.Files).NotTo(BeEmpty())
		Expect(manifest.Checkpoint.LastLogSequence).To(BeNumerically(">", 0))
		Expect(manifest.Checkpoint.LastAuditSequence).To(BeNumerically(">", 0))
		Expect(manifest.Exports).To(BeEmpty())

		// Verify all checkpoint files exist on S3 at their content-addressed keys.
		for _, cf := range manifest.Checkpoint.Files {
			Expect(cf.Key).To(HavePrefix(backupS3DataPrefix),
				"checkpoint object key must live under the data/ prefix")
			Expect(backupS3ObjectExists(ctx, s3Client, cf.Key)).To(BeTrue(),
				"S3 object %s should exist", cf.Key)
		}
	})

	It("should update the backup after adding more data on S3", func() {
		manifestBefore, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		timestampBefore := manifestBefore.Checkpoint.Timestamp

		// Add more data
		for i := range 5 {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction("s3-backup-test",
				[]*commonpb.Posting{
					actions.NewPosting("world", "users:bob", big.NewInt(int64(100*(i+1))), "EUR"),
				},
				nil,
			)))
			Expect(err).To(Succeed())
		}

		// Trigger another full backup
		resp, err := clusterClient.Backup(ctx, backupRequest())
		Expect(err).To(Succeed())
		Expect(resp.GetTotalFiles()).To(BeNumerically(">", 0))

		// Verify manifest was updated
		manifestAfter, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		Expect(manifestAfter.Checkpoint.Timestamp).NotTo(Equal(timestampBefore))

		// Verify all checkpoint files exist on S3 at their content-addressed keys.
		for _, cf := range manifestAfter.Checkpoint.Files {
			Expect(backupS3ObjectExists(ctx, s3Client, cf.Key)).To(BeTrue(),
				"S3 object %s should exist after backup", cf.Key)
		}
	})

	It("should make the deprecated incremental RPC publish a checkpoint", func() {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction("s3-backup-test",
			[]*commonpb.Posting{actions.NewPosting("world", "users:charlie", big.NewInt(500), "GBP")}, nil)))
		Expect(err).To(Succeed())
		response, err := clusterClient.IncrementalBackup(ctx, incrementalBackupRequest())
		Expect(err).To(Succeed())
		Expect(response.GetSegmentsUploaded()).To(BeZero())
		manifest, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		Expect(manifest.Exports).To(BeEmpty())
		Expect(manifest.Checkpoint.Files).NotTo(BeEmpty())
		for _, file := range manifest.Checkpoint.Files {
			Expect(backupS3ObjectExists(ctx, s3Client, file.Key)).To(BeTrue())
		}
	})
})
