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

	"github.com/formancehq/ledger-v3-poc/internal/infra/backup"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
)

const (
	backupMinioAccessKey = "minioadmin"
	backupMinioSecretKey = "minioadmin"
	backupS3Bucket       = "backup-e2e"
	backupS3Region       = "us-east-1"
	s3BackupHTTPPort2    = 15900
	s3BackupGRPCPort2    = 16000
	backupManifestKey    = "test-cluster/backups/manifest.json"
	backupS3DataPrefix   = "test-cluster/backups/data/"
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
		container, err := testcontainers.Run(context.Background(), "minio/minio:latest",
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
		ctx, client, clusterClient = testutil.SetupSingleNode(s3BackupHTTPPort2, s3BackupGRPCPort2)
	})

	It("should create an backup on S3", func() {
		// Create a ledger with data
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateLedgerAction("s3-backup-test", nil),
			},
		})
		Expect(err).To(Succeed())

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateForceTransactionAction("s3-backup-test",
					[]*commonpb.Posting{
						actions.NewPosting("world", "users:alice", big.NewInt(1000), "USD"),
					},
					nil,
				),
			},
		})
		Expect(err).To(Succeed())

		// Trigger backup via gRPC
		resp, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{
			Driver:     "s3",
			S3Bucket:   backupS3Bucket,
			S3Region:   backupS3Region,
			S3Endpoint: minioEndpoint,
		})
		Expect(err).To(Succeed())
		Expect(resp.GetTotalFiles()).To(BeNumerically(">", 0))
		Expect(resp.GetFilesUploaded()).To(BeNumerically(">", 0))

		// Verify manifest exists on S3
		manifest, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		Expect(manifest.Files).NotTo(BeEmpty())

		// Verify all referenced files exist on S3
		for filename := range manifest.Files {
			key := backupS3DataPrefix + filename
			Expect(backupS3ObjectExists(ctx, s3Client, key)).To(BeTrue(),
				"S3 object %s should exist", key)
		}
	})

	It("should update the backup after adding more data on S3", func() {
		manifestBefore, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		timestampBefore := manifestBefore.Timestamp

		// Add more data
		for i := range 5 {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction("s3-backup-test",
						[]*commonpb.Posting{
							actions.NewPosting("world", "users:bob", big.NewInt(int64(100*(i+1))), "EUR"),
						},
						nil,
					),
				},
			})
			Expect(err).To(Succeed())
		}

		// Trigger another backup
		resp, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{
			Driver:     "s3",
			S3Bucket:   backupS3Bucket,
			S3Region:   backupS3Region,
			S3Endpoint: minioEndpoint,
		})
		Expect(err).To(Succeed())
		Expect(resp.GetTotalFiles()).To(BeNumerically(">", 0))

		// Verify manifest was updated
		manifestAfter, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		Expect(manifestAfter.Timestamp).NotTo(Equal(timestampBefore))

		// Verify all files in updated manifest exist on S3
		for filename := range manifestAfter.Files {
			key := backupS3DataPrefix + filename
			Expect(backupS3ObjectExists(ctx, s3Client, key)).To(BeTrue(),
				"S3 object %s should exist after backup", key)
		}
	})
})
