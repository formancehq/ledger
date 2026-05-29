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

	backupRequest := func() *clusterpb.BackupRequest {
		return &clusterpb.BackupRequest{
			Driver:     "s3",
			BucketId:   "test-cluster",
			S3Bucket:   backupS3Bucket,
			S3Region:   backupS3Region,
			S3Endpoint: minioEndpoint,
		}
	}

	incrementalBackupRequest := func() *clusterpb.IncrementalBackupRequest {
		return &clusterpb.IncrementalBackupRequest{
			Driver:     "s3",
			BucketId:   "test-cluster",
			S3Bucket:   backupS3Bucket,
			S3Region:   backupS3Region,
			S3Endpoint: minioEndpoint,
		}
	}

	It("should create a full backup on S3 with checkpoint manifest", func() {
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

		// Verify all checkpoint files exist on S3
		for filename := range manifest.Checkpoint.Files {
			key := backupS3DataPrefix + filename
			Expect(backupS3ObjectExists(ctx, s3Client, key)).To(BeTrue(),
				"S3 object %s should exist", key)
		}
	})

	It("should update the backup after adding more data on S3", func() {
		manifestBefore, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		timestampBefore := manifestBefore.Checkpoint.Timestamp

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

		// Trigger another full backup
		resp, err := clusterClient.Backup(ctx, backupRequest())
		Expect(err).To(Succeed())
		Expect(resp.GetTotalFiles()).To(BeNumerically(">", 0))

		// Verify manifest was updated
		manifestAfter, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		Expect(manifestAfter.Checkpoint.Timestamp).NotTo(Equal(timestampBefore))

		// Verify all checkpoint files exist on S3
		for filename := range manifestAfter.Checkpoint.Files {
			key := backupS3DataPrefix + filename
			Expect(backupS3ObjectExists(ctx, s3Client, key)).To(BeTrue(),
				"S3 object %s should exist after backup", key)
		}
	})

	It("should succeed with incremental backup without a prior checkpoint", func() {
		// Use a fresh bucket-id with no prior manifest
		resp, err := clusterClient.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{
			Driver:     "s3",
			BucketId:   "no-prior",
			S3Bucket:   backupS3Bucket,
			S3Region:   backupS3Region,
			S3Endpoint: minioEndpoint,
		})
		Expect(err).To(Succeed())
		Expect(resp.GetLogEntriesExported()).To(BeNumerically(">", 0))
		Expect(resp.GetAuditEntriesExported()).To(BeNumerically(">", 0))

		// Verify manifest was created with no checkpoint and with exports
		manifest, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		// The "no-prior" bucket has its own manifest key, read it directly
		noPriorManifest, err := func() (*backup.Manifest, error) {
			output, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(backupS3Bucket),
				Key:    aws.String("no-prior/backups/manifest.json"),
			})
			if err != nil {
				return nil, err
			}
			defer func() { _ = output.Body.Close() }()
			data, err := io.ReadAll(output.Body)
			if err != nil {
				return nil, err
			}
			var m backup.Manifest
			if err := json.Unmarshal(data, &m); err != nil {
				return nil, err
			}
			return &m, nil
		}()
		Expect(err).To(Succeed())
		Expect(noPriorManifest.Checkpoint).To(BeNil())
		Expect(noPriorManifest.Exports).NotTo(BeEmpty())
		_ = manifest // main manifest is unaffected
	})

	It("should export new entries incrementally after a full backup", func() {
		// Ensure we have a clean full backup
		fullResp, err := clusterClient.Backup(ctx, backupRequest())
		Expect(err).To(Succeed())
		checkpointLogSeq := fullResp.GetLastLogSequence()
		checkpointAuditSeq := fullResp.GetLastAuditSequence()

		// Add more data
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateForceTransactionAction("s3-backup-test",
					[]*commonpb.Posting{
						actions.NewPosting("world", "users:charlie", big.NewInt(500), "GBP"),
					},
					nil,
				),
			},
		})
		Expect(err).To(Succeed())

		// Run incremental backup
		incrResp, err := clusterClient.IncrementalBackup(ctx, incrementalBackupRequest())
		Expect(err).To(Succeed())
		Expect(incrResp.GetLogEntriesExported()).To(BeNumerically(">", 0))
		Expect(incrResp.GetAuditEntriesExported()).To(BeNumerically(">", 0))
		Expect(incrResp.GetSegmentsUploaded()).To(BeNumerically(">", 0))
		Expect(incrResp.GetLastLogSequence()).To(BeNumerically(">", checkpointLogSeq))
		Expect(incrResp.GetLastAuditSequence()).To(BeNumerically(">", checkpointAuditSeq))

		// Verify manifest has exports
		manifest, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		Expect(manifest.Checkpoint).NotTo(BeNil())
		Expect(manifest.Exports).NotTo(BeEmpty())

		// Verify export segments exist on S3
		for _, seg := range manifest.Exports {
			Expect(backupS3ObjectExists(ctx, s3Client, seg.Key)).To(BeTrue(),
				"export segment %s should exist", seg.Key)
		}
	})

	It("should accumulate multiple incremental exports", func() {
		// First incremental is already done from previous test
		manifestBefore, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		exportCountBefore := len(manifestBefore.Exports)

		// Add more data
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateForceTransactionAction("s3-backup-test",
					[]*commonpb.Posting{
						actions.NewPosting("world", "users:dave", big.NewInt(200), "JPY"),
					},
					nil,
				),
			},
		})
		Expect(err).To(Succeed())

		// Run another incremental
		_, err = clusterClient.IncrementalBackup(ctx, incrementalBackupRequest())
		Expect(err).To(Succeed())

		// Verify exports accumulated
		manifestAfter, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		Expect(len(manifestAfter.Exports)).To(BeNumerically(">", exportCountBefore))
	})

	It("should no-op when no new data since last export", func() {
		resp, err := clusterClient.IncrementalBackup(ctx, incrementalBackupRequest())
		Expect(err).To(Succeed())
		Expect(resp.GetLogEntriesExported()).To(BeZero())
		Expect(resp.GetAuditEntriesExported()).To(BeZero())
		Expect(resp.GetSegmentsUploaded()).To(BeZero())
	})

	It("should clean up old exports when a new full backup is taken", func() {
		// Verify exports exist before
		manifestBefore, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		Expect(manifestBefore.Exports).NotTo(BeEmpty())

		oldExportKeys := make([]string, len(manifestBefore.Exports))
		for i, seg := range manifestBefore.Exports {
			oldExportKeys[i] = seg.Key
		}

		// Take a new full backup — this should clean up old exports
		_, err = clusterClient.Backup(ctx, backupRequest())
		Expect(err).To(Succeed())

		// Verify exports are cleared in manifest
		manifestAfter, err := readS3BackupManifest(ctx, s3Client)
		Expect(err).To(Succeed())
		Expect(manifestAfter.Exports).To(BeEmpty())

		// Verify old export segments were deleted from S3
		for _, key := range oldExportKeys {
			Expect(backupS3ObjectExists(ctx, s3Client, key)).To(BeFalse(),
				"old export segment %s should have been cleaned up", key)
		}
	})
})
