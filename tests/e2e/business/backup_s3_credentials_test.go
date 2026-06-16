//go:build e2e && s3

package business

import (
	"context"
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

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

const (
	s3CredsHTTPPort  = 16100
	s3CredsGRPCPort  = 16200
	s3CredsBucket    = "creds-e2e"
	s3CredsRegion    = "us-east-1"
	s3CredsAccessKey = "minioadmin"
	s3CredsSecretKey = "minioadmin"
)

var _ = Describe("S3 Backup with explicit credentials", Ordered, func() {
	var (
		ctx           context.Context
		client        servicepb.BucketServiceClient
		clusterClient clusterpb.ClusterServiceClient
		minioEndpoint string
	)

	BeforeAll(func() {
		// Start MinIO container
		container, err := testcontainers.Run(context.Background(), "minio/minio:latest",
			testcontainers.WithEnv(map[string]string{
				"MINIO_ROOT_USER":     s3CredsAccessKey,
				"MINIO_ROOT_PASSWORD": s3CredsSecretKey,
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

		// Create the bucket via the S3 API
		cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(s3CredsRegion),
			awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				s3CredsAccessKey, s3CredsSecretKey, "",
			)),
		)
		Expect(err).To(Succeed())

		s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(minioEndpoint)
			o.UsePathStyle = true
		})

		_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: aws.String(s3CredsBucket),
		})
		Expect(err).To(Succeed())

		// Set BOGUS AWS credentials so that the default credential chain cannot authenticate.
		// This ensures only explicit credentials from the request will work.
		GinkgoT().Setenv("AWS_ACCESS_KEY_ID", "INVALID_KEY")
		GinkgoT().Setenv("AWS_SECRET_ACCESS_KEY", "INVALID_SECRET")

		ctx, client, clusterClient = testutil.SetupSingleNode(s3CredsHTTPPort, s3CredsGRPCPort)
	})

	It("should succeed with explicit credentials in the backup request", func() {
		// Create a ledger with data
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateLedgerAction("creds-test", nil),
			),
		})
		Expect(err).To(Succeed())

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateForceTransactionAction("creds-test",
					[]*commonpb.Posting{
						actions.NewPosting("world", "users:alice", big.NewInt(1000), "USD"),
					},
					nil,
				),
			),
		})
		Expect(err).To(Succeed())

		// Trigger backup with explicit credentials — the server has bogus env vars,
		// so this will only succeed if the request-level credentials are used.
		resp, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{
			Storage: testutil.S3BackupStorage(&commonpb.S3StorageConfig{
				Bucket:          s3CredsBucket,
				Region:          s3CredsRegion,
				Endpoint:        minioEndpoint,
				AccessKeyId:     s3CredsAccessKey,
				SecretAccessKey: s3CredsSecretKey,
			}),
			BucketId: "creds-cluster",
		})
		Expect(err).To(Succeed())
		Expect(resp.GetTotalFiles()).To(BeNumerically(">", 0))
		Expect(resp.GetFilesUploaded()).To(BeNumerically(">", 0))
	})

	It("should succeed with explicit credentials in the incremental backup request", func() {
		// Add more data
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateForceTransactionAction("creds-test",
					[]*commonpb.Posting{
						actions.NewPosting("world", "users:bob", big.NewInt(500), "EUR"),
					},
					nil,
				),
			),
		})
		Expect(err).To(Succeed())

		resp, err := clusterClient.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{
			Storage: testutil.S3BackupStorage(&commonpb.S3StorageConfig{
				Bucket:          s3CredsBucket,
				Region:          s3CredsRegion,
				Endpoint:        minioEndpoint,
				AccessKeyId:     s3CredsAccessKey,
				SecretAccessKey: s3CredsSecretKey,
			}),
			BucketId: "creds-cluster",
		})
		Expect(err).To(Succeed())
		Expect(resp.GetLogEntriesExported()).To(BeNumerically(">", 0))
	})

	It("should fail without explicit credentials when env vars are bogus", func() {
		// Same request but without explicit credentials — should fail because
		// the server process has bogus AWS env vars.
		_, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{
			Storage: testutil.S3BackupStorage(&commonpb.S3StorageConfig{
				Bucket:   s3CredsBucket,
				Region:   s3CredsRegion,
				Endpoint: minioEndpoint,
			}),
			BucketId: "creds-cluster-fail",
		})
		Expect(err).To(HaveOccurred())
	})
})
