//go:build e2e && s3

package coldstorage

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

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
)

const (
	minioAccessKey = "minioadmin"
	minioSecretKey = "minioadmin"
	s3Bucket       = "cold-storage-e2e"
	s3Region       = "us-east-1"
)

// listAllPeriods collects all periods from the streaming RPC.
func listAllPeriods(ctx context.Context, client servicepb.BucketServiceClient) ([]*commonpb.Period, error) {
	return actions.ListAllPeriods(ctx, client)
}

var _ = Describe("Cold Storage S3", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		httpPort = 15500
		grpcPort = 15600
	)

	BeforeAll(func() {
		// Start MinIO container
		container, err := testcontainers.Run(context.Background(), "minio/minio:latest",
			testcontainers.WithEnv(map[string]string{
				"MINIO_ROOT_USER":     minioAccessKey,
				"MINIO_ROOT_PASSWORD": minioSecretKey,
			}),
			testcontainers.WithCmd("server", "/data"),
			testcontainers.WithExposedPorts("9000/tcp"),
			testcontainers.WithWaitStrategy(
				wait.ForHTTP("/minio/health/live").WithPort("9000/tcp").WithStartupTimeout(30*time.Second),
			),
		)
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = container.Terminate(context.Background()) })

		endpoint, err := container.Endpoint(context.Background(), "http")
		Expect(err).To(Succeed())

		// Create the S3 bucket
		cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(s3Region),
			awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				minioAccessKey, minioSecretKey, "",
			)),
		)
		Expect(err).To(Succeed())

		s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})

		_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: aws.String(s3Bucket),
		})
		Expect(err).To(Succeed())

		// Set AWS credentials for the ledger server process
		GinkgoT().Setenv("AWS_ACCESS_KEY_ID", minioAccessKey)
		GinkgoT().Setenv("AWS_SECRET_ACCESS_KEY", minioSecretKey)

		// Start single-node ledger server with S3 cold storage
		ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort,
			testserver.WithColdStorageS3(s3Bucket, s3Region, endpoint),
		)
	})

	It("Should archive a period to S3 and read back logs from cold storage", func() {
		const ledger = "cold-s3-test"

		// Create a ledger
		resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateLedgerAction(ledger, nil),
			},
		})
		Expect(err).To(Succeed())
		Expect(resp.Logs).To(HaveLen(1))

		// Record the log sequence of the CreateLedger log
		createLedgerSeq := resp.Logs[0].Sequence

		// Create a transaction
		resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateForceTransactionAction(ledger,
					[]*commonpb.Posting{
						actions.NewPosting("world", "users:alice", big.NewInt(1000), "USD"),
					},
					nil,
				),
			},
		})
		Expect(err).To(Succeed())
		Expect(resp.Logs).To(HaveLen(1))

		txSeq := resp.Logs[0].Sequence

		// Close the current period
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.ClosePeriodAction(),
			},
		})
		Expect(err).To(Succeed())

		// Wait for the period to be sealed (CLOSED)
		var closedPeriodID uint64
		Eventually(func(g Gomega) {
			periods, err := listAllPeriods(ctx, client)
			g.Expect(err).To(Succeed())
			g.Expect(len(periods)).To(BeNumerically(">=", 2))

			for _, p := range periods {
				if p.Status == commonpb.PeriodStatus_PERIOD_CLOSED {
					closedPeriodID = p.GetId()

					return
				}
			}

			g.Expect(false).To(BeTrue(), "no CLOSED period found")
		}).Within(15 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

		// Archive the closed period
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.ArchivePeriodAction(closedPeriodID),
			},
		})
		Expect(err).To(Succeed())

		// Wait for the period to become ARCHIVED
		Eventually(func(g Gomega) {
			periods, err := listAllPeriods(ctx, client)
			g.Expect(err).To(Succeed())

			for _, p := range periods {
				if p.GetId() == closedPeriodID {
					g.Expect(p.Status).To(Equal(commonpb.PeriodStatus_PERIOD_ARCHIVED))

					return
				}
			}

			g.Expect(false).To(BeTrue(), "archived period not found")
		}).Within(30 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())

		// Verify that logs from the archived period can still be read via GetLog
		// (cold storage fallback)
		log, err := actions.GetLog(ctx, client, createLedgerSeq)
		Expect(err).To(Succeed())
		Expect(log).NotTo(BeNil())
		Expect(log.Sequence).To(Equal(createLedgerSeq))
		Expect(log.Payload.GetCreateLedger()).NotTo(BeNil())

		log, err = actions.GetLog(ctx, client, txSeq)
		Expect(err).To(Succeed())
		Expect(log).NotTo(BeNil())
		Expect(log.Sequence).To(Equal(txSeq))
		Expect(log.Payload.GetApply()).NotTo(BeNil())
	})
})
