//go:build e2e && s3

package cluster

import (
	"context"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/store"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
)

const (
	bootstrapMinioAccessKey = "minioadmin"
	bootstrapMinioSecretKey = "minioadmin"
	bootstrapS3Bucket       = "bootstrap-backup"
	bootstrapS3Region       = "us-east-1"
	bootstrapBucketID       = "test-cluster"
)

var _ = Describe("Bootstrap from backup", Ordered, func() {
	const (
		httpPort   = testutil.TestSingleHTTPPort
		grpcPort   = testutil.TestSingleGRPCPort
		raftPort   = grpcPort - 1000
		ledgerName = "bootstrap-ledger"
		ledger2    = "bootstrap-ledger-2"
	)

	var (
		ctx              context.Context
		bootstrapWalDir  string
		bootstrapDataDir string
		minioEndpoint    string
	)

	BeforeAll(func() {
		ctx = logging.TestingContext()

		// Start MinIO container
		container, err := testcontainers.Run(context.Background(), "minio/minio:latest",
			testcontainers.WithEnv(map[string]string{
				"MINIO_ROOT_USER":     bootstrapMinioAccessKey,
				"MINIO_ROOT_PASSWORD": bootstrapMinioSecretKey,
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

		cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(bootstrapS3Region),
			awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				bootstrapMinioAccessKey, bootstrapMinioSecretKey, "",
			)),
		)
		Expect(err).To(Succeed())

		s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(minioEndpoint)
			o.UsePathStyle = true
		})

		_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: aws.String(bootstrapS3Bucket),
		})
		Expect(err).To(Succeed())

		GinkgoT().Setenv("AWS_ACCESS_KEY_ID", bootstrapMinioAccessKey)
		GinkgoT().Setenv("AWS_SECRET_ACCESS_KEY", bootstrapMinioSecretKey)

		bootstrapWalDir, err = os.MkdirTemp("", "bootstrap-wal-*")
		Expect(err).To(Succeed())
		bootstrapDataDir, err = os.MkdirTemp("", "bootstrap-data-*")
		Expect(err).To(Succeed())

		DeferCleanup(func() {
			_ = os.RemoveAll(bootstrapWalDir)
			_ = os.RemoveAll(bootstrapDataDir)
		})
	})

	// Phase 1: Start a normal cluster, create data, take a backup to S3.
	Describe("Phase 1: Create data and backup", Ordered, func() {
		var (
			sourceServer  *testservice.Service
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
			grpcConn      *grpc.ClientConn
		)

		BeforeAll(func() {
			walDir := GinkgoT().TempDir()
			dataDir := GinkgoT().TempDir()

			instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
				NodeID:    1,
				ClusterID: bootstrapBucketID,
				HTTPPort:  httpPort,
				RaftPort:  raftPort,
				GRPCPort:  grpcPort,
				WalDir:    walDir,
				DataDir:   dataDir,
				Debug:     testutil.Debug,
				Output:    GinkgoWriter,
			})
			instruments = append(instruments, testserver.WithBootstrap())

			sourceServer = testservice.New(cmdserver.NewRunCommand,
				testservice.WithInstruments(instruments...),
			)
			Expect(sourceServer.Start(ctx)).To(Succeed())

			var err error
			client, clusterClient, grpcConn, err = testutil.NewGRPCClient(grpcPort)
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) bool {
				state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
				g.Expect(err).To(Succeed())
				return state.Leader != 0
			}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(BeTrue())

			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, map[string]string{"env": "test"})))
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "bank", big.NewInt(10000), "USD"),
			}, map[string]string{"type": "funding"}, nil)))
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("bank", "alice", big.NewInt(3000), "USD"),
				actions.NewPosting("bank", "bob", big.NewInt(2000), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"role": "customer"})))
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger2, nil)))
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledger2, []*commonpb.Posting{
				actions.NewPosting("world", "treasury", big.NewInt(50000), "EUR"),
			}, nil, nil)))
			Expect(err).To(Succeed())
		})

		AfterAll(func() {
			_ = grpcConn.Close()
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(sourceServer.Stop(stopCtx)).To(Succeed())
		})

		It("should take a backup to S3 with sequence metadata", func() {
			resp, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{
				Storage: testutil.S3BackupStorage(&commonpb.S3StorageConfig{
					Bucket:   bootstrapS3Bucket,
					Region:   bootstrapS3Region,
					Endpoint: minioEndpoint,
				}),
				BucketId: bootstrapBucketID,
			})
			Expect(err).To(Succeed())
			Expect(resp.GetTotalFiles()).To(BeNumerically(">", 0))
			Expect(resp.GetLastLogSequence()).To(BeNumerically(">", 0))
			Expect(resp.GetLastAuditSequence()).To(BeNumerically(">", 0))
			Expect(resp.GetLastAppliedIndex()).To(BeNumerically(">", 0))
		})

		It("should run incremental backup after adding more data", func() {
			// Add more data after the full backup
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("bank", "eve", big.NewInt(500), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			// Run incremental backup
			incrResp, err := clusterClient.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{
				Storage: testutil.S3BackupStorage(&commonpb.S3StorageConfig{
					Bucket:   bootstrapS3Bucket,
					Region:   bootstrapS3Region,
					Endpoint: minioEndpoint,
				}),
				BucketId: bootstrapBucketID,
			})
			Expect(err).To(Succeed())
			Expect(incrResp.GetLogEntriesExported()).To(BeNumerically(">", 0))
			Expect(incrResp.GetAuditEntriesExported()).To(BeNumerically(">", 0))

			// Take a new full backup to include all data (clears exports)
			fullResp, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{
				Storage: testutil.S3BackupStorage(&commonpb.S3StorageConfig{
					Bucket:   bootstrapS3Bucket,
					Region:   bootstrapS3Region,
					Endpoint: minioEndpoint,
				}),
				BucketId: bootstrapBucketID,
			})
			Expect(err).To(Succeed())
			Expect(fullResp.GetTotalFiles()).To(BeNumerically(">", 0))
		})
	})

	// Phase 2: Run the offline bootstrap CLI — downloads from S3, no server.
	Describe("Phase 2: Offline bootstrap via CLI command", Ordered, func() {
		It("should refuse to bootstrap into a directory with existing checkpoints", func() {
			tmpDir := GinkgoT().TempDir()
			Expect(os.MkdirAll(filepath.Join(tmpDir, "checkpoints", "1"), 0o755)).To(Succeed())

			cmd := store.NewBootstrapCommand()
			cmd.SetArgs([]string{
				"--s3-bucket", bootstrapS3Bucket,
				"--s3-region", bootstrapS3Region,
				"--s3-endpoint", minioEndpoint,
				"--bucket-id", bootstrapBucketID,
				"--data-dir", tmpDir,
				"--yes",
			})

			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("checkpoints"))
		})

		It("should bootstrap with --validate --yes", func() {
			cmd := store.NewBootstrapCommand()
			cmd.SetArgs([]string{
				"--s3-bucket", bootstrapS3Bucket,
				"--s3-region", bootstrapS3Region,
				"--s3-endpoint", minioEndpoint,
				"--bucket-id", bootstrapBucketID,
				"--data-dir", bootstrapDataDir,
				"--validate",
				"--yes",
			})

			Expect(cmd.Execute()).To(Succeed())
		})

		It("should have created checkpoint 0 and RESTORED marker", func() {
			_, err := os.Stat(filepath.Join(bootstrapDataDir, "checkpoints", "0"))
			Expect(err).To(Succeed(), "checkpoint 0 directory should exist")

			_, err = os.Stat(filepath.Join(bootstrapDataDir, "RESTORED"))
			Expect(err).To(Succeed(), "RESTORED marker should exist")

			_, err = os.Stat(filepath.Join(bootstrapDataDir, "checkpoints", "0"))
			Expect(err).To(Succeed(), "checkpoint 0 should exist")
		})

		It("should have cleaned up the staging directory", func() {
			_, err := os.Stat(filepath.Join(bootstrapDataDir, "restore-staging"))
			Expect(os.IsNotExist(err)).To(BeTrue(), "staging directory should be removed")
		})
	})

	// Phase 3: Start server on bootstrapped data, verify everything.
	Describe("Phase 3: Server bootstrap from offline-prepared data", Ordered, func() {
		var (
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
			grpcConn      *grpc.ClientConn
			server        *testservice.Service
		)

		BeforeAll(func() {
			instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
				NodeID:    1,
				ClusterID: bootstrapBucketID,
				HTTPPort:  httpPort,
				RaftPort:  raftPort,
				GRPCPort:  grpcPort,
				WalDir:    bootstrapWalDir,
				DataDir:   bootstrapDataDir,
				Debug:     testutil.Debug,
				Output:    GinkgoWriter,
			})
			instruments = append(instruments, testserver.WithBootstrap())

			server = testservice.New(cmdserver.NewRunCommand,
				testservice.WithInstruments(instruments...),
			)
			Expect(server.Start(ctx)).To(Succeed())

			var err error
			client, clusterClient, grpcConn, err = testutil.NewGRPCClient(grpcPort)
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) bool {
				state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
				g.Expect(err).To(Succeed())
				return state.Leader != 0
			}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(BeTrue())
		})

		AfterAll(func() {
			_ = grpcConn.Close()
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(server.Stop(stopCtx)).To(Succeed())
		})

		It("should have consumed the RESTORED marker", func() {
			_, err := os.Stat(filepath.Join(bootstrapDataDir, "RESTORED"))
			Expect(os.IsNotExist(err)).To(BeTrue(), "RESTORED marker should be removed after bootstrap")
		})

		It("should have both ledgers", func() {
			ledgers, err := actions.ListLedgers(ctx, client)
			Expect(err).To(Succeed())
			Expect(ledgers).To(HaveKey(ledgerName))
			Expect(ledgers).To(HaveKey(ledger2))
		})

		It("should have the correct account balances", func() {
			aliceResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "alice"})
			Expect(err).To(Succeed())
			Expect(aliceResp.Volumes["USD"].Input).To(Equal("3000"))

			bankResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "bank"})
			Expect(err).To(Succeed())
			Expect(bankResp.Volumes["USD"].Input).To(Equal("10000"))
			Expect(bankResp.Volumes["USD"].Output).To(Equal("5000"))
		})

		It("should have the correct account metadata", func() {
			aliceResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "alice"})
			Expect(err).To(Succeed())
			Expect(commonpb.MetadataToGoMap(aliceResp.Metadata)).To(HaveKeyWithValue("role", "customer"))
		})

		It("should have the data added after the first backup (via second full backup)", func() {
			eveResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "eve"})
			Expect(err).To(Succeed())
			Expect(eveResp.Volumes["USD"].Input).To(Equal("500"))
		})

		It("should accept new transactions after bootstrap", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("bank", "charlie", big.NewInt(1000), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			charlieResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "charlie"})
			Expect(err).To(Succeed())
			Expect(charlieResp.Volumes["USD"].Input).To(Equal("1000"))
		})
	})
})
