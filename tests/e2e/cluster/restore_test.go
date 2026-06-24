//go:build e2e && s3

package cluster

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	restoreMinioAccessKey = "minioadmin"
	restoreMinioSecretKey = "minioadmin"
	restoreS3Bucket       = "restore-backup"
	restoreS3Region       = "us-east-1"
)

// newRestoreGRPCClient creates a gRPC connection with a RestoreServiceClient.
func newRestoreGRPCClient(grpcPort int) (restorepb.RestoreServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, err
	}
	return restorepb.NewRestoreServiceClient(conn), conn, nil
}

var _ = Describe("Restore", Ordered, func() {
	const (
		httpPort   = testutil.TestSingleHTTPPort
		grpcPort   = testutil.TestSingleGRPCPort
		raftPort   = grpcPort - 1000
		ledgerName = "restore-ledger"
		ledger2    = "restore-ledger-2"
	)

	var (
		ctx            context.Context
		restoreWalDir  string
		restoreDataDir string
		minioEndpoint  string
	)

	BeforeAll(func() {
		ctx = logging.TestingContext()

		// Start MinIO container
		container, err := testcontainers.Run(context.Background(), "minio/minio:latest",
			testcontainers.WithEnv(map[string]string{
				"MINIO_ROOT_USER":     restoreMinioAccessKey,
				"MINIO_ROOT_PASSWORD": restoreMinioSecretKey,
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

		// Create S3 client for bucket creation
		cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(restoreS3Region),
			awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				restoreMinioAccessKey, restoreMinioSecretKey, "",
			)),
		)
		Expect(err).To(Succeed())

		s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(minioEndpoint)
			o.UsePathStyle = true
		})

		// Create the backup bucket
		_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: aws.String(restoreS3Bucket),
		})
		Expect(err).To(Succeed())

		// Set AWS credentials for the ledger server process
		GinkgoT().Setenv("AWS_ACCESS_KEY_ID", restoreMinioAccessKey)
		GinkgoT().Setenv("AWS_SECRET_ACCESS_KEY", restoreMinioSecretKey)

		restoreWalDir, err = os.MkdirTemp("", "restore-wal-*")
		Expect(err).To(Succeed())
		restoreDataDir, err = os.MkdirTemp("", "restore-data-*")
		Expect(err).To(Succeed())
	})

	// Phase 1: Start a normal cluster, create data, take a backup to S3, then stop.
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
				ClusterID: "test-cluster",
				HTTPPort:  httpPort,
				RaftPort:  raftPort,
				GRPCPort:  grpcPort,
				WalDir:    walDir,
				DataDir:   dataDir,
				Debug:     testutil.Debug,
				Output:    GinkgoWriter,
			})
			instruments = append(instruments, testserver.WithBootstrap())
			// Low rotation threshold so the source rotates the cache and
			// persists bloom blocks to ZoneGlobal/SubGlobBloom before the
			// backup. The full backup then carries those blocks, so the
			// restore takes the RestoreFromStore path (loads stale blocks,
			// no full 0xF1 rescan) — the condition under which a
			// post-checkpoint account can be bloom-false-negatived.
			instruments = append(instruments, testserver.WithCacheRotationThreshold(3))

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

		It("should take a backup to S3", func() {
			resp, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{
				Storage: testutil.S3BackupStorage(&commonpb.S3StorageConfig{
					Bucket:   restoreS3Bucket,
					Region:   restoreS3Region,
					Endpoint: minioEndpoint,
				}),
			})
			Expect(err).To(Succeed())
			Expect(resp.GetTotalFiles()).To(BeNumerically(">", 0))
		})

		It("should write post-checkpoint data and take an incremental backup", func() {
			// This transaction is written AFTER the full checkpoint, so it
			// lives only in incremental export segments — never in the
			// checkpoint files. A restore that ignores exports loses it.
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "dave", big.NewInt(1500), "USD"),
			}, map[string]string{"type": "post-checkpoint"}, nil)))
			Expect(err).To(Succeed())

			resp, err := clusterClient.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{
				Storage: testutil.S3BackupStorage(&commonpb.S3StorageConfig{
					Bucket:   restoreS3Bucket,
					Region:   restoreS3Region,
					Endpoint: minioEndpoint,
				}),
			})
			Expect(err).To(Succeed())
			Expect(resp.GetLogEntriesExported()).To(BeNumerically(">", 0),
				"incremental backup must export the post-checkpoint log entries")
			Expect(resp.GetSegmentsUploaded()).To(BeNumerically(">", 0))
		})
	})

	// Phase 2: Start a restore-mode server, download from S3, validate, preview, finalize, then stop.
	Describe("Phase 2: Restore from backup", Ordered, func() {
		var (
			restoreClient restorepb.RestoreServiceClient
			grpcConn      *grpc.ClientConn
			server        *testservice.Service
		)

		BeforeAll(func() {
			server = testservice.New(cmdserver.NewRunCommand,
				testservice.WithInstruments(
					testservice.DebugInstrumentation(testutil.Debug),
					testservice.OutputInstrumentation(GinkgoWriter),
					testserver.WithNodeID(1),
					testserver.WithClusterID("test-cluster"),
					testserver.WithHTTPPort(httpPort),
					testserver.WithWalDir(restoreWalDir),
					testserver.WithDataDir(restoreDataDir),
					testserver.WithRaftPort(raftPort),
					testserver.WithGRPCPort(grpcPort),
					testserver.WithRestore(),
				),
			)
			Expect(server.Start(ctx)).To(Succeed())

			var err error
			restoreClient, grpcConn, err = newRestoreGRPCClient(grpcPort)
			Expect(err).To(Succeed())
		})

		AfterAll(func() {
			_ = grpcConn.Close()
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(server.Stop(stopCtx)).To(Succeed())
		})

		It("should download the backup from S3", func() {
			startResp, err := restoreClient.StartDownloadBackup(ctx, &restorepb.StartDownloadBackupRequest{
				Storage: testutil.S3BackupStorage(&commonpb.S3StorageConfig{
					Bucket:   restoreS3Bucket,
					Region:   restoreS3Region,
					Endpoint: minioEndpoint,
				}),
			})
			Expect(err).To(Succeed())
			Expect(startResp.GetJobId()).NotTo(BeEmpty())

			var final *restorepb.GetDownloadStatusResponse
			Eventually(func() restorepb.DownloadState {
				resp, statusErr := restoreClient.GetDownloadStatus(ctx, &restorepb.GetDownloadStatusRequest{
					JobId: startResp.GetJobId(),
				})
				Expect(statusErr).To(Succeed())
				final = resp
				return resp.GetState()
			}, 2*time.Minute, 500*time.Millisecond).Should(Equal(restorepb.DownloadState_DOWNLOAD_STATE_SUCCEEDED),
				"download must reach SUCCEEDED before timeout (last error: %q)",
				func() string {
					if final == nil {
						return "<nil>"
					}
					return final.GetErrorMessage()
				}())

			Expect(final.GetFilesDownloaded()).To(BeNumerically(">", 0))
			Expect(final.GetBytesDownloaded()).To(BeNumerically(">", 0))
			Expect(final.GetFilesDownloaded()).To(Equal(final.GetTotalFiles()))
		})

		It("should reject a duplicate download", func() {
			_, err := restoreClient.StartDownloadBackup(ctx, &restorepb.StartDownloadBackupRequest{
				Storage: testutil.S3BackupStorage(&commonpb.S3StorageConfig{
					Bucket:   restoreS3Bucket,
					Region:   restoreS3Region,
					Endpoint: minioEndpoint,
				}),
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("already downloaded"))
		})

		It("should validate the backup without errors", func() {
			stream, err := restoreClient.ValidateRestore(ctx, &restorepb.ValidateRestoreRequest{})
			Expect(err).To(Succeed())

			var gotErrors bool
			for {
				event, err := stream.Recv()
				if err == io.EOF {
					break
				}
				Expect(err).To(Succeed())

				if event.GetError() != nil {
					gotErrors = true
					GinkgoWriter.Printf("Validation error: %s\n", event.GetError().Message)
				}
			}

			Expect(gotErrors).To(BeFalse(), "validation should not report errors")
		})

		It("should preview the backup", func() {
			resp, err := restoreClient.PreviewRestore(ctx, &restorepb.PreviewRestoreRequest{})
			Expect(err).To(Succeed())

			Expect(resp.LedgerCount).To(Equal(uint32(2)))
			Expect(resp.LedgerNames).To(ConsistOf(ledgerName, ledger2))
			Expect(resp.LastAppliedIndex).To(BeNumerically(">", 0))
			Expect(resp.LastSequence).To(BeNumerically(">", 0))
		})

		It("should finalize the restore", func() {
			resp, err := restoreClient.FinalizeRestore(ctx, &restorepb.FinalizeRestoreRequest{})
			Expect(err).To(Succeed())
			Expect(resp.Message).To(ContainSubstring("Restore finalized"))
		})

		It("should have created the RESTORED marker and checkpoint 0", func() {
			_, err := os.Stat(restoreDataDir + "/RESTORED")
			Expect(err).To(Succeed(), "RESTORED marker should exist")

			_, err = os.Stat(restoreDataDir + "/checkpoints/0")
			Expect(err).To(Succeed(), "checkpoint 0 directory should exist")
		})
	})

	// Phase 3: Restart a normal server on the restored data and verify all data.
	Describe("Phase 3: Bootstrap from restored data", Ordered, func() {
		var (
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
			grpcConn      *grpc.ClientConn
			server        *testservice.Service
		)

		BeforeAll(func() {
			instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
				NodeID:    1,
				ClusterID: "test-cluster",
				HTTPPort:  httpPort,
				RaftPort:  raftPort,
				GRPCPort:  grpcPort,
				WalDir:    restoreWalDir,
				DataDir:   restoreDataDir,
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
			_ = os.RemoveAll(restoreWalDir)
			_ = os.RemoveAll(restoreDataDir)
		})

		It("should have consumed the RESTORED marker", func() {
			_, err := os.Stat(restoreDataDir + "/RESTORED")
			Expect(os.IsNotExist(err)).To(BeTrue(), "RESTORED marker should be removed after bootstrap")
		})

		It("should have both ledgers", func() {
			ledgers, err := actions.ListLedgers(ctx, client)
			Expect(err).To(Succeed())
			Expect(ledgers).To(HaveKey(ledgerName))
			Expect(ledgers).To(HaveKey(ledger2))
		})

		It("should have the correct account balances on ledger 1", func() {
			aliceResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "alice"})
			Expect(err).To(Succeed())
			Expect(aliceResp.Volumes["USD"].Input).To(Equal("3000"))

			bobResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "bob"})
			Expect(err).To(Succeed())
			Expect(bobResp.Volumes["USD"].Input).To(Equal("2000"))

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

		It("should have the correct data on ledger 2", func() {
			treasuryResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledger2, Address: "treasury"})
			Expect(err).To(Succeed())
			Expect(treasuryResp.Volumes["EUR"].Input).To(Equal("50000"))
		})

		It("should have post-checkpoint data restored from export segments", func() {
			// dave was funded after the full checkpoint, so this balance can
			// only be present if the restore applied the incremental exports.
			daveResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "dave"})
			Expect(err).To(Succeed())
			Expect(daveResp.Volumes["USD"].Input).To(Equal("1500"),
				"transaction written after the checkpoint must be restored from export segments")
		})

		It("should account for a post-checkpoint balance on the apply path after restore", func() {
			// The GetAccount above reads 0xF1, which the incremental restore
			// rebuilt correctly. The apply path instead reads balances from the
			// in-memory cache only (no Pebble reads on the hot path), warmed via
			// a bloom-gated preload. The restore rebuilds 0xF1 but not the
			// cache/bloom zones, so a post-checkpoint-only account can be
			// bloom-false-negatived and seen as {0,0} by apply. Funding dave
			// again and reading back exposes it: a cache-aware apply yields
			// 1500+500=2000; a cache-blind apply overwrites 0xF1 with 500.
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "dave", big.NewInt(500), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			daveResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "dave"})
			Expect(err).To(Succeed())
			Expect(daveResp.Volumes["USD"].Input).To(Equal("2000"),
				"apply must see dave's restored balance via the cache; a cache/bloom-blind apply yields 500")
		})

		It("should accept new transactions after restore", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("bank", "charlie", big.NewInt(1000), "USD"),
			}, map[string]string{"type": "post-restore"}, nil)))
			Expect(err).To(Succeed())

			charlieResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "charlie"})
			Expect(err).To(Succeed())
			Expect(charlieResp.Volumes["USD"].Input).To(Equal("1000"))
		})
	})
})
