//go:build e2e && s3

package cluster

import (
	"context"
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
)

// This suite proves the restored FSM cache serves checkpoint-era values for
// keys the delta modified. The account "mallory" is funded before the full
// checkpoint (its VolumePair lands in the checkpoint's 0xFF cache zone) and
// drained after it (the drain exists only in the incremental export).
// RebuildDelta rewrites the 0xF1 attribute zone from the export logs but the
// 0xFF cache zone stays byte-for-byte checkpoint-era, so the restored node
// boots with an in-memory cache holding mallory=(input 1000, output 0) while
// 0xF1 holds the true (1000, 1000).
//
// The rotation threshold is set far above the test's raft-index count, on
// both the source and the restored node, so:
//   - no rotation ever evicts mallory from the source cache before the
//     checkpoint (a low threshold hides the bug by evicting the entry);
//   - the restored node's CheckCache stays in the same-generation branch,
//     making the stale entry a CacheHit (coverage-only, no Pebble reload);
//   - the restored node's persisted cluster config matches its CLI flags, so
//     no cluster-config proposal fires ResetWithThreshold and wipes the
//     restored cache (a mismatched threshold hides the bug by clearing it).
var _ = Describe("Restore stale cache", Ordered, func() {
	const (
		httpPort          = testutil.TestSingleHTTPPort
		grpcPort          = testutil.TestSingleGRPCPort
		raftPort          = grpcPort - 1000
		ledgerName        = "stale-ledger"
		staleS3Bucket     = "restore-stale-cache"
		staleClusterID    = "stale-cluster"
		rotationThreshold = 1_000_000
	)

	var (
		ctx            context.Context
		restoreWalDir  string
		restoreDataDir string
		minioEndpoint  string
	)

	BeforeAll(func() {
		ctx = logging.TestingContext()

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

		_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: aws.String(staleS3Bucket),
		})
		Expect(err).To(Succeed())

		GinkgoT().Setenv("AWS_ACCESS_KEY_ID", restoreMinioAccessKey)
		GinkgoT().Setenv("AWS_SECRET_ACCESS_KEY", restoreMinioSecretKey)

		restoreWalDir, err = os.MkdirTemp("", "stale-restore-wal-*")
		Expect(err).To(Succeed())
		restoreDataDir, err = os.MkdirTemp("", "stale-restore-data-*")
		Expect(err).To(Succeed())
	})

	// Phase 1: fund mallory, checkpoint, drain mallory, export the delta.
	Describe("Phase 1: fund, checkpoint, drain, export", Ordered, func() {
		var (
			sourceServer  *testservice.Service
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
			grpcConn      *grpc.ClientConn
		)

		BeforeAll(func() {
			instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
				NodeID:    1,
				ClusterID: staleClusterID,
				HTTPPort:  httpPort,
				RaftPort:  raftPort,
				GRPCPort:  grpcPort,
				WalDir:    GinkgoT().TempDir(),
				DataDir:   GinkgoT().TempDir(),
				Debug:     testutil.Debug,
				Output:    GinkgoWriter,
			})
			instruments = append(instruments, testserver.WithBootstrap())
			instruments = append(instruments, testserver.WithCacheRotationThreshold(rotationThreshold))

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

			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			// mallory's VolumePair (input=1000, output=0) is cache-resident when
			// the checkpoint is taken: the threshold guarantees no rotation
			// evicts it between this order and the backup.
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "mallory", big.NewInt(1000), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
		})

		AfterAll(func() {
			_ = grpcConn.Close()
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(sourceServer.Stop(stopCtx)).To(Succeed())
		})

		It("should take a full backup to S3", func() {
			resp, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{
				Storage: testutil.S3BackupStorage(&commonpb.S3StorageConfig{
					Bucket:   staleS3Bucket,
					Region:   restoreS3Region,
					Endpoint: minioEndpoint,
				}),
			})
			Expect(err).To(Succeed())
			Expect(resp.GetTotalFiles()).To(BeNumerically(">", 0))
		})

		It("should drain mallory post-checkpoint and export the delta", func() {
			// The drain lives only in the incremental export: after restore,
			// 0xF1 says (1000, 1000) while the checkpoint's 0xFF cache entry
			// still says (1000, 0).
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("mallory", "world", big.NewInt(1000), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			resp, err := clusterClient.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{
				Storage: testutil.S3BackupStorage(&commonpb.S3StorageConfig{
					Bucket:   staleS3Bucket,
					Region:   restoreS3Region,
					Endpoint: minioEndpoint,
				}),
			})
			Expect(err).To(Succeed())
			Expect(resp.GetLogEntriesExported()).To(BeNumerically(">", 0))
		})
	})

	// Phase 2: download and finalize the restore on fresh directories.
	Describe("Phase 2: restore from backup", Ordered, func() {
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
					testserver.WithClusterID(staleClusterID),
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

		It("should download and finalize", func() {
			startResp, err := restoreClient.StartDownloadBackup(ctx, &restorepb.StartDownloadBackupRequest{
				Storage: testutil.S3BackupStorage(&commonpb.S3StorageConfig{
					Bucket:   staleS3Bucket,
					Region:   restoreS3Region,
					Endpoint: minioEndpoint,
				}),
			})
			Expect(err).To(Succeed())

			Eventually(func() restorepb.DownloadState {
				resp, statusErr := restoreClient.GetDownloadStatus(ctx, &restorepb.GetDownloadStatusRequest{
					JobId: startResp.GetJobId(),
				})
				Expect(statusErr).To(Succeed())
				return resp.GetState()
			}, 2*time.Minute, 500*time.Millisecond).Should(Equal(restorepb.DownloadState_DOWNLOAD_STATE_SUCCEEDED))

			_, err = restoreClient.FinalizeRestore(ctx, &restorepb.FinalizeRestoreRequest{})
			Expect(err).To(Succeed())
		})
	})

	// Phase 3: boot on the restored data and verify the apply path reads the
	// delta-rebuilt volumes, not the checkpoint-era cache entry.
	Describe("Phase 3: verify the apply path after restore", Ordered, func() {
		var (
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
			grpcConn      *grpc.ClientConn
			server        *testservice.Service
		)

		BeforeAll(func() {
			instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
				NodeID:    1,
				ClusterID: staleClusterID,
				HTTPPort:  httpPort,
				RaftPort:  raftPort,
				GRPCPort:  grpcPort,
				WalDir:    restoreWalDir,
				DataDir:   restoreDataDir,
				Debug:     testutil.Debug,
				Output:    GinkgoWriter,
			})
			instruments = append(instruments, testserver.WithBootstrap())
			// Same threshold as the source: the persisted cluster config
			// matches the CLI flags, so leadership acquisition does not
			// propose a config change (which would ResetWithThreshold and
			// clear the restored cache, masking the staleness).
			instruments = append(instruments, testserver.WithCacheRotationThreshold(rotationThreshold))

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

		It("should serve the drained volumes on the query path", func() {
			// 0xF1 sanity check: RebuildDelta replayed the drain, so the
			// query path (which reads 0xF1 directly) must see it. This
			// isolates any failure below to the cache, not the rebuild.
			resp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "mallory"})
			Expect(err).To(Succeed())
			Expect(resp.FindVolume("USD", "").GetInput()).To(Equal("1000"))
			Expect(resp.FindVolume("USD", "").GetOutput()).To(Equal("1000"))
		})

		It("should apply against the drained volumes, not the checkpoint-era cache entry", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "mallory", big.NewInt(500), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			resp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "mallory"})
			Expect(err).To(Succeed())
			Expect(resp.FindVolume("USD", "").GetInput()).To(Equal("1500"))
			Expect(resp.FindVolume("USD", "").GetOutput()).To(Equal("1000"),
				"the FSM read mallory's VolumePair from the restored 0xFF cache (checkpoint-era input=1000/output=0) instead of the delta-rebuilt 0xF1 value, clobbering the drain")
		})
	})
})
