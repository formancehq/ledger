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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"

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
)

// A chapter's last_audit_hash is recorded by the close, so for a chapter closed
// after the full checkpoint the anchor exists only in the exported delta's
// ClosedChapterLog — the audit entries it was derived from may be purged by the
// time anyone needs it. RebuildDelta therefore has to carry it from that log onto
// the restored row, and this pins the whole composition: closed after the
// checkpoint, exported, restored through the real restore service, then archived
// on the restored store so store check verifies the sealing-hash decomposition
// the anchor is folded into (verifySealingHash runs on ARCHIVED chapters only).
//
// Required by the incremental restore contract: an isolated RebuildDelta unit
// test cannot show that a real post-checkpoint close survives export and restore.
var _ = Describe("Restore chapter audit anchor", Ordered, func() {
	const (
		ledgerName = "chapter-anchor-ledger"
		s3Bucket   = "restore-chapter-audit-anchor"
		clusterID  = "chapter-anchor-cluster"
	)

	// The restored node comes up on the source node's ports, so the lease is held
	// across every phase.
	lease := testserver.AllocateNodeLease()
	ports := lease.Ports()

	var (
		ctx            context.Context
		restoreWalDir  string
		restoreDataDir string
		coldDir        string
		minioEndpoint  string

		// The chapter closed (and sealed) after the checkpoint, captured from the
		// source: the restored registry must reproduce it exactly.
		sealed *commonpb.Chapter
	)

	storage := func() *commonpb.BackupStorage {
		return testutil.S3BackupStorage(&commonpb.S3StorageConfig{
			Bucket:   s3Bucket,
			Region:   restoreS3Region,
			Endpoint: minioEndpoint,
		})
	}

	// Cold storage at a path shared by the source and restored nodes, so the
	// restored store can archive the chapter it inherited.
	withColdStorage := func() testservice.InstrumentationFunc {
		return func(_ context.Context, cfg *testservice.RunConfiguration) error {
			cfg.AppendArgs("--cold-storage-driver", "filesystem", "--cold-storage-path", coldDir)

			return nil
		}
	}

	commitTx := func(client servicepb.BucketServiceClient, dest string) {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", dest, big.NewInt(10), "USD"),
			}, nil, nil),
		))
		Expect(err).To(Succeed())
	}

	listChaptersByID := func(client servicepb.BucketServiceClient) map[uint64]*commonpb.Chapter {
		chapters, err := actions.ListAllChapters(ctx, client)
		Expect(err).To(Succeed())

		byID := make(map[uint64]*commonpb.Chapter, len(chapters))
		for _, chapter := range chapters {
			byID[chapter.GetId()] = chapter
		}

		return byID
	}

	awaitStatus := func(client servicepb.BucketServiceClient, id uint64, status commonpb.ChapterStatus) *commonpb.Chapter {
		var result *commonpb.Chapter

		Eventually(func(g Gomega) {
			chapter := listChaptersByID(client)[id]
			g.Expect(chapter).ToNot(BeNil())
			g.Expect(chapter.GetStatus()).To(Equal(status))
			result = chapter
		}).Within(30 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

		return result
	}

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
		_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(s3Bucket)})
		Expect(err).To(Succeed())

		GinkgoT().Setenv("AWS_ACCESS_KEY_ID", restoreMinioAccessKey)
		GinkgoT().Setenv("AWS_SECRET_ACCESS_KEY", restoreMinioSecretKey)

		restoreWalDir, err = os.MkdirTemp("", "chapter-anchor-wal-*")
		Expect(err).To(Succeed())
		restoreDataDir, err = os.MkdirTemp("", "chapter-anchor-data-*")
		Expect(err).To(Succeed())
		coldDir, err = os.MkdirTemp("", "chapter-anchor-cold-*")
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = os.RemoveAll(coldDir) })
	})

	Describe("Phase 1: close a chapter after the full checkpoint", Ordered, func() {
		var (
			sourceServer  *testservice.Service
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
			grpcConn      *grpc.ClientConn
		)

		BeforeAll(func() {
			instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
				NodeID:    1,
				ClusterID: clusterID,
				Ports:     ports,
				WalDir:    GinkgoT().TempDir(),
				DataDir:   GinkgoT().TempDir(),
				Debug:     testutil.Debug,
				Output:    GinkgoWriter,
			})
			instruments = append(instruments, testserver.WithBootstrap(), withColdStorage())

			sourceServer = lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
			Expect(sourceServer.Start(ctx)).To(Succeed())

			var err error
			client, clusterClient, grpcConn, err = testutil.NewGRPCClient(ports.GRPC())
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) bool {
				state, stateErr := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
				g.Expect(stateErr).To(Succeed())

				return state.Leader != 0
			}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(BeTrue())
		})

		AfterAll(func() {
			_ = grpcConn.Close()
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(sourceServer.Stop(stopCtx)).To(Succeed())
		})

		It("checkpoints meaningful state, then closes a chapter past it", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			commitTx(client, "acc:pre-checkpoint")

			// The checkpoint captures the ledger and its first transaction; the
			// close below lands past it, so the anchor travels only in the delta.
			backupResp, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{Storage: storage()})
			Expect(err).To(Succeed())
			Expect(backupResp.GetTotalFiles()).To(BeNumerically(">", 0))

			commitTx(client, "acc:post-checkpoint")

			var openID uint64
			for id, chapter := range listChaptersByID(client) {
				if chapter.GetStatus() == commonpb.ChapterStatus_CHAPTER_OPEN {
					openID = id
				}
			}
			Expect(openID).NotTo(BeZero())

			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CloseChapterAction()))
			Expect(err).To(Succeed())

			sealed = awaitStatus(client, openID, commonpb.ChapterStatus_CHAPTER_CLOSED)
			Expect(sealed.GetLastAuditHash()).NotTo(BeEmpty(),
				"the source chapter closed over audited history, so it carries the anchor")
			Expect(sealed.GetSealingHash()).NotTo(BeEmpty())

			// Activity past the close, so the delta extends into the successor.
			commitTx(client, "acc:post-close")
		})

		It("exports a delta covering the close", func() {
			incResp, err := clusterClient.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{Storage: storage()})
			Expect(err).To(Succeed())
			Expect(incResp.GetLogEntriesExported()).To(BeNumerically(">", 0),
				"the close must be inside a non-empty export, or the restore proves nothing")
		})
	})

	Describe("Phase 2: restore the checkpoint plus the delta", Ordered, func() {
		var (
			restoreClient restorepb.RestoreServiceClient
			grpcConn      *grpc.ClientConn
			server        *testservice.Service
		)

		BeforeAll(func() {
			server = lease.NewService(cmdserver.NewRunCommandWithBindings,
				testservice.WithInstruments(
					testservice.DebugInstrumentation(testutil.Debug),
					testservice.OutputInstrumentation(GinkgoWriter),
					testserver.WithNodeID(1),
					testserver.WithClusterID(clusterID),
					testserver.WithHTTPPort(ports.HTTP()),
					testserver.WithWalDir(restoreWalDir),
					testserver.WithDataDir(restoreDataDir),
					testserver.WithRaftPort(ports.Raft()),
					testserver.WithGRPCPort(ports.GRPC()),
					testserver.WithRestore(),
				),
			)
			Expect(server.Start(ctx)).To(Succeed())

			var err error
			restoreClient, grpcConn, err = newRestoreGRPCClient(ports.GRPC())
			Expect(err).To(Succeed())
		})

		AfterAll(func() {
			_ = grpcConn.Close()
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(server.Stop(stopCtx)).To(Succeed())
		})

		It("downloads and finalizes the backup", func() {
			startResp, err := restoreClient.StartDownloadBackup(ctx, &restorepb.StartDownloadBackupRequest{Storage: storage()})
			Expect(err).To(Succeed())

			Eventually(func() restorepb.DownloadState {
				resp, statusErr := restoreClient.GetDownloadStatus(ctx, &restorepb.GetDownloadStatusRequest{JobId: startResp.GetJobId()})
				Expect(statusErr).To(Succeed())

				return resp.GetState()
			}, 2*time.Minute, 500*time.Millisecond).Should(Equal(restorepb.DownloadState_DOWNLOAD_STATE_SUCCEEDED))

			_, err = restoreClient.FinalizeRestore(ctx, &restorepb.FinalizeRestoreRequest{})
			Expect(err).To(Succeed())
		})
	})

	Describe("Phase 3: the restored chapter is verifiable", Ordered, func() {
		var (
			client   servicepb.BucketServiceClient
			grpcConn *grpc.ClientConn
			server   *testservice.Service
		)

		BeforeAll(func() {
			instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
				NodeID:    1,
				ClusterID: clusterID,
				Ports:     ports,
				WalDir:    restoreWalDir,
				DataDir:   restoreDataDir,
				Debug:     testutil.Debug,
				Output:    GinkgoWriter,
			})
			instruments = append(instruments, testserver.WithBootstrap(), withColdStorage())

			server = lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
			Expect(server.Start(ctx)).To(Succeed())

			var clusterClient clusterpb.ClusterServiceClient
			var err error
			client, clusterClient, grpcConn, err = testutil.NewGRPCClient(ports.GRPC())
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) bool {
				state, stateErr := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
				g.Expect(stateErr).To(Succeed())

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

		It("restores the chapter with the anchor the close carried", func() {
			got := listChaptersByID(client)[sealed.GetId()]
			Expect(got).ToNot(BeNil(), "restore lost the chapter closed inside the delta")
			Expect(got.GetStatus()).To(Equal(commonpb.ChapterStatus_CHAPTER_CLOSED))
			Expect(got.GetCloseSequence()).To(Equal(sealed.GetCloseSequence()))
			Expect(got.GetCloseAuditSequence()).To(Equal(sealed.GetCloseAuditSequence()))
			Expect(got.GetSealingHash()).To(Equal(sealed.GetSealingHash()))
			Expect(got.GetStateHash()).To(Equal(sealed.GetStateHash()))
			Expect(got.GetLastAuditHash()).To(Equal(sealed.GetLastAuditHash()),
				"the anchor the close log carried must land on the restored row: the audit entries it came from are purged once the chapter is archived")
		})

		It("archives it and passes store check", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.ArchiveChapterAction(sealed.GetId())))
			Expect(err).To(Succeed())

			// ARCHIVED is what puts the row through verifySealingHash, which
			// recomputes the decomposition the anchor is folded into.
			awaitStatus(client, sealed.GetId(), commonpb.ChapterStatus_CHAPTER_ARCHIVED)

			result, err := actions.CollectCheckStoreEvents(ctx, client)
			Expect(err).To(Succeed())
			Expect(result.Errors).To(BeEmpty(),
				"a restored store that archives a chapter closed inside the delta must verify")
		})
	})
})
