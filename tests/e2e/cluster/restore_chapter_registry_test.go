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

// This suite pins the chapter-registry loss the archival+restore model test
// surfaced: chapter lifecycle transitions after the full checkpoint live only
// in the exported delta, and RebuildDelta replays CloseChapter / SealChapter /
// ArchiveChapter / ConfirmArchiveChapter logs as no-ops, so a restored node's
// registry (ZoneGlobal/SubGlobChapters + SubGlobNextChapterID) stays frozen at
// checkpoint time. A chapter archived inside the delta comes back OPEN (or
// absent), and the next CloseChapter re-closes it spanning [1, now]. Archiving
// that impostor finds the real chapter's SST already in cold storage
// (Exists + checksum verify the OLD object against itself), confirms WITHOUT
// uploading, and the deterministic purge then deletes ranges that exist
// nowhere — permanent data loss. The registry assertions here pin the source
// of that chain; the identity assertions pin the collision seed.
var _ = Describe("Restore chapter registry", Ordered, func() {
	const (
		ledgerName = "chapter-restore-ledger"
		s3Bucket   = "restore-chapter-registry"
		clusterID  = "chapter-restore-cluster"
	)

	// The restored node comes up on the source node's ports, so the lease is held
	// across both halves of the spec.
	lease := testserver.AllocateNodeLease()
	ports := lease.Ports()

	var (
		ctx            context.Context
		restoreWalDir  string
		restoreDataDir string
		coldDir        string
		minioEndpoint  string

		// The chapter archived (and purged) inside the exported delta, captured
		// pre-restore: the restored registry must reproduce it exactly.
		archived *commonpb.Chapter
	)

	storage := func() *commonpb.BackupStorage {
		return testutil.S3BackupStorage(&commonpb.S3StorageConfig{
			Bucket:   s3Bucket,
			Region:   restoreS3Region,
			Endpoint: minioEndpoint,
		})
	}

	// Filesystem cold storage at a path shared across the source and restored
	// nodes, mirroring production where cold storage outlives any one store.
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

	// listChaptersByID returns the registry keyed by chapter id.
	listChaptersByID := func(client servicepb.BucketServiceClient) map[uint64]*commonpb.Chapter {
		chapters, err := actions.ListAllChapters(ctx, client)
		Expect(err).To(Succeed())

		byID := make(map[uint64]*commonpb.Chapter, len(chapters))
		for _, ch := range chapters {
			byID[ch.GetId()] = ch
		}

		return byID
	}

	// archiveCurrentChapter closes the open chapter, waits for the background
	// Sealer (CLOSED), archives it, waits for the background Archiver
	// (ARCHIVED = uploaded + confirmed + purged), and returns the archived
	// chapter's registry entry.
	archiveCurrentChapter := func(client servicepb.BucketServiceClient) *commonpb.Chapter {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CloseChapterAction()))
		Expect(err).To(Succeed())

		var closedID uint64

		Eventually(func(g Gomega) {
			for id, ch := range listChaptersByID(client) {
				if ch.GetStatus() == commonpb.ChapterStatus_CHAPTER_CLOSED {
					closedID = id

					return
				}
			}

			g.Expect(false).To(BeTrue(), "no CLOSED chapter found yet")
		}).Within(15 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.ArchiveChapterAction(closedID)))
		Expect(err).To(Succeed())

		var result *commonpb.Chapter

		Eventually(func(g Gomega) {
			ch := listChaptersByID(client)[closedID]
			g.Expect(ch).ToNot(BeNil())
			g.Expect(ch.GetStatus()).To(Equal(commonpb.ChapterStatus_CHAPTER_ARCHIVED))
			result = ch
		}).Within(30 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())

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

		restoreWalDir, err = os.MkdirTemp("", "chapter-restore-wal-*")
		Expect(err).To(Succeed())
		restoreDataDir, err = os.MkdirTemp("", "chapter-restore-data-*")
		Expect(err).To(Succeed())
		coldDir, err = os.MkdirTemp("", "chapter-restore-cold-*")
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = os.RemoveAll(coldDir) })
	})

	Describe("Phase 1: archive a chapter inside the exported delta", Ordered, func() {
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

			sourceServer = testservice.New(cmdserver.NewRunCommand, testservice.WithInstruments(instruments...))
			Expect(sourceServer.Start(ctx)).To(Succeed())

			var err error
			client, clusterClient, grpcConn, err = testutil.NewGRPCClient(ports.GRPC())
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) bool {
				state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
				g.Expect(err).To(Succeed())
				return state.Leader != 0
			}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(BeTrue())

			// Full checkpoint on the EMPTY store: every chapter transition below
			// lands in the incremental delta, so the restored registry exists only
			// if RebuildDelta rebuilds it from the exported chapter logs.
			backupResp, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{Storage: storage()})
			Expect(err).To(Succeed())
			Expect(backupResp.GetTotalFiles()).To(BeNumerically(">", 0))
		})

		AfterAll(func() {
			_ = grpcConn.Close()
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(sourceServer.Stop(stopCtx)).To(Succeed())
		})

		It("archives a chapter and keeps committing past it", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.CreateLedgerAction(ledgerName, nil),
			))
			Expect(err).To(Succeed())

			commitTx(client, "acc:pre-archive")

			archived = archiveCurrentChapter(client)
			Expect(archived.GetStartSequence()).To(Equal(uint64(1)))
			Expect(archived.GetCloseSequence()).To(BeNumerically(">", archived.GetStartSequence()))

			// Post-archive activity so the delta extends beyond the archived
			// chapter into its successor.
			commitTx(client, "acc:post-archive")
		})

		It("exports the delta", func() {
			incResp, err := clusterClient.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{Storage: storage()})
			Expect(err).To(Succeed())
			Expect(incResp.GetLogEntriesExported()).To(BeNumerically(">", 0))
		})
	})

	Describe("Phase 2: restore", Ordered, func() {
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

	Describe("Phase 3: verify the restored chapter registry", Ordered, func() {
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

			server = testservice.New(cmdserver.NewRunCommand, testservice.WithInstruments(instruments...))
			Expect(server.Start(ctx)).To(Succeed())

			var clusterClient clusterpb.ClusterServiceClient
			var err error
			client, clusterClient, grpcConn, err = testutil.NewGRPCClient(ports.GRPC())
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

		It("keeps the archived chapter and its successor", func() {
			byID := listChaptersByID(client)

			got := byID[archived.GetId()]
			Expect(got).ToNot(BeNil(),
				"restore lost the archived chapter's registry entry")
			Expect(got.GetStatus()).To(Equal(commonpb.ChapterStatus_CHAPTER_ARCHIVED),
				"the restored registry must remember the chapter is ARCHIVED (its data lives only in cold storage)")
			Expect(got.GetStartSequence()).To(Equal(archived.GetStartSequence()))
			Expect(got.GetCloseSequence()).To(Equal(archived.GetCloseSequence()),
				"the archived chapter's range routes cold reads; a wrong range misroutes them")
			Expect(got.GetStartAuditSequence()).To(Equal(archived.GetStartAuditSequence()))
			Expect(got.GetCloseAuditSequence()).To(Equal(archived.GetCloseAuditSequence()))
			Expect(got.GetSealingHash()).To(Equal(archived.GetSealingHash()),
				"the sealing hash must survive the restore byte-for-byte")
			Expect(got.GetStateHash()).To(Equal(archived.GetStateHash()))
			Expect(got.GetLastAuditHash()).To(Equal(archived.GetLastAuditHash()),
				"the checker chains from this hash across the chapter's purged audit range")

			successor := byID[archived.GetId()+1]
			Expect(successor).ToNot(BeNil(), "the successor chapter opened at close must survive the restore")
			Expect(successor.GetStatus()).To(Equal(commonpb.ChapterStatus_CHAPTER_OPEN))
			Expect(successor.GetStartSequence()).To(Equal(archived.GetCloseSequence()+1))
		})

		It("closes the successor chapter, never a rewound impostor", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CloseChapterAction()))
			Expect(err).To(Succeed())

			// Wait for the close to seal, then pin the closed chapter's identity: it
			// must be the successor covering (archived.close, ...]. A registry frozen
			// at checkpoint time re-closes the ARCHIVED chapter's id spanning
			// [1, now]; archiving that impostor finds the real chapter's SST in cold
			// storage, confirms WITHOUT uploading, and purges ranges that exist
			// nowhere.
			var closed *commonpb.Chapter

			Eventually(func(g Gomega) {
				for _, ch := range listChaptersByID(client) {
					if ch.GetStatus() == commonpb.ChapterStatus_CHAPTER_CLOSED {
						closed = ch

						return
					}
				}

				g.Expect(false).To(BeTrue(), "no CLOSED chapter found yet")
			}).Within(15 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			Expect(closed.GetId()).To(Equal(archived.GetId()+1),
				"the closed chapter must be the successor, not a reused archived id")
			Expect(closed.GetStartSequence()).To(Equal(archived.GetCloseSequence()+1),
				"a start sequence of 1 would span already-archived history")

			// NextChapterID must have survived too: the chapter opened by this
			// close gets the next fresh id.
			Eventually(func(g Gomega) {
				opened := listChaptersByID(client)[archived.GetId()+2]
				g.Expect(opened).ToNot(BeNil(), "the close must open a chapter under a fresh id")
				g.Expect(opened.GetStatus()).To(Equal(commonpb.ChapterStatus_CHAPTER_OPEN))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})
})
