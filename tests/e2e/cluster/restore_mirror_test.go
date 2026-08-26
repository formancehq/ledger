//go:build e2e && s3

package cluster

import (
	"context"
	"fmt"
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

// This suite pins the mirror resume position across a backup/restore cycle
// (EN-1773, regression for EN-1776). Since EN-1513 there is no persisted mirror
// cursor: LedgerBoundaries.last_mirror_v2_log_id — the FSM-applied high-water
// mark — is the sole durable ingestion authority, and the worker resumes from
// it. RebuildDelta replays the exported ledger logs to reconstruct the
// boundaries, but it did not fold the mirror high-water mark, so a restored
// mirror ledger came back with last_mirror_v2_log_id == 0.
//
// The consequence is not a stalled mirror — it is a silent double ingestion.
// The worker restarts at source log 1, and processMirrorIngest sees v2LogID ==
// last+1 for every already-applied log, so it re-applies them instead of
// treating them as replays. Mirrored transactions carry the SOURCE transaction
// id, so the re-apply overwrites the same transaction rows (the count stays
// put) while applyPosting runs a second time with force=true: the account
// volumes silently double. That is why the volume comparison, not the
// transaction count, is the assertion that cannot be masked by a race with the
// mirror worker.
var _ = Describe("Restore mirror resume position", Ordered, func() {
	// One logical node returns three times (live, restore-mode, restarted), so
	// every phase runs off the same lease: it keeps the node's ports for the
	// whole test and rebinds them on each start, where a fresh set would
	// surface as a Raft failure rather than as a port mistake (EN-1784).
	lease := testserver.AllocateNodeLease()
	ports := lease.Ports()

	const (
		ledgerName = "mirror-restore-ledger"
		s3Bucket   = "restore-mirror-cursor"
		clusterID  = "mirror-restore-cluster"

		account = "users:001"
		asset   = "USD/2"

		// Source logs 1..4 carry transactions 0..3, each crediting `account`
		// with 100. sourceLogCount is therefore the cursor the mirror must hold
		// when the delta is exported.
		sourceLogCount = 4
		perTxAmount    = "100"

		// Appended after the restore to prove ingestion resumes rather than
		// merely stopping in a correct-looking state.
		postRestoreLogID = sourceLogCount + 1
		postRestoreTxID  = sourceLogCount
	)

	var (
		ctx            context.Context
		restoreWalDir  string
		restoreDataDir string
		minioEndpoint  string

		// The mock v2 source is an in-process httptest server: it must outlive
		// all three phases and keep the same URL, because the mirror config
		// persisted in the backup points at it and the restored node's worker
		// reconnects to that exact address.
		mockV2 *mockV2Server

		// Captured on the live node before the delta is exported, then compared
		// against the restored node.
		preBackupCursor  uint64
		preBackupTxCount int
		// Recorded rather than hard-coded so the comparison pins "unchanged by
		// the restore" instead of a particular amount encoding.
		preBackupInput string
		// The transaction projection is rebuilt from the exported ledger-log
		// delta. Keep an independent source-date oracle so the restore cannot
		// silently replace that field with its own apply time.
		sourceDateMicrosByTxID map[uint64]uint64

		// Read on the restored node as early as the gRPC surface answers.
		restoredCursor uint64
		// The mock's request count immediately before the restored node starts,
		// and again at the instant the cursor was sampled. Equal means no source
		// fetch completed before the read returned, so the cursor came from
		// RebuildDelta rather than from a re-ingest. The mock is shared across
		// all three phases, so the floor has to be captured, not assumed zero.
		preRestoreRequests int
		requestsAtSample   int
	)

	storage := func() *commonpb.BackupStorage {
		return testutil.S3BackupStorage(&commonpb.S3StorageConfig{
			Bucket:   s3Bucket,
			Region:   restoreS3Region,
			Endpoint: minioEndpoint,
		})
	}

	syncProgress := func(g Gomega, client servicepb.BucketServiceClient) *commonpb.MirrorSyncProgress {
		info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
		g.Expect(err).To(Succeed())

		return info.GetMirrorSyncProgress()
	}

	// expectSingleIngestion asserts the two observables a second ingestion of
	// the same source logs moves differently: the mirrored transaction count
	// (unchanged, because a re-apply reuses the source transaction ids) and the
	// account volumes (doubled, because the postings apply again).
	expectSingleIngestion := func(g Gomega, client servicepb.BucketServiceClient, phase string) {
		txs, err := listAllTransactions(ctx, client, ledgerName, 100, 0)
		g.Expect(err).To(Succeed())
		g.Expect(txs).To(HaveLen(preBackupTxCount), "%s: mirrored transaction count", phase)
		for _, tx := range txs {
			sourceDate, ok := sourceDateMicrosByTxID[tx.GetId()]
			g.Expect(ok).To(BeTrue(), "%s: missing source-date oracle for transaction %d", phase, tx.GetId())
			g.Expect(tx.GetInsertedAt().GetData()).To(Equal(sourceDate), "%s: transaction %d insertedAt", phase, tx.GetId())
			g.Expect(tx.GetUpdatedAt().GetData()).To(Equal(sourceDate), "%s: transaction %d updatedAt", phase, tx.GetId())
		}

		acct, err := actions.GetAccount(ctx, client, ledgerName, account)
		g.Expect(err).To(Succeed())

		vol := acct.FindVolume(asset, "")
		g.Expect(vol).ToNot(BeNil(), "%s: %s %s volumes missing", phase, account, asset)
		g.Expect(vol.GetInput()).To(Equal(preBackupInput), "%s: %s %s input", phase, account, asset)
	}

	BeforeAll(func() {
		ctx = logging.TestingContext()

		mockV2 = newMockV2Server()
		DeferCleanup(mockV2.Close)
		sourceDateMicrosByTxID = make(map[uint64]uint64, sourceLogCount)

		for i := 1; i <= sourceLogCount; i++ {
			// Source log ids are 1-based; the transactions they carry are
			// 0-based, matching a real v2 ledger.
			txID := uint64(i - 1)
			sourceDate := time.Date(2023, 11, 14, 22, 13+i, 0, i*1000, time.UTC)
			log := newV2TransactionLog(uint64(i), txID, "world", account, perTxAmount, asset)
			log.Date = sourceDate.Format(time.RFC3339Nano)
			mockV2.addLog(log)
			sourceDateMicrosByTxID[txID] = uint64(sourceDate.UnixMicro())
		}

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

		restoreWalDir, err = os.MkdirTemp("", "mirror-restore-wal-*")
		Expect(err).To(Succeed())
		restoreDataDir, err = os.MkdirTemp("", "mirror-restore-data-*")
		Expect(err).To(Succeed())
	})

	Describe("Phase 1: a fully synced mirror ledger in the exported delta", Ordered, func() {
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
			instruments = append(instruments, testserver.WithBootstrap())

			sourceServer = lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
			Expect(sourceServer.Start(ctx)).To(Succeed())

			var err error
			client, clusterClient, grpcConn, err = testutil.NewGRPCClient(ports.GRPC())
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) bool {
				state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
				g.Expect(err).To(Succeed())
				return state.Leader != 0
			}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(BeTrue())

			// Full checkpoint on the EMPTY store: the mirror ledger and every
			// ingest land in the incremental delta, so the restore has to
			// reconstruct last_mirror_v2_log_id by replaying the exported log
			// instead of copying a checkpoint that already holds it.
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

		It("creates a mirror ledger pointed at the v2 source", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
				Type: &servicepb.Request_CreateLedger{
					CreateLedger: &servicepb.CreateLedgerRequest{
						Name: ledgerName,
						Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
						MirrorSource: &commonpb.MirrorSourceConfig{
							LedgerName: "default",
							Type: &commonpb.MirrorSourceConfig_Http{
								Http: &commonpb.HttpMirrorSourceConfig{
									BaseUrl: mockV2.URL(),
								},
							},
						},
					},
				},
			}))
			Expect(err).To(Succeed())
		})

		It("syncs every source log before the delta is exported", func() {
			// FOLLOWING requires cursor >= source head, so it pins both that the
			// worker consumed the whole source and that the FSM advanced the
			// high-water mark for each ingest.
			Eventually(func(g Gomega) {
				progress := syncProgress(g, client)
				g.Expect(progress.GetError().GetMessage()).To(BeEmpty())
				g.Expect(progress.GetCursor()).To(Equal(uint64(sourceLogCount)))
				g.Expect(progress.GetSourceLogCount()).To(Equal(uint64(sourceLogCount)))
				g.Expect(progress.GetState()).To(Equal(commonpb.MirrorSyncState_MIRROR_SYNC_STATE_FOLLOWING))

				txs, err := listAllTransactions(ctx, client, ledgerName, 100, 0)
				g.Expect(err).To(Succeed())
				g.Expect(txs).To(HaveLen(sourceLogCount))
			}).Within(60 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())

			txs, err := listAllTransactions(ctx, client, ledgerName, 100, 0)
			Expect(err).To(Succeed())

			preBackupTxCount = len(txs)
			preBackupCursor = syncProgress(Default, client).GetCursor()

			acct, err := actions.GetAccount(ctx, client, ledgerName, account)
			Expect(err).To(Succeed())

			vol := acct.FindVolume(asset, "")
			Expect(vol).ToNot(BeNil(), "live: %s %s volumes missing", account, asset)
			preBackupInput = vol.GetInput()

			// Premise guard: without a single, complete ingestion on the live
			// node the post-restore comparisons would be vacuous.
			Expect(preBackupInput).ToNot(BeElementOf("", "0"), "live: %s %s input", account, asset)
			expectSingleIngestion(Default, client, "live")
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

	Describe("Phase 3: verify the restored mirror resume position", Ordered, func() {
		var (
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
			grpcConn      *grpc.ClientConn
			server        *testservice.Service
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
			instruments = append(instruments, testserver.WithBootstrap())

			server = lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))

			// Park the source before the node exists. Manager.reconcile starts
			// mirror workers on the leadership callback, which is not ordered
			// against the sampling read below — so without this the worker
			// could complete a fetch first and the sample would witness a
			// re-ingest rather than the rebuild. Pausing makes "no source fetch
			// has happened" a fact instead of a timing bet: parked requests are
			// held at the handler entry, before the counter moves.
			mockV2.pause()
			preRestoreRequests = mockV2.requestCount()

			Expect(server.Start(ctx)).To(Succeed())

			var err error
			client, clusterClient, grpcConn, err = testutil.NewGRPCClient(ports.GRPC())
			Expect(err).To(Succeed())

			// Sample the rebuilt high-water mark while the source is still
			// parked. GetLedger serves from local Pebble and needs no leader,
			// so this answers whatever RebuildDelta wrote.
			Eventually(func(g Gomega) {
				info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
				g.Expect(err).To(Succeed())
				restoredCursor = info.GetMirrorSyncProgress().GetCursor()
				requestsAtSample = mockV2.requestCount()
			}).Within(30 * time.Second).ProbeEvery(50 * time.Millisecond).Should(Succeed())

			mockV2.resume()

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

		It("rebuilds the mirror high-water mark from the replayed delta", func() {
			// The sample has to predate the mirror worker, or it proves nothing:
			// on an unfixed build the rebuilt mark is 0, the worker restarts at
			// source log 1 and re-applies logs 1..4, and processMirrorIngest
			// advances the cursor back to 4 — the same value a correct rebuild
			// leaves. No source request means no ingest, so the value below was
			// written by RebuildDelta.
			//
			// The mock was paused across the sample, so this holds by
			// construction rather than by winning a race with the election.
			Expect(requestsAtSample).To(Equal(preRestoreRequests),
				"the mirror worker fetched from the source before the cursor was sampled, so this read cannot distinguish a rebuilt mark from a re-ingested one")

			// EN-1776: RebuildDelta folded every other boundary field but left
			// last_mirror_v2_log_id at 0, so this read returned 0.
			Expect(restoredCursor).To(Equal(preBackupCursor),
				"restored mirror cursor must match the position the backup captured")
		})

		It("reports FOLLOWING for the idle restored mirror", func() {
			// A rebuilt mark ahead of the applied prefix would be rejected by
			// the FSM's contiguity guard and surface here as
			// ERROR_REASON_MIRROR_V2_LOG_ID_GAP; a mark behind it re-ingests
			// silently, which the next spec covers.
			//
			// ReadMirrorSyncProgress derives FOLLOWING from
			// `sourceHead > 0 && cursor >= sourceHead`, and the source head lives
			// in SubPLMirrorSourceHead, which RebuildDelta does NOT reconstruct.
			// A correctly restored mirror has nothing left to ingest, so the
			// ingest path — which normally bundles the source head into its data
			// proposal — never runs. To keep an idle restored mirror from
			// reporting SYNCING forever, the caught-up worker publishes the
			// refreshed source head on its own (EN-1773), so FOLLOWING becomes
			// observable without waiting for a new source log.
			Eventually(func(g Gomega) {
				progress := syncProgress(g, client)
				g.Expect(progress.GetError().GetMessage()).To(BeEmpty())
				g.Expect(progress.GetCursor()).To(Equal(preBackupCursor))
				g.Expect(progress.GetRemainingLogs()).To(BeZero())
				// Pin the published head itself, not just the derived state:
				// FOLLOWING is `sourceHead > 0 && cursor >= sourceHead` and
				// RemainingLogs is `sourceHead > cursor`, so both are satisfied
				// by ANY head from 1 to sourceLogCount. Only this assertion
				// separates "published the real head" from "published a head".
				g.Expect(progress.GetSourceLogCount()).To(Equal(uint64(sourceLogCount)))
				g.Expect(progress.GetState()).To(Equal(commonpb.MirrorSyncState_MIRROR_SYNC_STATE_FOLLOWING))
			}).Within(60 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})

		It("does not re-apply source logs it had already applied", func() {
			// The deterministic half of the regression: a worker restarting at
			// source log 1 re-applies all four ingests, and because mirrored
			// transactions keep the source transaction id the damage shows up
			// as doubled volumes rather than extra rows. Held over two poll
			// intervals so a re-ingest that has not yet ticked cannot pass.
			Consistently(func(g Gomega) {
				expectSingleIngestion(g, client, "restored")
			}, 12*time.Second, 1*time.Second).Should(Succeed())
		})

		It("passes the integrity checker after restoring the non-empty mirror delta", func() {
			result, err := actions.CollectCheckStoreEvents(ctx, client)
			Expect(err).To(Succeed())
			Expect(result.Errors).To(BeEmpty())
			Expect(result.Progress).ToNot(BeEmpty())
		})

		It("ingests new source logs and keeps transaction ids aligned", func() {
			mockV2.addLog(newV2TransactionLog(postRestoreLogID, postRestoreTxID, "world", account, perTxAmount, asset))

			Eventually(func(g Gomega) {
				progress := syncProgress(g, client)
				g.Expect(progress.GetError().GetMessage()).To(BeEmpty())
				g.Expect(progress.GetCursor()).To(Equal(uint64(postRestoreLogID)))

				txs, err := listAllTransactions(ctx, client, ledgerName, 100, 0)
				g.Expect(err).To(Succeed())
				g.Expect(txs).To(HaveLen(preBackupTxCount + 1))

				ids := make([]uint64, 0, len(txs))
				for _, tx := range txs {
					ids = append(ids, tx.GetId())
				}
				// Mirrored transactions carry the source id verbatim, so a
				// NextTransactionId that drifted across the restore would show
				// up as a missing or renumbered id here.
				g.Expect(ids).To(ContainElement(uint64(postRestoreTxID)),
					fmt.Sprintf("mirrored transaction ids: %v", ids))

				// FOLLOWING again after the fresh ingest advances both the
				// cursor and the source head in lock-step.
				g.Expect(progress.GetSourceLogCount()).To(Equal(uint64(postRestoreLogID)))
				g.Expect(progress.GetState()).To(Equal(commonpb.MirrorSyncState_MIRROR_SYNC_STATE_FOLLOWING))
				g.Expect(progress.GetRemainingLogs()).To(BeZero())
			}).Within(60 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})
	})
})
