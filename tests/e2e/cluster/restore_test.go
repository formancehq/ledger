//go:build e2e && s3

package cluster

import (
	"context"
	"encoding/json"
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
	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/infra/node"
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
	"google.golang.org/grpc/metadata"
)

const (
	restoreMinioAccessKey = "minioadmin"
	restoreMinioSecretKey = "minioadmin"
	restoreS3Bucket       = "restore-backup"
	restoreS3Region       = "us-east-1"
)

// readS3Manifest fetches and parses the backup manifest from S3. bucketID
// defaults to the cluster ID ("test-cluster") when the backup request omits it.
func readS3Manifest(ctx context.Context, client *s3.Client) (*backup.Manifest, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(restoreS3Bucket),
		Key:    aws.String("test-cluster/backups/manifest.json"),
	})
	if err != nil {
		return nil, err
	}

	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}

	var manifest backup.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

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
		httpPort    = testutil.TestSingleHTTPPort
		grpcPort    = testutil.TestSingleGRPCPort
		raftPort    = grpcPort - 1000
		ledgerName  = "restore-ledger"
		ledger2     = "restore-ledger-2"
		chartLedger = "restore-chart-ledger"
		deltaLedger = "restore-ledger-delta"
		deltaRef    = "delta-ref-1"
	)

	var (
		ctx            context.Context
		restoreWalDir  string
		restoreDataDir string
		minioEndpoint  string
		s3Client       *s3.Client
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

		s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
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
			// A 1-byte segment cap forces the incremental export to split at every
			// sequence boundary, so a multi-sequence post-checkpoint range fans out
			// into several segments per type — exercising split-and-restore below.
			instruments = append(instruments, testserver.WithBackupMaxSegmentBytes(1))

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

			// eve is funded BEFORE the checkpoint (input=1000, output=0). Its 0xFF
			// cache entry is captured in the checkpoint; the drain below (post
			// checkpoint) makes that cache entry stale. Phase 3 exercises it via apply.
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "eve", big.NewInt(1000), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger2, nil)))
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledger2, []*commonpb.Posting{
				actions.NewPosting("world", "treasury", big.NewInt(50000), "EUR"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			// chart-ledger has a restrictive account-type chart declared BEFORE the
			// checkpoint (type "main"), so strict enforcement is active. "wallet" is
			// added after the checkpoint (below), so Phase 3 can check the apply path
			// enforces the FULL chart — pre- and post-checkpoint types — on a restored
			// node, reading LedgerInfo.AccountTypes from the cache.
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(chartLedger, nil)))
			Expect(err).To(Succeed())
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.AddAccountTypeAction(chartLedger, "main", "main:{id}")))
			Expect(err).To(Succeed())
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(chartLedger, []*commonpb.Posting{
				actions.NewPosting("world", "main:1", big.NewInt(100), "USD"),
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

			// More post-checkpoint transactions so the incremental log/audit
			// ranges span many sequences; with the 1-byte segment cap the export
			// splits into several segments per type.
			for i := range 8 {
				_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", fmt.Sprintf("split-acct-%d", i), big.NewInt(100), "USD"),
				}, nil, nil)))
				Expect(err).To(Succeed())
			}

			// Drain eve (a PRE-checkpoint account) AFTER the checkpoint: its volume
			// changes only in the delta (output 0 → 1000, balance → 0). eve's 0xFF
			// cache entry is now checkpoint-era stale while 0xF1 is rebuilt fresh.
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("eve", "world", big.NewInt(1000), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			// Add an account type AFTER the checkpoint, so the chart-ledger's chart
			// changes only in the delta. Phase 3 checks the apply path enforces it.
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.AddAccountTypeAction(chartLedger, "wallet", "wallet:{id}")))
			Expect(err).To(Succeed())

			// A ledger created AFTER the full checkpoint lives only in the
			// incremental exports. Its LedgerInfo + LedgerBoundaries must be
			// rebuilt into the 0xF1 attribute zone the apply-path preload reads,
			// or a later write to it fails ErrLedgerNotFound. The funding tx
			// carries a reference so Phase 3 can check reference idempotency.
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(deltaLedger, nil)))
			Expect(err).To(Succeed())
			fundDelta := actions.CreateTransactionAction(deltaLedger, []*commonpb.Posting{
				actions.NewPosting("world", "founder", big.NewInt(9000), "USD"),
			}, nil, nil)
			fundDelta.GetApply().GetAction().GetCreateTransaction().Reference = deltaRef
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", fundDelta))
			Expect(err).To(Succeed())

			// Declare metadata field types AFTER the full checkpoint, so they live
			// only in the incremental export segments — the RebuildDelta replay
			// path, not the raw checkpoint copy. Phase 3 reads them back.
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.SetMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "tier", commonpb.MetadataType_METADATA_TYPE_STRING)))
			Expect(err).To(Succeed())
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.SetMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_LEDGER, "region", commonpb.MetadataType_METADATA_TYPE_STRING)))
			Expect(err).To(Succeed())
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.SetMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category", commonpb.MetadataType_METADATA_TYPE_INT64)))
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

			// The tiny segment cap must have split the log export into multiple
			// segments — proving splitting through the real server → S3 path.
			manifest, err := readS3Manifest(ctx, s3Client)
			Expect(err).To(Succeed())

			logSegments := 0
			for _, seg := range manifest.Exports {
				if seg.Type == "log" {
					logSegments++
				}
			}

			Expect(logSegments).To(BeNumerically(">", 1),
				"a 1-byte segment cap must split the multi-sequence log export into multiple segments")
		})

		It("should write more data and take a SECOND incremental backup", func() {
			// A second incremental round: data written here lives only in the
			// export segments appended by THIS run, on top of the segments the
			// first incremental already published. A restore must apply the full
			// checkpoint + BOTH incrementals to see it — the full + multiple
			// incrementals chain EN-888 requires.
			exportsBefore, err := readS3Manifest(ctx, s3Client)
			Expect(err).To(Succeed())
			segCountBefore := len(exportsBefore.Exports)

			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "erin", big.NewInt(2500), "USD"),
			}, map[string]string{"type": "second-incremental"}, nil)))
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
				"the second incremental must export its own post-checkpoint log entries")

			manifestAfter, err := readS3Manifest(ctx, s3Client)
			Expect(err).To(Succeed())
			Expect(len(manifestAfter.Exports)).To(BeNumerically(">", segCountBefore),
				"the second incremental must accumulate additional export segments in the manifest")
		})

		It("should be a no-op incremental when nothing changed since the last one", func() {
			// Not a concurrency test — the FSM per-destination mutual exclusion
			// (EN-1055) is covered deterministically by the unit/state tests
			// (internal/infra/state/backup_jobs_test.go). This documents the
			// sequential idempotency of a repeated incremental: with no new
			// data, it exports nothing and does not disturb the manifest.
			req := &clusterpb.IncrementalBackupRequest{
				Storage: testutil.S3BackupStorage(&commonpb.S3StorageConfig{
					Bucket:   restoreS3Bucket,
					Region:   restoreS3Region,
					Endpoint: minioEndpoint,
				}),
			}

			resp, err := clusterClient.IncrementalBackup(ctx, req)
			Expect(err).To(Succeed())
			Expect(resp.GetLogEntriesExported()).To(BeZero(),
				"a no-op incremental after the second round must export nothing")
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

			Expect(resp.LedgerCount).To(Equal(uint32(4)))
			Expect(resp.LedgerNames).To(ConsistOf(ledgerName, ledger2, chartLedger, deltaLedger))
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

		It("should carry the checkpoint's applied index in the marker as the genesis boundary", func() {
			// The restored bootstrap plants its WAL snapshot at the marker
			// index, and the FSM gap check requires the first post-restore
			// entry at exactly boundary+1 — so a marker diverging from the
			// store, or a boundary of 0/1 here, would either fail Phase 3
			// outright or silently re-open the learner log-replay hole.
			data, err := os.ReadFile(restoreDataDir + "/RESTORED")
			Expect(err).To(Succeed())

			var marker node.RestoredMarker
			Expect(json.Unmarshal(data, &marker)).To(Succeed())

			manifest, err := readS3Manifest(ctx, s3Client)
			Expect(err).To(Succeed())
			Expect(manifest.Checkpoint).NotTo(BeNil())

			Expect(marker.LastAppliedIndex).To(Equal(manifest.Checkpoint.LastAppliedIndex),
				"marker must preserve the checkpoint's applied index")
			Expect(marker.LastAppliedIndex).To(BeNumerically(">", 1),
				"this suite's checkpoint is taken after real traffic, so the preserved boundary must exercise the non-fallback (N > 1) path")
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
			// No background raft snapshot may exist when the learner-join spec
			// below runs: it exercises the window where the leader's log is
			// the only catch-up source, and a maintenance snapshot would
			// legitimately force the MsgSnap path and mask a log-replay
			// regression. The interval override wins over the default (pflag
			// keeps the last occurrence); the margin blocks the snapshot
			// trigger outright.
			instruments = append(instruments,
				testserver.WithMaintenanceInterval(time.Hour),
				testserver.WithRaftCompactionMargin(1_000_000),
			)
			// A rotation threshold far below the preserved genesis boundary:
			// admission's CheckCache compares Gen(boundary+1) against the
			// in-memory generation, and the restored store carries no
			// persisted generation meta (PrepareForBackup wipes the cache
			// zone) — the boot-side realignment is what keeps proposals
			// admissible; without it every write here trips the
			// CacheUnreachable horizon.
			instruments = append(instruments, testserver.WithCacheRotationThreshold(3))

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

		It("should preserve the metadata schema declared after the checkpoint", func() {
			// The field types were declared post-checkpoint, so they exist only in
			// the incremental exports and must be reconstructed by the RebuildDelta
			// replay. RebuildDelta currently rebuilds only volumes/metadata/tx state
			// and drops the schema, so this fails on a restored node.
			resp, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{Ledger: ledgerName})
			Expect(err).To(Succeed())

			Expect(resp.GetAccountFields()).To(HaveKey("tier"))
			Expect(resp.GetAccountFields()["tier"].GetDeclaredType()).To(Equal(commonpb.MetadataType_METADATA_TYPE_STRING))
			Expect(resp.GetLedgerFields()).To(HaveKey("region"))
			Expect(resp.GetTransactionFields()).To(HaveKey("category"))
			Expect(resp.GetTransactionFields()["category"].GetDeclaredType()).To(Equal(commonpb.MetadataType_METADATA_TYPE_INT64))
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

		It("should have data from the SECOND incremental restored", func() {
			// erin was funded in the second incremental round, so this balance
			// is present only if the restore applied the FULL checkpoint plus
			// BOTH incrementals — the full + multiple incrementals chain.
			erinResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "erin"})
			Expect(err).To(Succeed())
			Expect(erinResp.Volumes["USD"].Input).To(Equal("2500"),
				"transaction written in the second incremental must be restored from the full + multi-incremental chain")
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

		It("should apply against a PRE-checkpoint account's post-checkpoint balance after restore", func() {
			// eve was funded before the checkpoint (input=1000) and drained after
			// it (output=1000, balance 0). The drain lives only in the delta, so
			// eve's 0xFF cache entry is checkpoint-era (input=1000, output=0) while
			// 0xF1 is rebuilt fresh. The apply path reads the cache: a cache-blind
			// restore adds to the stale (1000,0) and writes it back to 0xF1,
			// clobbering the drain; a cache-aware restore refreshes 0xFF so apply
			// sees (1000,1000). GetAccount below reads 0xF1 and exposes the clobber.
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "eve", big.NewInt(500), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			eveResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "eve"})
			Expect(err).To(Succeed())
			Expect(eveResp.Volumes["USD"].Input).To(Equal("1500"))
			Expect(eveResp.Volumes["USD"].Output).To(Equal("1000"),
				"apply must see eve's post-checkpoint drain via a cache-aware restore; a cache-blind restore clobbers output to 0")
		})

		It("should enforce the restored account-type chart on the apply path after restore", func() {
			// chart-ledger's chart is "main" (pre-checkpoint) + "wallet"
			// (post-checkpoint). Strict enforcement reads the chart from
			// LedgerInfo.AccountTypes via the FSM cache. A restored node must apply
			// against the full, fresh chart. This is the LEDGER analogue of the eve
			// volume test: it proves the apply path (not just the 0xF1 query path)
			// sees rebuilt LedgerInfo — with #1554's 0xF1-only writes.

			// Pre-checkpoint type survived restore and is still enforced.
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(chartLedger, []*commonpb.Posting{
				actions.NewPosting("world", "main:2", big.NewInt(10), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed(), "pre-checkpoint account type must survive restore and match on the apply path")

			// Post-checkpoint type is present in the apply-path cache after restore.
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(chartLedger, []*commonpb.Posting{
				actions.NewPosting("world", "wallet:1", big.NewInt(10), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed(), "post-checkpoint account type must be applied on the restored node's apply path")

			// The chart is genuinely restrictive — an account matching neither type
			// is rejected. This guards that the two Succeeds above aren't passing
			// merely because enforcement is off / the chart was lost on restore.
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(chartLedger, []*commonpb.Posting{
				actions.NewPosting("world", "random:1", big.NewInt(10), "USD"),
			}, nil, nil)))
			Expect(err).To(HaveOccurred(), "under strict enforcement, an account matching no declared type must be rejected")
		})

		It("should have the delta ledger's data restored from export segments", func() {
			founderResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: deltaLedger, Address: "founder"})
			Expect(err).To(Succeed())
			Expect(founderResp.Volumes["USD"].Input).To(Equal("9000"))
		})

		It("should reconstruct ledger stats for a ledger created after the checkpoint", func() {
			// deltaLedger was created and funded (1 tx, 1 posting world->founder)
			// entirely in the delta. RebuildDelta must rebuild its boundary
			// counters from the exports, not leave them at zero.
			stats, err := actions.GetLedgerStats(ctx, client, deltaLedger)
			Expect(err).To(Succeed())
			Expect(stats.GetTransactionCount()).To(Equal(uint64(1)))
			Expect(stats.GetLogCount()).To(Equal(uint64(1)))
			Expect(stats.GetPostingCount()).To(Equal(uint64(1)))
			Expect(stats.GetVolumeCount()).To(Equal(uint64(2)), "world + founder")
			Expect(stats.GetMetadataCount()).To(Equal(uint64(0)))
			Expect(stats.GetRevertCount()).To(Equal(uint64(0)))
			Expect(stats.GetReferenceCount()).To(Equal(uint64(1)), "the funding tx carried a reference")
		})

		It("should enforce reference idempotency reconstructed from the delta", func() {
			// The delta funding tx carried deltaRef. RebuildDelta must rebuild
			// the reference index into the 0xF1 attribute zone, or reusing the
			// reference is wrongly accepted after restore.
			dup := actions.CreateTransactionAction(deltaLedger, []*commonpb.Posting{
				actions.NewPosting("founder", "someone", big.NewInt(1), "USD"),
			}, nil, nil)
			dup.GetApply().GetAction().GetCreateTransaction().Reference = deltaRef

			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", dup))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("already exists"))
		})

		It("should accept transactions on a ledger created after the checkpoint", func() {
			// deltaLedger was created after the full checkpoint, so its
			// LedgerInfo and LedgerBoundaries live only in the incremental
			// exports. Queries (ListLedgers, GetAccount) read the global/volume
			// zones and see it, but the apply path preloads the ledger and its
			// boundaries from the 0xF1 attribute zone: RebuildDelta must rebuild
			// both there, or loadLedger/loadBoundaries return ErrLedgerNotFound
			// and this write fails.
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(deltaLedger, []*commonpb.Posting{
				actions.NewPosting("founder", "employee", big.NewInt(1200), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			employeeResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: deltaLedger, Address: "employee"})
			Expect(err).To(Succeed())
			Expect(employeeResp.Volumes["USD"].Input).To(Equal("1200"))
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

		It("should transfer the restored state to a learner joining before any raft snapshot", func() {
			// A restored node's FSM genesis is the whole backup, but its raft
			// log is brand new. Unless bootstrap plants a snapshot above the
			// log start, the leader catches a fresh learner up by plain log
			// replay from index 1 — the learner applies the few post-restore
			// entries onto an EMPTY store and silently misses every restored
			// row (found by the Antithesis model test as an aggregated volume
			// imbalance on the joiner). The join must instead be forced
			// through the snapshot → checkpoint-sync path.
			const (
				joinerGRPCPort = grpcPort + 7
				joinerHTTPPort = httpPort + 7
				joinerRaftPort = raftPort + 7
			)

			// Committed on the restored leader only: its replication to the
			// learner proves the learner finished catching up on the
			// post-restore log. Its visibility is NOT proof of a correct
			// join — every proposal ships its cache preload, so entries
			// re-materialize the rows they touch even on a hollow store.
			// That masking is exactly why alice below is the real check.
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "join-fence", big.NewInt(42), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
				NodeID:    2,
				ClusterID: "test-cluster",
				HTTPPort:  joinerHTTPPort,
				RaftPort:  joinerRaftPort,
				GRPCPort:  joinerGRPCPort,
				WalDir:    GinkgoT().TempDir(),
				DataDir:   GinkgoT().TempDir(),
				Debug:     testutil.Debug,
				Output:    GinkgoWriter,
			})
			instruments = append(instruments, testserver.WithJoin(fmt.Sprintf("127.0.0.1:%d", raftPort)))

			joiner := testservice.New(cmdserver.NewRunCommand,
				testservice.WithInstruments(instruments...),
			)
			Expect(joiner.Start(ctx)).To(Succeed())
			DeferCleanup(func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = joiner.Stop(stopCtx)
			})

			joinerClient, _, joinerConn, err := testutil.NewGRPCClient(joinerGRPCPort)
			Expect(err).To(Succeed())
			DeferCleanup(func() { _ = joinerConn.Close() })

			// Stale consistency pins every read below to the learner's own
			// store: linearizable reads on a syncing node transparently fall
			// back to the leader (readCtrl's leader_fallback), which would
			// let node 1 answer for the learner and pass this test without
			// proving anything about the learner's state.
			staleCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "stale")

			// This converges only once the learner itself applied the fence
			// entry.
			Eventually(func(g Gomega) {
				resp, err := joinerClient.GetAccount(staleCtx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "join-fence"})
				g.Expect(err).To(Succeed())
				g.Expect(resp.GetVolumes()["USD"].GetInput()).To(Equal("42"))
			}).Within(60*time.Second).ProbeEvery(500*time.Millisecond).Should(Succeed(),
				"learner never caught up on the post-restore raft log")

			// alice was written only BEFORE the backup, so no post-restore
			// entry (and no preload) can re-materialize her: she is on the
			// learner iff the restored store itself was transferred.
			aliceResp, err := joinerClient.GetAccount(staleCtx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "alice"})
			Expect(err).To(Succeed())
			Expect(aliceResp.GetVolumes()).To(HaveKey("USD"),
				"learner caught up by log replay alone: the restored state never reached it")
			Expect(aliceResp.GetVolumes()["USD"].GetInput()).To(Equal("3000"))

			treasuryResp, err := joinerClient.GetAccount(staleCtx, &servicepb.GetAccountRequest{Ledger: ledger2, Address: "treasury"})
			Expect(err).To(Succeed())
			Expect(treasuryResp.GetVolumes()).To(HaveKey("EUR"),
				"ledger untouched since the restore must still reach the learner")
			Expect(treasuryResp.GetVolumes()["EUR"].GetInput()).To(Equal("50000"))
		})
	})
})
