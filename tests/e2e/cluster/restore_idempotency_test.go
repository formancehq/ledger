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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This suite pins the idempotency-key loss the model test surfaced: keys frozen
// after the last full checkpoint live only in the audit, and the raw-SST
// checkpoint copy can't carry them, so unless RebuildDelta re-derives them a
// restored node forgets every key in the exported delta. Forgotten, a retried
// key is re-executed instead of replayed (a duplicate), and a reused key with a
// different body is executed instead of rejected IDEMPOTENCY_KEY_CONFLICT — the
// same shape as the reversion-bitset loss (see restore_reversion_test.go).
//
// It also proves the frozen expires_at projection survives restore across both
// classifications (invariant #11): a keyed outcome frozen BEFORE the checkpoint
// (Preserved via the raw-SST copy) and outcomes frozen AFTER it (Rebuilt from the
// exported audit delta) both dedup after restore, and CheckStore on the restored
// node re-derives and verifies each expires_at against the chain.
var _ = Describe("Restore idempotency keys", Ordered, func() {
	const (
		ledgerName = "idem-restore-ledger"
		s3Bucket   = "restore-idempotency-keys"
		clusterID  = "idem-restore-cluster"

		account     = "acc:1"
		replayKey   = "idem-replay-key"
		conflictKey = "idem-conflict-key"

		// preservedKey is committed BEFORE the full checkpoint, so its outcome
		// rides the raw-SST checkpoint copy (the Preserved path); preservedAccount
		// keeps its volume separate from the delta-path keys' account.
		preservedKey     = "idem-preserved-key"
		preservedAccount = "acc:preserved"
	)

	// The source node is stopped and comes back in restore mode on fresh
	// directories, then once more as a normal node. That is one logical node
	// returning, so every phase reuses the same allocated ports: ports are
	// never released, and a restart on a fresh set would surface as a Raft
	// failure rather than a port mistake.
	lease := testserver.AllocateNodeLease()
	ports := lease.Ports()

	var (
		ctx            context.Context
		restoreWalDir  string
		restoreDataDir string
		minioEndpoint  string
	)

	// replayTx is the keyed commit re-sent to check the replay path: world -> acc
	// for 100 USD. conflictSeed freezes conflictKey with a 50 USD body;
	// conflictBody reuses that key with a different amount, which must conflict.
	replayTx := func() *servicepb.ApplyRequest {
		return actions.WithIdempotencyKey(replayKey,
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", account, big.NewInt(100), "USD"),
			}, nil, nil),
		)
	}
	conflictSeed := func() *servicepb.ApplyRequest {
		return actions.WithIdempotencyKey(conflictKey,
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", account, big.NewInt(50), "USD"),
			}, nil, nil),
		)
	}
	conflictBody := func() *servicepb.ApplyRequest {
		return actions.WithIdempotencyKey(conflictKey,
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", account, big.NewInt(999), "USD"),
			}, nil, nil),
		)
	}

	// preservedTx is a keyed commit made BEFORE the full checkpoint, so its
	// outcome (with its frozen expires_at and eviction time-index entry) is
	// carried in the raw-SST checkpoint copy — the Preserved path — rather than
	// the exported delta.
	preservedTx := func() *servicepb.ApplyRequest {
		return actions.WithIdempotencyKey(preservedKey,
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", preservedAccount, big.NewInt(77), "USD"),
			}, nil, nil),
		)
	}

	// expectIdempotency asserts both dedup directions against a node, then pins
	// the account volume. A forgotten key would re-execute the replay (input
	// climbs past 150) or execute the conflicting body (no error, input climbs),
	// so either divergence is caught.
	expectIdempotency := func(client servicepb.BucketServiceClient, phase string) {
		// Same key + same body: replays the committed success, no re-execution.
		_, err := client.Apply(ctx, replayTx())
		Expect(err).To(Succeed(), "%s: replaying a committed key must succeed", phase)

		// Same key + different body: rejected, not executed.
		_, err = client.Apply(ctx, conflictBody())
		Expect(err).To(HaveOccurred(), "%s: reusing a key with a different body must conflict", phase)
		Expect(status.Code(err)).To(Equal(codes.AlreadyExists), "%s: conflict must surface as AlreadyExists", phase)

		// The conflict must NOT have clobbered the committed outcome: re-sending the
		// ORIGINAL body under conflictKey still replays its committed success. A
		// restore that froze the conflict entry over the original (no overwrite
		// guard in the rebuild) would hash-mismatch here and conflict instead.
		_, err = client.Apply(ctx, conflictSeed())
		Expect(err).To(Succeed(), "%s: the original body must still replay after a conflict, not conflict itself", phase)

		// Each key moved funds exactly once: 100 + 50 = 150.
		acct, err := actions.GetAccount(ctx, client, ledgerName, account)
		Expect(err).To(Succeed())

		vol := acct.FindVolume("USD", "")
		Expect(vol).ToNot(BeNil(), "%s: %s USD volumes missing", phase, account)
		Expect(vol.GetInput()).To(Equal("150"), "%s: %s USD input (keys must dedup, no double-apply)", phase, account)
	}

	storage := func() *commonpb.BackupStorage {
		return testutil.S3BackupStorage(&commonpb.S3StorageConfig{
			Bucket:   s3Bucket,
			Region:   restoreS3Region,
			Endpoint: minioEndpoint,
		})
	}

	BeforeAll(func() {
		ctx = logging.TestingContext()

		container, err := testcontainers.Run(context.Background(), testutil.MinIOImage,
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

		restoreWalDir, err = os.MkdirTemp("", "idem-restore-wal-*")
		Expect(err).To(Succeed())
		restoreDataDir, err = os.MkdirTemp("", "idem-restore-data-*")
		Expect(err).To(Succeed())
	})

	Describe("Phase 1: keyed commits in the exported delta", Ordered, func() {
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

			// Pre-checkpoint state (invariant #11 "meaningful state before the full
			// checkpoint"): a ledger and one keyed outcome frozen BEFORE the
			// checkpoint, so its expires_at + eviction time-index entry ride the
			// raw-SST checkpoint copy — the Preserved path.
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
			_, err = client.Apply(ctx, preservedTx())
			Expect(err).To(Succeed())

			// Full checkpoint carries the ledger and the Preserved key; every keyed
			// commit AFTER it lands in the incremental delta (the Rebuilt path), so
			// the restore exercises both halves of the expires_at projection.
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

		It("commits two keyed transactions after the checkpoint", func() {
			// The ledger and the Preserved key were committed before the checkpoint
			// (see BeforeAll); these two land in the incremental delta.
			_, err := client.Apply(ctx, replayTx())
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, conflictSeed())
			Expect(err).To(Succeed())
		})

		It("dedups on the live node", func() {
			// Premise guard: if the live gate did not hold, the post-restore
			// assertions would be vacuous.
			expectIdempotency(client, "live")
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

	Describe("Phase 3: verify the restored idempotency keys", Ordered, func() {
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
			instruments = append(instruments, testserver.WithBootstrap())

			server = lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
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

		It("still dedups the delta (Rebuilt) keys after the rebuild", func() {
			expectIdempotency(client, "restored")
		})

		It("replays the pre-checkpoint (Preserved) key after the rebuild", func() {
			// preservedKey's outcome rode the raw-SST checkpoint copy, not the
			// exported delta; replaying it after restore proves the Preserved half
			// of the expires_at projection survived (CheckStore below re-verifies
			// its frozen expires_at against the audit chain).
			_, err := client.Apply(ctx, preservedTx())
			Expect(err).To(Succeed(), "restored: replaying the pre-checkpoint key must succeed")

			acct, err := actions.GetAccount(ctx, client, ledgerName, preservedAccount)
			Expect(err).To(Succeed())
			vol := acct.FindVolume("USD", "")
			Expect(vol).ToNot(BeNil(), "restored: %s USD volume missing", preservedAccount)
			Expect(vol.GetInput()).To(Equal("77"), "the pre-checkpoint key must dedup (no double-apply)")
		})

		It("passes CheckStore on the restored store", func() {
			// Each keyed outcome's frozen expires_at is re-derived from the audit
			// chain (Rebuilt keys by RebuildDelta; Preserved keys carried in the
			// checkpoint). CheckStore re-derives that expiry from the hash chain and
			// flags any divergence, so a clean result proves the frozen retention
			// deadline (and its eviction time-index entry) survived restore across
			// both halves — the cross-lifecycle proof for invariant #11.
			result, err := actions.CollectCheckStoreEvents(ctx, client)
			Expect(err).To(Succeed())
			Expect(result.Errors).To(BeEmpty(), "CheckStore errors on the restored store: %v", result.Errors)
		})
	})
})
