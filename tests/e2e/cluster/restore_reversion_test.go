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

// This suite pins the reversion-state loss the model test surfaced (a revert
// the model knew was already applied got re-attempted after a restore). The
// FSM's already-reverted gate reads the per-ledger reversion bitset
// (ZonePerLedger/SubPLReversions), which only the FSM hot path persists; the
// restore rebuild replays RevertedTransaction logs into the per-transaction
// RevertedByTransaction markers but must also rebuild the bitset — otherwise
// every revert inside the exported delta is forgotten by the gate, and a
// second revert of the same transaction passes it. When the account still
// holds funds, that second revert SUCCEEDS and moves them again (a double
// refund); otherwise it surfaces as a wrong rejection (insufficient funds /
// account-type mismatch) instead of TRANSACTION_ALREADY_REVERTED.
var _ = Describe("Restore reversion bitset", Ordered, func() {
	const (
		httpPort   = testutil.TestSingleHTTPPort
		grpcPort   = testutil.TestSingleGRPCPort
		raftPort   = grpcPort - 1000
		ledgerName = "revbits-ledger"
		s3Bucket   = "restore-reversion-bitset"
		clusterID  = "revbits-cluster"

		account = "acc:1"
		// revertedTxID is funded first (id 1) and reverted pre-backup; fundTxID
		// (id 2) keeps the account solvent so a post-restore double revert
		// would succeed rather than trip on the balance check.
		revertedTxID = 1
	)

	var (
		ctx            context.Context
		restoreWalDir  string
		restoreDataDir string
		minioEndpoint  string
	)

	// expectAlreadyReverted asserts a second revert of revertedTxID is
	// rejected by the already-reverted gate.
	expectAlreadyReverted := func(client servicepb.BucketServiceClient, phase string) {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.RevertTransactionAction(ledgerName, revertedTxID, false, false, nil),
		))
		Expect(err).To(HaveOccurred(), "%s: double revert of tx %d must be rejected", phase, revertedTxID)
		Expect(err.Error()).To(ContainSubstring("already reverted"), "%s: rejection reason", phase)
	}

	// expectAccountVolumes asserts the account's USD volumes: 600 in
	// (100 + 500 funding), 100 out (the single legitimate revert).
	expectAccountVolumes := func(client servicepb.BucketServiceClient, phase string) {
		acct, err := actions.GetAccount(ctx, client, ledgerName, account)
		Expect(err).To(Succeed())

		vol := acct.FindVolume("USD", "")
		Expect(vol).ToNot(BeNil(), "%s: %s USD volumes missing", phase, account)
		Expect(vol.GetInput()).To(Equal("600"), "%s: %s USD input", phase, account)
		Expect(vol.GetOutput()).To(Equal("100"), "%s: %s USD output", phase, account)
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

		restoreWalDir, err = os.MkdirTemp("", "revbits-restore-wal-*")
		Expect(err).To(Succeed())
		restoreDataDir, err = os.MkdirTemp("", "revbits-restore-data-*")
		Expect(err).To(Succeed())
	})

	Describe("Phase 1: a reverted transaction in the exported delta", Ordered, func() {
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
				HTTPPort:  httpPort,
				RaftPort:  raftPort,
				GRPCPort:  grpcPort,
				WalDir:    GinkgoT().TempDir(),
				DataDir:   GinkgoT().TempDir(),
				Debug:     testutil.Debug,
				Output:    GinkgoWriter,
			})
			instruments = append(instruments, testserver.WithBootstrap())

			sourceServer = testservice.New(cmdserver.NewRunCommand, testservice.WithInstruments(instruments...))
			Expect(sourceServer.Start(ctx)).To(Succeed())

			var err error
			client, clusterClient, grpcConn, err = testutil.NewGRPCClient(grpcPort)
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) bool {
				state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
				g.Expect(err).To(Succeed())
				return state.Leader != 0
			}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(BeTrue())

			// Full checkpoint on the EMPTY store: everything after it lands in
			// the incremental delta, so the restore reconstructs the reversion
			// state purely by replaying the exported log instead of copying
			// checkpoint files.
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

		It("creates, funds, and reverts a transaction", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.CreateLedgerAction(ledgerName, nil),
			))
			Expect(err).To(Succeed())

			// tx 1: the transaction that gets reverted.
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", account, big.NewInt(100), "USD"),
				}, nil, nil),
			))
			Expect(err).To(Succeed())

			// tx 2: independent funding, so the account stays solvent after
			// the revert and a double revert would not trip the balance check.
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", account, big.NewInt(500), "USD"),
				}, nil, nil),
			))
			Expect(err).To(Succeed())

			// Revert tx 1 (creates tx 3, moving the 100 back to world).
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.RevertTransactionAction(ledgerName, revertedTxID, false, false, nil),
			))
			Expect(err).To(Succeed())
		})

		It("rejects a double revert on the live node", func() {
			// Premise guard: if the live gate did not hold, the post-restore
			// assertions would be vacuous.
			expectAlreadyReverted(client, "live")
			expectAccountVolumes(client, "live")
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

	Describe("Phase 3: verify the restored reversion state", Ordered, func() {
		var (
			client   servicepb.BucketServiceClient
			grpcConn *grpc.ClientConn
			server   *testservice.Service
		)

		BeforeAll(func() {
			instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
				NodeID:    1,
				ClusterID: clusterID,
				HTTPPort:  httpPort,
				RaftPort:  raftPort,
				GRPCPort:  grpcPort,
				WalDir:    restoreWalDir,
				DataDir:   restoreDataDir,
				Debug:     testutil.Debug,
				Output:    GinkgoWriter,
			})
			instruments = append(instruments, testserver.WithBootstrap())

			server = testservice.New(cmdserver.NewRunCommand, testservice.WithInstruments(instruments...))
			Expect(server.Start(ctx)).To(Succeed())

			var clusterClient clusterpb.ClusterServiceClient
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

		It("still rejects a double revert after the rebuild", func() {
			expectAlreadyReverted(client, "restored")
			// A successful double revert would have moved another 100 out of
			// the account; the volumes pin that no funds moved.
			expectAccountVolumes(client, "restored")
		})
	})
})
