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
	"github.com/formancehq/ledger/v3/internal/domain"
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

// Frozen idempotency failures across an incremental restore. A keyed failure
// committed after the last full checkpoint lives only in the exported audit
// delta; RebuildDelta re-derives its frozen outcome from the audit reason
// (state.IdempotencyValueFromAudit, gated on domain.KindForReason). If that
// re-freeze were lost, a retry on the restored node would re-execute against
// the new state and commit — a different outcome for the same key. If it
// over-froze, a retryable failure would be pinned forever.
//
// Three keyed failures sit in the delta:
//   - frozenKey fails INSUFFICIENT_FUNDS (KindPrecondition, freezable). Its
//     source is funded afterwards, so only the frozen outcome keeps the retry
//     failing.
//   - execKey fails NUMSCRIPT_EXECUTION_ERROR (KindPrecondition, freezable):
//     it sends balance(@execSource) - 20, a negative amount until execSource is
//     funded afterwards.
//   - retryableKey fails PRELOAD_UNAVAILABLE (KindUnavailable, not freezable):
//     a meta() read of a missing key, which admission forwards to the FSM under
//     an idempotency key. Once the metadata exists, a retry must execute.
var _ = Describe("Restore frozen idempotency failures", Ordered, func() {
	const (
		ledgerName = "idem-failure-restore-ledger"
		s3Bucket   = "restore-idempotency-failures"
		clusterID  = "idem-failure-restore-cluster"

		frozenKey     = "idem-frozen-failure-key"
		poorAccount   = "acc:poor"
		frozenSink    = "acc:frozen-sink"
		execKey       = "idem-frozen-execution-key"
		execSource    = "acc:exec-source"
		execSink      = "acc:exec-sink"
		retryableKey  = "idem-retryable-failure-key"
		configAccount = "routing:cfg"
		routedAccount = "acc:routed"
	)

	lease := testserver.AllocateNodeLease()
	ports := lease.Ports()

	var (
		ctx            context.Context
		restoreWalDir  string
		restoreDataDir string
		minioEndpoint  string

		// The live structured rejections, which every replay must reproduce.
		frozenMetadata map[string]string
		execMetadata   map[string]string
	)

	frozenTx := func() *servicepb.ApplyRequest {
		return actions.WithIdempotencyKey(frozenKey,
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting(poorAccount, frozenSink, big.NewInt(100), "USD"),
			}, nil, nil),
		)
	}

	execTx := func() *servicepb.ApplyRequest {
		return actions.WithIdempotencyKey(execKey,
			actions.CreateScriptTransactionAction(ledgerName, `
vars {
  monetary $m = balance(@`+execSource+`, USD)
}

send $m - [USD 20] (
  source = @world
  destination = @`+execSink+`
)
`, nil, nil),
		)
	}

	retryableTx := func() *servicepb.ApplyRequest {
		return actions.WithIdempotencyKey(retryableKey,
			actions.CreateScriptTransactionAction(ledgerName, `
vars {
  account $dest = meta(@`+configAccount+`, "dest")
}

send [USD 10] (
  source = @world
  destination = $dest
)
`, nil, nil),
		)
	}

	// expectReplay asserts a keyed request replays its original structured
	// rejection, and that its sink was never credited.
	expectReplay := func(client servicepb.BucketServiceClient, phase string, tx *servicepb.ApplyRequest, reason string, metadata map[string]string, sink string) {
		_, err := client.Apply(ctx, tx)
		Expect(err).To(HaveOccurred(), "%s: a frozen failure must replay, not re-execute against the funded source", phase)
		Expect(status.Code(err)).To(Equal(codes.FailedPrecondition), "%s", phase)
		info := actions.ExtractGRPCErrorInfo(err)
		Expect(info).NotTo(BeNil(), "%s", phase)
		Expect(info.Reason).To(Equal(reason), "%s: replayed reason", phase)
		Expect(info.Metadata).To(Equal(metadata), "%s: replayed metadata", phase)

		acct, err := actions.GetAccount(ctx, client, ledgerName, sink)
		if status.Code(err) == codes.NotFound {
			return
		}
		Expect(err).To(Succeed())
		Expect(acct.GetVolumes()).To(BeEmpty(), "%s: %s must never be credited", phase, sink)
	}

	expectFrozenReplay := func(client servicepb.BucketServiceClient, phase string) {
		expectReplay(client, phase, frozenTx(), domain.ErrReasonInsufficientFunds, frozenMetadata, frozenSink)
	}

	expectExecReplay := func(client servicepb.BucketServiceClient, phase string) {
		expectReplay(client, phase, execTx(), domain.ErrReasonNumscriptExecutionError, execMetadata, execSink)
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

		restoreWalDir, err = os.MkdirTemp("", "idem-failure-restore-wal-*")
		Expect(err).To(Succeed())
		restoreDataDir, err = os.MkdirTemp("", "idem-failure-restore-data-*")
		Expect(err).To(Succeed())
	})

	Describe("Phase 1: keyed failures in the exported delta", Ordered, func() {
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

			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			// Every keyed failure after this checkpoint lands in the incremental
			// delta, so its frozen outcome can only come back through RebuildDelta.
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

		It("rejects frozenKey with INSUFFICIENT_FUNDS", func() {
			_, err := client.Apply(ctx, frozenTx())
			Expect(err).To(HaveOccurred())
			Expect(status.Code(err)).To(Equal(codes.FailedPrecondition))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonInsufficientFunds))

			frozenMetadata = info.Metadata
		})

		It("rejects execKey with NUMSCRIPT_EXECUTION_ERROR", func() {
			_, err := client.Apply(ctx, execTx())
			Expect(err).To(HaveOccurred())
			Expect(status.Code(err)).To(Equal(codes.FailedPrecondition))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonNumscriptExecutionError))

			execMetadata = info.Metadata
		})

		It("rejects retryableKey with PRELOAD_UNAVAILABLE", func() {
			_, err := client.Apply(ctx, retryableTx())
			Expect(err).To(HaveOccurred())
			Expect(status.Code(err)).To(Equal(codes.Unavailable))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonPreloadUnavailable))
		})

		It("audits every failure", func() {
			// Premise guard: a failure rejected before the FSM leaves no audit
			// entry, so the restore would have nothing to rebuild or skip.
			entries, err := actions.ListAuditEntries(ctx, client, true)
			Expect(err).To(Succeed())

			reasons := map[string]commonpb.ErrorReason{}
			for _, entry := range entries {
				reasons[entry.GetIdempotency().GetKey()] = entry.GetFailure().GetReason()
			}
			Expect(reasons).To(HaveKeyWithValue(frozenKey, commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS))
			Expect(reasons).To(HaveKeyWithValue(execKey, commonpb.ErrorReason_ERROR_REASON_NUMSCRIPT_EXECUTION_ERROR))
			Expect(reasons).To(HaveKeyWithValue(retryableKey, commonpb.ErrorReason_ERROR_REASON_PRELOAD_UNAVAILABLE))
		})

		It("replays the frozen keys on the live node after funding their sources", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", poorAccount, big.NewInt(500), "USD"),
					actions.NewPosting("world", execSource, big.NewInt(500), "USD"),
				}, nil, nil)))
			Expect(err).To(Succeed())

			// Premise guard: without the live gate, the post-restore assertion
			// would be vacuous.
			expectFrozenReplay(client, "live")
			expectExecReplay(client, "live")
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

			Expect(validateRestoreWithoutErrors(ctx, restoreClient)).To(Succeed())

			_, err = restoreClient.FinalizeRestore(ctx, &restorepb.FinalizeRestoreRequest{})
			Expect(err).To(Succeed())
		})
	})

	Describe("Phase 3: verify the restored failure outcomes", Ordered, func() {
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

		It("replays the frozen INSUFFICIENT_FUNDS failure", func() {
			acct, err := actions.GetAccount(ctx, client, ledgerName, poorAccount)
			Expect(err).To(Succeed())
			vol := acct.FindVolume("USD", "")
			Expect(vol).NotTo(BeNil())
			Expect(vol.GetInput().DecimalString()).To(Equal("500"), "the funding must have survived restore, so a fresh execution would succeed")

			expectFrozenReplay(client, "restored")
		})

		It("replays the frozen NUMSCRIPT_EXECUTION_ERROR failure", func() {
			acct, err := actions.GetAccount(ctx, client, ledgerName, execSource)
			Expect(err).To(Succeed())
			vol := acct.FindVolume("USD", "")
			Expect(vol).NotTo(BeNil())
			Expect(vol.GetInput().DecimalString()).To(Equal("500"), "the funding must have survived restore, so a fresh execution would succeed")

			expectExecReplay(client, "restored")
		})

		It("re-executes the non-freezable PRELOAD_UNAVAILABLE key", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.SaveAccountMetadataAction(ledgerName, configAccount, map[string]string{
					"dest": routedAccount,
				})))
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, retryableTx())
			Expect(err).To(Succeed(), "a non-freezable failure must not be re-frozen by the restore")

			acct, err := actions.GetAccount(ctx, client, ledgerName, routedAccount)
			Expect(err).To(Succeed())
			vol := acct.FindVolume("USD", "")
			Expect(vol).NotTo(BeNil())
			Expect(vol.GetInput().DecimalString()).To(Equal("10"))
		})

		It("passes CheckStore on the restored store", func() {
			result, err := actions.CollectCheckStoreEvents(ctx, client)
			Expect(err).To(Succeed())
			Expect(result.Errors).To(BeEmpty(), "CheckStore errors on the restored store: %v", result.Errors)
		})
	})
})
