//go:build e2e && s3

package cluster

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	clusterpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
)

// This suite proves the replicated cluster policy survives the real incremental
// restore lifecycle. A full backup on the fresh cluster captures the reconciler's
// bootstrap revision in the checkpoint; the test then bumps the policy to a
// higher revision so that revision lands purely in the exported delta. After a
// download + finalize + rebuild, the restored node must (1) reconstruct the
// post-checkpoint revision — proven by a clean CheckStore, whose cluster-policy
// verifier folds the audited SetClusterPolicy orders and compares them to the
// stored projection — and (2) reach write readiness: the admission gate only
// opens once a policy is committed, so a business write succeeding is direct
// evidence the policy was restored and reconciled.
var _ = Describe("Restore replicated cluster policy", Ordered, func() {
	const (
		s3Bucket    = "restore-cluster-policy"
		clusterID   = "clusterpolicy-cluster"
		seedLedger  = "policy-seed-ledger"
		deltaLedger = "policy-delta-ledger"

		// postCheckpointRevision is committed after the full backup, so it is
		// reconstructable only from the exported delta, never copied from the
		// checkpoint files.
		postCheckpointRevision = uint64(2)
		postCheckpointLimit    = uint64(5)
	)

	// One logical node returns across phases, so every phase reuses the same
	// allocated ports (see restore_metadata_type_test.go).
	lease := testserver.AllocateNodeLease()
	ports := lease.Ports()

	var (
		ctx            context.Context
		restoreWalDir  string
		restoreDataDir string
		minioEndpoint  string
		sourceWalDir   string
		sourceDataDir  string
	)

	storage := func() *clusterpb.BackupStorage {
		return testutil.S3BackupStorage(&clusterpb.S3StorageConfig{
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

		restoreWalDir, err = os.MkdirTemp("", "clusterpolicy-restore-wal-*")
		Expect(err).To(Succeed())
		restoreDataDir, err = os.MkdirTemp("", "clusterpolicy-restore-data-*")
		Expect(err).To(Succeed())
		sourceWalDir, err = os.MkdirTemp("", "clusterpolicy-source-wal-*")
		Expect(err).To(Succeed())
		sourceDataDir, err = os.MkdirTemp("", "clusterpolicy-source-data-*")
		Expect(err).To(Succeed())
	})

	Describe("Phase 1: post-checkpoint policy revision in the exported delta", Ordered, func() {
		var (
			sourceServer  *testservice.Service
			client        clusterpb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
			grpcConn      *grpc.ClientConn
		)

		startSource := func(revision, limit uint64) {
			instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
				NodeID:    1,
				ClusterID: clusterID,
				Ports:     ports,
				WalDir:    sourceWalDir,
				DataDir:   sourceDataDir,
				Debug:     testutil.Debug,
				Output:    GinkgoWriter,
			})
			instruments = append(instruments,
				testserver.WithBootstrap(),
				testserver.WithClusterPolicyRevision(revision),
				testserver.WithQueryCheckpointLimit(limit),
			)

			sourceServer = lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
			Expect(sourceServer.Start(ctx)).To(Succeed())
		}

		BeforeAll(func() {
			startSource(1, 10)

			var err error
			client, clusterClient, grpcConn, err = testutil.NewGRPCClient(ports.GRPC())
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) bool {
				state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
				g.Expect(err).To(Succeed())
				return state.Leader != 0
			}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(BeTrue())

			// Full checkpoint before the post-checkpoint revision exists: the
			// restore reconstructs that revision by replaying the exported log
			// rather than copying checkpoint files.
			var backupResp *clusterpb.BackupResponse
			Eventually(func() error {
				var err error
				backupResp, err = clusterClient.Backup(ctx, &clusterpb.BackupRequest{Storage: storage()})
				return err
			}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())
			Expect(backupResp.GetTotalFiles()).To(BeNumerically(">", 0))
		})

		AfterAll(func() {
			_ = grpcConn.Close()
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(sourceServer.Stop(stopCtx)).To(Succeed())
		})

		It("bumps the policy and writes business data after the checkpoint", func() {
			// Restart with a higher desired revision. The leader-side reconciler
			// submits the internal policy command, which keeps it out of the public
			// Apply envelope while still producing the post-checkpoint delta.
			stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			Expect(sourceServer.Stop(stopCtx)).To(Succeed())
			cancel()
			startSource(postCheckpointRevision, postCheckpointLimit)
			Eventually(func(g Gomega) bool {
				state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
				g.Expect(err).To(Succeed())
				return state.Leader != 0
			}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(BeTrue())

			// A business write proves the gate is open (policy committed) and puts
			// a post-checkpoint ledger in the delta, so a dropped delta on restore
			// surfaces as a missing ledger rather than passing silently.
			_, err := client.Apply(ctx, clusterpb.UnsignedApplyRequest("", actions.CreateLedgerAction(deltaLedger, nil)))
			Expect(err).To(Succeed())
		})

		It("exports a non-empty delta", func() {
			incResp, err := clusterClient.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{Storage: storage()})
			Expect(err).To(Succeed())
			Expect(incResp.GetLogEntriesExported()).To(BeNumerically(">", 0))
		})
	})

	Describe("Phase 2: restore", Ordered, func() {
		var (
			restoreClient clusterpb.RestoreServiceClient
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
			startResp, err := restoreClient.StartDownloadBackup(ctx, &clusterpb.StartDownloadBackupRequest{Storage: storage()})
			Expect(err).To(Succeed())

			Eventually(func() clusterpb.DownloadState {
				resp, statusErr := restoreClient.GetDownloadStatus(ctx, &clusterpb.GetDownloadStatusRequest{JobId: startResp.GetJobId()})
				Expect(statusErr).To(Succeed())
				return resp.GetState()
			}, 2*time.Minute, 500*time.Millisecond).Should(Equal(clusterpb.DownloadState_DOWNLOAD_STATE_SUCCEEDED))

			Expect(validateRestoreWithoutErrors(ctx, restoreClient)).To(Succeed())

			_, err = restoreClient.FinalizeRestore(ctx, &clusterpb.FinalizeRestoreRequest{})
			Expect(err).To(Succeed())
		})
	})

	Describe("Phase 3: verify the restored policy", Ordered, func() {
		var (
			client        clusterpb.BucketServiceClient
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
			Expect(server.Start(ctx)).To(Succeed())

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

		It("restored the post-checkpoint delta", func() {
			_, err := client.GetLedger(ctx, &clusterpb.GetLedgerRequest{Ledger: deltaLedger})
			Expect(err).To(Succeed(), "the post-checkpoint ledger must be restored from the delta")
		})

		It("reconstructs a cluster policy that CheckStore finds consistent", func() {
			result, err := actions.CollectCheckStoreEvents(ctx, client)
			Expect(err).To(Succeed())
			Expect(result.Errors).To(BeEmpty(),
				"CheckStore must report no integrity errors, including the cluster-policy verifier")
		})

		It("opens the write gate — the restored policy reconciled", func() {
			// The admission gate only lets business writes through once a policy
			// is committed; a succeeding write is direct evidence the restored
			// node carries the reconstructed policy.
			Eventually(func(g Gomega) {
				_, err := client.Apply(ctx, clusterpb.UnsignedApplyRequest("", actions.CreateLedgerAction(seedLedger, nil)))
				g.Expect(err).To(Succeed())
			}).Within(15 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})
})
