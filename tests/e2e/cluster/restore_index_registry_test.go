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

// This suite pins the dropped-index loss the model test surfaced
// (singleton_driver_model: remove-field-type dropped-index mismatch): after a
// backup/restore cycle, a removeFieldType on an indexed field reports nothing
// dropped even though the CreateIndex committed before the backup. The live
// FSM persists index registry rows in the main store, but the restore rebuild
// (backup.RebuildDelta → replay.ReplayLedgerLog) discards CreateIndex and
// DropIndex logs (internal/domain/replay/replay.go, "no state to track"), so
// a restored store keeps the metadata schema while losing every index
// registry row. The removal's cascade then finds no entry, dropped_index
// stays empty, and the index outlives its declaration on the read side.
var _ = Describe("Restore index registry", Ordered, func() {
	const (
		httpPort   = testutil.TestSingleHTTPPort
		grpcPort   = testutil.TestSingleGRPCPort
		raftPort   = grpcPort - 1000
		ledgerName = "idxreg-ledger"
		s3Bucket   = "restore-index-registry"
		clusterID  = "idxreg-cluster"
		fieldKey   = "k0"
	)

	var (
		ctx            context.Context
		restoreWalDir  string
		restoreDataDir string
		minioEndpoint  string
	)

	indexID := &commonpb.IndexID{
		Kind: &commonpb.IndexID_Metadata{
			Metadata: &commonpb.MetadataIndexID{
				Target: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:    fieldKey,
			},
		},
	}

	// expectRegisteredIndex asserts the (ACCOUNT, k0) metadata index is present
	// in the ledger's registry. Used both as the live-side premise guard and as
	// the post-restore check.
	expectRegisteredIndex := func(client servicepb.BucketServiceClient, phase string) {
		st, err := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: ledgerName})
		Expect(err).To(Succeed(), "%s: GetIndexStatus", phase)

		found := false
		for _, e := range st.GetIndexes() {
			meta := e.GetIndex().GetId().GetMetadata()
			if meta.GetTarget() == commonpb.TargetType_TARGET_TYPE_ACCOUNT && meta.GetKey() == fieldKey {
				found = true
			}
		}

		Expect(found).To(BeTrue(), "%s: metadata index (ACCOUNT, %s) missing from registry: %v", phase, fieldKey, st.GetIndexes())
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

		restoreWalDir, err = os.MkdirTemp("", "idxreg-restore-wal-*")
		Expect(err).To(Succeed())
		restoreDataDir, err = os.MkdirTemp("", "idxreg-restore-data-*")
		Expect(err).To(Succeed())
	})

	Describe("Phase 1: index registration in the exported delta", Ordered, func() {
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
			// the incremental delta, so the restore reconstructs the registry
			// purely by replaying the exported log instead of copying
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

		It("declares the field and creates its index", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.CreateLedgerAction(ledgerName, nil),
				actions.SetMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, fieldKey, commonpb.MetadataType_METADATA_TYPE_INT64),
				&servicepb.Request{
					Type: &servicepb.Request_CreateIndex{
						CreateIndex: &servicepb.CreateIndexRequest{Ledger: ledgerName, Id: indexID},
					},
				},
			))
			Expect(err).To(Succeed())
		})

		It("serves the index back on the live node", func() {
			// Premise guard: the live path must register the index; without it
			// the restore assertions would be vacuous.
			expectRegisteredIndex(client, "live")
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

	Describe("Phase 3: verify the restored registry", Ordered, func() {
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

		It("preserves the index registry entry across the rebuild", func() {
			expectRegisteredIndex(client, "restored")
		})

		It("drops the index when its field is removed", func() {
			resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.RemoveMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, fieldKey),
			))
			Expect(err).To(Succeed())

			var removed *commonpb.RemovedMetadataFieldTypeLog
			for _, lg := range resp.GetLogs() {
				if rm := lg.GetPayload().GetApply().GetLog().GetData().GetRemovedMetadataFieldType(); rm != nil {
					removed = rm
				}
			}

			Expect(removed).ToNot(BeNil(), "removal log missing from response: %v", resp.GetLogs())
			Expect(removed.GetDroppedIndex()).ToNot(BeNil(),
				"field removal reported nothing dropped — the registry lost the index across the restore")
		})
	})
})
