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
// FSM persists index registry rows in the main store, and the restore rebuild
// (backup.RebuildDelta → replay.ReplayLedgerLog) must fold CreateIndex /
// DropIndex / the removal cascade / the retype version bump back into them.
//
// Per the incremental-restore contract, the registry lifecycle spans the
// checkpoint boundary: rows seeded BEFORE the full checkpoint are then
// retyped (version fold over checkpoint state) and cascade-deleted by the
// delta, one row is created inside the delta, and the restored store must
// hold the complete logical rows the source held — verified field by field
// and by a clean CheckStore pass.
var _ = Describe("Restore index registry", Ordered, func() {
	const (
		ledgerName = "idxreg-ledger"
		s3Bucket   = "restore-index-registry"
		clusterID  = "idxreg-cluster"

		// retypedKey's index is seeded before the checkpoint and version-bumped
		// by the delta; removedKey's index is seeded before the checkpoint and
		// cascade-deleted by the delta; deltaKey's index only ever exists in
		// the delta.
		retypedKey = "k0"
		removedKey = "k1"
		deltaKey   = "k2"
	)

	// One logical node stops and returns across the three phases, so every
	// phase reuses the same allocated ports (cf. restore_metadata_type_test).
	lease := testserver.AllocateNodeLease()
	ports := lease.Ports()

	var (
		ctx            context.Context
		restoreWalDir  string
		restoreDataDir string
		minioEndpoint  string

		// sourceRows carries the source node's complete registry rows (keyed
		// by metadata field key) from Phase 1 into the Phase 3 comparison.
		sourceRows map[string]*commonpb.Index
	)

	metaIndexID := func(key string) *commonpb.IndexID {
		return &commonpb.IndexID{
			Kind: &commonpb.IndexID_Metadata{
				Metadata: &commonpb.MetadataIndexID{
					Target: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:    key,
				},
			},
		}
	}

	// registryRow returns the (ACCOUNT, key) metadata index row from the
	// ledger's registry, or nil when absent.
	registryRow := func(client servicepb.BucketServiceClient, key string) *commonpb.Index {
		st, err := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: ledgerName})
		Expect(err).To(Succeed(), "GetIndexStatus")

		for _, e := range st.GetIndexes() {
			meta := e.GetIndex().GetId().GetMetadata()
			if meta.GetTarget() == commonpb.TargetType_TARGET_TYPE_ACCOUNT && meta.GetKey() == key {
				return e.GetIndex()
			}
		}

		return nil
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

	Describe("Phase 1: registry lifecycle across the checkpoint boundary", Ordered, func() {
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
		})

		AfterAll(func() {
			_ = grpcConn.Close()
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(sourceServer.Stop(stopCtx)).To(Succeed())
		})

		It("seeds registry rows and checkpoints them", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.CreateLedgerAction(ledgerName, nil),
				actions.SetMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, retypedKey, commonpb.MetadataType_METADATA_TYPE_INT64),
				actions.SetMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, removedKey, commonpb.MetadataType_METADATA_TYPE_INT64),
				&servicepb.Request{
					Type: &servicepb.Request_CreateIndex{
						CreateIndex: &servicepb.CreateIndexRequest{Ledger: ledgerName, Id: metaIndexID(retypedKey)},
					},
				},
				&servicepb.Request{
					Type: &servicepb.Request_CreateIndex{
						CreateIndex: &servicepb.CreateIndexRequest{Ledger: ledgerName, Id: metaIndexID(removedKey)},
					},
				},
			))
			Expect(err).To(Succeed())

			// The full checkpoint carries both rows; everything after this
			// backup reaches the restored store only through the delta fold.
			backupResp, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{Storage: storage()})
			Expect(err).To(Succeed())
			Expect(backupResp.GetTotalFiles()).To(BeNumerically(">", 0))
		})

		It("mutates the registry in the delta", func() {
			// Retype: the delta must fold a version bump onto a checkpoint row.
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.SetMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, retypedKey, commonpb.MetadataType_METADATA_TYPE_UINT64),
			))
			Expect(err).To(Succeed())

			// Delta-created row.
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.SetMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, deltaKey, commonpb.MetadataType_METADATA_TYPE_INT64),
				&servicepb.Request{
					Type: &servicepb.Request_CreateIndex{
						CreateIndex: &servicepb.CreateIndexRequest{Ledger: ledgerName, Id: metaIndexID(deltaKey)},
					},
				},
			))
			Expect(err).To(Succeed())

			// Cascade-delete a checkpoint row; the live side must report the
			// drop (the model finding's premise).
			resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.RemoveMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, removedKey),
			))
			Expect(err).To(Succeed())

			var removed *commonpb.RemovedMetadataFieldTypeLog
			for _, lg := range resp.GetLogs() {
				if rm := lg.GetPayload().GetApply().GetLog().GetData().GetRemovedMetadataFieldType(); rm != nil {
					removed = rm
				}
			}
			Expect(removed).ToNot(BeNil())
			Expect(removed.GetDroppedIndex()).ToNot(BeNil(), "live premise: the removal must drop the checkpoint-seeded index")
		})

		It("exports the delta and captures the source registry", func() {
			incResp, err := clusterClient.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{Storage: storage()})
			Expect(err).To(Succeed())
			Expect(incResp.GetLogEntriesExported()).To(BeNumerically(">", 0))

			sourceRows = map[string]*commonpb.Index{
				retypedKey: registryRow(client, retypedKey),
				removedKey: registryRow(client, removedKey),
				deltaKey:   registryRow(client, deltaKey),
			}

			// Source premises: the comparison below is only meaningful if the
			// live node holds the expected end state.
			Expect(sourceRows[retypedKey]).ToNot(BeNil())
			Expect(sourceRows[retypedKey].GetForwardEncodingVersion()).To(Equal(uint32(2)), "the retype must have bumped the checkpoint row")
			Expect(sourceRows[removedKey]).To(BeNil(), "the cascade must have deleted the checkpoint row")
			Expect(sourceRows[deltaKey]).ToNot(BeNil())
			Expect(sourceRows[deltaKey].GetForwardEncodingVersion()).To(Equal(uint32(1)))
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

		It("restores the complete registry rows", func() {
			// expectSameRow compares the full logical row, not just presence:
			// identity, scope, forward-encoding version, build status and
			// creation date must all survive the checkpoint+delta composition.
			expectSameRow := func(key string) {
				source := sourceRows[key]
				restored := registryRow(client, key)
				Expect(restored).ToNot(BeNil(), "row for %q missing after restore", key)
				Expect(restored.GetId().GetMetadata().GetKey()).To(Equal(source.GetId().GetMetadata().GetKey()))
				Expect(restored.GetId().GetMetadata().GetTarget()).To(Equal(source.GetId().GetMetadata().GetTarget()))
				Expect(restored.GetLedger()).To(Equal(source.GetLedger()), "%q: ledger", key)
				Expect(restored.GetForwardEncodingVersion()).To(Equal(source.GetForwardEncodingVersion()), "%q: forward_encoding_version", key)
				Expect(restored.GetBuildStatus()).To(Equal(source.GetBuildStatus()), "%q: build_status", key)
				Expect(restored.GetCreatedAt().GetData()).To(Equal(source.GetCreatedAt().GetData()), "%q: created_at", key)
			}

			expectSameRow(retypedKey)
			expectSameRow(deltaKey)

			Expect(registryRow(client, removedKey)).To(BeNil(), "the cascade-deleted checkpoint row must not resurrect")
		})

		It("passes CheckStore on the restored store", func() {
			result, err := actions.CollectCheckStoreEvents(ctx, client)
			Expect(err).To(Succeed())
			Expect(result.Errors).To(BeEmpty(), "CheckStore errors on the restored store: %v", result.Errors)
		})

		It("drops the retyped index when its field is removed", func() {
			resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.RemoveMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, retypedKey),
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
