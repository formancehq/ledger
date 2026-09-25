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

func restoreEventSinkConfig(name string) *commonpb.SinkConfig {
	return &commonpb.SinkConfig{
		Name:         name,
		Format:       "json",
		BatchSize:    1,
		BatchDelayMs: 1,
		Type: &commonpb.SinkConfig_Http{
			Http: &commonpb.HttpSinkConfig{Endpoint: "https://example.invalid/events"},
		},
	}
}

func addRestoreEventSink(config *commonpb.SinkConfig) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_AddEventsSink{
		AddEventsSink: &servicepb.AddEventsSinkRequest{Config: config},
	}}
}

func removeRestoreEventSink(name string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_RemoveEventsSink{
		RemoveEventsSink: &servicepb.RemoveEventsSinkRequest{Name: name},
	}}
}

// The checkpoint contains a configured sink while the exported delta contains
// its removal. A real restore must preserve the source's durable absence, pass
// CheckStore, and allow the same sink name to be admitted again.
var _ = Describe("Restore removed event sink", Ordered, func() {
	const (
		s3Bucket  = "restore-removed-event-sink"
		clusterID = "restore-removed-event-sink-cluster"
		sinkName  = "removed-before-restore"
	)

	lease := testserver.AllocateNodeLease()
	ports := lease.Ports()

	var (
		ctx            context.Context
		restoreWalDir  string
		restoreDataDir string
		minioEndpoint  string
	)

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

		restoreWalDir, err = os.MkdirTemp("", "removed-sink-restore-wal-*")
		Expect(err).To(Succeed())
		restoreDataDir, err = os.MkdirTemp("", "removed-sink-restore-data-*")
		Expect(err).To(Succeed())
	})

	Describe("source checkpoint and delta", Ordered, func() {
		var (
			server        *testservice.Service
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
			grpcConn      *grpc.ClientConn
		)

		BeforeAll(func() {
			instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
				NodeID: 1, ClusterID: clusterID, Ports: ports,
				WalDir: GinkgoT().TempDir(), DataDir: GinkgoT().TempDir(),
				Debug: testutil.Debug, Output: GinkgoWriter,
			})
			instruments = append(instruments, testserver.WithBootstrap())
			server = lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
			Expect(server.Start(ctx)).To(Succeed())

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
		})

		It("creates the sink before the checkpoint", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", addRestoreEventSink(restoreEventSinkConfig(sinkName))))
			Expect(err).To(Succeed())

			resp, err := client.GetEventsSinks(ctx, &servicepb.GetEventsSinksRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetSinks()).To(HaveLen(1))
			Expect(resp.GetSinks()[0].GetName()).To(Equal(sinkName))

			backupResp, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{Storage: storage()})
			Expect(err).To(Succeed())
			Expect(backupResp.GetTotalFiles()).To(BeNumerically(">", 0))
		})

		It("removes the sink in a non-empty exported delta", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", removeRestoreEventSink(sinkName)))
			Expect(err).To(Succeed())

			resp, err := client.GetEventsSinks(ctx, &servicepb.GetEventsSinksRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetSinks()).To(BeEmpty(), "the uninterrupted source must persist the removal")

			incResp, err := clusterClient.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{Storage: storage()})
			Expect(err).To(Succeed())
			Expect(incResp.GetLogEntriesExported()).To(BeNumerically(">", 0), "the removal log must be exported")
		})
	})

	Describe("restore", Ordered, func() {
		var (
			server        *testservice.Service
			restoreClient restorepb.RestoreServiceClient
			grpcConn      *grpc.ClientConn
		)

		BeforeAll(func() {
			server = lease.NewService(cmdserver.NewRunCommandWithBindings,
				testservice.WithInstruments(
					testservice.DebugInstrumentation(testutil.Debug),
					testservice.OutputInstrumentation(GinkgoWriter),
					testserver.WithNodeID(1), testserver.WithClusterID(clusterID),
					testserver.WithHTTPPort(ports.HTTP()), testserver.WithWalDir(restoreWalDir),
					testserver.WithDataDir(restoreDataDir), testserver.WithRaftPort(ports.Raft()),
					testserver.WithGRPCPort(ports.GRPC()), testserver.WithRestore(),
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

		It("downloads and finalizes the checkpoint plus delta", func() {
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

	Describe("restored node", Ordered, func() {
		var (
			server        *testservice.Service
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
			grpcConn      *grpc.ClientConn
		)

		BeforeAll(func() {
			instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
				NodeID: 1, ClusterID: clusterID, Ports: ports,
				WalDir: restoreWalDir, DataDir: restoreDataDir,
				Debug: testutil.Debug, Output: GinkgoWriter,
			})
			instruments = append(instruments, testserver.WithBootstrap())
			server = lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
			Expect(server.Start(ctx)).To(Succeed())

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

		It("keeps the removed sink durably absent and passes CheckStore", func() {
			resp, err := client.GetEventsSinks(ctx, &servicepb.GetEventsSinksRequest{})
			Expect(err).To(Succeed())
			Expect(resp.GetSinks()).To(BeEmpty(), "the checkpoint-era sink must not be resurrected")

			result, err := actions.CollectCheckStoreEvents(ctx, client)
			Expect(err).To(Succeed())
			Expect(result.Errors).To(BeEmpty(), "CheckStore errors on the restored store: %v", result.Errors)
		})

		It("admits a new sink under the removed name", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", addRestoreEventSink(restoreEventSinkConfig(sinkName))))
			Expect(err).To(Succeed(), "a stale restored sink would reject this name as already existing")
		})
	})
})
