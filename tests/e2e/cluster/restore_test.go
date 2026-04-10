//go:build e2e && s3

package cluster

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger-v3-poc/cmd/server"
	"github.com/formancehq/ledger-v3-poc/internal/infra/backup"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/restorepb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	restoreMinioAccessKey = "minioadmin"
	restoreMinioSecretKey = "minioadmin"
	restoreS3Bucket       = "restore-backup"
	restoreS3Region       = "us-east-1"
)

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

// downloadBackupAsTar downloads all backup data files from S3 and creates a tar archive.
func downloadBackupAsTar(ctx context.Context, s3Client *s3.Client, bucket string) ([]byte, string) {
	// Read manifest to get file list
	manifestOutput, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("test-cluster/backups/manifest.json"),
	})
	Expect(err).To(Succeed())

	manifestData, err := io.ReadAll(manifestOutput.Body)
	_ = manifestOutput.Body.Close()
	Expect(err).To(Succeed())

	var manifest backup.Manifest
	Expect(json.Unmarshal(manifestData, &manifest)).To(Succeed())
	Expect(manifest.Files).NotTo(BeEmpty())

	// Download each file and add to tar
	var buf bytes.Buffer
	hash := sha256.New()
	mw := io.MultiWriter(&buf, hash)
	tw := tar.NewWriter(mw)

	for filename, size := range manifest.Files {
		key := "test-cluster/backups/data/" + filename

		output, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		Expect(err).To(Succeed())

		data, err := io.ReadAll(output.Body)
		_ = output.Body.Close()
		Expect(err).To(Succeed())

		// Handle nested paths (e.g., "subdir/file.sst")
		parts := strings.Split(filename, "/")
		for i := range len(parts) - 1 {
			dirName := strings.Join(parts[:i+1], "/")
			_ = tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeDir,
				Name:     dirName + "/",
				Mode:     0o755,
			})
		}

		Expect(tw.WriteHeader(&tar.Header{
			Name: filename,
			Size: size,
			Mode: 0o644,
		})).To(Succeed())

		_, err = tw.Write(data)
		Expect(err).To(Succeed())
	}

	Expect(tw.Close()).To(Succeed())

	return buf.Bytes(), hex.EncodeToString(hash.Sum(nil))
}

var _ = Describe("Restore", Ordered, func() {
	const (
		httpPort   = testutil.TestSingleHTTPPort
		grpcPort   = testutil.TestSingleGRPCPort
		raftPort   = grpcPort - 1000
		ledgerName = "restore-ledger"
		ledger2    = "restore-ledger-2"
	)

	var (
		ctx            context.Context
		backupData     []byte
		backupHash     string
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

		// Create S3 client
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
			Bucket: aws.String(bucket),
		})
		Expect(err).To(Succeed())

		// Set AWS credentials for the ledger server process
		GinkgoT().Setenv("AWS_ACCESS_KEY_ID", restoreMinioAccessKey)
		GinkgoT().Setenv("AWS_SECRET_ACCESS_KEY", restoreMinioSecretKey)

		// Create temp dirs for restore phases
		restoreWalDir, err = os.MkdirTemp("", "restore-wal-*")
		Expect(err).To(Succeed())
		restoreDataDir, err = os.MkdirTemp("", "restore-data-*")
		Expect(err).To(Succeed())
	})

	// Phase 1: Start a normal cluster, create data, take a backup, then stop.
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

			sourceServer = testservice.New(cmdserver.NewRunCommand,
				testservice.WithInstruments(instruments...),
			)
			Expect(sourceServer.Start(ctx)).To(Succeed())

			var err error
			client, clusterClient, grpcConn, err = testutil.NewGRPCClient(grpcPort)
			Expect(err).To(Succeed())

			// Wait for leader election
			Eventually(func(g Gomega) bool {
				state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
				g.Expect(err).To(Succeed())
				return state.Leader != 0
			}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(BeTrue())

			// Create first ledger with metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerName, map[string]string{"env": "test"}),
				},
			})
			Expect(err).To(Succeed())

			// Transaction 1: fund the bank
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(10000), "USD"),
					}, map[string]string{"type": "funding"}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Transaction 2: distribute from bank
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("bank", "alice", big.NewInt(3000), "USD"),
						actions.NewPosting("bank", "bob", big.NewInt(2000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Add account metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"role": "customer"}),
				},
			})
			Expect(err).To(Succeed())

			// Create second ledger with a transaction
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledger2, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledger2, []*commonpb.Posting{
						actions.NewPosting("world", "treasury", big.NewInt(50000), "EUR"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		AfterAll(func() {
			// Must stop the source server before Phase 2 starts on the same ports
			_ = grpcConn.Close()
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(sourceServer.Stop(stopCtx)).To(Succeed())
		})

		It("should take a backup to S3", func() {
			resp, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{
				Driver:     "s3",
				S3Bucket:   restoreS3Bucket,
				S3Region:   restoreS3Region,
				S3Endpoint: minioEndpoint,
			})
			Expect(err).To(Succeed())
			Expect(resp.GetTotalFiles()).To(BeNumerically(">", 0))
			Expect(resp.GetFilesUploaded()).To(BeNumerically(">", 0))

			// Download backup from S3 and create tar for restore
			backupData, backupHash = downloadBackupAsTar(ctx, s3Client, restoreS3Bucket)
			Expect(backupData).NotTo(BeEmpty())
		})
	})

	// Phase 2: Start a restore-mode server, upload, validate, preview, finalize, then stop.
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
			// Must stop the restore server before Phase 3 starts on the same ports
			_ = grpcConn.Close()
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(server.Stop(stopCtx)).To(Succeed())
		})

		It("should upload the backup", func() {
			stream, err := restoreClient.UploadBackup(ctx)
			Expect(err).To(Succeed())

			// Send data in 64KB chunks
			const chunkSize = 64 * 1024
			for offset := 0; offset < len(backupData); offset += chunkSize {
				end := offset + chunkSize
				if end > len(backupData) {
					end = len(backupData)
				}
				err := stream.Send(&restorepb.UploadBackupRequest{
					Data: backupData[offset:end],
				})
				Expect(err).To(Succeed())
			}

			// Send EOF with hash
			err = stream.Send(&restorepb.UploadBackupRequest{
				Eof:           true,
				ContentSha256: backupHash,
			})
			Expect(err).To(Succeed())

			resp, err := stream.CloseAndRecv()
			Expect(err).To(Succeed())
			Expect(resp.BytesReceived).To(Equal(uint64(len(backupData))))
			Expect(resp.Sha256).To(Equal(backupHash))
		})

		It("should reject a duplicate upload", func() {
			stream, err := restoreClient.UploadBackup(ctx)
			Expect(err).To(Succeed())

			err = stream.Send(&restorepb.UploadBackupRequest{Data: []byte("test")})
			Expect(err).To(Succeed())

			_, err = stream.CloseAndRecv()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("already uploaded"))
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

			Expect(resp.LedgerCount).To(Equal(uint32(2)))
			Expect(resp.LedgerNames).To(ConsistOf(ledgerName, ledger2))
			// Preview runs before FinalizeRestore compaction, so index is non-zero
			Expect(resp.LastAppliedIndex).To(BeNumerically(">", 0))
			Expect(resp.LastSequence).To(BeNumerically(">", 0))
		})

		It("should finalize the restore", func() {
			resp, err := restoreClient.FinalizeRestore(ctx, &restorepb.FinalizeRestoreRequest{})
			Expect(err).To(Succeed())
			Expect(resp.Message).To(ContainSubstring("Restore finalized"))
		})

		It("should have created the RESTORED marker and CURRENT_CHECKPOINT", func() {
			_, err := os.Stat(restoreDataDir + "/RESTORED")
			Expect(err).To(Succeed(), "RESTORED marker should exist")

			cpData, err := os.ReadFile(restoreDataDir + "/CURRENT_CHECKPOINT")
			Expect(err).To(Succeed())
			Expect(string(cpData)).To(Equal("0"))
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

			server = testservice.New(cmdserver.NewRunCommand,
				testservice.WithInstruments(instruments...),
			)
			Expect(server.Start(ctx)).To(Succeed())

			var err error
			client, clusterClient, grpcConn, err = testutil.NewGRPCClient(grpcPort)
			Expect(err).To(Succeed())

			// Wait for leader election
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

		It("should have the correct transactions on ledger 1", func() {
			txs, err := listAllTransactions(ctx, client, ledgerName, 100, 0)
			Expect(err).To(Succeed())
			// 2 transactions: world->bank and bank->alice,bob
			Expect(txs).To(HaveLen(2))
		})

		It("should have the correct account balances on ledger 1", func() {
			// Check alice: should have 3000 USD (input)
			aliceResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "alice",
			})
			Expect(err).To(Succeed())
			Expect(aliceResp.Volumes).To(HaveKey("USD"))
			Expect(aliceResp.Volumes["USD"].Input).To(Equal("3000"))

			// Check bob: should have 2000 USD (input)
			bobResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "bob",
			})
			Expect(err).To(Succeed())
			Expect(bobResp.Volumes).To(HaveKey("USD"))
			Expect(bobResp.Volumes["USD"].Input).To(Equal("2000"))

			// Check bank: 10000 in, 5000 out (3000+2000)
			bankResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "bank",
			})
			Expect(err).To(Succeed())
			Expect(bankResp.Volumes).To(HaveKey("USD"))
			Expect(bankResp.Volumes["USD"].Input).To(Equal("10000"))
			Expect(bankResp.Volumes["USD"].Output).To(Equal("5000"))
		})

		It("should have the correct account metadata", func() {
			aliceResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "alice",
			})
			Expect(err).To(Succeed())
			Expect(aliceResp.Metadata).NotTo(BeNil())
			Expect(commonpb.MetadataSetToMap(aliceResp.Metadata)).To(HaveKeyWithValue("role", "customer"))
		})

		It("should have the correct data on ledger 2", func() {
			txs, err := listAllTransactions(ctx, client, ledger2, 100, 0)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(1))

			treasuryResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledger2,
				Address: "treasury",
			})
			Expect(err).To(Succeed())
			Expect(treasuryResp.Volumes).To(HaveKey("EUR"))
			Expect(treasuryResp.Volumes["EUR"].Input).To(Equal("50000"))
		})

		It("should accept new transactions after restore", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("bank", "charlie", big.NewInt(1000), "USD"),
					}, map[string]string{"type": "post-restore"}, nil),
				},
			})
			Expect(err).To(Succeed())

			charlieResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "charlie",
			})
			Expect(err).To(Succeed())
			Expect(charlieResp.Volumes).To(HaveKey("USD"))
			Expect(charlieResp.Volumes["USD"].Input).To(Equal("1000"))

			bankResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "bank",
			})
			Expect(err).To(Succeed())
			Expect(bankResp.Volumes["USD"].Output).To(Equal("6000"))
		})

		It("should accept new ledger creation after restore", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("post-restore-ledger", nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction("post-restore-ledger", []*commonpb.Posting{
						actions.NewPosting("world", "user1", big.NewInt(100), "BTC"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			ledgers, err := actions.ListLedgers(ctx, client)
			Expect(err).To(Succeed())
			Expect(ledgers).To(HaveLen(3))
		})
	})
})
