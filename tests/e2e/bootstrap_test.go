//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	cmdserver "github.com/formancehq/ledger-v3-poc/cmd/server"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/store"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

var _ = Describe("Bootstrap from backup", Ordered, func() {
	const (
		httpPort   = testSingleHTTPPort
		grpcPort   = testSingleGRPCPort
		raftPort   = grpcPort - 1000
		ledgerName = "bootstrap-ledger"
		ledger2    = "bootstrap-ledger-2"
	)

	var (
		ctx              context.Context
		backupTarPath    string
		bootstrapWalDir  string
		bootstrapDataDir string
	)

	BeforeAll(func() {
		ctx = logging.TestingContext()

		var err error
		bootstrapWalDir, err = os.MkdirTemp("", "bootstrap-wal-*")
		Expect(err).To(Succeed())
		bootstrapDataDir, err = os.MkdirTemp("", "bootstrap-data-*")
		Expect(err).To(Succeed())

		// Ensure cleanup regardless of which phase fails.
		DeferCleanup(func() {
			_ = os.RemoveAll(bootstrapWalDir)
			_ = os.RemoveAll(bootstrapDataDir)
			if backupTarPath != "" {
				_ = os.Remove(backupTarPath)
			}
		})
	})

	// Phase 1: Start a normal cluster, create data, take a backup.
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

			sourceServer = testservice.New(cmdserver.NewRunCommand,
				testservice.WithInstruments(
					testservice.DebugInstrumentation(debug),
					testservice.OutputInstrumentation(GinkgoWriter),
					testserver.WithNodeID(1),
					testserver.WithHTTPPort(httpPort),
					testserver.WithWalDir(walDir),
					testserver.WithDataDir(dataDir),
					testserver.WithRaftPort(raftPort),
					testserver.WithGRPCPort(grpcPort),
					testserver.WithSnapshotThreshold(10),
					testserver.WithRaftTickInterval(10*time.Millisecond),
					testserver.WithRaftHeartbeatTick(1),
					testserver.WithRaftElectionTick(10),
					testserver.WithBootstrap(),
				),
			)
			Expect(sourceServer.Start(ctx)).To(Succeed())

			var err error
			client, clusterClient, grpcConn, err = newGRPCClient(grpcPort)
			Expect(err).To(Succeed())

			// Wait for leader election.
			Eventually(func(g Gomega) bool {
				state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
				g.Expect(err).To(Succeed())
				return state.Leader != 0
			}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(BeTrue())

			// Create first ledger with metadata.
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction(ledgerName, map[string]string{"env": "test"}),
				},
			})
			Expect(err).To(Succeed())

			// Transaction 1: fund the bank.
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(10000), "USD"),
					}, map[string]string{"type": "funding"}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Transaction 2: distribute from bank.
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank", "alice", big.NewInt(3000), "USD"),
						newPosting("bank", "bob", big.NewInt(2000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Add account metadata.
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "alice", map[string]string{"role": "customer"}),
				},
			})
			Expect(err).To(Succeed())

			// Create second ledger with a transaction.
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction(ledger2, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledger2, []*commonpb.Posting{
						newPosting("world", "treasury", big.NewInt(50000), "EUR"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		AfterAll(func() {
			_ = grpcConn.Close()
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(sourceServer.Stop(stopCtx)).To(Succeed())
		})

		It("should take a backup and write it to a tar file", func() {
			stream, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{})
			Expect(err).To(Succeed())

			var buf bytes.Buffer
			hash := sha256.New()
			var expectedHash string

			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				Expect(err).To(Succeed())

				if resp.Eof {
					expectedHash = resp.ContentSha256
					break
				}

				buf.Write(resp.Data)
				_, err = hash.Write(resp.Data)
				Expect(err).To(Succeed())
			}

			actualHash := hex.EncodeToString(hash.Sum(nil))
			Expect(actualHash).To(Equal(expectedHash))
			Expect(buf.Len()).To(BeNumerically(">", 0))

			// Write to a temp tar file for the CLI command to consume.
			tmpFile, err := os.CreateTemp("", "bootstrap-backup-*.tar")
			Expect(err).To(Succeed())

			_, err = tmpFile.Write(buf.Bytes())
			Expect(err).To(Succeed())
			Expect(tmpFile.Close()).To(Succeed())

			backupTarPath = tmpFile.Name()
		})
	})

	// Phase 2: Run the CLI command offline — no server.
	Describe("Phase 2: Offline bootstrap via CLI command", Ordered, func() {
		It("should refuse to bootstrap into a directory with CURRENT_CHECKPOINT", func() {
			tmpDir := GinkgoT().TempDir()
			Expect(os.WriteFile(filepath.Join(tmpDir, "CURRENT_CHECKPOINT"), []byte("0"), 0644)).To(Succeed())

			cmd := store.NewBootstrapCommand()
			cmd.SetArgs([]string{
				"--input", backupTarPath,
				"--data-dir", tmpDir,
				"--yes",
			})

			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("CURRENT_CHECKPOINT"))
		})

		It("should bootstrap with --validate --yes", func() {
			cmd := store.NewBootstrapCommand()
			cmd.SetArgs([]string{
				"--input", backupTarPath,
				"--data-dir", bootstrapDataDir,
				"--validate",
				"--yes",
			})

			Expect(cmd.Execute()).To(Succeed())
		})

		It("should have created CURRENT_CHECKPOINT and RESTORED marker", func() {
			cpData, err := os.ReadFile(filepath.Join(bootstrapDataDir, "CURRENT_CHECKPOINT"))
			Expect(err).To(Succeed())
			Expect(string(cpData)).To(Equal("0"))

			_, err = os.Stat(filepath.Join(bootstrapDataDir, "RESTORED"))
			Expect(err).To(Succeed(), "RESTORED marker should exist")

			_, err = os.Stat(filepath.Join(bootstrapDataDir, "checkpoints", "0"))
			Expect(err).To(Succeed(), "checkpoint 0 should exist")
		})

		It("should have cleaned up the staging directory", func() {
			_, err := os.Stat(filepath.Join(bootstrapDataDir, "restore-staging"))
			Expect(os.IsNotExist(err)).To(BeTrue(), "staging directory should be removed")
		})
	})

	// Phase 3: Start server on bootstrapped data, verify everything.
	Describe("Phase 3: Server bootstrap from offline-prepared data", Ordered, func() {
		var (
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
			grpcConn      *grpc.ClientConn
			server        *testservice.Service
		)

		BeforeAll(func() {
			server = testservice.New(cmdserver.NewRunCommand,
				testservice.WithInstruments(
					testservice.DebugInstrumentation(debug),
					testservice.OutputInstrumentation(GinkgoWriter),
					testserver.WithNodeID(1),
					testserver.WithHTTPPort(httpPort),
					testserver.WithWalDir(bootstrapWalDir),
					testserver.WithDataDir(bootstrapDataDir),
					testserver.WithRaftPort(raftPort),
					testserver.WithGRPCPort(grpcPort),
					testserver.WithRaftTickInterval(10*time.Millisecond),
					testserver.WithRaftHeartbeatTick(1),
					testserver.WithRaftElectionTick(10),
					testserver.WithBootstrap(),
				),
			)
			Expect(server.Start(ctx)).To(Succeed())

			var err error
			client, clusterClient, grpcConn, err = newGRPCClient(grpcPort)
			Expect(err).To(Succeed())

			// Wait for leader election.
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
		})

		It("should have consumed the RESTORED marker", func() {
			_, err := os.Stat(filepath.Join(bootstrapDataDir, "RESTORED"))
			Expect(os.IsNotExist(err)).To(BeTrue(), "RESTORED marker should be removed after bootstrap")
		})

		It("should have both ledgers", func() {
			ledgers, err := getAllLedgersInfo(ctx, client)
			Expect(err).To(Succeed())
			Expect(ledgers).To(HaveKey(ledgerName))
			Expect(ledgers).To(HaveKey(ledger2))
		})

		It("should have the correct transactions on ledger 1", func() {
			txs, err := listAllTransactions(ctx, client, ledgerName, 100, 0)
			Expect(err).To(Succeed())
			Expect(txs).To(HaveLen(2))
		})

		It("should have the correct account balances on ledger 1", func() {
			aliceResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "alice",
			})
			Expect(err).To(Succeed())
			Expect(aliceResp.Volumes).To(HaveKey("USD"))
			Expect(aliceResp.Volumes["USD"].Input).To(Equal("3000"))

			bobResp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "bob",
			})
			Expect(err).To(Succeed())
			Expect(bobResp.Volumes).To(HaveKey("USD"))
			Expect(bobResp.Volumes["USD"].Input).To(Equal("2000"))

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

		It("should accept new transactions after bootstrap", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank", "charlie", big.NewInt(1000), "USD"),
					}, map[string]string{"type": "post-bootstrap"}, nil),
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

		It("should accept new ledger creation after bootstrap", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction("post-bootstrap-ledger", nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction("post-bootstrap-ledger", []*commonpb.Posting{
						newPosting("world", "user1", big.NewInt(100), "BTC"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			ledgers, err := getAllLedgersInfo(ctx, client)
			Expect(err).To(Succeed())
			Expect(ledgers).To(HaveLen(3))
		})
	})
})
