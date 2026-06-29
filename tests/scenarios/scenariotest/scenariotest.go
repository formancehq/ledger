//go:build scenario

package scenariotest

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Port constants for scenario tests.
// Using 16xxx range to avoid conflicts with e2e tests (15xxx).
const (
	GRPCPort = 16100
	HTTPPort = 16200
)

// ScenarioCluster holds the state for a single-node scenario test cluster.
// It supports restart (stop + start with same WAL/data dirs) to test WAL replay.
type ScenarioCluster struct {
	t       *testing.T
	ctx     context.Context
	server  *testservice.Service
	conn    *grpc.ClientConn
	Client  servicepb.BucketServiceClient
	Cluster clusterpb.ClusterServiceClient

	// Config captured at setup time, reused on restart.
	httpPort  int
	grpcPort  int
	walDir    string
	dataDir   string
	extra     []testservice.Instrumentation
	bootstrap bool // true on first boot, false on restart
}

// Ctx returns the cluster's context.
func (sc *ScenarioCluster) Ctx() context.Context {
	return sc.ctx
}

// Restart stops the server and starts it again with the same WAL/data dirs.
// After restart, Client and Cluster fields point to fresh connections.
func (sc *ScenarioCluster) Restart() {
	sc.t.Helper()

	// Close old gRPC connection first to avoid in-flight requests racing with shutdown.
	_ = sc.conn.Close()

	// Stop server.
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(sc.t, sc.server.Stop(stopCtx), "failed to stop server for restart")

	// Restart without --bootstrap (WAL replay).
	sc.bootstrap = false
	sc.startServer()
}

func (sc *ScenarioCluster) startServer() {
	sc.t.Helper()

	raftPort := sc.grpcPort - 1000

	instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
		NodeID:    1,
		ClusterID: "scenario-cluster",
		HTTPPort:  sc.httpPort,
		RaftPort:  raftPort,
		GRPCPort:  sc.grpcPort,
		WalDir:    sc.walDir,
		DataDir:   sc.dataDir,
		Debug:     os.Getenv("DEBUG") == "true",
		Output:    os.Stderr,
	})
	instruments = append(instruments, testserver.WithCacheRotationThreshold(50))
	if sc.bootstrap {
		instruments = append(instruments, testserver.WithBootstrap())
	}
	instruments = append(instruments, sc.extra...)

	sc.server = testservice.New(cmdserver.NewRunCommand,
		testservice.WithInstruments(instruments...),
	)
	require.NoError(sc.t, sc.server.Start(sc.ctx))

	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", sc.grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(actions.GRPCRetryPolicy),
	)
	require.NoError(sc.t, err)

	sc.conn = conn
	sc.Client = servicepb.NewBucketServiceClient(conn)
	sc.Cluster = clusterpb.NewClusterServiceClient(conn)

	// Wait for leader election.
	require.Eventually(sc.t, func() bool {
		state, err := sc.Cluster.GetClusterState(sc.ctx, &clusterpb.GetClusterStateRequest{})
		if err != nil {
			return false
		}
		return state.Leader != 0
	}, 10*time.Second, 100*time.Millisecond, "leader election timed out")
}

// SetupSingleNode creates a single-node cluster for scenario tests.
// Returns the ScenarioCluster which holds ctx, clients, and supports Restart().
// Cleanup is handled via t.Cleanup.
func SetupSingleNode(t *testing.T, httpPort, grpcPort int, extra ...testservice.Instrumentation) *ScenarioCluster {
	t.Helper()

	sc := &ScenarioCluster{
		t:         t,
		ctx:       logging.TestingContext(),
		httpPort:  httpPort,
		grpcPort:  grpcPort,
		walDir:    t.TempDir(),
		dataDir:   t.TempDir(),
		extra:     extra,
		bootstrap: true,
	}

	sc.startServer()

	t.Cleanup(func() {
		_ = sc.conn.Close()
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = sc.server.Stop(stopCtx) // best-effort cleanup
	})

	return sc
}

// ---------------------------------------------------------------------------
// Store integrity & backup checks
// ---------------------------------------------------------------------------

// CheckStoreIntegrity runs CheckStore and requires no integrity errors.
func CheckStoreIntegrity(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient) {
	t.Helper()

	result, err := actions.CollectCheckStoreEvents(ctx, client)
	require.NoError(t, err, "CheckStore RPC failed")

	for _, e := range result.Errors {
		t.Logf("  CheckStore error: [%s] %s (log=%d, ledger=%s, account=%s, asset=%s, tx=%d)",
			e.ErrorType, e.Message, e.LogSequence, e.Ledger, e.Account, e.Asset, e.TransactionId)
	}
	require.Empty(t, result.Errors, "store should have no integrity errors")
	require.NotEmpty(t, result.Progress, "should emit at least one progress event")
}

// RunPostTestPhases runs the standard tail phases common to all scenario tests:
// StoreCheck -> RestartAndVerify.
// The verifyFn callback runs the scenario-specific invariant checks; it receives
// the current client (which may change after restart).
func RunPostTestPhases(t *testing.T, sc *ScenarioCluster, verifyFn func(t *testing.T, client servicepb.BucketServiceClient)) {
	t.Helper()

	ctx := sc.ctx

	t.Run("StoreCheck", func(t *testing.T) {
		CheckStoreIntegrity(t, ctx, sc.Client)
	})

	t.Run("RestartAndVerify", func(t *testing.T) {
		sc.Restart()
		CheckStoreIntegrity(t, ctx, sc.Client)
		verifyFn(t, sc.Client)
	})
}

// ---------------------------------------------------------------------------
// Chapter close helper
// ---------------------------------------------------------------------------

// CloseChapterAndWait closes the current chapter and waits for a new OPEN chapter to appear.
func CloseChapterAndWait(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient, msgAndArgs ...interface{}) {
	t.Helper()

	ApplyActions(t, ctx, client, actions.CloseChapterAction())
	require.Eventually(t, func() bool {
		chapters, err := actions.ListAllChapters(ctx, client)
		if err != nil {
			return false
		}
		return len(chapters) >= 2 && chapters[len(chapters)-1].Status == commonpb.ChapterStatus_CHAPTER_OPEN
	}, 10*time.Second, 200*time.Millisecond, msgAndArgs...)
}

// ---------------------------------------------------------------------------
// Invariant checks (Antithesis-ready)
// ---------------------------------------------------------------------------

// CheckPositiveBalance verifies that the uncolored balance of an account
// for a given asset is strictly positive.
func CheckPositiveBalance(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient, ledgerName, address, asset string) {
	t.Helper()

	acct, err := actions.GetAccount(ctx, client, ledgerName, address)
	require.NoError(t, err, "failed to get account %s", address)

	vol := acct.FindVolume(asset, "")
	require.NotNil(t, vol, "account %s has no volumes for asset %s (uncolored)", address, asset)

	balance, ok := new(big.Int).SetString(vol.GetBalance(), 10)
	require.True(t, ok, "invalid balance %q for account %s asset %s", vol.GetBalance(), address, asset)
	require.True(t, balance.Sign() > 0,
		"account %s asset %s: expected positive balance, got %s", address, asset, balance.String())
}

// CheckDoubleEntryBalance verifies that for every (asset, color) tuple, the
// sum of all account balances equals zero (double-entry invariant). Each
// (asset, color) bucket is its own segregated double-entry universe.
func CheckDoubleEntryBalance(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) {
	t.Helper()

	accounts, err := actions.ListAllAccounts(ctx, client, ledgerName)
	require.NoError(t, err, "failed to list accounts for double-entry check")

	type bucket struct{ asset, color string }
	sums := make(map[bucket]*big.Int)
	for _, acct := range accounts {
		for _, entry := range acct.GetVolumes() {
			vol := entry.GetVolumes()
			balance, ok := new(big.Int).SetString(vol.GetBalance(), 10)
			require.True(t, ok, "invalid balance %q for account %s asset %s color %q",
				vol.GetBalance(), acct.GetAddress(), entry.GetAsset(), entry.GetColor())

			k := bucket{asset: entry.GetAsset(), color: entry.GetColor()}
			if sums[k] == nil {
				sums[k] = new(big.Int)
			}
			sums[k].Add(sums[k], balance)
		}
	}

	for k, sum := range sums {
		require.Equal(t, 0, sum.Sign(),
			"double-entry violated for asset %s color %q: sum of balances = %s (expected 0)",
			k.asset, k.color, sum.String())
	}
}

// CheckAccountBalance verifies the uncolored balance of an account for a
// given asset matches the expected amount. For colored buckets, use
// CheckColoredAccountBalance.
func CheckAccountBalance(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient, ledgerName, address, asset string, expected *big.Int) {
	t.Helper()
	CheckColoredAccountBalance(t, ctx, client, ledgerName, address, asset, "", expected)
}

// CheckColoredAccountBalance verifies the balance for a specific
// (account, asset, color) bucket. Color "" is the uncolored bucket.
func CheckColoredAccountBalance(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient, ledgerName, address, asset, color string, expected *big.Int) {
	t.Helper()

	acct, err := actions.GetAccount(ctx, client, ledgerName, address)
	require.NoError(t, err, "failed to get account %s", address)

	vol := acct.FindVolume(asset, color)
	require.NotNil(t, vol, "account %s has no volumes for asset %s color %q", address, asset, color)

	balance, ok := new(big.Int).SetString(vol.GetBalance(), 10)
	require.True(t, ok, "invalid balance %q for account %s asset %s color %q",
		vol.GetBalance(), address, asset, color)

	require.Equal(t, 0, expected.Cmp(balance),
		"account %s asset %s color %q: expected balance %s, got %s",
		address, asset, color, expected.String(), balance.String())
}

// CheckNoNegativeBalances verifies no (account, asset, color) bucket has a
// negative balance, except for explicitly listed exceptions (e.g., @world,
// overdraft accounts).
func CheckNoNegativeBalances(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient, ledgerName string, exceptions []string) {
	t.Helper()

	exceptionSet := make(map[string]bool, len(exceptions))
	for _, e := range exceptions {
		exceptionSet[e] = true
	}

	accounts, err := actions.ListAllAccounts(ctx, client, ledgerName)
	require.NoError(t, err, "failed to list accounts for negative balance check")

	for _, acct := range accounts {
		if exceptionSet[acct.GetAddress()] {
			continue
		}
		for _, entry := range acct.GetVolumes() {
			vol := entry.GetVolumes()
			balance, ok := new(big.Int).SetString(vol.GetBalance(), 10)
			require.True(t, ok, "invalid balance %q for account %s asset %s color %q",
				vol.GetBalance(), acct.GetAddress(), entry.GetAsset(), entry.GetColor())
			require.True(t, balance.Sign() >= 0,
				"negative balance on account %s asset %s color %q: %s",
				acct.GetAddress(), entry.GetAsset(), entry.GetColor(), balance.String())
		}
	}
}

// ---------------------------------------------------------------------------
// Audit trail checks
// ---------------------------------------------------------------------------

// AuditExpectation describes expected transaction counts for a ledger.
type AuditExpectation struct {
	Ledger           string
	MinTransactions  int // minimum expected total transactions (created + reverts)
	ExpectedReverted int // exact number of reverted original transactions
}

// CheckAuditTrail verifies the integrity of the log chain and transaction audit trail.
func CheckAuditTrail(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient, expectations []AuditExpectation) {
	t.Helper()

	// 1. Verify log chain integrity for the first ledger.
	require.NotEmpty(t, expectations, "at least one AuditExpectation is required")

	logs, err := actions.ListAllLogs(ctx, client, expectations[0].Ledger)
	require.NoError(t, err, "ListAllLogs failed")
	require.NotEmpty(t, logs, "should have at least one log")

	for i, log := range logs {
		require.NotZero(t, log.Sequence, "log %d has zero sequence", i)

		if i > 0 {
			require.Greater(t, log.Sequence, logs[i-1].Sequence,
				"log sequences must be strictly increasing: seq[%d]=%d, seq[%d]=%d",
				i-1, logs[i-1].Sequence, i, log.Sequence)
		}
	}
	t.Logf("Log chain OK: %d logs, sequences %d..%d", len(logs), logs[0].Sequence, logs[len(logs)-1].Sequence)

	// 2. Verify transactions per ledger.
	for _, exp := range expectations {
		txs, err := actions.ListAllTransactions(ctx, client, exp.Ledger)
		require.NoError(t, err, "ListAllTransactions failed for ledger %s", exp.Ledger)
		require.GreaterOrEqual(t, len(txs), exp.MinTransactions,
			"ledger %s: expected at least %d transactions, got %d",
			exp.Ledger, exp.MinTransactions, len(txs))

		// Check transaction IDs are unique and positive.
		seenIDs := make(map[uint64]bool, len(txs))
		var revertedCount int
		for _, tx := range txs {
			require.NotZero(t, tx.Id, "transaction has zero ID in ledger %s", exp.Ledger)
			require.False(t, seenIDs[tx.Id],
				"duplicate transaction ID %d in ledger %s", tx.Id, exp.Ledger)
			seenIDs[tx.Id] = true

			if tx.Reverted {
				revertedCount++
			}
		}

		require.Equal(t, exp.ExpectedReverted, revertedCount,
			"ledger %s: expected %d reverted transactions, got %d",
			exp.Ledger, exp.ExpectedReverted, revertedCount)

		t.Logf("Audit OK for ledger %q: %d transactions (%d reverted)",
			exp.Ledger, len(txs), revertedCount)
	}
}

// ---------------------------------------------------------------------------
// Apply helpers
// ---------------------------------------------------------------------------

// ApplyActions is a helper to apply a batch of actions and require success.
func ApplyActions(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient, actions ...*servicepb.Request) *servicepb.ApplyResponse {
	t.Helper()

	resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions...))
	require.NoError(t, err, "Apply failed")
	return resp
}

// ApplyBatch applies a pre-built ApplyRequest (signed or unsigned) and requires success.
func ApplyBatch(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient, req *servicepb.ApplyRequest) *servicepb.ApplyResponse {
	t.Helper()

	resp, err := client.Apply(ctx, req)
	require.NoError(t, err, "Apply failed")
	return resp
}

// ApplyActionsExpectError applies actions and returns the error (nil if success).
func ApplyActionsExpectError(ctx context.Context, client servicepb.BucketServiceClient, actions ...*servicepb.Request) error {
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions...))
	return err
}

// GetCreatedTransactionID extracts the transaction ID from the first log entry of an Apply response.
func GetCreatedTransactionID(t *testing.T, resp *servicepb.ApplyResponse) uint64 {
	t.Helper()
	require.NotEmpty(t, resp.Logs, "expected at least one log entry")
	applyLog := resp.Logs[0].Payload.GetApply()
	require.NotNil(t, applyLog, "expected apply log payload")
	tx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, tx, "expected created transaction in log")
	return tx.Transaction.Id
}
