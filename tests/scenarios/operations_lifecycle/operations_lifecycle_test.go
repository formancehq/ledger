//go:build scenario

package operationslifecycle

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/scenario"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/tests/scenarios/scenariotest"
)

// TestOperationsLifecycle covers admin/ops operations:
// maintenance mode, audit config, period schedule, request signing, and delete ledger.
// Generates ~30 Apply calls with moderate operational complexity.
func TestOperationsLifecycle(t *testing.T) {
	const (
		ledger    = scenario.OperationsLifecycleLedger
		numDeposits = 5
	)

	sc := scenariotest.SetupSingleNode(t, scenariotest.HTTPPort+4, scenariotest.GRPCPort+4)
	ctx, client := sc.Ctx(), sc.Client

	// --- Phase 1: Setup ---
	t.Run("Setup", func(t *testing.T) {
		scenariotest.ApplyActions(t, ctx, client, scenario.OperationsLifecycleSetupActions()...)

		// Create a few transactions
		actions := make([]*servicepb.Request, 0, numDeposits)
		for i := 1; i <= numDeposits; i++ {
			actions = append(actions, testutil.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
				"account": fmt.Sprintf("ops:%d", i),
				"amount":  "USD/2 10000",
			}, nil))
		}
		scenariotest.ApplyActions(t, ctx, client, actions...)
	})

	// --- Phase 2: Maintenance Mode ---
	t.Run("MaintenanceMode", func(t *testing.T) {
		// Enable maintenance mode
		scenariotest.ApplyActions(t, ctx, client, testutil.SetMaintenanceModeAction(true))

		// Transaction should fail during maintenance
		err := scenariotest.ApplyActionsExpectError(ctx, client,
			testutil.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
				"account": "ops:1",
				"amount":  "USD/2 100",
			}, nil),
		)
		require.Error(t, err, "expected error during maintenance mode")

		// Disable maintenance mode
		scenariotest.ApplyActions(t, ctx, client, testutil.SetMaintenanceModeAction(false))

		// Transaction should succeed after disabling maintenance
		scenariotest.ApplyActions(t, ctx, client,
			testutil.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
				"account": "ops:1",
				"amount":  "USD/2 100",
			}, nil),
		)
	})

	// --- Phase 3: Audit Config ---
	t.Run("AuditConfig", func(t *testing.T) {
		// Enable audit logging
		scenariotest.ApplyActions(t, ctx, client, testutil.SetAuditConfigAction(true))

		// 3 successful transactions
		for i := 1; i <= 3; i++ {
			scenariotest.ApplyActions(t, ctx, client,
				testutil.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
					"account": fmt.Sprintf("ops:%d", i),
					"amount":  "USD/2 50",
				}, nil),
			)
		}

		// 1 failing transaction (insufficient funds: ops:1 sending more than it has to ops:2)
		_ = scenariotest.ApplyActionsExpectError(ctx, client,
			testutil.CreateTransactionAction(ledger, []*commonpb.Posting{
				testutil.NewPosting("ops:1", "ops:2", big.NewInt(999_999_999), "USD/2"),
			}, nil, nil),
		)

		// ListAuditEntries (all): should have entries
		allEntries, err := testutil.ListAuditEntries(ctx, client, false)
		require.NoError(t, err, "ListAuditEntries(all) failed")
		require.GreaterOrEqual(t, len(allEntries), 4, "should have at least 4 audit entries (3 success + 1 failure)")
		t.Logf("Audit entries (all): %d", len(allEntries))

		// ListAuditEntries (failures only): should have at least 1
		failures, err := testutil.ListAuditEntries(ctx, client, true)
		require.NoError(t, err, "ListAuditEntries(failures) failed")
		require.GreaterOrEqual(t, len(failures), 1, "should have at least 1 failure entry")
		t.Logf("Audit entries (failures): %d", len(failures))

		// Verify the failure entry has a failure outcome
		for _, entry := range failures {
			require.NotNil(t, entry.GetFailure(), "failure entry should have failure outcome")
		}

		// Disable audit logging
		scenariotest.ApplyActions(t, ctx, client, testutil.SetAuditConfigAction(false))
	})

	// --- Phase 3b: GetAuditEntry ---
	t.Run("GetAuditEntry", func(t *testing.T) {
		// Get all audit entries and verify we can fetch one individually
		allEntries, err := testutil.ListAuditEntries(ctx, client, false)
		require.NoError(t, err, "ListAuditEntries failed")
		require.NotEmpty(t, allEntries, "should have audit entries")

		// Fetch the first entry by sequence
		firstSeq := allEntries[0].GetSequence()
		entry, err := testutil.GetAuditEntry(ctx, client, firstSeq)
		require.NoError(t, err, "GetAuditEntry failed")
		require.Equal(t, firstSeq, entry.GetSequence(),
			"GetAuditEntry sequence should match")
		// Verify outcome is present (either success or failure)
		require.True(t, entry.GetSuccess() != nil || entry.GetFailure() != nil,
			"audit entry should have an outcome")
	})

	// --- Regression: ArchivePeriod + CheckStore ---
	// After archiving a period, CheckStore must account for purged logs by
	// reading archived period metadata (start_sequence, close_sequence, last_log_hash)
	// and resuming the hash chain from the correct boundary.
	t.Run("ArchivePeriodCheckStore", func(t *testing.T) {
		// Create a few transactions to have content in the current period
		for i := 1; i <= 3; i++ {
			scenariotest.ApplyActions(t, ctx, client,
				testutil.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
					"account": fmt.Sprintf("ops:%d", i),
					"amount":  "USD/2 25",
				}, nil),
			)
		}

		// Close the period → creates a CLOSING period
		scenariotest.ClosePeriodAndWait(t, ctx, client, "period close timed out for archive test")

		// Find the CLOSED period (the one just sealed)
		var closedPeriodID uint64
		require.Eventually(t, func() bool {
			periods, err := testutil.ListAllPeriods(ctx, client)
			if err != nil {
				return false
			}
			for _, p := range periods {
				if p.Status == commonpb.PeriodStatus_PERIOD_CLOSED {
					closedPeriodID = p.GetId()

					return true
				}
			}

			return false
		}, 15*time.Second, 200*time.Millisecond, "should have a CLOSED period")
		require.NotZero(t, closedPeriodID, "should have found a closed period ID")

		// Archive the closed period
		scenariotest.ApplyActions(t, ctx, client, testutil.ArchivePeriodAction(closedPeriodID))

		// Wait for the period to become ARCHIVED
		require.Eventually(t, func() bool {
			periods, err := testutil.ListAllPeriods(ctx, client)
			if err != nil {
				return false
			}
			for _, p := range periods {
				if p.GetId() == closedPeriodID && p.Status == commonpb.PeriodStatus_PERIOD_ARCHIVED {
					return true
				}
			}

			return false
		}, 30*time.Second, 200*time.Millisecond, "period should become ARCHIVED")

		t.Logf("Period %d successfully archived", closedPeriodID)

		// CheckStore must pass after archiving: it reads archived period metadata
		// to skip purged log ranges and resume the hash chain correctly.
		stream, err := client.CheckStore(ctx, &servicepb.CheckStoreRequest{})
		require.NoError(t, err, "CheckStore RPC failed")

		var storeErrors []*servicepb.CheckStoreError
		for {
			msg, err := stream.Recv()
			if err != nil {
				break
			}
			if msg.GetError() != nil {
				storeErrors = append(storeErrors, msg.GetError())
			}
		}
		require.Empty(t, storeErrors,
			"CheckStore should report no errors after archiving, got %d errors",
			len(storeErrors))
	})

	// --- Phase 4: Period Schedule ---
	t.Run("PeriodSchedule", func(t *testing.T) {
		// Set a period schedule
		scenariotest.ApplyActions(t, ctx, client, testutil.SetPeriodScheduleAction("0 0 * * *"))

		// Verify cron was set
		cron, err := testutil.GetPeriodSchedule(ctx, client)
		require.NoError(t, err, "GetPeriodSchedule failed")
		require.Equal(t, "0 0 * * *", cron, "cron should match")

		// Delete period schedule
		scenariotest.ApplyActions(t, ctx, client, testutil.DeletePeriodScheduleAction())

		// Verify cron is empty
		cron, err = testutil.GetPeriodSchedule(ctx, client)
		require.NoError(t, err, "GetPeriodSchedule after delete failed")
		require.Empty(t, cron, "cron should be empty after delete")
	})

	// --- Phase 5: Request Signing (key lifecycle) ---
	t.Run("RequestSigning", func(t *testing.T) {
		// Generate two keypairs
		pubKey1, privKey1, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err, "failed to generate Ed25519 keypair 1")
		pubKey2, _, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err, "failed to generate Ed25519 keypair 2")

		// Register first key (bootstrap: unsigned when no keys exist)
		scenariotest.ApplyActions(t, ctx, client, testutil.RegisterSigningKeyAction("key-1", pubKey1))

		// Verify the key is registered
		keys := listSigningKeys(t, ctx, client)
		require.NotNil(t, testutil.FindSigningKey(keys, "key-1"), "key-1 should be registered")

		// Register second key (must be signed by existing key)
		regReq := testutil.RegisterSigningKeyAction("key-2", pubKey2)
		require.NoError(t, signing.Sign(regReq, "key-1", privKey1))
		scenariotest.ApplyActions(t, ctx, client, regReq)

		// Verify both keys are registered
		keys = listSigningKeys(t, ctx, client)
		require.Len(t, keys, 2, "should have 2 signing keys")
		require.NotNil(t, testutil.FindSigningKey(keys, "key-1"), "key-1 should exist")
		require.NotNil(t, testutil.FindSigningKey(keys, "key-2"), "key-2 should exist")

		// Verify key-2 has key-1 as parent
		key2 := testutil.FindSigningKey(keys, "key-2")
		require.Equal(t, "key-1", key2.GetParentKeyId(), "key-2 parent should be key-1")

		// Revoke key-2 (must be signed since keys exist)
		revokeReq := testutil.RevokeSigningKeyAction("key-2", false)
		require.NoError(t, signing.Sign(revokeReq, "key-1", privKey1))
		scenariotest.ApplyActions(t, ctx, client, revokeReq)

		// Verify key-2 is removed
		keys = listSigningKeys(t, ctx, client)
		require.Len(t, keys, 1, "should have 1 signing key after revocation")
		require.Nil(t, testutil.FindSigningKey(keys, "key-2"), "key-2 should be removed after revocation")
		require.NotNil(t, testutil.FindSigningKey(keys, "key-1"), "key-1 should still exist")

		// Signed transaction should persist signature in log
		signedTxReq := testutil.CreateTransactionAction(ledger, []*commonpb.Posting{
			testutil.NewPosting("world", "ops:1", big.NewInt(10), "USD/2"),
		}, nil, nil)
		require.NoError(t, signing.Sign(signedTxReq, "key-1", privKey1))
		txResp := scenariotest.ApplyActions(t, ctx, client, signedTxReq)
		require.NotEmpty(t, txResp.Logs)
		require.NotNil(t, txResp.Logs[0].Signature, "signed transaction should have signature in log")
		require.Equal(t, "key-1", txResp.Logs[0].Signature.GetKeyId())
	})

	// --- Phase 6: Delete Ledger ---
	t.Run("DeleteLedger", func(t *testing.T) {
		// Create a temporary ledger
		scenariotest.ApplyActions(t, ctx, client,
			testutil.CreateLedgerAction("temp-ledger", nil),
		)

		// Verify it exists
		ledgers, err := testutil.ListLedgers(ctx, client)
		require.NoError(t, err)
		require.Contains(t, ledgers, "temp-ledger", "temp-ledger should exist")

		// Make 1 transaction
		scenariotest.ApplyActions(t, ctx, client,
			testutil.CreateForceTransactionAction("temp-ledger", []*commonpb.Posting{
				testutil.NewPosting("world", "user:1", big.NewInt(1000), "USD/2"),
			}, nil),
		)

		// Delete the ledger
		scenariotest.ApplyActions(t, ctx, client, testutil.DeleteLedgerAction("temp-ledger"))

		// Verify it's gone from the active list
		ledgers, err = testutil.ListLedgers(ctx, client)
		require.NoError(t, err)
		if info, exists := ledgers["temp-ledger"]; exists {
			// If still listed, it should have a deleted_at timestamp
			require.NotNil(t, info.GetDeletedAt(), "temp-ledger should be marked as deleted")
		}
	})

	// --- Phase 7: Final Invariants ---
	t.Run("FinalInvariants", func(t *testing.T) {
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
		scenariotest.CheckNoNegativeBalances(t, ctx, client, ledger, []string{"world"})

		// Verify stats
		stats, err := testutil.GetLedgerStats(ctx, client, ledger)
		require.NoError(t, err, "GetLedgerStats failed")
		require.Greater(t, stats.GetAccountCount(), uint64(0), "should have accounts")
		require.Greater(t, stats.GetTransactionCount(), uint64(0), "should have transactions")
		t.Logf("LedgerStats: %d accounts, %d transactions",
			stats.GetAccountCount(), stats.GetTransactionCount())
	})

	// --- Tail phases: StoreCheck, Backup, Restart+Verify, BackupRestore+Verify ---
	scenariotest.RunPostTestPhases(t, sc, func(t *testing.T, client servicepb.BucketServiceClient) {
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
		scenariotest.CheckNoNegativeBalances(t, ctx, client, ledger, []string{"world"})
	})
}

// listSigningKeys collects all signing keys without depending on Gomega.
func listSigningKeys(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient) []*commonpb.SigningKey {
	t.Helper()

	stream, err := client.ListSigningKeys(ctx, &servicepb.ListSigningKeysRequest{})
	require.NoError(t, err, "ListSigningKeys failed")

	var keys []*commonpb.SigningKey
	for {
		key, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "ListSigningKeys recv failed")
		keys = append(keys, key)
	}

	return keys
}
