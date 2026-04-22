package check

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// --- applyPostings tests ---

func TestApplyPostingsSinglePosting(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	postings := []*commonpb.Posting{
		newPosting("alice", "bob", "USD", 100),
	}

	require.NoError(t, applyPostings("ledger", postings, rs))

	// Source (alice): output increased by 100
	sourceKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "ledger", Account: "alice"},
		Asset:      "USD",
	}
	srcPair := readVolume(t, rs, sourceKey.Bytes())
	require.Equal(t, "0", srcPair.GetInput().ToBigInt().String())
	require.Equal(t, "100", srcPair.GetOutput().ToBigInt().String())

	// Destination (bob): input increased by 100
	destKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "ledger", Account: "bob"},
		Asset:      "USD",
	}
	dstPair := readVolume(t, rs, destKey.Bytes())
	require.Equal(t, "100", dstPair.GetInput().ToBigInt().String())
	require.Equal(t, "0", dstPair.GetOutput().ToBigInt().String())
}

func TestApplyPostingsMultiplePostings(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	postings := []*commonpb.Posting{
		newPosting("treasury", "alice", "USD", 500),
		newPosting("treasury", "bob", "USD", 300),
	}

	require.NoError(t, applyPostings("ledger", postings, rs))

	// Treasury: output = 500 + 300 = 800
	treasuryKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "ledger", Account: "treasury"},
		Asset:      "USD",
	}
	pair := readVolume(t, rs, treasuryKey.Bytes())
	require.Equal(t, "0", pair.GetInput().ToBigInt().String())
	require.Equal(t, "800", pair.GetOutput().ToBigInt().String())
}

func TestApplyPostingsAccumulatesAcrossCalls(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	require.NoError(t, applyPostings("ledger", []*commonpb.Posting{
		newPosting("world", "alice", "USD", 100),
	}, rs))
	require.NoError(t, applyPostings("ledger", []*commonpb.Posting{
		newPosting("world", "alice", "USD", 200),
	}, rs))

	aliceKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "ledger", Account: "alice"},
		Asset:      "USD",
	}
	pair := readVolume(t, rs, aliceKey.Bytes())
	require.Equal(t, "300", pair.GetInput().ToBigInt().String())
}

// --- simulateEphemeralPurge tests ---

func TestSimulateEphemeralPurgeDeletesZeroBalance(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	// Create an ephemeral account type matching "orders:*"
	ledgerAccountTypes := map[string][]accounttype.CompiledType{
		"ledger": accounttype.CompileTypes(map[string]*commonpb.AccountType{
			"orders": {
				Name:      "orders",
				Pattern:   "orders:{id}",
				Ephemeral: true,
			},
		}),
	}

	// Fund order account: world -> orders:123  100 USD
	postings := []*commonpb.Posting{
		newPosting("world", "orders:123", "USD", 100),
	}
	require.NoError(t, applyPostings("ledger", postings, rs))

	// Purge should NOT delete (input=100, output=0 => not zero balance)
	require.NoError(t, simulateEphemeralPurge("ledger", postings, rs, ledgerAccountTypes))

	orderKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "ledger", Account: "orders:123"},
		Asset:      "USD",
	}
	pair, err := rs.getVolume(orderKey.Bytes())
	require.NoError(t, err)
	require.NotNil(t, pair, "volume should still exist (non-zero balance)")

	// Now drain: orders:123 -> world  100 USD
	drainPostings := []*commonpb.Posting{
		newPosting("orders:123", "world", "USD", 100),
	}
	require.NoError(t, applyPostings("ledger", drainPostings, rs))

	// After drain, orders:123/USD has input=100, output=100 => zero balance
	require.NoError(t, simulateEphemeralPurge("ledger", drainPostings, rs, ledgerAccountTypes))

	pair, err = rs.getVolume(orderKey.Bytes())
	require.NoError(t, err)
	require.Nil(t, pair, "volume should be purged after zero balance")
}

func TestSimulateEphemeralPurgeSkipsNonEphemeral(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	// No ephemeral types
	ledgerAccountTypes := map[string][]accounttype.CompiledType{
		"ledger": accounttype.CompileTypes(map[string]*commonpb.AccountType{
			"users": {
				Name:    "users",
				Pattern: "users:{id}",
				// Ephemeral is false (default)
			},
		}),
	}

	postings := []*commonpb.Posting{
		newPosting("world", "users:alice", "USD", 100),
	}
	require.NoError(t, applyPostings("ledger", postings, rs))

	drainPostings := []*commonpb.Posting{
		newPosting("users:alice", "world", "USD", 100),
	}
	require.NoError(t, applyPostings("ledger", drainPostings, rs))

	// Purge should NOT delete because account type is not ephemeral
	require.NoError(t, simulateEphemeralPurge("ledger", drainPostings, rs, ledgerAccountTypes))

	userKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "ledger", Account: "users:alice"},
		Asset:      "USD",
	}
	pair, err := rs.getVolume(userKey.Bytes())
	require.NoError(t, err)
	require.NotNil(t, pair, "non-ephemeral account volume should not be purged")
}

func TestSimulateEphemeralPurgeNoAccountTypes(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	// Empty account types — should be a no-op
	ledgerAccountTypes := map[string][]accounttype.CompiledType{}

	postings := []*commonpb.Posting{
		newPosting("world", "account", "USD", 100),
	}
	require.NoError(t, applyPostings("ledger", postings, rs))
	require.NoError(t, simulateEphemeralPurge("ledger", postings, rs, ledgerAccountTypes))

	// Volume should still exist
	key := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "ledger", Account: "account"},
		Asset:      "USD",
	}
	pair, err := rs.getVolume(key.Bytes())
	require.NoError(t, err)
	require.NotNil(t, pair)
}

func TestSimulateEphemeralPurgeSkipsWorldAccount(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	ledgerAccountTypes := map[string][]accounttype.CompiledType{
		"ledger": accounttype.CompileTypes(map[string]*commonpb.AccountType{
			"world-type": {
				Name:      "world-type",
				Pattern:   "world",
				Ephemeral: true,
			},
		}),
	}

	postings := []*commonpb.Posting{
		newPosting("world", "alice", "USD", 100),
	}
	require.NoError(t, applyPostings("ledger", postings, rs))

	// Should not error — world is explicitly skipped
	require.NoError(t, simulateEphemeralPurge("ledger", postings, rs, ledgerAccountTypes))
}

// --- checkReversionInvariants tests ---

func TestCheckReversionInvariantsValidCreationAndRevert(t *testing.T) {
	t.Parallel()

	knownTxIDs := make(map[string]*domain.ReversionBitset)
	revertedTxIDs := make(map[string]*domain.ReversionBitset)
	var errors []*servicepb.CheckStoreError

	callback := func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
			errors = append(errors, e.Error)
		}
	}

	// Create tx 1
	checkReversionInvariants("ledger", 1, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{Id: 1},
			},
		},
	}, knownTxIDs, revertedTxIDs, callback)

	require.Empty(t, errors)

	// Revert tx 1 (valid)
	checkReversionInvariants("ledger", 2, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: 1,
				RevertTransaction:     &commonpb.Transaction{Id: 2},
			},
		},
	}, knownTxIDs, revertedTxIDs, callback)

	require.Empty(t, errors, "valid revert should produce no errors")

	// Verify tx 1 is tracked as reverted
	require.True(t, revertedTxIDs["ledger"].IsReverted(1))
	// Verify revert tx (ID 2) is tracked as known
	require.True(t, knownTxIDs["ledger"].IsReverted(2))
}

func TestCheckReversionInvariantsDoubleRevert(t *testing.T) {
	t.Parallel()

	knownTxIDs := make(map[string]*domain.ReversionBitset)
	revertedTxIDs := make(map[string]*domain.ReversionBitset)
	var errors []*servicepb.CheckStoreError

	callback := func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
			errors = append(errors, e.Error)
		}
	}

	// Create tx 1
	checkReversionInvariants("ledger", 1, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{Id: 1},
			},
		},
	}, knownTxIDs, revertedTxIDs, callback)

	// Revert tx 1 (valid)
	checkReversionInvariants("ledger", 2, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: 1,
				RevertTransaction:     &commonpb.Transaction{Id: 2},
			},
		},
	}, knownTxIDs, revertedTxIDs, callback)

	require.Empty(t, errors)

	// Double-revert tx 1
	checkReversionInvariants("ledger", 3, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: 1,
				RevertTransaction:     &commonpb.Transaction{Id: 3},
			},
		},
	}, knownTxIDs, revertedTxIDs, callback)

	require.Len(t, errors, 1)
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERTED_MISMATCH, errors[0].GetErrorType())
	require.Contains(t, errors[0].GetMessage(), "double-reverts")
}

func TestCheckReversionInvariantsRevertNonExistent(t *testing.T) {
	t.Parallel()

	knownTxIDs := make(map[string]*domain.ReversionBitset)
	revertedTxIDs := make(map[string]*domain.ReversionBitset)
	var errors []*servicepb.CheckStoreError

	callback := func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
			errors = append(errors, e.Error)
		}
	}

	// Revert tx 999 without ever creating it
	checkReversionInvariants("ledger", 1, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: 999,
				RevertTransaction:     &commonpb.Transaction{Id: 1},
			},
		},
	}, knownTxIDs, revertedTxIDs, callback)

	require.Len(t, errors, 1)
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERTED_MISMATCH, errors[0].GetErrorType())
	require.Contains(t, errors[0].GetMessage(), "non-existent")
}

func TestCheckReversionInvariantsMultipleLedgersIsolated(t *testing.T) {
	t.Parallel()

	knownTxIDs := make(map[string]*domain.ReversionBitset)
	revertedTxIDs := make(map[string]*domain.ReversionBitset)
	var errors []*servicepb.CheckStoreError

	callback := func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
			errors = append(errors, e.Error)
		}
	}

	// Create tx 1 in ledger-a
	checkReversionInvariants("ledger-a", 1, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{Id: 1},
			},
		},
	}, knownTxIDs, revertedTxIDs, callback)

	// Try to revert tx 1 from ledger-b (different ledger — tx 1 doesn't exist there)
	checkReversionInvariants("ledger-b", 2, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: 1,
				RevertTransaction:     &commonpb.Transaction{Id: 1},
			},
		},
	}, knownTxIDs, revertedTxIDs, callback)

	require.Len(t, errors, 1, "ledgers should be isolated for reversion tracking")
	require.Contains(t, errors[0].GetMessage(), "non-existent")
}

func TestCheckReversionInvariantsNilPayload(t *testing.T) {
	t.Parallel()

	knownTxIDs := make(map[string]*domain.ReversionBitset)
	revertedTxIDs := make(map[string]*domain.ReversionBitset)
	var errors []*servicepb.CheckStoreError

	callback := func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
			errors = append(errors, e.Error)
		}
	}

	// Nil RevertedTransaction payload should not panic
	checkReversionInvariants("ledger", 1, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: nil,
		},
	}, knownTxIDs, revertedTxIDs, callback)

	require.Empty(t, errors)
}

func TestApplyPostingsMultipleAssets(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	postings := []*commonpb.Posting{
		newPosting("world", "alice", "USD", 100),
		newPosting("world", "alice", "EUR", 200),
	}

	require.NoError(t, applyPostings("ledger", postings, rs))

	usdKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "ledger", Account: "alice"},
		Asset:      "USD",
	}
	eurKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "ledger", Account: "alice"},
		Asset:      "EUR",
	}

	usdPair := readVolume(t, rs, usdKey.Bytes())
	require.Equal(t, "100", usdPair.GetInput().ToBigInt().String())

	eurPair := readVolume(t, rs, eurKey.Bytes())
	require.Equal(t, "200", eurPair.GetInput().ToBigInt().String())
}

func TestSimulateEphemeralPurgeMultipleAssets(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	ledgerAccountTypes := map[string][]accounttype.CompiledType{
		"ledger": accounttype.CompileTypes(map[string]*commonpb.AccountType{
			"orders": {
				Name:      "orders",
				Pattern:   "orders:{id}",
				Ephemeral: true,
			},
		}),
	}

	// Fund in two assets
	fundPostings := []*commonpb.Posting{
		newPosting("world", "orders:1", "USD", 100),
		newPosting("world", "orders:1", "EUR", 200),
	}
	require.NoError(t, applyPostings("ledger", fundPostings, rs))

	// Drain only USD
	drainPostings := []*commonpb.Posting{
		newPosting("orders:1", "world", "USD", 100),
	}
	require.NoError(t, applyPostings("ledger", drainPostings, rs))
	require.NoError(t, simulateEphemeralPurge("ledger", drainPostings, rs, ledgerAccountTypes))

	// USD should be purged (input==output==100)
	usdKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "ledger", Account: "orders:1"},
		Asset:      "USD",
	}
	pair, err := rs.getVolume(usdKey.Bytes())
	require.NoError(t, err)
	require.Nil(t, pair, "USD volume should be purged")

	// EUR should remain (input=200, output=0)
	eurKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "ledger", Account: "orders:1"},
		Asset:      "EUR",
	}
	pair, err = rs.getVolume(eurKey.Bytes())
	require.NoError(t, err)
	require.NotNil(t, pair, "EUR volume should still exist")
	require.Equal(t, "200", pair.GetInput().ToBigInt().String())
}

func TestApplyPostingsZeroAmount(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	postings := []*commonpb.Posting{
		newPosting("alice", "bob", "USD", 0),
	}

	require.NoError(t, applyPostings("ledger", postings, rs))

	// Both source and dest should have zero volumes
	srcKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "ledger", Account: "alice"},
		Asset:      "USD",
	}
	srcPair := readVolume(t, rs, srcKey.Bytes())
	require.Equal(t, big.NewInt(0), srcPair.GetOutput().ToBigInt())
	require.Equal(t, big.NewInt(0), srcPair.GetInput().ToBigInt())
}
