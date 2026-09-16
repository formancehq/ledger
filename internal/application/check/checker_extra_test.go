package check

import (
	"math/big"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	domainreplay "github.com/formancehq/ledger/v3/internal/domain/replay"
	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// --- applyPostings tests ---

func TestApplyPostingsSinglePosting(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	postings := []*commonpb.Posting{
		newPosting("alice", "bob", "USD", 100),
	}

	require.NoError(t, domainreplay.ApplyPostings("ledger", postings, rs))

	// Source (alice): output increased by 100
	sourceKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "alice"},
		Asset:      "USD",
	}
	srcPair := readVolume(t, rs, sourceKey.Bytes())
	require.Equal(t, "0", srcPair.GetInput().ToBigInt().String())
	require.Equal(t, "100", srcPair.GetOutput().ToBigInt().String())

	// Destination (bob): input increased by 100
	destKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "bob"},
		Asset:      "USD",
	}
	dstPair := readVolume(t, rs, destKey.Bytes())
	require.Equal(t, "100", dstPair.GetInput().ToBigInt().String())
	require.Equal(t, "0", dstPair.GetOutput().ToBigInt().String())
}

func TestComparePurgedAccountProjections(t *testing.T) {
	t.Parallel()

	alice := domain.AccountKey{LedgerName: "ledger", Account: "alice"}
	bob := domain.AccountKey{LedgerName: "ledger", Account: "bob"}
	var events []*servicepb.CheckStoreEvent
	comparePurgedAccountProjections(
		map[domain.AccountKey]uint64{alice: 42},
		map[domain.AccountKey]struct{}{bob: {}},
		map[string]uint64{"ledger": 42},
		42,
		func(event *servicepb.CheckStoreEvent) { events = append(events, event) },
	)

	require.Len(t, events, 2)
	require.Contains(t, events[0].GetError().GetMessage()+events[1].GetError().GetMessage(), "alice")
	require.Contains(t, events[0].GetError().GetMessage()+events[1].GetError().GetMessage(), "bob")
}

func TestComparePurgedAccountProjectionsRejectsNonTerminalAnnotation(t *testing.T) {
	t.Parallel()

	account := domain.AccountKey{LedgerName: "ledger", Account: "alice"}
	var events []*servicepb.CheckStoreEvent
	comparePurgedAccountProjections(
		map[domain.AccountKey]uint64{account: 41},
		map[domain.AccountKey]struct{}{account: {}},
		map[string]uint64{"ledger": 42},
		42,
		func(event *servicepb.CheckStoreEvent) { events = append(events, event) },
	)

	require.Len(t, events, 1)
	require.Contains(t, events[0].GetError().GetMessage(), "terminal ledger log 42")
}

func TestAccountPurgeDoesNotBecomeVolumeExclusion(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "ephemeral"},
		Asset:      "USD",
	}
	require.NoError(t, rs.AddVolumeDelta(key.Bytes(), big.NewInt(1), big.NewInt(1)))
	collected := 0
	require.NoError(t, rs.PurgeAccount("ledger", "ephemeral", func(_, _, _, _ string) { collected++ }))
	require.Zero(t, collected)
	require.Contains(t, rs.takePendingPurgedAccounts(), key.AccountKey)
}

func TestHistoricalExclusionDoesNotHideRefundedAccount(t *testing.T) {
	t.Parallel()

	volume := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "ephemeral"},
		Asset:      "USD",
	}
	metadata := domain.MetadataKey{AccountKey: volume.AccountKey, Key: "status"}
	excluded := excludedVolumesSet{
		"ledger": {domain.AccountAssetKey{Account: "ephemeral", Asset: "USD"}: {}},
	}

	require.True(t, excluded.excludesCurrentVolume(volume, false, false))
	require.False(t, excluded.excludesCurrentVolume(volume, true, true))
	require.False(t, excluded.excludesCurrentVolume(volume, false, true), "a re-funded account must not hide an older absent cell")
	require.True(t, excluded.excludesCurrentMetadata(metadata, false))
	require.False(t, excluded.excludesCurrentMetadata(metadata, true))
}

func TestApplyPostingsMultiplePostings(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	postings := []*commonpb.Posting{
		newPosting("treasury", "alice", "USD", 500),
		newPosting("treasury", "bob", "USD", 300),
	}

	require.NoError(t, domainreplay.ApplyPostings("ledger", postings, rs))

	// Treasury: output = 500 + 300 = 800
	treasuryKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "treasury"},
		Asset:      "USD",
	}
	pair := readVolume(t, rs, treasuryKey.Bytes())
	require.Equal(t, "0", pair.GetInput().ToBigInt().String())
	require.Equal(t, "800", pair.GetOutput().ToBigInt().String())
}

func TestApplyPostingsAccumulatesAcrossCalls(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	require.NoError(t, domainreplay.ApplyPostings("ledger", []*commonpb.Posting{
		newPosting("world", "alice", "USD", 100),
	}, rs))
	require.NoError(t, domainreplay.ApplyPostings("ledger", []*commonpb.Posting{
		newPosting("world", "alice", "USD", 200),
	}, rs))

	aliceKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "alice"},
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
				Name:        "orders",
				Pattern:     "orders:{id}",
				Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
			},
		}),
	}

	// Fund order account: world -> orders:123  100 USD
	postings := []*commonpb.Posting{
		newPosting("world", "orders:123", "USD", 100),
	}
	require.NoError(t, domainreplay.ApplyPostings("ledger", postings, rs))

	// Purge should NOT delete (input=100, output=0 => not zero balance)
	require.NoError(t, domainreplay.SimulateEphemeralPurge("ledger", postings, rs, ledgerAccountTypes, nil))

	orderKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "orders:123"},
		Asset:      "USD",
	}
	pair, err := rs.GetVolume(orderKey.Bytes())
	require.NoError(t, err)
	require.NotNil(t, pair, "volume should still exist (non-zero balance)")

	// Now drain: orders:123 -> world  100 USD
	drainPostings := []*commonpb.Posting{
		newPosting("orders:123", "world", "USD", 100),
	}
	require.NoError(t, domainreplay.ApplyPostings("ledger", drainPostings, rs))

	// After drain, orders:123/USD has input=100, output=100 => zero balance
	require.NoError(t, domainreplay.SimulateEphemeralPurge("ledger", drainPostings, rs, ledgerAccountTypes, nil))

	pair, err = rs.GetVolume(orderKey.Bytes())
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
	require.NoError(t, domainreplay.ApplyPostings("ledger", postings, rs))

	drainPostings := []*commonpb.Posting{
		newPosting("users:alice", "world", "USD", 100),
	}
	require.NoError(t, domainreplay.ApplyPostings("ledger", drainPostings, rs))

	// Purge should NOT delete because account type is not ephemeral
	require.NoError(t, domainreplay.SimulateEphemeralPurge("ledger", drainPostings, rs, ledgerAccountTypes, nil))

	userKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "users:alice"},
		Asset:      "USD",
	}
	pair, err := rs.GetVolume(userKey.Bytes())
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
	require.NoError(t, domainreplay.ApplyPostings("ledger", postings, rs))
	require.NoError(t, domainreplay.SimulateEphemeralPurge("ledger", postings, rs, ledgerAccountTypes, nil))

	// Volume should still exist
	key := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "account"},
		Asset:      "USD",
	}
	pair, err := rs.GetVolume(key.Bytes())
	require.NoError(t, err)
	require.NotNil(t, pair)
}

func TestSimulateEphemeralPurgeSkipsWorldAccount(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	ledgerAccountTypes := map[string][]accounttype.CompiledType{
		"ledger": accounttype.CompileTypes(map[string]*commonpb.AccountType{
			"world-type": {
				Name:        "world-type",
				Pattern:     "world",
				Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
			},
		}),
	}

	postings := []*commonpb.Posting{
		newPosting("world", "alice", "USD", 100),
	}
	require.NoError(t, domainreplay.ApplyPostings("ledger", postings, rs))

	// Should not error — world is explicitly skipped
	require.NoError(t, domainreplay.SimulateEphemeralPurge("ledger", postings, rs, ledgerAccountTypes, nil))
}

func TestEphemeralPurgeBufferDerivesAccountWidePurgeAndAllowsRefund(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	types := map[string][]accounttype.CompiledType{
		"ledger": accounttype.CompileTypes(map[string]*commonpb.AccountType{
			"orders": {
				Name:        "orders",
				Pattern:     "orders:{id}",
				Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
			},
		}),
	}
	account := "orders:1"
	metadataKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: account},
		Key:        "owner",
	}.Bytes()
	replayProposal := func(postings ...*commonpb.Posting) {
		t.Helper()
		buffer := domainreplay.NewEphemeralPurgeBuffer()
		require.NoError(t, domainreplay.ApplyPostings("ledger", postings, rs))
		buffer.Add("ledger", postings)
		require.NoError(t, buffer.Flush(rs, types, nil))
	}

	replayProposal(
		newPosting("world", account, "USD", 5),
		newPosting("world", account, "EUR", 7),
	)
	require.NoError(t, rs.SetMetadata(metadataKey, commonpb.NewStringValue("old")))
	replayProposal(
		newPosting(account, "world", "USD", 5),
		newPosting(account, "world", "EUR", 7),
	)

	for _, asset := range []string{"USD", "EUR"} {
		volume, err := rs.GetVolume(domain.NewVolumeKey("ledger", account, asset, "").Bytes())
		require.NoError(t, err)
		require.Nil(t, volume, "account-wide purge must remove every zero volume")
	}
	_, closer, err := rs.db.Get(replayKey(replayPrefixMetadata, metadataKey))
	if closer != nil {
		_ = closer.Close()
	}
	require.ErrorIs(t, err, pebble.ErrNotFound, "account-wide purge must remove metadata")

	replayProposal(newPosting("world", account, "USD", 3))
	volume, err := rs.GetVolume(domain.NewVolumeKey("ledger", account, "USD", "").Bytes())
	require.NoError(t, err)
	require.NotNil(t, volume, "later funding must create a fresh current incarnation")
}

func TestEphemeralPurgeBufferCollectsTouchedCellsBeforeAccountPurge(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	types := map[string][]accounttype.CompiledType{
		"ledger": accounttype.CompileTypes(map[string]*commonpb.AccountType{
			"ephemeral": {
				Name:        "ephemeral",
				Pattern:     "ephemeral:{id}",
				Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
			},
		}),
	}
	account := "ephemeral:1"
	postings := []*commonpb.Posting{newPosting("world", account, "USD", 5), newPosting(account, "world", "USD", 5)}
	require.NoError(t, domainreplay.ApplyPostings("ledger", postings, rs))

	buffer := domainreplay.NewEphemeralPurgeBuffer()
	buffer.Add("ledger", postings)
	var collected []domain.AccountAssetKey
	require.NoError(t, buffer.Flush(rs, types, func(_, account, asset, color string) {
		collected = append(collected, domain.AccountAssetKey{Account: account, Asset: asset, Color: color})
	}))

	require.Equal(t, []domain.AccountAssetKey{{Account: account, Asset: "USD"}}, collected)
	volume, err := rs.GetVolume(domain.NewVolumeKey("ledger", account, "USD", "").Bytes())
	require.NoError(t, err)
	require.Nil(t, volume)
}

// --- checkReversionInvariants tests ---

func TestCheckReversionInvariantsValidCreationAndRevert(t *testing.T) {
	t.Parallel()

	knownTxIDs := make(map[string]*bitset.Bitset)
	revertedTxIDs := make(map[string]*bitset.Bitset)
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
	require.True(t, revertedTxIDs["ledger"].Test(1))
	// Verify revert tx (ID 2) is tracked as known
	require.True(t, knownTxIDs["ledger"].Test(2))
}

func TestCheckReversionInvariantsDoubleRevert(t *testing.T) {
	t.Parallel()

	knownTxIDs := make(map[string]*bitset.Bitset)
	revertedTxIDs := make(map[string]*bitset.Bitset)
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

	knownTxIDs := make(map[string]*bitset.Bitset)
	revertedTxIDs := make(map[string]*bitset.Bitset)
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

	knownTxIDs := make(map[string]*bitset.Bitset)
	revertedTxIDs := make(map[string]*bitset.Bitset)
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

	knownTxIDs := make(map[string]*bitset.Bitset)
	revertedTxIDs := make(map[string]*bitset.Bitset)
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

	require.NoError(t, domainreplay.ApplyPostings("ledger", postings, rs))

	usdKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "alice"},
		Asset:      "USD",
	}
	eurKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "alice"},
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
				Name:        "orders",
				Pattern:     "orders:{id}",
				Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
			},
		}),
	}

	// Fund in two assets
	fundPostings := []*commonpb.Posting{
		newPosting("world", "orders:1", "USD", 100),
		newPosting("world", "orders:1", "EUR", 200),
	}
	require.NoError(t, domainreplay.ApplyPostings("ledger", fundPostings, rs))

	// Drain only USD
	drainPostings := []*commonpb.Posting{
		newPosting("orders:1", "world", "USD", 100),
	}
	require.NoError(t, domainreplay.ApplyPostings("ledger", drainPostings, rs))
	require.NoError(t, domainreplay.SimulateEphemeralPurge("ledger", drainPostings, rs, ledgerAccountTypes, nil))

	// USD should be purged (input==output==100)
	usdKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "orders:1"},
		Asset:      "USD",
	}
	pair, err := rs.GetVolume(usdKey.Bytes())
	require.NoError(t, err)
	require.Nil(t, pair, "USD volume should be purged")

	// EUR should remain (input=200, output=0)
	eurKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "orders:1"},
		Asset:      "EUR",
	}
	pair, err = rs.GetVolume(eurKey.Bytes())
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

	require.NoError(t, domainreplay.ApplyPostings("ledger", postings, rs))

	// Both source and dest should have zero volumes
	srcKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "alice"},
		Asset:      "USD",
	}
	srcPair := readVolume(t, rs, srcKey.Bytes())
	require.Equal(t, big.NewInt(0), srcPair.GetOutput().ToBigInt())
	require.Equal(t, big.NewInt(0), srcPair.GetInput().ToBigInt())
}
