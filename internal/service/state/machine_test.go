package state

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/crypto/keystore"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/cache"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"
)

// newTestMachineWithThreshold creates a Machine with a configurable generation threshold.
func newTestMachineWithThreshold(t *testing.T, generationThreshold uint64) (*Machine, *dal.Store, *attributes.Attributes) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dataStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	attrs := attributes.New()

	c, err := cache.New(generationThreshold, meter)
	require.NoError(t, err)

	// Persist audit config before creating the machine (NewMachine reads from Pebble)
	batch := dataStore.NewBatch()
	require.NoError(t, SaveAuditConfig(batch, true))
	require.NoError(t, batch.Commit())

	machine, err := NewMachine(logger, dataStore, meter, c, attrs, generationThreshold, keystore.NewKeyStore(), NewSharedState(), NoopEventNotifier{}, 0)
	require.NoError(t, err)

	return machine, dataStore, attrs
}

// newTestMachine creates a Machine backed by a real Pebble store for testing.
func newTestMachine(t *testing.T) (*Machine, *dal.Store, *attributes.Attributes) {
	t.Helper()
	return newTestMachineWithThreshold(t, 1000)
}

// makeProposal builds a Proposal protobuf with the given orders.
func makeProposal(id uint64, orders ...*raftcmdpb.Order) *raftcmdpb.Proposal {
	return &raftcmdpb.Proposal{
		Id:     id,
		Orders: orders,
		Date:   &commonpb.Timestamp{Data: 1700000000 + id},
	}
}

// makeEntry marshals a proposal into a raft entry at the given index.
func makeEntry(t *testing.T, index uint64, proposal *raftcmdpb.Proposal) raftpb.Entry {
	t.Helper()

	entryData, err := proto.Marshal(proposal)
	require.NoError(t, err)

	return raftpb.Entry{
		Index: index,
		Term:  1,
		Type:  raftpb.EntryNormal,
		Data:  entryData,
	}
}

func createLedgerOrder(name string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_CreateLedger{
			CreateLedger: &raftcmdpb.CreateLedgerOrder{
				Name: name,
			},
		},
	}
}

func createTransactionOrder(ledger string, force bool, postings ...*commonpb.Posting) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledger,
				Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
					CreateTransaction: &raftcmdpb.CreateTransactionOrder{
						Postings: postings,
						Force:    force,
					},
				},
			},
		},
	}
}

func revertTransactionOrder(ledger string, txID uint64) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledger,
				Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
					RevertTransaction: &raftcmdpb.RevertTransactionOrder{
						TransactionId: txID,
					},
				},
			},
		},
	}
}

func newPosting(source, destination, asset string, amount int64) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      source,
		Destination: destination,
		Amount:      commonpb.NewUint256FromUint64(uint64(amount)),
		Asset:       asset,
	}
}

// effectiveVolumeInput extracts the effective input value from a VolumePair.
// InputKnown takes precedence (includes preloaded base + diffs), otherwise InputDiff.
func effectiveVolumeInput(vp *raftcmdpb.VolumePair) int64 {
	if vp == nil {
		return 0
	}
	if vp.InputKnown != nil {
		return vp.InputKnown.ToBigInt().Int64()
	}
	if vp.InputDiff != nil {
		return vp.InputDiff.ToBigInt().Int64()
	}
	return 0
}

// effectiveVolumeOutput extracts the effective output value from a VolumePair.
// OutputKnown takes precedence (includes preloaded base + diffs), otherwise OutputDiff.
func effectiveVolumeOutput(vp *raftcmdpb.VolumePair) int64 {
	if vp == nil {
		return 0
	}
	if vp.OutputKnown != nil {
		return vp.OutputKnown.ToBigInt().Int64()
	}
	if vp.OutputDiff != nil {
		return vp.OutputDiff.ToBigInt().Int64()
	}
	return 0
}

// TestMachineMemoryNotCorruptedOnError verifies that when a proposal fails,
// the failed proposal's movements are NOT persisted in the cache or in the
// Pebble store. Valid proposals before and after the failure must remain visible.
//
// Scenario: an e-commerce platform with the following accounts:
//   - "platform:revenue"  : the platform's revenue account
//   - "merchant:alice"    : merchant Alice's account
//   - "merchant:bob"      : merchant Bob's account
//   - "customer:carol"    : customer Carol's account
//   - "customer:dave"     : customer Dave's account
//
// Entry 1: create ledger "ecommerce"
//
// Entry 2 - Batch 1 (SUCCESS): seed funds + first sales
//   - world -> customer:carol  1000 EUR (customer tops up)
//   - world -> customer:dave    500 EUR (customer tops up)
//   - customer:carol -> merchant:alice   200 EUR (Carol buys from Alice)
//   - customer:carol -> platform:revenue  20 EUR (platform fee)
//
// Entry 3 - Batch 2 (FAILURE): valid transactions + a bad revert
//   - customer:dave -> merchant:bob      300 EUR (modifies buffer)
//   - customer:dave -> platform:revenue   30 EUR (modifies buffer)
//   - REVERT transaction 9999 (does not exist => error, entire batch rolled back)
//
// Entry 4 - Batch 3 (SUCCESS): Dave makes valid purchases
//   - customer:dave -> merchant:bob      300 EUR
//   - customer:dave -> platform:revenue   30 EUR
//
// Verification:
//   - merchant:bob   input = 300 (batch 3 only, NOT 600)
//   - platform:revenue input = 50 (20 from batch 1 + 30 from batch 3, NOT 80)
//   - customer:dave  output = 330 (batch 3 only, NOT 660)
//   - merchant:alice input = 200 (batch 1 only, unchanged)
func TestMachineMemoryNotCorruptedOnError(t *testing.T) {
	t.Parallel()

	machine, dataStore, attrs := newTestMachine(t)
	ctx := context.Background()

	const ledgerName = "ecommerce"

	// ---------------------------------------------------------------
	// Entry 1: create the ledger
	// ---------------------------------------------------------------
	result, err := machine.ApplyEntries(ctx,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error)

	// ---------------------------------------------------------------
	// Entry 2 - Batch 1 (SUCCESS): seed funds + first sales
	// All transactions use force=true (bypasses balance checks).
	// ---------------------------------------------------------------
	result, err = machine.ApplyEntries(ctx,
		makeEntry(t, 2, makeProposal(2,
			createTransactionOrder(ledgerName, true,
				newPosting("world", "customer:carol", "EUR", 1000),
			),
			createTransactionOrder(ledgerName, true,
				newPosting("world", "customer:dave", "EUR", 500),
			),
			createTransactionOrder(ledgerName, true,
				newPosting("customer:carol", "merchant:alice", "EUR", 200),
			),
			createTransactionOrder(ledgerName, true,
				newPosting("customer:carol", "platform:revenue", "EUR", 20),
			),
		)),
	)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error, "batch 1 should succeed")

	// ---------------------------------------------------------------
	// Entry 3 - Batch 2 (FAILURE): valid transactions + bad revert
	// The first two orders succeed in the buffer but the revert of
	// a non-existent transaction causes the entire proposal to fail.
	// ---------------------------------------------------------------
	result, err = machine.ApplyEntries(ctx,
		makeEntry(t, 3, makeProposal(3,
			createTransactionOrder(ledgerName, true,
				newPosting("customer:dave", "merchant:bob", "EUR", 300),
			),
			createTransactionOrder(ledgerName, true,
				newPosting("customer:dave", "platform:revenue", "EUR", 30),
			),
			// This order will fail: transaction 9999 does not exist.
			revertTransactionOrder(ledgerName, 9999),
		)),
	)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.Error(t, result.Results[0].Error, "batch 2 should fail")
	require.Contains(t, result.Results[0].Error.Error(), "does not exist")

	// ---------------------------------------------------------------
	// Entry 4 - Batch 3 (SUCCESS): Dave makes valid purchases
	// ---------------------------------------------------------------
	result, err = machine.ApplyEntries(ctx,
		makeEntry(t, 4, makeProposal(4,
			createTransactionOrder(ledgerName, true,
				newPosting("customer:dave", "merchant:bob", "EUR", 300),
			),
			createTransactionOrder(ledgerName, true,
				newPosting("customer:dave", "platform:revenue", "EUR", 30),
			),
		)),
	)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error, "batch 3 should succeed")

	// ---------------------------------------------------------------
	// Verify volumes in cache (Machine's KeyStores)
	// ---------------------------------------------------------------
	// Expected final state:
	//   customer:carol  : input=1000, output=220
	//   customer:dave   : input=500,  output=330 (NOT 660)
	//   merchant:alice  : input=200,  output=0
	//   merchant:bob    : input=300,  output=0   (NOT 600)
	//   platform:revenue: input=50,   output=0   (NOT 80)

	type volumeExpectation struct {
		account     string
		asset       string
		wantInput   int64
		wantOutput  int64
		wantBalance int64
	}
	expectations := []volumeExpectation{
		{"customer:carol", "EUR", 1000, 220, 780},
		{"customer:dave", "EUR", 500, 330, 170},
		{"merchant:alice", "EUR", 200, 0, 200},
		{"merchant:bob", "EUR", 300, 0, 300},
		{"platform:revenue", "EUR", 50, 0, 50},
	}

	getVolumeFromCache := func(account, asset string) (input, output int64) {
		key := domain.VolumeKey{
			AccountKey: domain.AccountKey{
				Ledger:  ledgerName,
				Account: account,
			},
			Asset: asset,
		}

		pair, _, err := machine.Volumes.Get(key.Bytes())
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("unexpected error reading cache volumes for %s: %v", account, err)
		}
		input = effectiveVolumeInput(pair)
		output = effectiveVolumeOutput(pair)

		return input, output
	}

	t.Run("cache volumes are correct", func(t *testing.T) {
		t.Parallel()

		for _, exp := range expectations {
			gotInput, gotOutput := getVolumeFromCache(exp.account, exp.asset)
			balance := gotInput - gotOutput

			require.Equal(t, exp.wantInput, gotInput,
				"cache input mismatch for %s/%s", exp.account, exp.asset)
			require.Equal(t, exp.wantOutput, gotOutput,
				"cache output mismatch for %s/%s", exp.account, exp.asset)
			require.Equal(t, exp.wantBalance, balance,
				"cache balance mismatch for %s/%s", exp.account, exp.asset)
		}
	})

	// ---------------------------------------------------------------
	// Verify volumes in the Pebble store (via attributes.ComputeValue)
	// ---------------------------------------------------------------
	t.Run("store volumes are correct", func(t *testing.T) {
		t.Parallel()

		lastIndex := uint64(4)

		for _, exp := range expectations {
			key := domain.VolumeKey{
				AccountKey: domain.AccountKey{
					Ledger:  ledgerName,
					Account: exp.account,
				},
				Asset: exp.asset,
			}
			canonicalKey := key.Bytes()

			pair, err := attrs.Volume.ComputeValue(dataStore, lastIndex, canonicalKey)
			require.NoError(t, err, "store volume read error for %s/%s", exp.account, exp.asset)

			var gotInput, gotOutput int64
			if pair != nil {
				if pair.InputKnown != nil {
					gotInput = pair.InputKnown.ToBigInt().Int64()
				}
				if pair.OutputKnown != nil {
					gotOutput = pair.OutputKnown.ToBigInt().Int64()
				}
			}

			balance := gotInput - gotOutput

			require.Equal(t, exp.wantInput, gotInput,
				"store input mismatch for %s/%s", exp.account, exp.asset)
			require.Equal(t, exp.wantOutput, gotOutput,
				"store output mismatch for %s/%s", exp.account, exp.asset)
			require.Equal(t, exp.wantBalance, balance,
				"store balance mismatch for %s/%s", exp.account, exp.asset)
		}
	})

	// ---------------------------------------------------------------
	// Dedicated assertions: no trace of failed batch 2
	// ---------------------------------------------------------------
	t.Run("failed batch leaves no trace for merchant:bob", func(t *testing.T) {
		t.Parallel()

		// Cache: merchant:bob should have input=300 (batch 3 only)
		gotInput, gotOutput := getVolumeFromCache("merchant:bob", "EUR")
		require.Equal(t, int64(300), gotInput,
			"merchant:bob cache input should be 300 (batch 3 only), not 600")
		require.Equal(t, int64(0), gotOutput,
			"merchant:bob cache output should be 0")

		// Store: same verification
		canonicalKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledgerName, Account: "merchant:bob"},
			Asset:      "EUR",
		}.Bytes()

		pair, err := attrs.Volume.ComputeValue(dataStore, 4, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, pair)
		require.NotNil(t, pair.InputKnown)
		require.Equal(t, int64(300), pair.InputKnown.ToBigInt().Int64(),
			"merchant:bob store input should be 300 (batch 3 only)")
	})

	t.Run("failed batch leaves no trace for customer:dave output", func(t *testing.T) {
		t.Parallel()

		// Cache: dave output should be 330 (batch 3 only), NOT 660
		_, gotOutput := getVolumeFromCache("customer:dave", "EUR")
		require.Equal(t, int64(330), gotOutput,
			"customer:dave cache output should be 330 (batch 3 only), not 660")

		// Store: same verification
		canonicalKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledgerName, Account: "customer:dave"},
			Asset:      "EUR",
		}.Bytes()

		pair, err := attrs.Volume.ComputeValue(dataStore, 4, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, pair)
		require.NotNil(t, pair.OutputKnown)
		require.Equal(t, int64(330), pair.OutputKnown.ToBigInt().Int64(),
			"customer:dave store output should be 330 (batch 3 only)")
	})

	t.Run("failed batch leaves no trace for platform:revenue", func(t *testing.T) {
		t.Parallel()

		// Cache: platform:revenue input should be 50 (20 batch1 + 30 batch3), NOT 80
		gotInput, _ := getVolumeFromCache("platform:revenue", "EUR")
		require.Equal(t, int64(50), gotInput,
			"platform:revenue cache input should be 50 (20+30), not 80")

		// Store: same verification
		canonicalKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledgerName, Account: "platform:revenue"},
			Asset:      "EUR",
		}.Bytes()

		pair, err := attrs.Volume.ComputeValue(dataStore, 4, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, pair)
		require.NotNil(t, pair.InputKnown)
		require.Equal(t, int64(50), pair.InputKnown.ToBigInt().Int64(),
			"platform:revenue store input should be 50 (20+30)")
	})
}

// attributeEntryInfo holds information about a single raw PebbleDB entry for an attribute key.
type attributeEntryInfo struct {
	RaftIndex uint64
	IsBase    bool // true = base (type 0), false = diff (type 1)
}

// listRawAttributeEntries returns all raw PebbleDB entries for a given attribute prefix and canonical key.
// This is used by tests to verify the physical state of the store after compaction.
func listRawAttributeEntries(t *testing.T, store *dal.Store, attrPrefix byte, canonicalKey []byte) []attributeEntryInfo {
	t.Helper()

	// Key layout: [0xF1][CanonicalKey][AttrType][RaftIndex 8B][EntryType 1B]
	// Build lower bound: [0xF1][CanonicalKey][AttrType]
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixAttributes).
		PutBytes(canonicalKey).
		PutByte(attrPrefix)
	lowerBound := kb.Snapshot()
	kb.PutByte(0xFF) // upper bound
	upperBound := kb.Build()

	iter, err := store.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	require.NoError(t, err)
	defer func() { _ = iter.Close() }()

	var entries []attributeEntryInfo
	for iter.First(); iter.Valid(); iter.Next() {
		iterKey := iter.Key()
		raftIndex := binary.BigEndian.Uint64(iterKey[len(iterKey)-9 : len(iterKey)-1])
		entryType := iterKey[len(iterKey)-1]
		entries = append(entries, attributeEntryInfo{
			RaftIndex: raftIndex,
			IsBase:    entryType == 0,
		})
	}
	return entries
}

// TestVolumeDiffCompactionAtGenerationRotation verifies that old volume diffs
// are pruned when compactVolumeDiffs is called.
//
// Compaction uses a prune-only strategy: it calls DeleteOldest to remove
// superseded diffs, but does NOT create a new base. This is safe because
// volume diffs are cumulative (each diff stores the total delta since the
// original base), so only the latest diff is needed by ComputeValue.
//
// This test directly exercises compactVolumeDiffs by:
//  1. Writing cumulative diffs to PebbleDB using the attribute layer
//  2. Calling compactVolumeDiffs at a given compaction index
//  3. Verifying that old diffs are removed and no base is created
//  4. Verifying that computed values remain correct after pruning
func TestVolumeDiffCompactionAtGenerationRotation(t *testing.T) {
	t.Parallel()

	machine, dataStore, attrs := newTestMachine(t)

	aliceInputKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test", Account: "users:alice"},
		Asset:      "EUR",
	}.Bytes()

	worldOutputKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test", Account: "world"},
		Asset:      "EUR",
	}.Bytes()

	// Write cumulative diffs at indexes 2,3,4,5: 100, 200, 300, 400
	// Each value represents the total delta since the implicit base (0).
	// For alice: input diffs (VolumePair with InputKnown set)
	for i, amount := range []int64{100, 200, 300, 400} {
		batch := dataStore.NewBatch()
		err := attrs.Volume.AddDiff(batch, uint64(i+2), aliceInputKey, &raftcmdpb.VolumePair{
			InputKnown: commonpb.NewUint256FromUint64(uint64(amount)),
		})
		require.NoError(t, err)
		require.NoError(t, batch.Commit())
	}

	// For world: output diffs (VolumePair with OutputKnown set)
	for i, amount := range []int64{100, 200, 300, 400} {
		batch := dataStore.NewBatch()
		err := attrs.Volume.AddDiff(batch, uint64(i+2), worldOutputKey, &raftcmdpb.VolumePair{
			OutputKnown: commonpb.NewUint256FromUint64(uint64(amount)),
		})
		require.NoError(t, err)
		require.NoError(t, batch.Commit())
	}

	// Verify initial state: 4 diff entries for each key
	inputEntries := listRawAttributeEntries(t, dataStore, dal.AttributePrefixVolume, aliceInputKey)
	require.Len(t, inputEntries, 4, "should have 4 diff entries initially")
	for _, e := range inputEntries {
		require.False(t, e.IsBase, "all initial entries should be diffs")
	}

	outputEntries := listRawAttributeEntries(t, dataStore, dal.AttributePrefixVolume, worldOutputKey)
	require.Len(t, outputEntries, 4, "should have 4 output diff entries initially")

	// Verify computed values before compaction: latest cumul diff = 400
	alicePair, err := attrs.Volume.ComputeValue(dataStore, 5, aliceInputKey)
	require.NoError(t, err)
	require.Equal(t, int64(400), alicePair.InputKnown.ToBigInt().Int64())

	worldPair, err := attrs.Volume.ComputeValue(dataStore, 5, worldOutputKey)
	require.NoError(t, err)
	require.Equal(t, int64(400), worldPair.OutputKnown.ToBigInt().Int64())

	// ---------------------------------------------------------------
	// First compaction at index 4: prunes diffs strictly before 4
	// Removes diffs at indexes 2 and 3; diffs at 4 and 5 remain.
	// ---------------------------------------------------------------
	dirtyKeys := map[string]uint32{
		string(aliceInputKey):  4,
		string(worldOutputKey): 4,
	}
	// Simulate newer entries existing (as after the dirty key shift in ApplyEntries).
	// slot[1] or slot[2] must contain the keys for compaction to proceed.
	machine.dirtyVolumeKeys[1] = map[string]uint32{
		string(aliceInputKey):  1,
		string(worldOutputKey): 1,
	}
	batch := dataStore.NewBatch()
	_, err = machine.compactVolumeDiffs(batch, 4, dirtyKeys)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	inputEntries = listRawAttributeEntries(t, dataStore, dal.AttributePrefixVolume, aliceInputKey)
	require.Len(t, inputEntries, 2, "should have 2 diffs remaining after compaction")
	for _, e := range inputEntries {
		require.False(t, e.IsBase, "prune-only compaction must not create bases")
	}
	require.Equal(t, uint64(4), inputEntries[0].RaftIndex)
	require.Equal(t, uint64(5), inputEntries[1].RaftIndex)

	outputEntries = listRawAttributeEntries(t, dataStore, dal.AttributePrefixVolume, worldOutputKey)
	require.Len(t, outputEntries, 2, "should have 2 output diffs remaining after compaction")

	// Computed values unchanged: latest cumul diff = 400
	alicePair, err = attrs.Volume.ComputeValue(dataStore, 5, aliceInputKey)
	require.NoError(t, err)
	require.Equal(t, int64(400), alicePair.InputKnown.ToBigInt().Int64(),
		"alice input should still be 400 after compaction")

	worldPair, err = attrs.Volume.ComputeValue(dataStore, 5, worldOutputKey)
	require.NoError(t, err)
	require.Equal(t, int64(400), worldPair.OutputKnown.ToBigInt().Int64(),
		"world output should still be 400 after compaction")

	// ---------------------------------------------------------------
	// Add more cumulative diffs at indexes 6(500) and 7(600)
	// Still cumulative from the implicit base 0.
	// ---------------------------------------------------------------
	for i, amount := range []int64{500, 600} {
		batch := dataStore.NewBatch()
		err := attrs.Volume.AddDiff(batch, uint64(i+6), aliceInputKey, &raftcmdpb.VolumePair{
			InputKnown: commonpb.NewUint256FromUint64(uint64(amount)),
		})
		require.NoError(t, err)
		require.NoError(t, batch.Commit())
	}

	// Latest cumul diff at index 7 = 600 -> computed value = 0 + 600 = 600
	alicePair, err = attrs.Volume.ComputeValue(dataStore, 7, aliceInputKey)
	require.NoError(t, err)
	require.Equal(t, int64(600), alicePair.InputKnown.ToBigInt().Int64(),
		"alice input should be 600 (latest cumulative diff from implicit base 0)")

	// ---------------------------------------------------------------
	// Second compaction at index 6: prunes entries < 6
	// Removes diffs at indexes 4 and 5; diffs at 6 and 7 remain.
	// ---------------------------------------------------------------
	batch = dataStore.NewBatch()
	_, err = machine.compactVolumeDiffs(batch, 6, dirtyKeys)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	inputEntries = listRawAttributeEntries(t, dataStore, dal.AttributePrefixVolume, aliceInputKey)
	require.Len(t, inputEntries, 2, "should have 2 diffs remaining after second compaction")
	for _, e := range inputEntries {
		require.False(t, e.IsBase, "prune-only compaction must not create bases")
	}
	require.Equal(t, uint64(6), inputEntries[0].RaftIndex)
	require.Equal(t, uint64(7), inputEntries[1].RaftIndex)

	// Computed value unchanged: latest cumul diff at index 7 = 600
	alicePair, err = attrs.Volume.ComputeValue(dataStore, 7, aliceInputKey)
	require.NoError(t, err)
	require.Equal(t, int64(600), alicePair.InputKnown.ToBigInt().Int64(),
		"alice input should remain 600 after second compaction")
}

// TestVolumeDiffCompactionSkipsInactiveKeys verifies that compaction is skipped
// for keys that have no entries in a more recent generation. This prevents
// DeleteOldest from removing the only remaining Pebble entries (including bases)
// for accounts that were active in the compacted generation but dormant since.
func TestVolumeDiffCompactionSkipsInactiveKeys(t *testing.T) {
	t.Parallel()

	machine, dataStore, attrs := newTestMachine(t)

	activeKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test", Account: "users:active"},
		Asset:      "EUR",
	}.Bytes()

	dormantKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test", Account: "users:dormant"},
		Asset:      "EUR",
	}.Bytes()

	// Write 4 diffs for both keys at indexes 2-5
	for i, amount := range []int64{100, 200, 300, 400} {
		batch := dataStore.NewBatch()
		err := attrs.Volume.AddDiff(batch, uint64(i+2), activeKey, &raftcmdpb.VolumePair{
			InputKnown: commonpb.NewUint256FromUint64(uint64(amount)),
		})
		require.NoError(t, err)
		err = attrs.Volume.AddDiff(batch, uint64(i+2), dormantKey, &raftcmdpb.VolumePair{
			InputKnown: commonpb.NewUint256FromUint64(uint64(amount)),
		})
		require.NoError(t, err)
		require.NoError(t, batch.Commit())
	}

	// Both keys are in the generation being compacted
	dirtyKeys := map[string]uint32{
		string(activeKey):  4,
		string(dormantKey): 4,
	}

	// Only the active key has entries in a newer generation (slot[1] after shift)
	machine.dirtyVolumeKeys[1] = map[string]uint32{
		string(activeKey): 1,
	}
	// dormantKey is NOT in slot[1] or slot[2] → should be skipped

	batch := dataStore.NewBatch()
	_, err := machine.compactVolumeDiffs(batch, 4, dirtyKeys)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Active key: compacted (old diffs removed)
	activeEntries := listRawAttributeEntries(t, dataStore, dal.AttributePrefixVolume, activeKey)
	require.Len(t, activeEntries, 2, "active key should have 2 diffs remaining after compaction")
	require.Equal(t, uint64(4), activeEntries[0].RaftIndex)
	require.Equal(t, uint64(5), activeEntries[1].RaftIndex)

	// Dormant key: NOT compacted (all 4 diffs preserved)
	dormantEntries := listRawAttributeEntries(t, dataStore, dal.AttributePrefixVolume, dormantKey)
	require.Len(t, dormantEntries, 4, "dormant key should keep all 4 diffs (compaction skipped)")

	// Both computed values are still correct
	activePair, err := attrs.Volume.ComputeValue(dataStore, 5, activeKey)
	require.NoError(t, err)
	require.Equal(t, int64(400), activePair.InputKnown.ToBigInt().Int64())

	dormantPair, err := attrs.Volume.ComputeValue(dataStore, 5, dormantKey)
	require.NoError(t, err)
	require.Equal(t, int64(400), dormantPair.InputKnown.ToBigInt().Int64())
}

// makeLedgerPreloadSet creates a PreloadSet that injects ledger info into the cache.
// lastPersistedIndex should be machine.Cache.BaseIndex.Gen0 at the time of creation.
// This mimics what the admission layer does for entries where the ledger info
// may have been evicted from cache due to generation rotation.
func makeLedgerPreloadSet(lastPersistedIndex uint64, ledgerName string, ledgerInfo *commonpb.LedgerInfo) *raftcmdpb.PreloadSet {
	hasher := attributes.NewKeyHasher(attributes.DefaultSeeds)
	id, tag := hasher.MakeKey(domain.LedgerKey{Name: ledgerName}.Bytes())

	return &raftcmdpb.PreloadSet{
		LastPersistedIndex: lastPersistedIndex,
		Preloads: []*raftcmdpb.Preload{
			{
				Type: &raftcmdpb.Preload_Ledger{
					Ledger: &raftcmdpb.PreloadLedger{
						Id: &raftcmdpb.AttributeID{
							Id:  id[:],
							Tag: tag,
						},
						Info: ledgerInfo,
					},
				},
			},
		},
	}
}

// TestVolumeDiffCompactionIntegration verifies that compaction is triggered
// automatically during generation rotation in the full ApplyEntries pipeline.
//
// With K=10 and entries 1-42:
//   - Rotation 1 (entry 12): compactVolumeDiffs(0) -- no-op
//   - Rotation 2 (entry 22): compactVolumeDiffs(0) -- no-op
//   - Rotation 3 (entry 32): compactVolumeDiffs(10) -- prunes diffs at indexes 2-9
//   - Rotation 4 (entry 42): compactVolumeDiffs(20) -- prunes diffs at indexes 10-19
//
// After all processing: 41 diffs initially, 18 pruned = 23 remaining.
// Ledger info is injected via preloads after cache eviction (mimics admission layer).
func TestVolumeDiffCompactionIntegration(t *testing.T) {
	t.Parallel()

	const generationThreshold = 10
	machine, dataStore, attrs := newTestMachineWithThreshold(t, generationThreshold)
	ctx := context.Background()

	const ledgerName = "integration-test"

	// Index 1: create the ledger
	result, err := machine.ApplyEntries(ctx,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error)

	// Capture ledger info from cache for preloads
	ledgerInfo, _, err := machine.Ledgers.Get(domain.LedgerKey{Name: ledgerName}.Bytes())
	require.NoError(t, err)

	aliceVolumeKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: ledgerName, Account: "users:alice"},
		Asset:      "EUR",
	}.Bytes()

	// Apply 41 transactions at indexes 2-42 (triggers 4 rotations).
	// Each proposal includes a ledger preload so that the ledger info
	// is available even after cache eviction (same as the real admission layer).
	for i := uint64(2); i <= 42; i++ {
		proposal := makeProposal(i,
			createTransactionOrder(ledgerName, true,
				newPosting("world", "users:alice", "EUR", 100),
			),
		)
		proposal.Preload = makeLedgerPreloadSet(
			machine.Cache.BaseIndex.Gen0,
			ledgerName,
			ledgerInfo,
		)

		result, err = machine.ApplyEntries(ctx, makeEntry(t, i, proposal))
		require.NoError(t, err)
		require.Len(t, result.Results, 1)
		require.NoError(t, result.Results[0].Error, "entry %d should succeed", i)
	}

	// Verify the final computed value: 41 transactions * 100 = 4100
	pair, err := attrs.Volume.ComputeValue(dataStore, 42, aliceVolumeKey)
	require.NoError(t, err)
	require.NotNil(t, pair)
	require.NotNil(t, pair.InputKnown)
	require.Equal(t, int64(4100), pair.InputKnown.ToBigInt().Int64(),
		"users:alice input should be 4100 (41 * 100)")

	// Verify that compaction pruned old entries.
	// 41 diffs initially, 18 pruned (8 at rotation 3 + 10 at rotation 4) = 23 remaining.
	entries := listRawAttributeEntries(t, dataStore, dal.AttributePrefixVolume, aliceVolumeKey)
	require.Equal(t, 23, len(entries),
		"compaction should have pruned old diffs, leaving 23 entries (diffs at indexes 20-42)")

	// All remaining entries should be diffs (prune-only compaction creates no bases)
	for _, e := range entries {
		require.False(t, e.IsBase, "prune-only compaction must not create base entries")
	}

	// Verify the range of remaining entries
	require.Equal(t, uint64(20), entries[0].RaftIndex,
		"first remaining diff should be at index 20")
	require.Equal(t, uint64(42), entries[len(entries)-1].RaftIndex,
		"last remaining diff should be at index 42")
}
