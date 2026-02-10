package state

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/cache"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"
)

// newTestMachine creates a Machine backed by a real Pebble store for testing.
func newTestMachine(t *testing.T) (*Machine, *data.Store, *attributes.Attributes) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dataStore, err := data.NewStore(t.TempDir(), logger, meter, data.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	attrs := attributes.New()

	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	machine, err := NewMachine(logger, dataStore, meter, c, attrs, 1000)
	require.NoError(t, err)

	return machine, dataStore, attrs
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
		Amount:      commonpb.NewBigInt(big.NewInt(amount)),
		Asset:       asset,
	}
}

// effectiveVolume extracts the effective value from a VolumeHolder.
// Known takes precedence (includes preloaded base + diffs), otherwise DiffSinceBaseIndex.
func effectiveVolume(vh *raftcmdpb.VolumeHolder) int64 {
	if vh == nil {
		return 0
	}
	if vh.Known != nil {
		return vh.Known.Value().Int64()
	}
	if vh.DiffSinceBaseIndex != nil {
		return vh.DiffSinceBaseIndex.Value().Int64()
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
	results, err := machine.ApplyEntries(ctx,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.NoError(t, results[0].Error)

	// The ledger was assigned ID=1
	const ledgerID = uint32(1)

	// ---------------------------------------------------------------
	// Entry 2 - Batch 1 (SUCCESS): seed funds + first sales
	// All transactions use force=true (bypasses balance checks).
	// ---------------------------------------------------------------
	results, err = machine.ApplyEntries(ctx,
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
	require.Len(t, results, 1)
	require.NoError(t, results[0].Error, "batch 1 should succeed")

	// ---------------------------------------------------------------
	// Entry 3 - Batch 2 (FAILURE): valid transactions + bad revert
	// The first two orders succeed in the buffer but the revert of
	// a non-existent transaction causes the entire proposal to fail.
	// ---------------------------------------------------------------
	results, err = machine.ApplyEntries(ctx,
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
	require.Len(t, results, 1)
	require.Error(t, results[0].Error, "batch 2 should fail")
	require.Contains(t, results[0].Error.Error(), "does not exist")

	// ---------------------------------------------------------------
	// Entry 4 - Batch 3 (SUCCESS): Dave makes valid purchases
	// ---------------------------------------------------------------
	results, err = machine.ApplyEntries(ctx,
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
	require.Len(t, results, 1)
	require.NoError(t, results[0].Error, "batch 3 should succeed")

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
		key := data.VolumeKey{
			AccountKey: data.AccountKey{
				LedgerID: ledgerID,
				Account:  account,
			},
			Asset: asset,
		}

		inputHolder, _, err := machine.Input.Get(key.Bytes())
		if err != nil && !errors.Is(err, data.ErrNotFound) {
			t.Fatalf("unexpected error reading cache input for %s: %v", account, err)
		}
		input = effectiveVolume(inputHolder)

		outputHolder, _, err := machine.Output.Get(key.Bytes())
		if err != nil && !errors.Is(err, data.ErrNotFound) {
			t.Fatalf("unexpected error reading cache output for %s: %v", account, err)
		}
		output = effectiveVolume(outputHolder)

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
			key := data.VolumeKey{
				AccountKey: data.AccountKey{
					LedgerID: ledgerID,
					Account:  exp.account,
				},
				Asset: exp.asset,
			}
			canonicalKey := key.Bytes()

			inputVal, err := attrs.Input.ComputeValue(dataStore, lastIndex, canonicalKey)
			require.NoError(t, err, "store input read error for %s/%s", exp.account, exp.asset)

			var gotInput int64
			if inputVal != nil {
				gotInput = inputVal.Value().Int64()
			}

			outputVal, err := attrs.Output.ComputeValue(dataStore, lastIndex, canonicalKey)
			require.NoError(t, err, "store output read error for %s/%s", exp.account, exp.asset)

			var gotOutput int64
			if outputVal != nil {
				gotOutput = outputVal.Value().Int64()
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
		canonicalKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "merchant:bob"},
			Asset:      "EUR",
		}.Bytes()

		inputVal, err := attrs.Input.ComputeValue(dataStore, 4, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, inputVal)
		require.Equal(t, int64(300), inputVal.Value().Int64(),
			"merchant:bob store input should be 300 (batch 3 only)")
	})

	t.Run("failed batch leaves no trace for customer:dave output", func(t *testing.T) {
		t.Parallel()

		// Cache: dave output should be 330 (batch 3 only), NOT 660
		_, gotOutput := getVolumeFromCache("customer:dave", "EUR")
		require.Equal(t, int64(330), gotOutput,
			"customer:dave cache output should be 330 (batch 3 only), not 660")

		// Store: same verification
		canonicalKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "customer:dave"},
			Asset:      "EUR",
		}.Bytes()

		outputVal, err := attrs.Output.ComputeValue(dataStore, 4, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, outputVal)
		require.Equal(t, int64(330), outputVal.Value().Int64(),
			"customer:dave store output should be 330 (batch 3 only)")
	})

	t.Run("failed batch leaves no trace for platform:revenue", func(t *testing.T) {
		t.Parallel()

		// Cache: platform:revenue input should be 50 (20 batch1 + 30 batch3), NOT 80
		gotInput, _ := getVolumeFromCache("platform:revenue", "EUR")
		require.Equal(t, int64(50), gotInput,
			"platform:revenue cache input should be 50 (20+30), not 80")

		// Store: same verification
		canonicalKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "platform:revenue"},
			Asset:      "EUR",
		}.Bytes()

		inputVal, err := attrs.Input.ComputeValue(dataStore, 4, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, inputVal)
		require.Equal(t, int64(50), inputVal.Value().Int64(),
			"platform:revenue store input should be 50 (20+30)")
	})
}
