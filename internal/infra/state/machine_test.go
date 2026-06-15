package state

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
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

	registry := NewStateRegistry(c, attrs, 0)
	snapshotter := NewCacheSnapshotter(logger, registry, nil)

	machine, err := NewMachine(logger, registry, snapshotter, dataStore, dal.NewSentinelFactory(dataStore, false), meter, keystore.NewKeyStore(), NewSharedState(), noopNotifier{}, nil, "test-cluster", 0)
	require.NoError(t, err)

	// NewMachine no longer performs the initial recoverState — the caller is
	// expected to wire a Recovery and invoke it. Tests share that contract.
	require.NoError(t, NewRecovery(machine, dataStore).RecoverState())

	return machine, dataStore, attrs
}

// newTestMachine creates a Machine backed by a real Pebble store for testing.
func newTestMachine(t *testing.T) (*Machine, *dal.Store, *attributes.Attributes) {
	t.Helper()

	return newTestMachineWithThreshold(t, 1000)
}

// makeProposal builds a Proposal protobuf with the given orders.
// It automatically generates volume preloads with zero values for all
// accounts referenced in postings (simulating what the admission layer does).
func makeProposal(id uint64, orders ...*raftcmdpb.Order) *raftcmdpb.Proposal {
	// The FSM assigns ledger IDs starting from 1. In single-ledger tests,
	// the first ledger always gets ID 1.
	preloads := buildVolumePreloads(orders, 1)

	return &raftcmdpb.Proposal{
		Id:      id,
		Orders:  orders,
		Date:    &commonpb.Timestamp{Data: 1700000000 + id},
		Preload: &raftcmdpb.PreloadSet{Preloads: preloads},
	}
}

// buildVolumePreloads extracts all (ledger, account, asset) tuples from posting
// orders and creates zero-value volume preloads for each unique combination.
func buildVolumePreloads(orders []*raftcmdpb.Order, ledgerID uint32) []*raftcmdpb.Preload {
	type volumeKey struct {
		ledger  string
		account string
		asset   string
	}
	seen := make(map[volumeKey]struct{})
	var preloads []*raftcmdpb.Preload

	zero := commonpb.NewUint256FromUint64(0)

	for _, order := range orders {
		apply := order.GetApply()
		if apply == nil {
			continue
		}
		ledger := apply.GetLedger()

		var postings []*commonpb.Posting
		if ct := apply.GetCreateTransaction(); ct != nil {
			postings = ct.GetPostings()
		}
		if rt := apply.GetRevertTransaction(); rt != nil {
			// Revert doesn't have postings in the order; they're resolved at apply time.
			continue
		}

		for _, p := range postings {
			for _, account := range []string{p.GetSource(), p.GetDestination()} {
				key := volumeKey{ledger: ledger, account: account, asset: p.GetAsset()}
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}

				canonicalKey := domain.VolumeKey{
					AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: account},
					Asset:      p.GetAsset(),
				}
				id, tag := attributes.MakeKey(canonicalKey.Bytes())

				preloads = append(preloads, &raftcmdpb.Preload{
					Type: &raftcmdpb.Preload_Volume{
						Volume: &raftcmdpb.PreloadVolume{
							Id:    &raftcmdpb.AttributeID{Id: id[:], Tag: tag},
							Value: &raftcmdpb.VolumePair{Input: zero, Output: zero},
						},
					},
				})
			}
		}
	}

	return preloads
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
func effectiveVolumeInput(vp *raftcmdpb.VolumePair) int64 {
	if vp == nil {
		return 0
	}

	if vp.GetInput() != nil {
		return vp.GetInput().ToBigInt().Int64()
	}

	return 0
}

// effectiveVolumeOutput extracts the effective output value from a VolumePair.
func effectiveVolumeOutput(vp *raftcmdpb.VolumePair) int64 {
	if vp == nil {
		return 0
	}

	if vp.GetOutput() != nil {
		return vp.GetOutput().ToBigInt().Int64()
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
	result, err := machine.ApplyEntries(ctx, dataStore,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error)

	// ---------------------------------------------------------------
	// Entry 2 - Batch 1 (SUCCESS): seed funds + first sales
	// All transactions use force=true (bypasses balance checks).
	// ---------------------------------------------------------------
	result, err = machine.ApplyEntries(ctx, dataStore,
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
	result, err = machine.ApplyEntries(ctx, dataStore,
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
	result, err = machine.ApplyEntries(ctx, dataStore,
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
				LedgerID: 1,
				Account:  account,
			},
			Asset: asset,
		}

		pair, _, err := machine.Registry.Volumes.Get(key.Bytes())
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
	// Verify volumes in the Pebble store (via attributes.Get)
	// ---------------------------------------------------------------
	t.Run("store volumes are correct", func(t *testing.T) {
		t.Parallel()

		for _, exp := range expectations {
			key := domain.VolumeKey{
				AccountKey: domain.AccountKey{
					LedgerID: 1,
					Account:  exp.account,
				},
				Asset: exp.asset,
			}
			canonicalKey := key.Bytes()

			pair, err := attrs.Volume.Get(dataStore, canonicalKey)
			require.NoError(t, err, "store volume read error for %s/%s", exp.account, exp.asset)

			var gotInput, gotOutput int64

			if pair != nil {
				if pair.GetInput() != nil {
					gotInput = pair.GetInput().ToBigInt().Int64()
				}

				if pair.GetOutput() != nil {
					gotOutput = pair.GetOutput().ToBigInt().Int64()
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
			AccountKey: domain.AccountKey{LedgerID: 1, Account: "merchant:bob"},
			Asset:      "EUR",
		}.Bytes()

		pair, err := attrs.Volume.Get(dataStore, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, pair)
		require.NotNil(t, pair.GetInput())
		require.Equal(t, int64(300), pair.GetInput().ToBigInt().Int64(),
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
			AccountKey: domain.AccountKey{LedgerID: 1, Account: "customer:dave"},
			Asset:      "EUR",
		}.Bytes()

		pair, err := attrs.Volume.Get(dataStore, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, pair)
		require.NotNil(t, pair.GetOutput())
		require.Equal(t, int64(330), pair.GetOutput().ToBigInt().Int64(),
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
			AccountKey: domain.AccountKey{LedgerID: 1, Account: "platform:revenue"},
			Asset:      "EUR",
		}.Bytes()

		pair, err := attrs.Volume.Get(dataStore, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, pair)
		require.NotNil(t, pair.GetInput())
		require.Equal(t, int64(50), pair.GetInput().ToBigInt().Int64(),
			"platform:revenue store input should be 50 (20+30)")
	})
}

// TestNextLedgerIDRecovery verifies that the nextLedgerID counter is correctly
// persisted to Pebble and recovered when a new Machine is created on the same store.
func TestNextLedgerIDRecovery(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dataStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	attrs := attributes.New()

	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	// Create first machine and 3 ledgers.
	reg1 := NewStateRegistry(c, attrs, 0)
	snap1 := NewCacheSnapshotter(logger, reg1, nil)
	machine1, err := NewMachine(logger, reg1, snap1, dataStore, dal.NewSentinelFactory(dataStore, false), meter, keystore.NewKeyStore(), NewSharedState(), noopNotifier{}, nil, "test-cluster", 0)
	require.NoError(t, err)
	require.NoError(t, NewRecovery(machine1, dataStore).RecoverState())

	result, err := machine1.ApplyEntries(ctx, dataStore,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder("ledger-a"))),
	)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error)

	result, err = machine1.ApplyEntries(ctx, dataStore,
		makeEntry(t, 2, makeProposal(2, createLedgerOrder("ledger-b"))),
	)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error)

	result, err = machine1.ApplyEntries(ctx, dataStore,
		makeEntry(t, 3, makeProposal(3, createLedgerOrder("ledger-c"))),
	)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error)

	// Verify nextLedgerID is 4 (1, 2, 3 assigned, next is 4).
	require.Equal(t, uint32(4), machine1.nextLedgerID)

	// Simulate restart: create a new Machine on the same Pebble store.
	c2, err := cache.New(1000, meter)
	require.NoError(t, err)

	attrs2 := attributes.New()

	reg2 := NewStateRegistry(c2, attrs2, 0)
	snap2 := NewCacheSnapshotter(logger, reg2, nil)
	machine2, err := NewMachine(logger, reg2, snap2, dataStore, dal.NewSentinelFactory(dataStore, false), meter, keystore.NewKeyStore(), NewSharedState(), noopNotifier{}, nil, "test-cluster", 0)
	require.NoError(t, err)
	require.NoError(t, NewRecovery(machine2, dataStore).RecoverState())

	// The recovered machine should have nextLedgerID = 4.
	require.Equal(t, uint32(4), machine2.nextLedgerID)

	// Create another ledger — it should get ID=4.
	result, err = machine2.ApplyEntries(ctx, dataStore,
		makeEntry(t, 4, makeProposal(4, createLedgerOrder("ledger-d"))),
	)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error)

	// Verify the new ledger got ID=4 by checking the log.
	require.Len(t, result.Results[0].Logs, 1)
	createLog := result.Results[0].Logs[0].GetCreatedLog()
	require.NotNil(t, createLog)
	createLedgerLog := createLog.GetPayload().GetCreateLedger()
	require.NotNil(t, createLedgerLog)
	require.Equal(t, uint32(4), createLedgerLog.GetId())

	// And nextLedgerID should now be 5.
	require.Equal(t, uint32(5), machine2.nextLedgerID)
}
