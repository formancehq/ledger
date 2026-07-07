package state

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/sirupsen/logrus"
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
	meterProvider := noop.NewMeterProvider()
	meter := meterProvider.Meter("test")

	dataStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	attrs := attributes.New()

	c, err := cache.New(generationThreshold, meter)
	require.NoError(t, err)

	registry := NewStateRegistry(c, attrs, 0)
	snapshotter := NewCacheSnapshotter(logger, registry, nil)

	machine, err := NewMachine(logger, registry, snapshotter, dataStore, dal.NewSentinelFactory(dataStore, false), meterProvider, keystore.NewKeyStore(), NewSharedState(), newNoopNotifier(t), nil, "test-cluster", 0, noopConfChangeHandler)
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
// It automatically generates a ExecutionPlan that declares every key the FSM
// will read during apply (simulating what the admission layer does):
//   - Volumes preloaded with zero values for each posting source/destination.
//   - Ledgers / Boundaries / Transactions / Metadata declared (no value
//     payload) so the Plan admits reads on them.
func makeProposal(id uint64, orders ...*raftcmdpb.Order) *raftcmdpb.Proposal {
	return &raftcmdpb.Proposal{
		Id:     id,
		Orders: orders,
		Date:   1700000000 + id,
		ExecutionPlan: &raftcmdpb.ExecutionPlan{
			Attributes: append(buildVolumePreloads(orders), buildOrderDeclarations(orders)...),
		},
	}
}

// sealProposal stamps every order's CoverageBits to flag every
// AttributeCoverage in the proposal's ExecutionPlan. This is the
// "admit-all per order" shortcut tests rely on: production goes
// through plan.Builder.Run which derives per-order bits from each
// order's declared Coverage via plan.bitsForNeeds. Rebuilding that path
// here would require pulling admission.extractPreloadNeeds (and its
// 200+ lines of order-type switch) into a shared package. Tracked
// for a follow-up. None of the existing tests exercise per-order
// isolation, so the shortcut is acceptable.
func sealProposal(p *raftcmdpb.Proposal) *raftcmdpb.Proposal {
	plans := p.GetExecutionPlan().GetAttributes()
	if len(plans) == 0 {
		return p
	}

	bits := make([]byte, (len(plans)+7)/8)
	for i := range plans {
		bits[i/8] |= 1 << (i % 8)
	}

	for _, order := range p.GetOrders() {
		order.CoverageBits = bits
	}

	for _, tu := range p.GetTechnicalUpdates() {
		tu.CoverageBits = bits
	}

	return p
}

// declareTestPlan builds a coverage-only AttributeCoverage over an
// attribute (code, U128) pair. Used by tests that simulate what
// admission's resolve layer emits when a key is already CacheHit.
func declareTestPlan(id attributes.U128, attrCode byte) *raftcmdpb.AttributeCoverage {
	return &raftcmdpb.AttributeCoverage{
		Id:       &raftcmdpb.AttributeID{Id: id[:]},
		AttrCode: uint32(attrCode),
	}
}

// preloadTestPlan wraps an AttributeValue payload into a seeded
// AttributeCoverage. attrID carries the canonical U128 + xxh3 collision
// tag; attrCode (dal.SubAttrXxx) drives the unmarshal dispatch.
func preloadTestPlan(attrID *raftcmdpb.AttributeID, attrCode byte, value *raftcmdpb.AttributeValue) *raftcmdpb.AttributeCoverage {
	return &raftcmdpb.AttributeCoverage{
		Id:       attrID,
		AttrCode: uint32(attrCode),
		Value:    value,
	}
}

// rawPreload marshals value into a fresh Preload{attr_code, raw_value}
// using vtproto's encoding. Tests use this to assemble ExecutionPlan entries
// without re-typing the (now wire-erased) variant scaffolding every time.
// Marshal errors on a fresh proto indicate the test fixture itself is
// broken — panic rather than threading t.Helper through every helper.
func rawPreload[V interface {
	MarshalVT() ([]byte, error)
}](_ *testing.T, attrCode byte, value V) *raftcmdpb.AttributeValue {
	return rawPreloadNoT(attrCode, value)
}

func rawPreloadNoT[V interface {
	MarshalVT() ([]byte, error)
}](attrCode byte, value V) *raftcmdpb.AttributeValue {
	raw, err := value.MarshalVT()
	if err != nil {
		panic(fmt.Errorf("rawPreload: marshal attrCode=0x%x: %w", attrCode, err))
	}

	_ = attrCode // attr_code lives on the parent AttributeCoverage now

	return &raftcmdpb.AttributeValue{
		RawValue: raw,
	}
}

// buildOrderDeclarations emits Declare-intent AttributeCoverages for the
// side-channel keys the FSM reads while applying orders but that
// buildVolumePreloads does not cover: the ledger itself, its boundaries,
// target transactions for reverts, and metadata keys.
//
// The ledger name comes from the order itself (apply.GetLedger()), matching
// the LedgerName-based canonical key layout introduced by #469.
func buildOrderDeclarations(orders []*raftcmdpb.Order) []*raftcmdpb.AttributeCoverage {
	type acctMetaKey struct {
		ledgerName string
		account    string
		key        string
	}

	ledgers := map[string]struct{}{}
	type txKeyT struct {
		ledgerName string
		id         uint64
	}
	txs := map[txKeyT]struct{}{}
	accMeta := map[acctMetaKey]struct{}{}

	for _, order := range orders {
		ls := order.GetLedgerScoped()
		if ls == nil {
			continue
		}

		// Non-apply ledger-scoped orders also touch the cache (CreateLedger
		// reads the LedgerKey to confirm the slot is empty before writing).
		switch ls.GetPayload().(type) {
		case *raftcmdpb.LedgerScopedOrder_CreateLedger,
			*raftcmdpb.LedgerScopedOrder_DeleteLedger,
			*raftcmdpb.LedgerScopedOrder_PromoteLedger,
			*raftcmdpb.LedgerScopedOrder_MirrorIngest:
			ledgers[ls.GetLedger()] = struct{}{}
		}

		apply := ls.GetApply()
		if apply == nil {
			continue
		}

		ledgerName := ls.GetLedger()
		if ledgerName != "" {
			ledgers[ledgerName] = struct{}{}
		}

		if rt := apply.GetRevertTransaction(); rt != nil {
			txs[txKeyT{ledgerName: ledgerName, id: rt.GetTransactionId()}] = struct{}{}
		}

		if sm := apply.GetAddMetadata(); sm != nil {
			if acc := sm.GetTarget().GetAccount(); acc != nil {
				for key := range sm.GetMetadata() {
					accMeta[acctMetaKey{ledgerName: ledgerName, account: acc.GetAddr(), key: key}] = struct{}{}
				}
			}
		}

		if dm := apply.GetDeleteMetadata(); dm != nil {
			if acc := dm.GetTarget().GetAccount(); acc != nil {
				accMeta[acctMetaKey{ledgerName: ledgerName, account: acc.GetAddr(), key: dm.GetKey()}] = struct{}{}
			}
		}
	}

	var declared []*raftcmdpb.AttributeCoverage

	for name := range ledgers {
		ledgerKeyID, _ := attributes.MakeKey(domain.LedgerKey{Name: name}.Bytes())
		declared = append(declared,
			declareTestPlan(ledgerKeyID, dal.SubAttrLedger),
			declareTestPlan(ledgerKeyID, dal.SubAttrBoundary),
		)
	}

	for tk := range txs {
		txKey := domain.TransactionKey{LedgerName: tk.ledgerName, ID: tk.id}
		txID, _ := attributes.MakeKey(txKey.Bytes())
		declared = append(declared, declareTestPlan(txID, dal.SubAttrTransaction))
	}

	for k := range accMeta {
		mkBytes := domain.MetadataKey{
			AccountKey: domain.AccountKey{LedgerName: k.ledgerName, Account: k.account},
			Key:        k.key,
		}.Bytes()
		mkID, _ := attributes.MakeKey(mkBytes)
		declared = append(declared, declareTestPlan(mkID, dal.SubAttrMetadata))
	}

	return declared
}

// buildVolumePreloads extracts all (ledger, account, asset) tuples from posting
// orders and creates zero-value volume preloads for each unique combination.
// The second parameter is the default ledger name for orders that do not carry
// one (none in practice today, but kept for callers passing a literal sentinel).
func buildVolumePreloads(orders []*raftcmdpb.Order) []*raftcmdpb.AttributeCoverage {
	type volumeKey struct {
		ledger  string
		account string
		asset   string
	}
	seen := make(map[volumeKey]struct{})
	var plans []*raftcmdpb.AttributeCoverage

	zero := commonpb.NewUint256FromUint64(0)

	for _, order := range orders {
		ls := order.GetLedgerScoped()
		if ls == nil {
			continue
		}

		apply := ls.GetApply()
		if apply == nil {
			continue
		}
		ledger := ls.GetLedger()

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
					AccountKey: domain.AccountKey{LedgerName: ledger, Account: account},
					Asset:      p.GetAsset(),
				}
				id, tag := attributes.MakeKey(canonicalKey.Bytes())

				attrID := &raftcmdpb.AttributeID{Id: id[:], Tag: tag}
				plans = append(plans, preloadTestPlan(attrID, dal.SubAttrVolume,
					rawPreloadNoT(dal.SubAttrVolume, &raftcmdpb.VolumePair{Input: zero, Output: zero})))
			}
		}
	}

	return plans
}

// makeEntry marshals a proposal into a raft entry at the given index.
// sealProposal is applied first so every order's CoverageBits flags
// every AttributeCoverage in the proposal, matching what the admission
// runner does for production proposals — tests built inline would
// otherwise hit *ErrCoverageMiss on the first cache read.
func makeEntry(t *testing.T, index uint64, proposal *raftcmdpb.Proposal) raftpb.Entry {
	t.Helper()

	sealProposal(proposal)

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
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: name,
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{},
				},
			},
		},
	}
}

func createTransactionOrder(ledger string, force bool, postings ...*commonpb.Posting) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
						CreateTransaction: &raftcmdpb.CreateTransactionOrder{
							Postings: postings,
							Force:    force,
						},
					},
					},
				},
			},
		},
	}
}

func revertTransactionOrder(ledger string, txID uint64) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
						RevertTransaction: &raftcmdpb.RevertTransactionOrder{
							TransactionId: txID,
						},
					},
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
				LedgerName: ledgerName,
				Account:    account,
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
					LedgerName: ledgerName,
					Account:    exp.account,
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
			AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: "merchant:bob"},
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
			AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: "customer:dave"},
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
			AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: "platform:revenue"},
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
	meterProvider := noop.NewMeterProvider()
	meter := meterProvider.Meter("test")

	dataStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	attrs := attributes.New()

	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	// Create first machine and 3 ledgers.
	reg1 := NewStateRegistry(c, attrs, 0)
	snap1 := NewCacheSnapshotter(logger, reg1, nil)
	machine1, err := NewMachine(logger, reg1, snap1, dataStore, dal.NewSentinelFactory(dataStore, false), meterProvider, keystore.NewKeyStore(), NewSharedState(), newNoopNotifier(t), nil, "test-cluster", 0, noopConfChangeHandler)
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
	require.Equal(t, uint32(4), machine1.State.NextLedgerID)

	// Simulate restart: create a new Machine on the same Pebble store.
	c2, err := cache.New(1000, meter)
	require.NoError(t, err)

	attrs2 := attributes.New()

	reg2 := NewStateRegistry(c2, attrs2, 0)
	snap2 := NewCacheSnapshotter(logger, reg2, nil)
	machine2, err := NewMachine(logger, reg2, snap2, dataStore, dal.NewSentinelFactory(dataStore, false), meterProvider, keystore.NewKeyStore(), NewSharedState(), newNoopNotifier(t), nil, "test-cluster", 0, noopConfChangeHandler)
	require.NoError(t, err)
	require.NoError(t, NewRecovery(machine2, dataStore).RecoverState())

	// The recovered machine should have nextLedgerID = 4.
	require.Equal(t, uint32(4), machine2.State.NextLedgerID)

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
	require.Equal(t, uint32(5), machine2.State.NextLedgerID)
}

// TestPrepareEntriesTraceLogPipeliningLag is a regression test for issue #427:
// PrepareEntries used to read entries[0].Index unconditionally inside a
// trace-level log when lastPersistedIndex lagged lastAppliedIndex. Calling
// it with an empty entries slice (e.g. the empty `head` slice produced by
// applyEntriesToFSM when a batch boundary lands on the first entry) and
// trace logging enabled would panic with index out of range. The non-empty
// sub-test exercises the trace-log code path itself to keep coverage on the
// guarded block.
func TestPrepareEntriesTraceLogPipeliningLag(t *testing.T) {
	t.Parallel()

	newMachineWithTraceLogger := func(t *testing.T) (*Machine, *dal.Store) {
		t.Helper()

		logrusLogger := logrus.New()
		logrusLogger.SetOutput(io.Discard)
		logrusLogger.SetLevel(logrus.TraceLevel)
		logger := logging.NewLogrus(logrusLogger)
		require.True(t, logger.Enabled(logging.TraceLevel))

		meterProvider := noop.NewMeterProvider()
		meter := meterProvider.Meter("test")

		dataStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = dataStore.Close() })

		c, err := cache.New(1000, meter)
		require.NoError(t, err)

		registry := NewStateRegistry(c, attributes.New(), 0)
		snapshotter := NewCacheSnapshotter(logger, registry, nil)

		machine, err := NewMachine(logger, registry, snapshotter, dataStore, dal.NewSentinelFactory(dataStore, false), meterProvider, keystore.NewKeyStore(), NewSharedState(), newNoopNotifier(t), nil, "test-cluster", 0, noopConfChangeHandler)
		require.NoError(t, err)
		require.NoError(t, NewRecovery(machine, dataStore).RecoverState())

		// Simulate the pipelining state where a previous batch's commit is
		// still in flight: lastAppliedIndex has been bumped by a prior
		// PrepareEntries, but lastPersistedIndex (advanced by
		// CommitPreparedBatch) lags behind.
		machine.State.LastAppliedIndex = 5

		return machine, dataStore
	}

	t.Run("empty entries does not panic", func(t *testing.T) {
		t.Parallel()

		machine, dataStore := newMachineWithTraceLogger(t)

		require.NotPanics(t, func() {
			pb, err := machine.PrepareEntries(context.Background(), dataStore)
			require.NoError(t, err)
			if pb != nil {
				pb.Close()
			}
		})
	})

	t.Run("non-empty entries logs first index", func(t *testing.T) {
		t.Parallel()

		machine, dataStore := newMachineWithTraceLogger(t)

		// A no-op entry (empty Data) is enough to drive the trace branch;
		// applyProposal is skipped, no Pebble writes happen, but the
		// log fields are evaluated.
		require.NotPanics(t, func() {
			pb, err := machine.PrepareEntries(context.Background(), dataStore, raftpb.Entry{
				Index: 6,
				Term:  1,
				Type:  raftpb.EntryNormal,
			})
			require.NoError(t, err)
			if pb != nil {
				pb.Close()
			}
		})
	})
}
