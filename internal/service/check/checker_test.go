package check

import (
	"context"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/cache"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"
)

func createTestStore(t *testing.T) *data.Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := data.NewStore(t.TempDir(), logger, meter, data.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

// testEngine wraps the processing pipeline to build a realistic store state.
// It uses the RequestProcessor to create logs, then writes them (with attributes) to the store
// using the same mechanisms as the real state machine.
type testEngine struct {
	t         *testing.T
	store     *data.Store
	attrs     *attributes.Attributes
	processor *processing.RequestProcessor
	cache     *cache.Cache

	// In-memory state tracking (mirroring the state machine)
	nextLedgerID   uint32
	nextSequenceID uint64
	lastLogHash    []byte
	ledgers        map[string]*commonpb.LedgerInfo
	boundaries     map[string]*raftcmdpb.LedgerBoundaries
	inputs         map[string]*raftcmdpb.VolumeHolder
	outputs        map[string]*raftcmdpb.VolumeHolder
	metadata       map[string]*commonpb.MetadataValue
	reverted       map[string]bool
	idempotency    map[string]*commonpb.IdempotencyKeyValue
	references     map[string]*commonpb.TransactionReferenceValue
	hasher         *blake3.Hasher
	raftIndex      uint64
}

func newTestEngine(t *testing.T) *testEngine {
	t.Helper()

	store := createTestStore(t)
	attrs := attributes.New()

	meter := noop.NewMeterProvider().Meter("test")
	proc, err := processing.NewRequestProcessor(meter)
	require.NoError(t, err)

	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	return &testEngine{
		t:              t,
		store:          store,
		attrs:          attrs,
		processor:      proc,
		cache:          c,
		nextLedgerID:   1,
		nextSequenceID: 1,
		ledgers:        make(map[string]*commonpb.LedgerInfo),
		boundaries:     make(map[string]*raftcmdpb.LedgerBoundaries),
		inputs:         make(map[string]*raftcmdpb.VolumeHolder),
		outputs:        make(map[string]*raftcmdpb.VolumeHolder),
		metadata:       make(map[string]*commonpb.MetadataValue),
		reverted:       make(map[string]bool),
		idempotency:    make(map[string]*commonpb.IdempotencyKeyValue),
		references:     make(map[string]*commonpb.TransactionReferenceValue),
		hasher:         blake3.New(),
		raftIndex:      1,
	}
}

// processAndCommit processes orders through the RequestProcessor, then writes the resulting
// logs and attributes to the store, matching the real state machine's behavior.
func (e *testEngine) processAndCommit(orders ...*raftcmdpb.Order) []*commonpb.Log {
	e.t.Helper()

	proposal := &raftcmdpb.Proposal{
		Id:     e.raftIndex,
		Orders: orders,
		Date:   &commonpb.Timestamp{Data: 1700000000 + e.raftIndex},
	}

	// Track which volume keys were modified in this batch
	modifiedInputs := make(map[string]struct{})
	modifiedOutputs := make(map[string]struct{})
	modifiedMetadata := make(map[string]struct{})

	store := &inMemoryStore{
		engine:           e,
		date:             proposal.Date,
		modifiedInputs:   modifiedInputs,
		modifiedOutputs:  modifiedOutputs,
		modifiedMetadata: modifiedMetadata,
	}
	resp, err := e.processor.ProcessProposal(proposal, store)
	require.NoError(e.t, err)

	// Collect actual logs from the response
	var logs []*commonpb.Log
	for _, logOrRef := range resp.Logs {
		switch t := logOrRef.Type.(type) {
		case *raftcmdpb.CreatedLogOrReference_CreatedLog:
			logs = append(logs, t.CreatedLog)
		}
	}

	// Write logs and attributes to the store (mimicking Buffered.Merge)
	batch := e.store.NewBatch()
	defer func() { _ = batch.Cancel() }()

	err = batch.AppendLogs(logs...)
	require.NoError(e.t, err)

	// Write only modified volume attributes (inputs and outputs)
	for key := range modifiedInputs {
		vh := e.inputs[key]
		canonicalKey := []byte(key)
		if vh.Known != nil {
			err := e.attrs.Input.SetBase(batch, e.raftIndex, canonicalKey, vh.Known)
			require.NoError(e.t, err)
		} else if vh.DiffSinceBaseIndex != nil {
			err := e.attrs.Input.AddDiff(batch, e.raftIndex, canonicalKey, vh.DiffSinceBaseIndex)
			require.NoError(e.t, err)
		}
	}

	for key := range modifiedOutputs {
		vh := e.outputs[key]
		canonicalKey := []byte(key)
		if vh.Known != nil {
			err := e.attrs.Output.SetBase(batch, e.raftIndex, canonicalKey, vh.Known)
			require.NoError(e.t, err)
		} else if vh.DiffSinceBaseIndex != nil {
			err := e.attrs.Output.AddDiff(batch, e.raftIndex, canonicalKey, vh.DiffSinceBaseIndex)
			require.NoError(e.t, err)
		}
	}

	// Write only modified metadata attributes
	for key := range modifiedMetadata {
		value, ok := e.metadata[key]
		canonicalKey := []byte(key)
		if ok {
			err := e.attrs.Metadata.AddDiff(batch, e.raftIndex, canonicalKey, value)
			require.NoError(e.t, err)
		} else {
			// Metadata was deleted
			err := e.attrs.Metadata.Delete(batch, canonicalKey)
			require.NoError(e.t, err)
		}
	}

	// Write ledger info
	for _, info := range e.ledgers {
		err := batch.SaveLedger(info)
		require.NoError(e.t, err)
		err = e.attrs.Ledger.SetBase(batch, e.raftIndex, data.LedgerKey{Name: info.Name}.Bytes(), info)
		require.NoError(e.t, err)
	}

	err = batch.SetAppliedIndex(e.raftIndex)
	require.NoError(e.t, err)

	err = batch.Commit()
	require.NoError(e.t, err)

	// After commit, consolidate DiffSinceBaseIndex into Known for all modified volumes.
	// This mimics the cache preloading behavior in the real system.
	for key := range modifiedInputs {
		e.consolidateVolumeHolder(e.inputs, key)
	}
	for key := range modifiedOutputs {
		e.consolidateVolumeHolder(e.outputs, key)
	}

	e.raftIndex++
	return logs
}

// consolidateVolumeHolder converts a VolumeHolder from DiffSinceBaseIndex form to Known form.
// In the real system, the cache does this when preloading from the store.
func (e *testEngine) consolidateVolumeHolder(volumes map[string]*raftcmdpb.VolumeHolder, key string) {
	vh, ok := volumes[key]
	if !ok || vh == nil {
		return
	}

	if vh.Known != nil {
		// Already consolidated
		return
	}

	if vh.DiffSinceBaseIndex != nil {
		// Read the actual value from the attribute store to get the fully computed value
		// This includes the base + any prior diffs + the new diff
		vh.Known = vh.DiffSinceBaseIndex
		vh.DiffSinceBaseIndex = nil
	}
}

// inMemoryStore implements processing.Store using the testEngine's in-memory state.
type inMemoryStore struct {
	engine           *testEngine
	date             *commonpb.Timestamp
	modifiedInputs   map[string]struct{}
	modifiedOutputs  map[string]struct{}
	modifiedMetadata map[string]struct{}
}

func (s *inMemoryStore) GetLedger(name string) (*commonpb.LedgerInfo, bool) {
	info, ok := s.engine.ledgers[name]
	return info, ok
}

func (s *inMemoryStore) PutLedger(name string, info *commonpb.LedgerInfo) {
	s.engine.ledgers[name] = info
}

func (s *inMemoryStore) GetBoundaries(ledger string) (*raftcmdpb.LedgerBoundaries, bool) {
	b, ok := s.engine.boundaries[ledger]
	return b, ok
}

func (s *inMemoryStore) PutBoundaries(ledger string, boundaries *raftcmdpb.LedgerBoundaries) {
	s.engine.boundaries[ledger] = boundaries
}

func (s *inMemoryStore) GetInput(key data.VolumeKey) (*raftcmdpb.VolumeHolder, error) {
	vh, ok := s.engine.inputs[string(key.Bytes())]
	if !ok {
		return nil, data.ErrNotFound
	}
	return proto.CloneOf(vh), nil
}

func (s *inMemoryStore) PutInput(key data.VolumeKey, value *raftcmdpb.VolumeHolder) {
	k := string(key.Bytes())
	s.engine.inputs[k] = value
	s.modifiedInputs[k] = struct{}{}
}

func (s *inMemoryStore) GetOutput(key data.VolumeKey) (*raftcmdpb.VolumeHolder, error) {
	vh, ok := s.engine.outputs[string(key.Bytes())]
	if !ok {
		return nil, data.ErrNotFound
	}
	return proto.CloneOf(vh), nil
}

func (s *inMemoryStore) PutOutput(key data.VolumeKey, value *raftcmdpb.VolumeHolder) {
	k := string(key.Bytes())
	s.engine.outputs[k] = value
	s.modifiedOutputs[k] = struct{}{}
}

func (s *inMemoryStore) GetAccountMetadata(key data.MetadataKey) (*commonpb.MetadataValue, error) {
	v, ok := s.engine.metadata[string(key.Bytes())]
	if !ok {
		return nil, data.ErrNotFound
	}
	return v, nil
}

func (s *inMemoryStore) PutAccountMetadata(key data.MetadataKey, value *commonpb.MetadataValue) {
	k := string(key.Bytes())
	s.engine.metadata[k] = value
	s.modifiedMetadata[k] = struct{}{}
}

func (s *inMemoryStore) DeleteAccountMetadata(key data.MetadataKey) {
	k := string(key.Bytes())
	delete(s.engine.metadata, k)
	s.modifiedMetadata[k] = struct{}{}
}

func (s *inMemoryStore) PutLedgerMetadata(_ data.LedgerMetadataKey, _ *commonpb.MetadataValue) {}

func (s *inMemoryStore) GetReverted(key data.TransactionKey) (bool, error) {
	reverted, ok := s.engine.reverted[string(key.Bytes())]
	if !ok {
		return false, data.ErrNotFound
	}
	return reverted, nil
}

func (s *inMemoryStore) PutReverted(key data.TransactionKey, reverted bool) {
	s.engine.reverted[string(key.Bytes())] = reverted
}

func (s *inMemoryStore) GetIdempotencyKey(key data.IdempotencyKey) (*commonpb.IdempotencyKeyValue, error) {
	v, ok := s.engine.idempotency[key.Key]
	if !ok {
		return nil, data.ErrNotFound
	}
	return v, nil
}

func (s *inMemoryStore) PutIdempotencyKey(key data.IdempotencyKey, value *commonpb.IdempotencyKeyValue) {
	s.engine.idempotency[key.Key] = value
}

func (s *inMemoryStore) GetTransactionReference(key data.TransactionReferenceKey) (*commonpb.TransactionReferenceValue, error) {
	v, ok := s.engine.references[string(key.Bytes())]
	if !ok {
		return nil, data.ErrNotFound
	}
	return v, nil
}

func (s *inMemoryStore) PutTransactionReference(key data.TransactionReferenceKey, value *commonpb.TransactionReferenceValue) {
	s.engine.references[string(key.Bytes())] = value
}

func (s *inMemoryStore) AddTransactionUpdate(_ data.TransactionKey, _ *commonpb.TransactionUpdate) {}

func (s *inMemoryStore) GetLastLogHash() []byte {
	return s.engine.lastLogHash
}

func (s *inMemoryStore) SetLastLogHash(hash []byte) {
	s.engine.lastLogHash = hash
}

func (s *inMemoryStore) GetNextLedgerID() uint32 {
	return s.engine.nextLedgerID
}

func (s *inMemoryStore) IncrementNextLedgerID() uint32 {
	id := s.engine.nextLedgerID
	s.engine.nextLedgerID++
	return id
}

func (s *inMemoryStore) GetNextSequenceID() uint64 {
	return s.engine.nextSequenceID
}

func (s *inMemoryStore) IncrementNextSequenceID() uint64 {
	id := s.engine.nextSequenceID
	s.engine.nextSequenceID++
	return id
}

func (s *inMemoryStore) GetDate() *commonpb.Timestamp {
	return s.date
}

// Helper functions for building orders

func newPosting(source, destination, asset string, amount int64) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      source,
		Destination: destination,
		Amount:      commonpb.NewBigInt(big.NewInt(amount)),
		Asset:       asset,
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

func createLedgerWithMetadataOrder(name string, metadata map[string]string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_CreateLedger{
			CreateLedger: &raftcmdpb.CreateLedgerOrder{
				Name:     name,
				Metadata: commonpb.MetadataSetFromMap(metadata),
			},
		},
	}
}

func deleteLedgerOrder(name string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_DeleteLedger{
			DeleteLedger: &raftcmdpb.DeleteLedgerOrder{
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

func createTransactionWithMetadataOrder(ledger string, force bool, metadata map[string]string, accountMeta map[string]*commonpb.MetadataSet, postings ...*commonpb.Posting) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledger,
				Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
					CreateTransaction: &raftcmdpb.CreateTransactionOrder{
						Postings:        postings,
						Force:           force,
						Metadata:        commonpb.MetadataSetFromMap(metadata),
						AccountMetadata: accountMeta,
					},
				},
			},
		},
	}
}

func revertTransactionOrder(ledger string, txID uint64, originalPostings []*commonpb.Posting) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledger,
				Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
					RevertTransaction: &raftcmdpb.RevertTransactionOrder{
						TransactionId:    txID,
						OriginalPostings: originalPostings,
					},
				},
			},
		},
	}
}

func saveAccountMetadataOrder(ledger, account string, metadata map[string]string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledger,
				Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
					AddMetadata: &raftcmdpb.SaveMetadataOrder{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{
									Addr: account,
								},
							},
						},
						Metadata: commonpb.MetadataSetFromMap(metadata),
					},
				},
			},
		},
	}
}

func deleteAccountMetadataOrder(ledger, account, key string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledger,
				Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
					DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{
									Addr: account,
								},
							},
						},
						Key: key,
					},
				},
			},
		},
	}
}

// collectCheckErrors runs the checker and returns all error events.
func collectCheckErrors(t *testing.T, store *data.Store, attrs *attributes.Attributes) []*servicepb.CheckStoreError {
	t.Helper()

	checker := NewChecker(store, attrs)
	var errors []*servicepb.CheckStoreError

	err := checker.Check(context.Background(), func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.Type.(*servicepb.CheckStoreEvent_Error); ok {
			errors = append(errors, e.Error)
		}
	})
	require.NoError(t, err)

	return errors
}

// TestCheckerEmptyStore verifies that an empty store passes all checks.
func TestCheckerEmptyStore(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	errors := collectCheckErrors(t, store, attrs)
	require.Empty(t, errors, "empty store should have no errors")
}

// TestCheckerComprehensive performs a thorough check with many different write types:
// - Multiple ledger creations
// - Multiple ledger deletion
// - Transactions with varying amounts, multiple assets, multiple accounts
// - Save account metadata (single and multiple keys)
// - Delete account metadata
// - Transaction reverts
// - Transactions with account metadata attached
// All should result in no check errors.
func TestCheckerComprehensive(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	// --- Step 1: Create multiple ledgers ---
	engine.processAndCommit(createLedgerOrder("trading"))
	engine.processAndCommit(createLedgerOrder("payments"))
	engine.processAndCommit(createLedgerWithMetadataOrder("savings", map[string]string{
		"type": "savings-account",
		"tier": "premium",
	}))

	// --- Step 2: Fund accounts from world (creates volumes) ---
	// Trading ledger: fund multiple accounts with multiple assets
	engine.processAndCommit(createTransactionOrder("trading", true,
		newPosting("world", "bank", "USD", 100000),
	))
	engine.processAndCommit(createTransactionOrder("trading", true,
		newPosting("world", "bank", "EUR", 50000),
	))
	engine.processAndCommit(createTransactionOrder("trading", true,
		newPosting("world", "treasury", "USD", 500000),
	))

	// Payments ledger: fund accounts
	engine.processAndCommit(createTransactionOrder("payments", true,
		newPosting("world", "merchant:alice", "USD", 10000),
	))
	engine.processAndCommit(createTransactionOrder("payments", true,
		newPosting("world", "merchant:bob", "EUR", 20000),
	))

	// Savings ledger: fund
	engine.processAndCommit(createTransactionOrder("savings", true,
		newPosting("world", "user:carol", "USD", 5000),
	))

	// --- Step 3: Transfer between accounts (tests both source output + dest input) ---
	engine.processAndCommit(createTransactionOrder("trading", false,
		newPosting("bank", "user:alice", "USD", 1000),
	))
	engine.processAndCommit(createTransactionOrder("trading", false,
		newPosting("bank", "user:bob", "USD", 2500),
	))
	engine.processAndCommit(createTransactionOrder("trading", false,
		newPosting("bank", "user:alice", "EUR", 500),
	))

	// Multi-posting transaction (single transaction with multiple postings)
	engine.processAndCommit(createTransactionOrder("trading", false,
		newPosting("treasury", "bank", "USD", 10000),
		newPosting("bank", "user:charlie", "USD", 5000),
	))

	// --- Step 4: Save account metadata ---
	engine.processAndCommit(saveAccountMetadataOrder("trading", "user:alice", map[string]string{
		"status":   "active",
		"verified": "true",
		"tier":     "gold",
	}))
	engine.processAndCommit(saveAccountMetadataOrder("trading", "user:bob", map[string]string{
		"status": "pending",
	}))
	engine.processAndCommit(saveAccountMetadataOrder("payments", "merchant:alice", map[string]string{
		"business": "retail",
		"country":  "FR",
	}))

	// --- Step 5: Update metadata (overwrite existing key) ---
	engine.processAndCommit(saveAccountMetadataOrder("trading", "user:bob", map[string]string{
		"status": "active",
	}))

	// --- Step 6: Delete account metadata ---
	engine.processAndCommit(deleteAccountMetadataOrder("trading", "user:alice", "tier"))

	// --- Step 7: Transactions with account metadata attached ---
	engine.processAndCommit(createTransactionWithMetadataOrder("payments", true,
		map[string]string{"type": "deposit"},
		map[string]*commonpb.MetadataSet{
			"customer:dave": commonpb.MetadataSetFromMap(map[string]string{
				"joined": "2026-01-01",
			}),
		},
		newPosting("world", "customer:dave", "USD", 3000),
	))

	// --- Step 8: Revert a transaction ---
	// Revert the first user:alice USD transfer (tx ID 4 in trading ledger, 0-indexed tx=3 -> 4th tx)
	// The postings were: bank -> user:alice 1000 USD
	engine.processAndCommit(revertTransactionOrder("trading", 4,
		[]*commonpb.Posting{
			newPosting("bank", "user:alice", "USD", 1000),
		},
	))

	// --- Step 9: More transactions after revert ---
	engine.processAndCommit(createTransactionOrder("trading", false,
		newPosting("bank", "user:alice", "USD", 750),
	))

	// Cross-asset transfer in savings
	engine.processAndCommit(createTransactionOrder("savings", true,
		newPosting("world", "user:carol", "EUR", 2000),
	))
	engine.processAndCommit(createTransactionOrder("savings", false,
		newPosting("user:carol", "user:dave", "USD", 1000),
	))

	// --- Step 10: Delete a ledger ---
	engine.processAndCommit(deleteLedgerOrder("savings"))

	// --- Step 11: More operations on remaining ledgers after deletion ---
	engine.processAndCommit(createTransactionOrder("payments", true,
		newPosting("world", "merchant:charlie", "GBP", 15000),
	))
	engine.processAndCommit(saveAccountMetadataOrder("payments", "merchant:charlie", map[string]string{
		"business": "tech",
		"country":  "UK",
	}))

	// --- Now verify: the checker should find no errors ---
	errors := collectCheckErrors(t, engine.store, engine.attrs)
	for _, e := range errors {
		t.Logf("Check error: [%s] %s (log=%d, ledger=%s, account=%s, asset=%s)",
			e.ErrorType, e.Message, e.LogSequence, e.Ledger, e.Account, e.Asset)
	}
	require.Empty(t, errors, "store built from valid operations should have no integrity errors")
}

// TestCheckerDetectsHashMismatch verifies the checker detects a corrupted hash chain.
func TestCheckerDetectsHashMismatch(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	// Manually write a log with a bad hash
	hasher := blake3.New()
	log1 := &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerLog{
					Info: &commonpb.LedgerInfo{
						Name:      "test",
						CreatedAt: &commonpb.Timestamp{Data: 1700000000},
						Id:        1,
					},
				},
			},
		},
	}
	// Compute correct hash for log1
	log1.Hash = computeCorrectHash(hasher, nil, log1)

	// Create log2 with WRONG hash
	log2 := &commonpb.Log{
		Sequence: 2,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerLog{
					Info: &commonpb.LedgerInfo{
						Name:      "test2",
						CreatedAt: &commonpb.Timestamp{Data: 1700000001},
						Id:        2,
					},
				},
			},
		},
	}
	log2.Hash = []byte("this-is-a-bad-hash")

	batch := store.NewBatch()
	require.NoError(t, batch.AppendLogs(log1, log2))
	require.NoError(t, batch.SaveLedger(log1.Payload.GetCreateLedger().Info))
	require.NoError(t, batch.SaveLedger(log2.Payload.GetCreateLedger().Info))
	require.NoError(t, batch.Commit())

	// Write ledger attributes
	batch2 := store.NewBatch()
	require.NoError(t, attrs.Ledger.SetBase(batch2, 1, data.LedgerKey{Name: "test"}.Bytes(), log1.Payload.GetCreateLedger().Info))
	require.NoError(t, attrs.Ledger.SetBase(batch2, 1, data.LedgerKey{Name: "test2"}.Bytes(), log2.Payload.GetCreateLedger().Info))
	require.NoError(t, batch2.Commit())

	errors := collectCheckErrors(t, store, attrs)
	require.Len(t, errors, 1, "should detect exactly one hash mismatch")
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH, errors[0].ErrorType)
	require.Equal(t, uint64(2), errors[0].LogSequence)
}

// TestCheckerDetectsSequenceGap verifies the checker detects missing log entries.
func TestCheckerDetectsSequenceGap(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	hasher := blake3.New()

	// Write log at sequence 1
	log1 := &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerLog{
					Info: &commonpb.LedgerInfo{
						Name:      "test",
						CreatedAt: &commonpb.Timestamp{Data: 1700000000},
						Id:        1,
					},
				},
			},
		},
	}
	log1.Hash = computeCorrectHash(hasher, nil, log1)

	// Skip sequence 2 and write log at sequence 3
	log3 := &commonpb.Log{
		Sequence: 3,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerLog{
					Info: &commonpb.LedgerInfo{
						Name:      "test2",
						CreatedAt: &commonpb.Timestamp{Data: 1700000002},
						Id:        2,
					},
				},
			},
		},
	}
	// Even with correct hash chaining from log1, the gap will be detected
	log3.Hash = computeCorrectHash(hasher, log1.Hash, log3)

	batch := store.NewBatch()
	require.NoError(t, batch.AppendLogs(log1, log3))
	require.NoError(t, batch.SaveLedger(log1.Payload.GetCreateLedger().Info))
	require.NoError(t, batch.SaveLedger(log3.Payload.GetCreateLedger().Info))
	require.NoError(t, batch.Commit())

	errors := collectCheckErrors(t, store, attrs)

	// Should detect the gap at sequence 2
	var gapErrors []*servicepb.CheckStoreError
	for _, e := range errors {
		if e.ErrorType == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP {
			gapErrors = append(gapErrors, e)
		}
	}
	require.NotEmpty(t, gapErrors, "should detect sequence gap")
	require.Equal(t, uint64(2), gapErrors[0].LogSequence)
}

// TestCheckerProgressEvents verifies that progress events are emitted.
func TestCheckerProgressEvents(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	// Create a ledger and a few transactions
	engine.processAndCommit(createLedgerOrder("test"))
	for i := 0; i < 5; i++ {
		engine.processAndCommit(createTransactionOrder("test", true,
			newPosting("world", "user:alice", "USD", int64(100*(i+1))),
		))
	}

	checker := NewChecker(engine.store, engine.attrs)
	var progressEvents []*servicepb.CheckStoreProgress

	err := checker.Check(context.Background(), func(event *servicepb.CheckStoreEvent) {
		if p, ok := event.Type.(*servicepb.CheckStoreEvent_Progress); ok {
			progressEvents = append(progressEvents, p.Progress)
		}
	})
	require.NoError(t, err)

	require.NotEmpty(t, progressEvents, "should emit at least one progress event")
	lastProgress := progressEvents[len(progressEvents)-1]
	require.Equal(t, lastProgress.LogsChecked, lastProgress.TotalLogs, "final progress should show all logs checked")
}

// TestCheckerManyOperationTypes tests with a broad variety of operations:
// different assets, many accounts, metadata updates/deletes, multiple reverts.
func TestCheckerManyOperationTypes(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	// Create 3 ledgers
	engine.processAndCommit(createLedgerOrder("main"))
	engine.processAndCommit(createLedgerOrder("secondary"))
	engine.processAndCommit(createLedgerOrder("archive"))

	// Fund with 4 different asset types
	assets := []struct {
		asset  string
		amount int64
	}{
		{"USD", 100000},
		{"EUR", 80000},
		{"GBP", 60000},
		{"BTC", 10},
	}
	for _, a := range assets {
		engine.processAndCommit(createTransactionOrder("main", true,
			newPosting("world", "treasury", a.asset, a.amount),
		))
	}

	// Distribute from treasury to 5 users in different assets
	users := []string{"user:alpha", "user:beta", "user:gamma", "user:delta", "user:epsilon"}
	distributionAmounts := []int64{1000, 1000, 1000, 1, 1000} // BTC (index 3) only has 10, use 1
	for i, user := range users {
		engine.processAndCommit(createTransactionOrder("main", false,
			newPosting("treasury", user, assets[i%len(assets)].asset, distributionAmounts[i]),
		))
	}

	// Multi-posting: distribute from treasury to multiple users in one transaction
	engine.processAndCommit(createTransactionOrder("main", false,
		newPosting("treasury", "user:alpha", "USD", 500),
		newPosting("treasury", "user:beta", "USD", 300),
		newPosting("treasury", "user:gamma", "USD", 200),
	))

	// Transfers between users
	engine.processAndCommit(createTransactionOrder("main", false,
		newPosting("user:alpha", "user:beta", "USD", 100),
	))
	engine.processAndCommit(createTransactionOrder("main", false,
		newPosting("user:beta", "user:gamma", "EUR", 200),
	))

	// Metadata on many accounts
	for _, user := range users {
		engine.processAndCommit(saveAccountMetadataOrder("main", user, map[string]string{
			"status":     "active",
			"created_by": "test",
		}))
	}

	// Update metadata
	engine.processAndCommit(saveAccountMetadataOrder("main", "user:alpha", map[string]string{
		"status": "premium",
		"level":  "5",
	}))

	// Delete metadata on some accounts
	engine.processAndCommit(deleteAccountMetadataOrder("main", "user:beta", "created_by"))
	engine.processAndCommit(deleteAccountMetadataOrder("main", "user:gamma", "status"))

	// Operations on secondary ledger
	engine.processAndCommit(createTransactionOrder("secondary", true,
		newPosting("world", "escrow", "USD", 50000),
	))
	engine.processAndCommit(createTransactionOrder("secondary", false,
		newPosting("escrow", "beneficiary:1", "USD", 10000),
	))
	engine.processAndCommit(saveAccountMetadataOrder("secondary", "escrow", map[string]string{
		"type": "escrow",
	}))

	// Revert the user:alpha -> user:beta transfer (tx ID 11: 4 funding + 5 distribution + 1 multi + 1 alpha->beta)
	engine.processAndCommit(revertTransactionOrder("main", 11,
		[]*commonpb.Posting{
			newPosting("user:alpha", "user:beta", "USD", 100),
		},
	))

	// Revert the escrow -> beneficiary:1 transfer
	engine.processAndCommit(revertTransactionOrder("secondary", 2,
		[]*commonpb.Posting{
			newPosting("escrow", "beneficiary:1", "USD", 10000),
		},
	))

	// More transactions after reverts
	engine.processAndCommit(createTransactionOrder("main", false,
		newPosting("user:alpha", "user:delta", "USD", 300),
	))
	engine.processAndCommit(createTransactionOrder("secondary", false,
		newPosting("escrow", "beneficiary:2", "USD", 5000),
	))

	// Delete the archive ledger
	engine.processAndCommit(deleteLedgerOrder("archive"))

	// Verify no check errors
	errors := collectCheckErrors(t, engine.store, engine.attrs)
	for _, e := range errors {
		t.Logf("Check error: [%s] %s (log=%d, ledger=%s, account=%s, asset=%s)",
			e.ErrorType, e.Message, e.LogSequence, e.Ledger, e.Account, e.Asset)
	}
	require.Empty(t, errors, "store built from valid operations should have no integrity errors")
}

// computeCorrectHash computes the correct hash for a log entry, matching the production logic.
func computeCorrectHash(hasher *blake3.Hasher, lastHash []byte, log *commonpb.Log) []byte {
	logForHash := &commonpb.Log{
		Sequence:    log.Sequence,
		Payload:     log.Payload,
		Idempotency: log.Idempotency,
	}

	logBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(logForHash)
	if err != nil {
		panic(err)
	}
	hasher.Reset()
	if len(lastHash) > 0 {
		_, _ = hasher.Write(lastHash)
	}
	_, _ = hasher.Write(logBytes)
	return hasher.Sum(nil)
}
