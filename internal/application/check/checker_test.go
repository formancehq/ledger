package check

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

func createTestStore(t *testing.T) *dal.Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

// testEngine wraps the processing pipeline to build a realistic store state.
// It uses the RequestProcessor to create logs, then writes them (with attributes) to the store
// using the same mechanisms as the real state machine.
type testEngine struct {
	t         *testing.T
	store     *dal.Store
	attrs     *attributes.Attributes
	processor *processing.RequestProcessor
	cache     *cache.Cache

	// In-memory state tracking (mirroring the state machine)
	nextSequenceID    uint64
	lastLogHash       []byte
	ledgers           map[string]*commonpb.LedgerInfo
	boundaries        map[string]*raftcmdpb.LedgerBoundaries
	volumes           map[string]*raftcmdpb.VolumePair
	metadata          map[string]*commonpb.MetadataValue
	idempotency       map[string]*commonpb.IdempotencyKeyValue
	references        map[string]*commonpb.TransactionReferenceValue
	currentOpenPeriod *commonpb.Period
	closingPeriod     *commonpb.Period
	nextPeriodID      uint64
	hasher            *blake3.Hasher
	raftIndex         uint64
}

func newTestEngine(t *testing.T) *testEngine {
	t.Helper()

	store := createTestStore(t)
	attrs := attributes.New()

	meter := noop.NewMeterProvider().Meter("test")
	proc, err := processing.NewRequestProcessor(meter, 0)
	require.NoError(t, err)

	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	return &testEngine{
		t:              t,
		store:          store,
		attrs:          attrs,
		processor:      proc,
		cache:          c,
		nextSequenceID: 1,
		ledgers:        make(map[string]*commonpb.LedgerInfo),
		boundaries:     make(map[string]*raftcmdpb.LedgerBoundaries),
		volumes:        make(map[string]*raftcmdpb.VolumePair),
		metadata:       make(map[string]*commonpb.MetadataValue),
		idempotency:    make(map[string]*commonpb.IdempotencyKeyValue),
		references:     make(map[string]*commonpb.TransactionReferenceValue),
		nextPeriodID:   1,
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
	modifiedVolumes := make(map[string]struct{})
	modifiedMetadata := make(map[string]struct{})

	store := &inMemoryStore{
		engine:             e,
		date:               proposal.GetDate(),
		modifiedVolumes:    modifiedVolumes,
		modifiedMetadata:   modifiedMetadata,
		transactionUpdates: make(map[string][]*commonpb.TransactionUpdate),
		reverted:           make(map[string]bool),
	}
	resp, err := e.processor.ProcessOrders(proposal.GetOrders(), store)
	require.NoError(e.t, err)

	// Collect actual logs from the response
	var logs []*commonpb.Log

	for _, logOrRef := range resp {
		if t, ok := logOrRef.GetType().(*raftcmdpb.CreatedLogOrReference_CreatedLog); ok {
			logs = append(logs, t.CreatedLog)
		}
	}

	// Write logs and attributes to the store (mimicking Buffered.Merge)
	batch := e.store.NewBatch()

	defer func() { _ = batch.Cancel() }()

	err = state.AppendLogs(batch, logs...)
	require.NoError(e.t, err)

	// Write only modified volume attributes.
	// Values are always stored as Input/Output.
	for key := range modifiedVolumes {
		vp := e.volumes[key]
		canonicalKey := []byte(key)

		storePair := &raftcmdpb.VolumePair{
			Input:  vp.GetInput(),
			Output: vp.GetOutput(),
		}
		if storePair.GetInput() != nil || storePair.GetOutput() != nil {
			err := e.attrs.Volume.Set(batch, e.raftIndex, canonicalKey, storePair)
			require.NoError(e.t, err)
		}
	}

	// Write only modified metadata attributes
	for key := range modifiedMetadata {
		value, ok := e.metadata[key]

		canonicalKey := []byte(key)
		if ok {
			err := e.attrs.Metadata.Set(batch, e.raftIndex, canonicalKey, value)
			require.NoError(e.t, err)
		} else {
			// Metadata was deleted
			err := e.attrs.Metadata.Delete(batch, canonicalKey)
			require.NoError(e.t, err)
		}
	}

	// Write transaction updates to Pebble
	for keyStr, updates := range store.transactionUpdates {
		var tk domain.TransactionKey

		err := tk.Unmarshal([]byte(keyStr))
		require.NoError(e.t, err)

		for _, update := range updates {
			err := state.StoreTransactionUpdate(batch, tk, update)
			require.NoError(e.t, err)
		}
	}

	// Write ledger info
	for _, info := range e.ledgers {
		err := state.SaveLedger(batch, info)
		require.NoError(e.t, err)
		err = e.attrs.Ledger.Set(batch, e.raftIndex, domain.LedgerKey{Name: info.GetName()}.Bytes(), info)
		require.NoError(e.t, err)
	}

	err = state.SetAppliedIndex(batch, e.raftIndex)
	require.NoError(e.t, err)

	err = batch.Commit()
	require.NoError(e.t, err)

	e.raftIndex++

	return logs
}

// inMemoryStore implements processing.InMemoryStore using the testEngine's in-memory state.
type inMemoryStore struct {
	engine           *testEngine
	date             *commonpb.Timestamp
	modifiedVolumes  map[string]struct{}
	modifiedMetadata map[string]struct{}
	// Transaction updates accumulated during this proposal
	transactionUpdates map[string][]*commonpb.TransactionUpdate // txKey -> updates
	// In-memory reverted status (bitset-like, not persisted to Pebble)
	reverted map[string]bool
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

func (s *inMemoryStore) GetVolume(key domain.VolumeKey) (*raftcmdpb.VolumePair, error) {
	vp, ok := s.engine.volumes[string(key.Bytes())]
	if !ok {
		// Simulate preloaded zero volumes (in production, admission always preloads)
		return &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(0),
			Output: commonpb.NewUint256FromUint64(0),
		}, nil
	}

	return proto.CloneOf(vp), nil
}

func (s *inMemoryStore) PutVolume(key domain.VolumeKey, value *raftcmdpb.VolumePair) {
	k := string(key.Bytes())
	s.engine.volumes[k] = value
	s.modifiedVolumes[k] = struct{}{}
}

func (s *inMemoryStore) GetAccountMetadata(key domain.MetadataKey) (*commonpb.MetadataValue, error) {
	v, ok := s.engine.metadata[string(key.Bytes())]
	if !ok {
		return nil, domain.ErrNotFound
	}

	return v, nil
}

func (s *inMemoryStore) PutAccountMetadata(key domain.MetadataKey, value *commonpb.MetadataValue) {
	k := string(key.Bytes())
	s.engine.metadata[k] = value
	s.modifiedMetadata[k] = struct{}{}
}

func (s *inMemoryStore) DeleteAccountMetadata(key domain.MetadataKey) {
	k := string(key.Bytes())
	delete(s.engine.metadata, k)
	s.modifiedMetadata[k] = struct{}{}
}

func (s *inMemoryStore) GetReverted(key domain.TransactionKey) (bool, error) {
	return s.reverted[string(key.Bytes())], nil
}

func (s *inMemoryStore) PutReverted(key domain.TransactionKey, reverted bool) {
	s.reverted[string(key.Bytes())] = reverted
}

func (s *inMemoryStore) GetIdempotencyKey(key domain.IdempotencyKey) (*commonpb.IdempotencyKeyValue, error) {
	v, ok := s.engine.idempotency[key.Key]
	if !ok {
		return nil, domain.ErrNotFound
	}

	return v, nil
}

func (s *inMemoryStore) PutIdempotencyKey(key domain.IdempotencyKey, value *commonpb.IdempotencyKeyValue) {
	s.engine.idempotency[key.Key] = value
}

func (s *inMemoryStore) GetTransactionReference(key domain.TransactionReferenceKey) (*commonpb.TransactionReferenceValue, error) {
	v, ok := s.engine.references[string(key.Bytes())]
	if !ok {
		return nil, domain.ErrNotFound
	}

	return v, nil
}

func (s *inMemoryStore) PutTransactionReference(key domain.TransactionReferenceKey, value *commonpb.TransactionReferenceValue) {
	s.engine.references[string(key.Bytes())] = value
}

func (s *inMemoryStore) AddTransactionUpdate(key domain.TransactionKey, update *commonpb.TransactionUpdate) {
	k := string(key.Bytes())
	s.transactionUpdates[k] = append(s.transactionUpdates[k], update)
}

func (s *inMemoryStore) AddSigningKey(_ string, _ []byte, _ string)           {}
func (s *inMemoryStore) RemoveSigningKey(_ string)                            {}
func (s *inMemoryStore) GetSigningKeyChildren(_ string) []string              { return nil }
func (s *inMemoryStore) SetRequireSignatures(_ bool)                          {}
func (s *inMemoryStore) SetMaintenanceMode(_ bool)                            {}
func (s *inMemoryStore) SetAuditEnabled(_ bool)                               {}
func (s *inMemoryStore) SetPeriodSchedule(_ string)                           {}
func (s *inMemoryStore) DeletePeriodSchedule()                                {}
func (s *inMemoryStore) GetSinkConfig(_ string) (*commonpb.SinkConfig, error) { return nil, nil }
func (s *inMemoryStore) AddSinkConfig(_ *commonpb.SinkConfig)                 {}
func (s *inMemoryStore) RemoveSinkConfig(_ string)                            {}

func (s *inMemoryStore) GetLastLogHash() []byte {
	return s.engine.lastLogHash
}

func (s *inMemoryStore) SetLastLogHash(hash []byte) {
	s.engine.lastLogHash = hash
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

func (s *inMemoryStore) GetCurrentOpenPeriod() (*commonpb.Period, bool) {
	if s.engine.currentOpenPeriod != nil {
		return s.engine.currentOpenPeriod, true
	}

	return nil, false
}

func (s *inMemoryStore) GetClosingPeriod() (*commonpb.Period, bool) {
	if s.engine.closingPeriod != nil {
		return s.engine.closingPeriod, true
	}

	return nil, false
}

func (s *inMemoryStore) SetCurrentOpenPeriod(period *commonpb.Period) {
	s.engine.currentOpenPeriod = period
}

func (s *inMemoryStore) SetClosingPeriod(period *commonpb.Period) {
	s.engine.closingPeriod = period
}

func (s *inMemoryStore) ClearClosingPeriod() {
	s.engine.closingPeriod = nil
}

func (s *inMemoryStore) GetNextPeriodID() uint64 {
	return s.engine.nextPeriodID
}

func (s *inMemoryStore) IncrementNextPeriodID() uint64 {
	id := s.engine.nextPeriodID
	s.engine.nextPeriodID++

	return id
}

func (s *inMemoryStore) GetPeriodByID(_ uint64) (*commonpb.Period, bool) {
	return nil, false
}

func (s *inMemoryStore) UpdatePeriod(_ *commonpb.Period) {}

func (s *inMemoryStore) SetPurgeRange(_, _, _ uint64) {}

func (s *inMemoryStore) SetPendingArchive(_, _, _ uint64) {}

func (s *inMemoryStore) AddMetadataConvertRequest(_ string, _ commonpb.TargetType, _ string, _ commonpb.MetadataType) {
}

func (s *inMemoryStore) GetPreparedQuery(_, _ string) (*commonpb.PreparedQuery, error) {
	return nil, nil
}
func (s *inMemoryStore) PutPreparedQuery(_ *commonpb.PreparedQuery)         {}
func (s *inMemoryStore) DeletePreparedQuery(_, _ string)                    {}
func (s *inMemoryStore) GetNumscriptLatestVersion(_ string) (string, error) { return "", nil }
func (s *inMemoryStore) NumscriptVersionExists(_, _ string) (bool, error)   { return false, nil }
func (s *inMemoryStore) PutNumscript(_ *commonpb.NumscriptInfo)             {}
func (s *inMemoryStore) DeleteNumscriptLatest(_ string)                     {}

// Helper functions for building orders

func newPosting(source, destination, asset string, amount int64) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      source,
		Destination: destination,
		Amount:      commonpb.NewUint256FromUint64(uint64(amount)),
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
func collectCheckErrors(t *testing.T, store *dal.Store, attrs *attributes.Attributes) []*servicepb.CheckStoreError {
	t.Helper()

	checker := NewChecker(store, attrs)

	var errors []*servicepb.CheckStoreError

	err := checker.Check(context.Background(), func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
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
	engine.processAndCommit(createLedgerOrder("savings"))

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

	// --- Step 7b: Transaction metadata operations ---
	// Save metadata on the first trading transaction (tx ID 1)
	engine.processAndCommit(saveTransactionMetadataOrder("trading", 1, map[string]string{
		"category": "funding",
		"note":     "initial bank funding",
	}))
	// Delete transaction metadata
	engine.processAndCommit(deleteTransactionMetadataOrder("trading", 1, "note"))

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
			e.GetErrorType(), e.GetMessage(), e.GetLogSequence(), e.GetLedger(), e.GetAccount(), e.GetAsset())
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
					},
				},
			},
		},
	}
	log2.Hash = []byte("this-is-a-bad-hash")

	batch := store.NewBatch()
	require.NoError(t, state.AppendLogs(batch, log1, log2))
	require.NoError(t, state.SaveLedger(batch, log1.GetPayload().GetCreateLedger().GetInfo()))
	require.NoError(t, state.SaveLedger(batch, log2.GetPayload().GetCreateLedger().GetInfo()))
	require.NoError(t, batch.Commit())

	// Write ledger attributes
	batch2 := store.NewBatch()
	require.NoError(t, attrs.Ledger.Set(batch2, 1, domain.LedgerKey{Name: "test"}.Bytes(), log1.GetPayload().GetCreateLedger().GetInfo()))
	require.NoError(t, attrs.Ledger.Set(batch2, 1, domain.LedgerKey{Name: "test2"}.Bytes(), log2.GetPayload().GetCreateLedger().GetInfo()))
	require.NoError(t, batch2.Commit())

	errors := collectCheckErrors(t, store, attrs)
	require.Len(t, errors, 1, "should detect exactly one hash mismatch")
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH, errors[0].GetErrorType())
	require.Equal(t, uint64(2), errors[0].GetLogSequence())
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
					},
				},
			},
		},
	}
	// Even with correct hash chaining from log1, the gap will be detected
	log3.Hash = computeCorrectHash(hasher, log1.GetHash(), log3)

	batch := store.NewBatch()
	require.NoError(t, state.AppendLogs(batch, log1, log3))
	require.NoError(t, state.SaveLedger(batch, log1.GetPayload().GetCreateLedger().GetInfo()))
	require.NoError(t, state.SaveLedger(batch, log3.GetPayload().GetCreateLedger().GetInfo()))
	require.NoError(t, batch.Commit())

	errors := collectCheckErrors(t, store, attrs)

	// Should detect the gap at sequence 2
	var gapErrors []*servicepb.CheckStoreError

	for _, e := range errors {
		if e.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP {
			gapErrors = append(gapErrors, e)
		}
	}

	require.NotEmpty(t, gapErrors, "should detect sequence gap")
	require.Equal(t, uint64(2), gapErrors[0].GetLogSequence())
}

// TestCheckerProgressEvents verifies that progress events are emitted.
func TestCheckerProgressEvents(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	// Create a ledger and a few transactions
	engine.processAndCommit(createLedgerOrder("test"))

	for i := range 5 {
		engine.processAndCommit(createTransactionOrder("test", true,
			newPosting("world", "user:alice", "USD", int64(100*(i+1))),
		))
	}

	checker := NewChecker(engine.store, engine.attrs)

	var progressEvents []*servicepb.CheckStoreProgress

	err := checker.Check(context.Background(), func(event *servicepb.CheckStoreEvent) {
		if p, ok := event.GetType().(*servicepb.CheckStoreEvent_Progress); ok {
			progressEvents = append(progressEvents, p.Progress)
		}
	})
	require.NoError(t, err)

	require.NotEmpty(t, progressEvents, "should emit at least one progress event")
	lastProgress := progressEvents[len(progressEvents)-1]
	require.Equal(t, lastProgress.GetLogsChecked(), lastProgress.GetTotalLogs(), "final progress should show all logs checked")
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
			e.GetErrorType(), e.GetMessage(), e.GetLogSequence(), e.GetLedger(), e.GetAccount(), e.GetAsset())
	}

	require.Empty(t, errors, "store built from valid operations should have no integrity errors")
}

func saveTransactionMetadataOrder(ledger string, txID uint64, metadata map[string]string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledger,
				Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
					AddMetadata: &raftcmdpb.SaveMetadataOrder{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{
									Id: txID,
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

func deleteTransactionMetadataOrder(ledger string, txID uint64, key string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledger,
				Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
					DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{
									Id: txID,
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

// computeCorrectHash computes the correct hash for a log entry, matching the production logic.
func computeCorrectHash(hasher *blake3.Hasher, lastHash []byte, log *commonpb.Log) []byte {
	return processing.ComputeLogHash(hasher, lastHash, log)
}

// TestCheckerDetectsTransactionUpdateMismatch verifies the checker detects
// corrupted transaction updates.
func TestCheckerDetectsTransactionUpdateMismatch(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	engine.processAndCommit(createLedgerOrder("test"))
	engine.processAndCommit(createTransactionOrder("test", true,
		newPosting("world", "user:alice", "USD", 1000),
	))

	// Write a spurious transaction update to Pebble for tx 1 (ledger ID 1)
	batch := engine.store.NewBatch()
	err := state.StoreTransactionUpdate(batch, domain.TransactionKey{Ledger: "test", ID: 1}, &commonpb.TransactionUpdate{
		ByLog: 999,
		Updates: []*commonpb.TransactionUpdateType{{
			TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationAddMetadata{
				TransactionModificationAddMetadata: &commonpb.TransactionUpdateAddMetadata{
					Metadata: &commonpb.Metadata{
						Key:   "spurious",
						Value: commonpb.NewStringValue("data"),
					},
				},
			},
		}},
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	errors := collectCheckErrors(t, engine.store, engine.attrs)

	var txErrors []*servicepb.CheckStoreError

	for _, e := range errors {
		if e.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH {
			txErrors = append(txErrors, e)
		}
	}

	require.NotEmpty(t, txErrors, "should detect transaction update mismatch")
	require.Equal(t, uint64(1), txErrors[0].GetTransactionId())
}

// TestCheckerWithTransactionMetadata verifies that transaction metadata
// operations (save and delete) are properly tracked and verified.
func TestCheckerWithTransactionMetadata(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	engine.processAndCommit(createLedgerOrder("test"))
	engine.processAndCommit(createTransactionOrder("test", true,
		newPosting("world", "user:alice", "USD", 1000),
	))
	engine.processAndCommit(createTransactionOrder("test", true,
		newPosting("world", "user:bob", "USD", 2000),
	))

	// Save metadata on transaction 1
	engine.processAndCommit(saveTransactionMetadataOrder("test", 1, map[string]string{
		"category": "funding",
		"approved": "true",
	}))

	// Save metadata on transaction 2
	engine.processAndCommit(saveTransactionMetadataOrder("test", 2, map[string]string{
		"category": "payment",
	}))

	// Delete metadata on transaction 1
	engine.processAndCommit(deleteTransactionMetadataOrder("test", 1, "approved"))

	// Verify no check errors
	errors := collectCheckErrors(t, engine.store, engine.attrs)
	for _, e := range errors {
		t.Logf("Check error: [%s] %s (log=%d, ledger=%s, account=%s, asset=%s, tx=%d)",
			e.GetErrorType(), e.GetMessage(), e.GetLogSequence(), e.GetLedger(), e.GetAccount(), e.GetAsset(), e.GetTransactionId())
	}

	require.Empty(t, errors, "store with transaction metadata should have no integrity errors")
}

// TestCheckerDetectsDoubleRevert verifies the checker detects a transaction reverted twice.
func TestCheckerDetectsDoubleRevert(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()
	hasher := blake3.New()

	// Build a valid log chain manually:
	// 1. Create ledger "test"
	// 2. Create transaction (tx 1): world -> alice 1000 USD
	// 3. Revert tx 1 (creates tx 2)
	// 4. Revert tx 1 AGAIN (double revert — should be detected)

	var lastHash []byte

	seqID := uint64(1)

	// Log 1: Create ledger
	log1 := buildLog(hasher, &lastHash, &seqID, &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreateLedgerLog{
				Info: &commonpb.LedgerInfo{Name: "test", CreatedAt: &commonpb.Timestamp{Data: 1700000000}},
			},
		},
	})

	// Log 2: Create transaction (tx 1)
	posting := newPosting("world", "user:alice", "USD", 1000)
	log2 := buildLog(hasher, &lastHash, &seqID, &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: "test",
				Log: &commonpb.LedgerLog{
					Data: &commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
							CreatedTransaction: &commonpb.CreatedTransaction{
								Transaction: &commonpb.Transaction{Id: 1, Postings: []*commonpb.Posting{posting}},
							},
						},
					},
				},
			},
		},
	})

	// Log 3: Revert tx 1 (valid)
	log3 := buildLog(hasher, &lastHash, &seqID, &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: "test",
				Log: &commonpb.LedgerLog{
					Data: &commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
							RevertedTransaction: &commonpb.RevertedTransaction{
								RevertedTransactionId: 1,
								RevertTransaction:     &commonpb.Transaction{Id: 2, Postings: []*commonpb.Posting{reversePosting(posting)}},
							},
						},
					},
				},
			},
		},
	})

	// Log 4: Revert tx 1 AGAIN (double revert)
	log4 := buildLog(hasher, &lastHash, &seqID, &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: "test",
				Log: &commonpb.LedgerLog{
					Data: &commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
							RevertedTransaction: &commonpb.RevertedTransaction{
								RevertedTransactionId: 1,
								RevertTransaction:     &commonpb.Transaction{Id: 3, Postings: []*commonpb.Posting{reversePosting(posting)}},
							},
						},
					},
				},
			},
		},
	})

	batch := store.NewBatch()
	require.NoError(t, state.AppendLogs(batch, log1, log2, log3, log4))
	require.NoError(t, state.SaveLedger(batch, log1.GetPayload().GetCreateLedger().GetInfo()))
	require.NoError(t, writeVolumes(batch, attrs, posting, "test", 1))
	require.NoError(t, batch.Commit())

	errors := collectCheckErrors(t, store, attrs)

	var revertErrors []*servicepb.CheckStoreError

	for _, e := range errors {
		if e.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERTED_MISMATCH {
			revertErrors = append(revertErrors, e)
		}
	}

	require.NotEmpty(t, revertErrors, "should detect double revert")
	require.Contains(t, revertErrors[0].GetMessage(), "double-reverts")
}

// TestCheckerDetectsRevertOfNonExistentTransaction verifies the checker detects
// a revert targeting a transaction ID that was never created.
func TestCheckerDetectsRevertOfNonExistentTransaction(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()
	hasher := blake3.New()

	var lastHash []byte

	seqID := uint64(1)

	// Log 1: Create ledger
	log1 := buildLog(hasher, &lastHash, &seqID, &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreateLedgerLog{
				Info: &commonpb.LedgerInfo{Name: "test", CreatedAt: &commonpb.Timestamp{Data: 1700000000}},
			},
		},
	})

	// Log 2: Revert tx 999 (which was never created)
	posting := newPosting("user:alice", "world", "USD", 1000)
	log2 := buildLog(hasher, &lastHash, &seqID, &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: "test",
				Log: &commonpb.LedgerLog{
					Data: &commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
							RevertedTransaction: &commonpb.RevertedTransaction{
								RevertedTransactionId: 999,
								RevertTransaction:     &commonpb.Transaction{Id: 1, Postings: []*commonpb.Posting{posting}},
							},
						},
					},
				},
			},
		},
	})

	batch := store.NewBatch()
	require.NoError(t, state.AppendLogs(batch, log1, log2))
	require.NoError(t, state.SaveLedger(batch, log1.GetPayload().GetCreateLedger().GetInfo()))
	require.NoError(t, batch.Commit())

	errors := collectCheckErrors(t, store, attrs)

	var revertErrors []*servicepb.CheckStoreError

	for _, e := range errors {
		if e.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERTED_MISMATCH {
			revertErrors = append(revertErrors, e)
		}
	}

	require.NotEmpty(t, revertErrors, "should detect revert of non-existent transaction")
	require.Contains(t, revertErrors[0].GetMessage(), "non-existent")
}

// buildLog creates a log entry with the correct hash chain.
func buildLog(hasher *blake3.Hasher, lastHash *[]byte, seqID *uint64, payload *commonpb.LogPayload) *commonpb.Log {
	log := &commonpb.Log{
		Sequence: *seqID,
		Payload:  payload,
	}
	log.Hash = processing.ComputeLogHash(hasher, *lastHash, log)
	*lastHash = log.GetHash()
	*seqID++

	return log
}

// reversePosting returns a posting with source and destination swapped.
func reversePosting(p *commonpb.Posting) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      p.GetDestination(),
		Destination: p.GetSource(),
		Amount:      p.GetAmount(),
		Asset:       p.GetAsset(),
	}
}

// writeVolumes writes volume attributes for a posting to make the store consistent.
func writeVolumes(batch *dal.Batch, attrs *attributes.Attributes, posting *commonpb.Posting, ledger string, raftIndex uint64) error {
	sourceKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: ledger, Account: posting.GetSource()},
		Asset:      posting.GetAsset(),
	}
	destKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: ledger, Account: posting.GetDestination()},
		Asset:      posting.GetAsset(),
	}

	err := attrs.Volume.Set(batch, raftIndex, sourceKey.Bytes(), &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: posting.GetAmount(),
	})
	if err != nil {
		return err
	}

	return attrs.Volume.Set(batch, raftIndex, destKey.Bytes(), &raftcmdpb.VolumePair{
		Input:  posting.GetAmount(),
		Output: commonpb.NewUint256FromUint64(0),
	})
}
