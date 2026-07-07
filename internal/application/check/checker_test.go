package check

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
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
	clusterID string

	// In-memory state tracking (mirroring the state machine)
	nextSequenceID         uint64
	lastLogHash            []byte
	ledgers                map[string]*commonpb.LedgerInfo
	boundaries             map[string]*raftcmdpb.LedgerBoundaries
	volumes                map[string]*raftcmdpb.VolumePair
	metadata               map[string]*commonpb.MetadataValue
	idempotency            map[string]*commonpb.IdempotencyKeyValue
	references             map[string]*commonpb.TransactionReferenceValue
	transactionStates      map[string]*commonpb.TransactionState
	currentOpenChapter     *commonpb.Chapter
	closingChapters        []*commonpb.Chapter
	nextLedgerID           uint32
	nextAuditSequenceID    uint64
	lastAuditHash          []byte
	nextChapterID          uint64
	raftIndex              uint64
	pendingLedgerDeletions []string
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
		t:                   t,
		store:               store,
		attrs:               attrs,
		processor:           proc,
		cache:               c,
		clusterID:           "test-cluster",
		nextSequenceID:      1,
		nextLedgerID:        1,
		ledgers:             make(map[string]*commonpb.LedgerInfo),
		boundaries:          make(map[string]*raftcmdpb.LedgerBoundaries),
		volumes:             make(map[string]*raftcmdpb.VolumePair),
		metadata:            make(map[string]*commonpb.MetadataValue),
		idempotency:         make(map[string]*commonpb.IdempotencyKeyValue),
		references:          make(map[string]*commonpb.TransactionReferenceValue),
		transactionStates:   make(map[string]*commonpb.TransactionState),
		nextChapterID:       1,
		nextAuditSequenceID: 1,
		raftIndex:           1,
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

	// Track which keys were modified in this batch
	modifiedVolumes := make(map[string]struct{})
	modifiedMetadata := make(map[string]struct{})
	modifiedTxStates := make(map[string]struct{})

	store := &scopeImpl{
		engine:           e,
		date:             proposal.GetDate(),
		modifiedVolumes:  modifiedVolumes,
		modifiedMetadata: modifiedMetadata,
		modifiedTxStates: modifiedTxStates,
		reverted:         make(map[string]bool),
	}
	resp, procErr := e.processor.ProcessOrders(proposal.GetOrders(), constantCheckScopeFactory{scope: store}, &emitterImpl{engine: e})
	require.NoError(e.t, procErr)

	// CreatedLogs is pre-filtered by ProcessOrders.
	logs := resp.CreatedLogs

	// Write logs and attributes to the store (mimicking WriteSet.Merge)
	batch := e.store.OpenWriteSession()

	defer func() { _ = batch.Cancel() }()

	err := state.AppendLogs(batch, logs)
	require.NoError(e.t, err)

	e.appendAuditEntry(batch, proposal, resp.Logs)

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
			_, err := e.attrs.Volume.Set(batch, canonicalKey, storePair)
			require.NoError(e.t, err)
		}
	}

	// Write only modified metadata attributes
	for key := range modifiedMetadata {
		value, ok := e.metadata[key]

		canonicalKey := []byte(key)
		if ok {
			_, err := e.attrs.Metadata.Set(batch, canonicalKey, value)
			require.NoError(e.t, err)
		} else {
			// Metadata was deleted
			err := e.attrs.Metadata.Delete(batch, canonicalKey)
			require.NoError(e.t, err)
		}
	}

	// Write modified transaction states to Pebble via attributes
	for keyStr := range modifiedTxStates {
		txState := e.transactionStates[keyStr]
		_, err := e.attrs.Transaction.Set(batch, []byte(keyStr), txState)
		require.NoError(e.t, err)
	}

	// Write ledger info
	for _, info := range e.ledgers {
		err := state.SaveLedger(batch, info)
		require.NoError(e.t, err)
		_, err = e.attrs.Ledger.Set(batch, domain.LedgerKey{Name: info.GetName()}.Bytes(), info)
		require.NoError(e.t, err)
	}

	// For deleted ledgers, remove boundary from in-memory state (blocks
	// subsequent operations) but keep all other data — it stays in Pebble
	// until the purge cycle cleans it up.
	for _, ledgerName := range e.pendingLedgerDeletions {
		require.NoError(e.t, e.attrs.Boundary.Delete(batch, domain.LedgerKey{Name: ledgerName}.Bytes()))
		delete(e.boundaries, ledgerName)
	}

	e.pendingLedgerDeletions = nil

	err = state.SetAppliedIndex(batch, e.raftIndex)
	require.NoError(e.t, err)

	err = batch.Commit()
	require.NoError(e.t, err)

	e.raftIndex++

	return logs
}

func (e *testEngine) appendAuditEntry(batch *dal.WriteSession, proposal *raftcmdpb.Proposal, results []*raftcmdpb.CreatedLogOrReference) {
	e.t.Helper()

	hashAlgorithm := commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3
	hashGenerator := processing.NewHashGenerator(hashAlgorithm, e.clusterID)
	serializedOrders := marshalOrdersForTest(proposal.GetOrders())
	minLogSeq, maxLogSeq := testLogSequenceRange(results)

	entry := &auditpb.AuditEntry{
		Sequence:    e.nextAuditSequenceID,
		Timestamp:   proposal.GetDate(),
		ProposalId:  proposal.GetId(),
		OrderCount:  uint32(len(proposal.GetOrders())),
		HashVersion: uint32(hashAlgorithm),
		Outcome: &auditpb.AuditEntry_Success{
			Success: &auditpb.AuditSuccess{
				MinLogSequence: minLogSeq,
				MaxLogSequence: maxLogSeq,
			},
		},
	}

	items := testAuditItems(serializedOrders, results)

	headerPayload, err := state.BuildHashedHeaderPayload(entry)
	require.NoError(e.t, err)

	hashSlices := make([][]byte, 0, 1+len(items))
	hashSlices = append(hashSlices, headerPayload)

	for _, item := range items {
		hashSlices = append(hashSlices, state.BuildPerItemPayload(item))
	}

	_, auditHash := hashGenerator.Compute(nil, e.lastAuditHash, hashSlices)
	entry.Hash = auditHash

	batch.KeyBuilder.
		PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).
		PutUint64(entry.GetSequence())
	require.NoError(e.t, batch.SetProto(batch.KeyBuilder.Consume(), entry))

	for _, item := range items {
		batch.KeyBuilder.
			PutZonePrefix(dal.ZoneCold, dal.SubColdAuditItem).
			PutUint64(entry.GetSequence()).
			PutUint32(item.GetOrderIndex())
		require.NoError(e.t, batch.SetProto(batch.KeyBuilder.Consume(), item))
	}

	e.lastAuditHash = auditHash
	e.nextAuditSequenceID++
}

func testLogSequenceRange(results []*raftcmdpb.CreatedLogOrReference) (uint64, uint64) {
	var minSeq, maxSeq uint64

	for _, result := range results {
		var seq uint64
		if created := result.GetCreatedLog(); created != nil {
			seq = created.GetSequence()
		} else {
			seq = result.GetReferenceSequence()
		}

		if seq == 0 {
			continue
		}

		if minSeq == 0 || seq < minSeq {
			minSeq = seq
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	return minSeq, maxSeq
}

// marshalOrdersForTest mirrors the apply-path helper so tests feed the
// hash generator and AuditItem persistence the same per-order byte
// slices the real FSM does.
func marshalOrdersForTest(orders []*raftcmdpb.Order) [][]byte {
	out := make([][]byte, len(orders))
	for i, order := range orders {
		out[i] = order.MarshalDeterministicVT(nil)
	}

	return out
}

func testAuditItems(serializedOrders [][]byte, results []*raftcmdpb.CreatedLogOrReference) []*auditpb.AuditItem {
	items := make([]*auditpb.AuditItem, len(serializedOrders))

	for i, payload := range serializedOrders {
		item := &auditpb.AuditItem{
			OrderIndex:      uint32(i),
			SerializedOrder: payload,
		}

		if i < len(results) {
			if created := results[i].GetCreatedLog(); created != nil {
				item.LogSequence = created.GetSequence()
			} else if refSeq := results[i].GetReferenceSequence(); refSeq > 0 {
				item.LogSequence = refSeq
			}
		}

		items[i] = item
	}

	return items
}

// scopeImpl implements processing.Scope using the testEngine's in-memory state.
type scopeImpl struct {
	engine           *testEngine
	date             *commonpb.Timestamp
	modifiedVolumes  map[string]struct{}
	modifiedMetadata map[string]struct{}
	modifiedTxStates map[string]struct{}
	// In-memory reverted status (bitset-like, not persisted to Pebble)
	reverted map[string]bool
}

// constantCheckScopeFactory yields the same Scope for every NewScope call.
// The checker's per-order coverage is enforced upstream (proposal.Validate);
// here we just need a stub that returns the test's scopeImpl.
type constantCheckScopeFactory struct{ scope processing.Scope }

func (f constantCheckScopeFactory) NewScope(_ []byte) (processing.Scope, error) {
	return f.scope, nil
}

func (f constantCheckScopeFactory) NewProposalScope() (processing.Scope, error) {
	return f.scope, nil
}

// ForOrder is a no-op for the test scopeImpl: the checker runs without
// an FSM-side coverage gate, so per-order narrowing returns the same
// scope (all keys remain admitted).
func (s *scopeImpl) ForOrder(_, _ []byte) processing.Scope { return s }

// CheckCoverage is a no-op for the test scopeImpl: there is no coverage
// to enforce, every read is admitted.
func (s *scopeImpl) CheckCoverage(_ byte, _ []byte) error { return nil }

// scopeFuncAccessor is the generic test-side Accessor: each operation is
// a function closure wired against the scope's in-memory maps. The Put /
// Delete functions tolerate being nil for kinds that the checker tests
// never mutate, so callers stay terse for the read-only kinds.
type scopeFuncAccessor[K processing.AccessorKey, V any, R any] struct {
	get    func(K) (R, error)
	put    func(K, V)
	delete func(K)
}

func (a *scopeFuncAccessor[K, V, R]) Get(key K) (R, error) {
	return a.get(key)
}

func (a *scopeFuncAccessor[K, V, R]) Put(key K, value V) {
	if a.put != nil {
		a.put(key, value)
	}
}

func (a *scopeFuncAccessor[K, V, R]) Delete(key K) {
	if a.delete != nil {
		a.delete(key)
	}
}

func (s *scopeImpl) Ledgers() processing.Accessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader] {
	return &scopeFuncAccessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader]{
		get: func(key domain.LedgerKey) (commonpb.LedgerInfoReader, error) {
			info, ok := s.engine.ledgers[key.Name]
			if !ok {
				return nil, domain.ErrNotFound
			}

			return info.AsReader(), nil
		},
		put: func(key domain.LedgerKey, info *commonpb.LedgerInfo) {
			s.engine.ledgers[key.Name] = info
		},
	}
}

func (s *scopeImpl) Boundaries() processing.Accessor[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader] {
	return &scopeFuncAccessor[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader]{
		get: func(key domain.LedgerKey) (raftcmdpb.LedgerBoundariesReader, error) {
			b, ok := s.engine.boundaries[key.Name]
			if !ok {
				return nil, domain.ErrNotFound
			}

			return b.AsReader(), nil
		},
		put: func(key domain.LedgerKey, b *raftcmdpb.LedgerBoundaries) {
			s.engine.boundaries[key.Name] = b
		},
	}
}

func (s *scopeImpl) Volumes() processing.Accessor[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader] {
	return &scopeFuncAccessor[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader]{
		get: func(key domain.VolumeKey) (raftcmdpb.VolumePairReader, error) {
			vp, ok := s.engine.volumes[string(key.Bytes())]
			if !ok {
				// Simulate preloaded zero volumes (in production, admission always preloads)
				vp = &raftcmdpb.VolumePair{
					Input:  commonpb.NewUint256FromUint64(0),
					Output: commonpb.NewUint256FromUint64(0),
				}
			}

			return vp.AsReader(), nil
		},
		put: func(key domain.VolumeKey, value *raftcmdpb.VolumePair) {
			k := string(key.Bytes())
			s.engine.volumes[k] = value
			s.modifiedVolumes[k] = struct{}{}
		},
	}
}

func (s *scopeImpl) AccountMetadata() processing.Accessor[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader] {
	return &scopeFuncAccessor[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]{
		get: func(key domain.MetadataKey) (commonpb.MetadataValueReader, error) {
			v, ok := s.engine.metadata[string(key.Bytes())]
			if !ok {
				return nil, domain.ErrNotFound
			}

			return v.AsReader(), nil
		},
		put: func(key domain.MetadataKey, value *commonpb.MetadataValue) {
			k := string(key.Bytes())
			s.engine.metadata[k] = value
			s.modifiedMetadata[k] = struct{}{}
		},
		delete: func(key domain.MetadataKey) {
			k := string(key.Bytes())
			delete(s.engine.metadata, k)
			s.modifiedMetadata[k] = struct{}{}
		},
	}
}

func (s *scopeImpl) LedgerMetadata() processing.Accessor[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader] {
	return &scopeFuncAccessor[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]{
		get: func(_ domain.LedgerMetadataKey) (commonpb.MetadataValueReader, error) {
			return nil, domain.ErrNotFound
		},
	}
}

func (s *scopeImpl) TransactionReferences() processing.Accessor[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader] {
	return &scopeFuncAccessor[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader]{
		get: func(key domain.TransactionReferenceKey) (commonpb.TransactionReferenceValueReader, error) {
			v, ok := s.engine.references[string(key.Bytes())]
			if !ok {
				return nil, domain.ErrNotFound
			}

			return v.AsReader(), nil
		},
		put: func(key domain.TransactionReferenceKey, value *commonpb.TransactionReferenceValue) {
			s.engine.references[string(key.Bytes())] = value
		},
	}
}

func (s *scopeImpl) TransactionStates() processing.Accessor[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader] {
	return &scopeFuncAccessor[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader]{
		get: func(key domain.TransactionKey) (commonpb.TransactionStateReader, error) {
			st := s.engine.transactionStates[string(key.Bytes())]
			if st == nil {
				return nil, nil
			}

			return st.AsReader(), nil
		},
		put: func(key domain.TransactionKey, txState *commonpb.TransactionState) {
			k := string(key.Bytes())
			s.engine.transactionStates[k] = txState
			s.modifiedTxStates[k] = struct{}{}
		},
	}
}

func (s *scopeImpl) PreparedQueries() processing.Accessor[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader] {
	return &scopeFuncAccessor[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader]{
		get: func(_ domain.PreparedQueryKey) (commonpb.PreparedQueryReader, error) {
			return nil, nil
		},
	}
}

func (s *scopeImpl) Indexes() processing.Accessor[domain.IndexKey, *commonpb.Index, commonpb.IndexReader] {
	return &scopeFuncAccessor[domain.IndexKey, *commonpb.Index, commonpb.IndexReader]{
		get: func(_ domain.IndexKey) (commonpb.IndexReader, error) {
			return nil, domain.ErrNotFound
		},
	}
}

func (s *scopeImpl) GetReverted(key domain.TransactionKey) (bool, error) {
	return s.reverted[string(key.Bytes())], nil
}

func (s *scopeImpl) PutReverted(key domain.TransactionKey, reverted bool) {
	s.reverted[string(key.Bytes())] = reverted
}

func (s *scopeImpl) AddSigningKey(_ string, _ []byte, _ string)                {}
func (s *scopeImpl) RemoveSigningKey(_ string)                                 {}
func (s *scopeImpl) GetSigningKeyChildren(_ string) []string                   { return nil }
func (s *scopeImpl) SetRequireSignatures(_ bool)                               {}
func (s *scopeImpl) SetMaintenanceMode(_ bool)                                 {}
func (s *scopeImpl) GetSinkConfig(_ string) (commonpb.SinkConfigReader, error) { return nil, nil }

func (s *scopeImpl) GetLastLogHash() []byte {
	return s.engine.lastLogHash
}

func (s *scopeImpl) SetLastLogHash(hash []byte) {
	s.engine.lastLogHash = hash
}

func (s *scopeImpl) GetNextSequenceID() uint64 {
	return s.engine.nextSequenceID
}

func (s *scopeImpl) IncrementNextSequenceID() uint64 {
	id := s.engine.nextSequenceID
	s.engine.nextSequenceID++

	return id
}

func (s *scopeImpl) GetNextLedgerID() uint32 {
	return s.engine.nextLedgerID
}

func (s *scopeImpl) IncrementNextLedgerID() uint32 {
	id := s.engine.nextLedgerID
	s.engine.nextLedgerID++

	return id
}

func (s *scopeImpl) GetDate() commonpb.TimestampReader {
	if s.date == nil {
		return nil
	}

	return s.date.AsReader()
}

func (s *scopeImpl) GetCurrentOpenChapter() (commonpb.ChapterReader, bool) {
	if s.engine.currentOpenChapter != nil {
		return s.engine.currentOpenChapter.AsReader(), true
	}

	return nil, false
}

func (s *scopeImpl) GetClosingChapters() []commonpb.ChapterReader {
	if s.engine.closingChapters == nil {
		return nil
	}

	out := make([]commonpb.ChapterReader, len(s.engine.closingChapters))
	for i, c := range s.engine.closingChapters {
		out[i] = c.AsReader()
	}

	return out
}

func (s *scopeImpl) GetClosingChapterByID(chapterID uint64) (commonpb.ChapterReader, bool) {
	for _, p := range s.engine.closingChapters {
		if p.GetId() == chapterID {
			return p.AsReader(), true
		}
	}

	return nil, false
}

func (s *scopeImpl) SetCurrentOpenChapter(chapter *commonpb.Chapter) {
	s.engine.currentOpenChapter = chapter
}

func (s *scopeImpl) AddClosingChapter(chapter *commonpb.Chapter) {
	s.engine.closingChapters = append(s.engine.closingChapters, chapter)
}

func (s *scopeImpl) RemoveClosingChapter(chapterID uint64) {
	for i, p := range s.engine.closingChapters {
		if p.GetId() == chapterID {
			s.engine.closingChapters = append(s.engine.closingChapters[:i], s.engine.closingChapters[i+1:]...)

			return
		}
	}
}

func (s *scopeImpl) GetNextChapterID() uint64 {
	return s.engine.nextChapterID
}

func (s *scopeImpl) IncrementNextChapterID() uint64 {
	id := s.engine.nextChapterID
	s.engine.nextChapterID++

	return id
}

func (s *scopeImpl) GetChapterByID(_ uint64) (commonpb.ChapterReader, bool) {
	return nil, false
}

func (s *scopeImpl) GetNextAuditSequenceID() uint64 { return 0 }

func (s *scopeImpl) UpdateChapter(_ *commonpb.Chapter) {}

func (s *scopeImpl) GetPreparedQuery(_ string, _ string) (commonpb.PreparedQueryReader, error) {
	return nil, nil
}
func (s *scopeImpl) PutPreparedQuery(_ string, _ *commonpb.PreparedQuery)         {}
func (s *scopeImpl) DeletePreparedQuery(_ string, _ string)                       {}
func (s *scopeImpl) GetNumscriptLatestVersion(_ string, _ string) (string, error) { return "", nil }
func (s *scopeImpl) NumscriptVersionExists(_ string, _, _ string) (bool, error) {
	return false, nil
}
func (s *scopeImpl) PutNumscript(_ string, _ *commonpb.NumscriptInfo)      {}
func (s *scopeImpl) DeleteNumscriptLatest(_ string, _ string)              {}
func (s *scopeImpl) GetNextQueryCheckpointID() uint64                      { return 1 }
func (s *scopeImpl) IncrementNextQueryCheckpointID() uint64                { return 1 }
func (s *scopeImpl) SaveQueryCheckpoint(_ *raftcmdpb.QueryCheckpointState) {}
func (s *scopeImpl) DeleteQueryCheckpoint(_ uint64)                        {}
func (s *scopeImpl) ResolveNumscriptContent(_ string, _, _ string) (commonpb.NumscriptInfoReader, error) {
	return nil, nil
}

// Index registry stubs — the check engine doesn't exercise CreateIndex /
// DropIndex orders, so the test scope can no-op these. Tests that need
// real index behavior use the MockScope generated by mockgen.
func (s *scopeImpl) GetIndex(_ domain.IndexKey) (commonpb.IndexReader, error) {
	return nil, domain.ErrNotFound
}
func (s *scopeImpl) PutIndex(_ domain.IndexKey, _ *commonpb.Index) {}
func (s *scopeImpl) DeleteIndex(_ domain.IndexKey)                 {}

// emitterImpl satisfies processing.SignalSink for replay tests. Only
// the DeleteLedger payload has observable state
// (engine.pendingLedgerDeletions); every other log payload is a no-op
// because the checker reconstructs persisted projections from Pebble,
// not from in-memory signal accumulation.
type emitterImpl struct{ engine *testEngine }

func (e *emitterImpl) Absorb(_ *raftcmdpb.Order, log *commonpb.Log) {
	if p, ok := log.GetPayload().GetType().(*commonpb.LogPayload_DeleteLedger); ok {
		e.engine.pendingLedgerDeletions = append(e.engine.pendingLedgerDeletions, p.DeleteLedger.GetName())
	}
}

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

func addAccountTypeOrder(ledger, name, pattern string, persistence commonpb.AccountTypePersistence) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{
						AddAccountType: &raftcmdpb.AddAccountTypeOrder{
							AccountType: &commonpb.AccountType{
								Name:        name,
								Pattern:     pattern,
								Persistence: persistence,
							},
						},
					},
					},
				},
			},
		},
	}
}

func deleteLedgerOrder(name string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: name,
				Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedger{
					DeleteLedger: &raftcmdpb.DeleteLedgerOrder{},
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

func createTransactionWithMetadataOrder(ledger string, force bool, metadata map[string]string, accountMeta map[string]*commonpb.MetadataMap, postings ...*commonpb.Posting) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
						CreateTransaction: &raftcmdpb.CreateTransactionOrder{
							Postings:        postings,
							Force:           force,
							Metadata:        commonpb.MetadataFromGoMap(metadata),
							AccountMetadata: accountMeta,
						},
					},
					},
				},
			},
		},
	}
}

func revertTransactionOrder(ledger string, txID uint64, originalPostings []*commonpb.Posting) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
						RevertTransaction: &raftcmdpb.RevertTransactionOrder{
							TransactionId:    txID,
							OriginalPostings: originalPostings,
						},
					},
					},
				},
			},
		},
	}
}

func saveAccountMetadataOrder(ledger, account string, metadata map[string]string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
						AddMetadata: &raftcmdpb.SaveMetadataOrder{
							Target: &commonpb.Target{
								Target: &commonpb.Target_Account{
									Account: &commonpb.TargetAccount{
										Addr: account,
									},
								},
							},
							Metadata: commonpb.MetadataFromGoMap(metadata),
						},
					},
					},
				},
			},
		},
	}
}

func deleteAccountMetadataOrder(ledger, account, key string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
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
			},
		},
	}
}

// collectCheckErrors runs the checker and returns all error events.
func collectCheckErrors(t *testing.T, store *dal.Store, attrs *attributes.Attributes) []*servicepb.CheckStoreError {
	t.Helper()

	checker := NewChecker(store, attrs, "test-cluster", nil, nil, logging.Testing())

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
		map[string]*commonpb.MetadataMap{
			"customer:dave": commonpb.MetadataMapFromGoMap(map[string]string{
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

func TestCheckerReplaysEphemeralPurgeAtProposalBoundary(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	engine.processAndCommit(createLedgerOrder("ledger"))
	engine.processAndCommit(addAccountTypeOrder(
		"ledger",
		"orders",
		"orders:{id}",
		commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
	))

	fund := createTransactionOrder("ledger", true,
		newPosting("world", "orders:1", "USD", 5),
	)
	drain := createTransactionOrder("ledger", true,
		newPosting("orders:1", "world", "USD", 5),
	)
	refund := createTransactionOrder("ledger", true,
		newPosting("world", "orders:1", "USD", 3),
	)

	engine.processAndCommit(fund, drain, refund)

	errors := collectCheckErrors(t, engine.store, engine.attrs)
	require.Empty(t, errors, "ephemeral purge must use the proposal boundary, not each transaction log")
}

// TestCheckerDetectsSequenceGap verifies the checker detects missing log entries.
func TestCheckerDetectsSequenceGap(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := attributes.New()

	// Write log at sequence 1
	log1 := &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreatedLedgerLog{
					Name:      "test",
					CreatedAt: &commonpb.Timestamp{Data: 1700000000},
				},
			},
		},
	}
	// Hash chain is now verified via audit entries, not per-log.

	// Skip sequence 2 and write log at sequence 3
	log3 := &commonpb.Log{
		Sequence: 3,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreatedLedgerLog{
					Name:      "test2",
					CreatedAt: &commonpb.Timestamp{Data: 1700000002},
				},
			},
		},
	}
	// Even with correct chaining from log1, the gap will be detected

	batch := store.OpenWriteSession()
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{log1, log3}))
	require.NoError(t, state.SaveLedger(batch, log1.GetPayload().GetCreateLedger().ToLedgerInfo()))
	require.NoError(t, state.SaveLedger(batch, log3.GetPayload().GetCreateLedger().ToLedgerInfo()))
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

	checker := NewChecker(engine.store, engine.attrs, engine.clusterID, nil, nil, logging.Testing())

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
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
						AddMetadata: &raftcmdpb.SaveMetadataOrder{
							Target: &commonpb.Target{
								Target: &commonpb.Target_TransactionId{TransactionId: txID},
							},
							Metadata: commonpb.MetadataFromGoMap(metadata),
						},
					},
					},
				},
			},
		},
	}
}

func deleteTransactionMetadataOrder(ledger string, txID uint64, key string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
						DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
							Target: &commonpb.Target{
								Target: &commonpb.Target_TransactionId{TransactionId: txID},
							},
							Key: key,
						},
					},
					},
				},
			},
		},
	}
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

	// Write a spurious transaction state to Pebble for tx 1.
	// Use a high raft index so it overrides the correct state.
	batch := engine.store.OpenWriteSession()
	txKey := domain.TransactionKey{LedgerName: "test", ID: 1}
	_, err := engine.attrs.Transaction.Set(batch, txKey.Bytes(), &commonpb.TransactionState{
		CreatedByLog: 999,
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

// TestCheckerDetectsLiveOnlyTransaction is a regression test for #347:
// compareTransactions used to build allKeys from replay ∪ baseline only,
// so a transaction present in the live store without a matching log entry
// (fabricated state, FSM bug, direct Pebble write) escaped detection. The
// fix widens allKeys with the live store and reports the rogue entry.
func TestCheckerDetectsLiveOnlyTransaction(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	engine.processAndCommit(createLedgerOrder("test"))
	engine.processAndCommit(createTransactionOrder("test", true,
		newPosting("world", "user:alice", "USD", 1000),
	))

	// Write a transaction state into live attrs with an ID that no log ever
	// produced (the only tx the log produced is ID 1). This simulates
	// fabricated state or a direct Pebble write.
	batch := engine.store.OpenWriteSession()
	rogueKey := domain.TransactionKey{LedgerName: "test", ID: 9999}
	_, err := engine.attrs.Transaction.Set(batch, rogueKey.Bytes(), &commonpb.TransactionState{
		CreatedByLog: 9999,
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	errors := collectCheckErrors(t, engine.store, engine.attrs)

	var (
		hasRogue bool
		count    int
	)

	for _, e := range errors {
		if e.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH &&
			e.GetTransactionId() == 9999 {
			hasRogue = true
		}

		count++
	}

	require.True(t, hasRogue, "checker must flag a transaction present in live but absent from replay/baseline (got %d errors total)", count)
}

// TestCheckerDetectsSymmetricVolumeMutation is a regression test for #347:
// the previous "input == output → skip" heuristic in compareVolumes masked
// every symmetric mutation, not just transient accounts. A normal account
// whose volume was tampered with from (100, 100) to (999, 999) went
// undetected. With audit-driven exclusion the heuristic is gone — only
// accounts that the audit log explicitly marks as transient or purged are
// skipped, so a tampered normal account triggers VOLUME_MISMATCH.
func TestCheckerDetectsSymmetricVolumeMutation(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	engine.processAndCommit(createLedgerOrder("test"))
	// Fund: world → alice 100 USD  (alice now has input=100, output=0)
	engine.processAndCommit(createTransactionOrder("test", true,
		newPosting("world", "user:alice", "USD", 100),
	))
	// Drain back: alice → world 100 USD  (alice now has input=100, output=100)
	engine.processAndCommit(createTransactionOrder("test", false,
		newPosting("user:alice", "world", "USD", 100),
	))

	// Tamper with the live volume on a NORMAL account: replace (100, 100)
	// with (999, 999). Both sides remain balanced, so the old heuristic
	// would skip the comparison entirely; the audit log doesn't list
	// user:alice as transient/purged, so the new path reports it.
	batch := engine.store.OpenWriteSession()
	tamperedKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "user:alice"},
		Asset:      "USD",
	}
	_, err := engine.attrs.Volume.Set(batch, tamperedKey.Bytes(), &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(999),
		Output: commonpb.NewUint256FromUint64(999),
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	errors := collectCheckErrors(t, engine.store, engine.attrs)

	var hasVolErr bool

	for _, e := range errors {
		if e.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH &&
			e.GetAccount() == "user:alice" {
			hasVolErr = true
		}
	}

	require.True(t, hasVolErr, "checker must flag a symmetric volume mutation on a non-transient account")
}

// TestCheckerSurfacesCorruptAuditEntry is a regression test for the #399
// review finding: the audit-cursor loops in collectExcludedAccounts and
// verifyAuditHashChain used to treat ANY cursor error as end-of-cursor,
// silently truncating the scan. A corrupted audit entry then either let
// the hash chain partially-verify or skipped later transient/purged
// accounts in the exclusion set, masking the very integrity issue the
// checker is meant to detect. Both loops now distinguish io.EOF from
// real errors and propagate the latter as a Check error.
func TestCheckerSurfacesCorruptAuditEntry(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	// Need at least one log so Check doesn't short-circuit on empty store.
	engine.processAndCommit(createLedgerOrder("test"))

	// Inject a malformed audit entry directly into Pebble at sequence 1.
	// The ProtoCursor used by query.ReadAuditEntries will fail to
	// UnmarshalVT these bytes, returning a non-EOF error.
	batch := engine.store.OpenWriteSession()
	auditKey := dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).
		PutUint64(1).
		Build()
	require.NoError(t, batch.SetBytes(auditKey, []byte{0xFF, 0xFF, 0xFF, 0xFF}))
	require.NoError(t, batch.Commit())

	checker := NewChecker(engine.store, engine.attrs, engine.clusterID, nil, nil, logging.Testing())
	err := checker.Check(context.Background(), func(_ *servicepb.CheckStoreEvent) {})

	// Before the fix: Check returned nil; the cursor break swallowed the
	// unmarshal error and the integrity scan completed "successfully".
	// After the fix: the error is surfaced via the Check return value.
	require.Error(t, err, "Check must surface a non-EOF cursor error from an audit scan")
	require.Contains(t, err.Error(), "audit entry",
		"the wrapped error should reference the failed audit-entry read (got: %v)", err)
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

	// Build a valid log chain manually:
	// 1. Create ledger "test"
	// 2. Create transaction (tx 1): world -> alice 1000 USD
	// 3. Revert tx 1 (creates tx 2)
	// 4. Revert tx 1 AGAIN (double revert — should be detected)

	var lastHash []byte

	seqID := uint64(1)

	// Log 1: Create ledger
	log1 := buildLog(&lastHash, &seqID, &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{
				Name: "test", CreatedAt: &commonpb.Timestamp{Data: 1700000000},
			},
		},
	})

	// Log 2: Create transaction (tx 1)
	posting := newPosting("world", "user:alice", "USD", 1000)
	log2 := buildLog(&lastHash, &seqID, &commonpb.LogPayload{
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
	log3 := buildLog(&lastHash, &seqID, &commonpb.LogPayload{
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
	log4 := buildLog(&lastHash, &seqID, &commonpb.LogPayload{
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

	batch := store.OpenWriteSession()
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{log1, log2, log3, log4}))
	require.NoError(t, state.SaveLedger(batch, log1.GetPayload().GetCreateLedger().ToLedgerInfo()))
	require.NoError(t, writeVolumes(batch, attrs, posting, "test"))
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

	var lastHash []byte

	seqID := uint64(1)

	// Log 1: Create ledger
	log1 := buildLog(&lastHash, &seqID, &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{
				Name: "test", CreatedAt: &commonpb.Timestamp{Data: 1700000000},
			},
		},
	})

	// Log 2: Revert tx 999 (which was never created)
	posting := newPosting("user:alice", "world", "USD", 1000)
	log2 := buildLog(&lastHash, &seqID, &commonpb.LogPayload{
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

	batch := store.OpenWriteSession()
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{log1, log2}))
	require.NoError(t, state.SaveLedger(batch, log1.GetPayload().GetCreateLedger().ToLedgerInfo()))
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

// buildLog creates a log entry for testing.
func buildLog(_ *[]byte, seqID *uint64, payload *commonpb.LogPayload) *commonpb.Log {
	log := &commonpb.Log{
		Sequence: *seqID,
		Payload:  payload,
	}
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
func writeVolumes(batch *dal.WriteSession, attrs *attributes.Attributes, posting *commonpb.Posting, ledger string) error {
	sourceKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: posting.GetSource()},
		Asset:      posting.GetAsset(),
	}
	destKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: posting.GetDestination()},
		Asset:      posting.GetAsset(),
	}

	_, err := attrs.Volume.Set(batch, sourceKey.Bytes(), &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: posting.GetAmount(),
	})
	if err != nil {
		return err
	}

	_, err = attrs.Volume.Set(batch, destKey.Bytes(), &raftcmdpb.VolumePair{
		Input:  posting.GetAmount(),
		Output: commonpb.NewUint256FromUint64(0),
	})

	return err
}

// TestCompareExclusionProjections_Identical asserts the corruption-detection
// pass stays silent when the stored AppliedProposal/LedgerLog projections
// match the replay-derived set. This is the happy path the FSM produces on
// every well-formed proposal.
func TestCompareExclusionProjections_Identical(t *testing.T) {
	t.Parallel()

	set := excludedVolumesSet{
		"L1": map[domain.AccountAssetKey]struct{}{
			{Account: "ephemeral:a", Asset: "USD"}: {},
			{Account: "transient:b", Asset: "EUR"}: {},
		},
		"L2": map[domain.AccountAssetKey]struct{}{
			{Account: "x", Asset: "JPY"}: {},
		},
	}
	other := excludedVolumesSet{
		"L1": map[domain.AccountAssetKey]struct{}{
			{Account: "ephemeral:a", Asset: "USD"}: {},
			{Account: "transient:b", Asset: "EUR"}: {},
		},
		"L2": map[domain.AccountAssetKey]struct{}{
			{Account: "x", Asset: "JPY"}: {},
		},
	}

	var events []*servicepb.CheckStoreEvent
	compareExclusionProjections(set, other, func(e *servicepb.CheckStoreEvent) {
		events = append(events, e)
	})

	require.Empty(t, events, "compareExclusionProjections must stay silent on matching sets")
}

// TestCompareExclusionProjections_ExtraInStored mimics a corruption on the
// AppliedProposal / LedgerLog projection caches (e.g. a tampered byte on
// disk adding a spurious exclusion entry). The checker must surface it so
// the operator can rebuild the projection from the audit log.
func TestCompareExclusionProjections_ExtraInStored(t *testing.T) {
	t.Parallel()

	stored := excludedVolumesSet{
		"L1": map[domain.AccountAssetKey]struct{}{
			{Account: "ephemeral:a", Asset: "USD"}:      {},
			{Account: "tampered:phantom", Asset: "USD"}: {}, // not in derived
		},
	}
	derived := excludedVolumesSet{
		"L1": map[domain.AccountAssetKey]struct{}{
			{Account: "ephemeral:a", Asset: "USD"}: {},
		},
	}

	var events []*servicepb.CheckStoreEvent
	compareExclusionProjections(stored, derived, func(e *servicepb.CheckStoreEvent) {
		events = append(events, e)
	})

	require.Len(t, events, 1)
	err := events[0].GetError()
	require.NotNil(t, err)
	require.Equal(t,
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_EXCLUSION_RECORD_MISMATCH,
		err.GetErrorType())
	require.Equal(t, "L1", err.GetLedger())
	require.Equal(t, "tampered:phantom", err.GetAccount())
	require.Equal(t, "USD", err.GetAsset())
}

// TestCompareExclusionProjections_MissingFromStored covers the symmetric
// failure mode: the projection is missing a record the replay says should
// be there (e.g. a partial write). Also a tampering / hardware-fault
// signal — the index builder will produce a wrong index in this case.
func TestCompareExclusionProjections_MissingFromStored(t *testing.T) {
	t.Parallel()

	stored := excludedVolumesSet{
		"L1": map[domain.AccountAssetKey]struct{}{
			{Account: "ephemeral:a", Asset: "USD"}: {},
		},
	}
	derived := excludedVolumesSet{
		"L1": map[domain.AccountAssetKey]struct{}{
			{Account: "ephemeral:a", Asset: "USD"}: {},
			{Account: "ephemeral:b", Asset: "EUR"}: {}, // missing from stored
		},
	}

	var events []*servicepb.CheckStoreEvent
	compareExclusionProjections(stored, derived, func(e *servicepb.CheckStoreEvent) {
		events = append(events, e)
	})

	require.Len(t, events, 1)
	err := events[0].GetError()
	require.NotNil(t, err)
	require.Equal(t,
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_EXCLUSION_RECORD_MISMATCH,
		err.GetErrorType())
	require.Equal(t, "L1", err.GetLedger())
	require.Equal(t, "ephemeral:b", err.GetAccount())
	require.Equal(t, "EUR", err.GetAsset())
}

// indexCheckerFor wires a Checker against a fresh store and writes the given
// Index entries directly so the projection-compare path can be exercised
// without driving CreateIndex orders through the full pipeline.
func indexCheckerFor(t *testing.T, stored map[domain.IndexKey]*commonpb.Index) (*Checker, *dal.Store) {
	t.Helper()

	store := createTestStore(t)
	attrs := attributes.New()

	if len(stored) > 0 {
		batch := store.OpenWriteSession()
		for key, idx := range stored {
			_, err := attrs.Index.Set(batch, key.Bytes(), idx)
			require.NoError(t, err)
		}
		require.NoError(t, batch.Commit())
	}

	ctx := logging.TestingContext()

	return NewChecker(store, attrs, "test-cluster", nil, nil, logging.FromContext(ctx)), store
}

// TestCompareIndexes_Identical pins the happy path: when the SubAttrIndex
// projection matches the audit-derived set, the verifier stays silent.
func TestCompareIndexes_Identical(t *testing.T) {
	t.Parallel()

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	key := domain.IndexKey{LedgerName: "L1", Canonical: indexes.Canonical(id)}

	checker, store := indexCheckerFor(t, map[domain.IndexKey]*commonpb.Index{
		key: {Id: id, Ledger: "L1"},
	})

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	var events []*servicepb.CheckStoreEvent
	checker.compareIndexes(reader, map[domain.IndexKey]*commonpb.Index{
		key: {Id: id, Ledger: "L1"},
	}, nil, nil, false, nil, func(e *servicepb.CheckStoreEvent) { events = append(events, e) })

	require.Empty(t, events, "compareIndexes must stay silent on matching sets")
}

// TestCompareIndexes_ExtraInStored covers the tampering shape: a registry
// entry that has no matching CreateIndex in the audit chain. The checker
// must flag it so the audit-derived projection can be rebuilt.
func TestCompareIndexes_ExtraInStored(t *testing.T) {
	t.Parallel()

	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	storedKey := domain.IndexKey{LedgerName: "phantom", Canonical: indexes.Canonical(id)}

	checker, store := indexCheckerFor(t, map[domain.IndexKey]*commonpb.Index{
		storedKey: {Id: id, Ledger: "phantom"},
	})

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	var events []*servicepb.CheckStoreEvent
	checker.compareIndexes(reader, map[domain.IndexKey]*commonpb.Index{}, nil, nil, false, nil, func(e *servicepb.CheckStoreEvent) { events = append(events, e) })

	require.Len(t, events, 1)
	require.Equal(t,
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
		events[0].GetError().GetErrorType())
	require.Equal(t, "phantom", events[0].GetError().GetLedger())
}

// TestCompareIndexes_MissingFromStored covers the symmetric failure mode: an
// expected entry derived from the audit log is absent from the projection
// (e.g. a partial write or post-deletion resurrection cleanup that wiped a
// live row). The checker surfaces it as INDEX_MISMATCH.
func TestCompareIndexes_MissingFromStored(t *testing.T) {
	t.Parallel()

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "tier")
	key := domain.IndexKey{LedgerName: "L2", Canonical: indexes.Canonical(id)}

	checker, store := indexCheckerFor(t, nil)

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	var events []*servicepb.CheckStoreEvent
	checker.compareIndexes(reader, map[domain.IndexKey]*commonpb.Index{
		key: {Id: id, Ledger: "L2"},
	}, nil, nil, false, nil, func(e *servicepb.CheckStoreEvent) { events = append(events, e) })

	require.Len(t, events, 1)
	require.Equal(t,
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
		events[0].GetError().GetErrorType())
	require.Equal(t, "L2", events[0].GetError().GetLedger())
}

// TestCompareIndexes_LedgerDrift verifies the identity-comparison branch:
// a stored entry whose Ledger field disagrees with the audit-derived ground
// truth (e.g. a tampered row moved between ledgers under the same canonical
// key) must surface a mismatch instead of silently sharing the slot.
func TestCompareIndexes_LedgerDrift(t *testing.T) {
	t.Parallel()

	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
	key := domain.IndexKey{LedgerName: "L1", Canonical: indexes.Canonical(id)}

	checker, store := indexCheckerFor(t, map[domain.IndexKey]*commonpb.Index{
		key: {Id: id, Ledger: "tampered"},
	})

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	var events []*servicepb.CheckStoreEvent
	checker.compareIndexes(reader, map[domain.IndexKey]*commonpb.Index{
		key: {Id: id, Ledger: "L1"},
	}, nil, nil, false, nil, func(e *servicepb.CheckStoreEvent) { events = append(events, e) })

	require.Len(t, events, 1)
	require.Equal(t,
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
		events[0].GetError().GetErrorType())
	require.Contains(t, events[0].GetError().GetMessage(), "stored Ledger")
}

// TestCompareIndexes_BucketScopeIgnored anchors the documented gap: bucket-
// scoped Index rows (LedgerName == "", reserved for audit-style indexes in
// #436) have no audit-chain producer yet, so the verifier MUST NOT emit a
// false positive on them. The contract is to skip silently until the
// producer lands and threads an audit-bound order through the same
// machinery.
func TestCompareIndexes_BucketScopeIgnored(t *testing.T) {
	t.Parallel()

	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	bucketKey := domain.IndexKey{LedgerName: "", Canonical: indexes.Canonical(id)}

	checker, store := indexCheckerFor(t, map[domain.IndexKey]*commonpb.Index{
		bucketKey: {Id: id, Ledger: ""},
	})

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	var events []*servicepb.CheckStoreEvent
	checker.compareIndexes(reader, map[domain.IndexKey]*commonpb.Index{}, nil, nil, false, nil, func(e *servicepb.CheckStoreEvent) { events = append(events, e) })

	require.Empty(t, events, "bucket-scoped entries must be silently skipped until a producer lands")
}

// TestCompareIndexes_ArchiveOrphanIgnored anchors the archive-boundary
// guard: when a stored entry has no matching CreateIndex / DropIndex /
// RemovedMetadataFieldType in the verified replay range (empty
// replayActivity), AND archives exist, the CreateIndex log may live in an
// archived chapter — we cannot re-derive it. Mirror
// compareIdempotencyOutcomes' verifiedRangeStartTs trade-off and silently
// skip instead of flagging the stored entry as tampering. The same key
// MUST flag once any replay activity touches it (covered by
// TestCompareIndexes_PostArchiveDropFlagged).
func TestCompareIndexes_ArchiveOrphanIgnored(t *testing.T) {
	t.Parallel()

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "tier")
	key := domain.IndexKey{LedgerName: "L1", Canonical: indexes.Canonical(id)}

	checker, store := indexCheckerFor(t, map[domain.IndexKey]*commonpb.Index{
		key: {Id: id, Ledger: "L1"},
	})

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	var events []*servicepb.CheckStoreEvent
	checker.compareIndexes(reader, map[domain.IndexKey]*commonpb.Index{}, nil, nil, true, nil, func(e *servicepb.CheckStoreEvent) { events = append(events, e) })

	require.Empty(t, events, "stored entry with no replay activity and archives present must be treated as archive-orphan")
}

// TestCompareIndexes_PostArchiveDropFlagged pins the tightening that
// fixes the per-ledger archive guard's blind spot: even when archives
// exist, a stored entry whose key DID see replay activity (typically a
// DropIndex or RemovedMetadataFieldType cascade in the verified range)
// is no longer an archive-orphan — the replay decided the projection
// should not hold it, so a surviving stored row is tampering and must
// surface as INDEX_MISMATCH.
func TestCompareIndexes_PostArchiveDropFlagged(t *testing.T) {
	t.Parallel()

	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	key := domain.IndexKey{LedgerName: "L1", Canonical: indexes.Canonical(id)}

	checker, store := indexCheckerFor(t, map[domain.IndexKey]*commonpb.Index{
		key: {Id: id, Ledger: "L1"},
	})

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	replayActivity := map[domain.IndexKey]struct{}{key: {}}

	var events []*servicepb.CheckStoreEvent
	checker.compareIndexes(reader, map[domain.IndexKey]*commonpb.Index{}, replayActivity, nil, true, nil, func(e *servicepb.CheckStoreEvent) { events = append(events, e) })

	require.Len(t, events, 1)
	require.Equal(t,
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
		events[0].GetError().GetErrorType())
	require.Equal(t, "L1", events[0].GetError().GetLedger())
}

// TestCompareIndexes_PendingCleanupIgnored anchors the deferred-purge
// guard: between a DeleteLedger apply and the chapter-purge that catches
// its sequence and runs deleteLedgerData, the SubAttrIndex entries for
// the doomed ledger are still on disk while replay has already wiped
// them from expected. The pendingCleanupLedgers set lets the checker
// skip the transient window instead of flagging it as tampering.
func TestCompareIndexes_PendingCleanupIgnored(t *testing.T) {
	t.Parallel()

	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	key := domain.IndexKey{LedgerName: "doomed", Canonical: indexes.Canonical(id)}

	checker, store := indexCheckerFor(t, map[domain.IndexKey]*commonpb.Index{
		key: {Id: id, Ledger: "doomed"},
	})

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	pending := map[string]struct{}{"doomed": {}}

	var events []*servicepb.CheckStoreEvent
	checker.compareIndexes(reader, map[domain.IndexKey]*commonpb.Index{}, nil, nil, false, pending, func(e *servicepb.CheckStoreEvent) { events = append(events, e) })

	require.Empty(t, events, "stored entry for a ledger awaiting deferred purge must not trigger a mismatch")
}

// TestCompareIndexes_AccountBuiltinAsset_Identical pins the happy path for the
// account-asset builtin index: when the SubAttrIndex registry entry matches the
// audit-derived expected set, compareIndexes must stay silent. This exercises
// the generic indexes.Canonical / indexes.Equal path with an account_builtin
// IndexID to confirm no special-casing is needed.
func TestCompareIndexes_AccountBuiltinAsset_Identical(t *testing.T) {
	t.Parallel()

	id := indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET)
	key := domain.IndexKey{LedgerName: "L1", Canonical: indexes.Canonical(id)}

	checker, store := indexCheckerFor(t, map[domain.IndexKey]*commonpb.Index{
		key: {Id: id, Ledger: "L1"},
	})

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	var events []*servicepb.CheckStoreEvent
	checker.compareIndexes(reader, map[domain.IndexKey]*commonpb.Index{
		key: {Id: id, Ledger: "L1"},
	}, nil, nil, false, nil, func(e *servicepb.CheckStoreEvent) { events = append(events, e) })

	require.Empty(t, events, "compareIndexes must stay silent when account-asset registry matches audit-derived set")
}

// TestCompareIndexes_AccountBuiltinAsset_ExtraInStored covers the tamper path
// for the account-asset builtin index: a SubAttrIndex registry entry exists but
// there is no matching CreateIndex in the audit chain. The checker must surface
// CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH.
func TestCompareIndexes_AccountBuiltinAsset_ExtraInStored(t *testing.T) {
	t.Parallel()

	id := indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET)
	key := domain.IndexKey{LedgerName: "L1", Canonical: indexes.Canonical(id)}

	checker, store := indexCheckerFor(t, map[domain.IndexKey]*commonpb.Index{
		key: {Id: id, Ledger: "L1"},
	})

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	// Pass an empty expected map (no CreateIndex in audit chain).
	var events []*servicepb.CheckStoreEvent
	checker.compareIndexes(reader, map[domain.IndexKey]*commonpb.Index{}, nil, nil, false, nil, func(e *servicepb.CheckStoreEvent) { events = append(events, e) })

	require.Len(t, events, 1)
	require.Equal(t,
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
		events[0].GetError().GetErrorType())
	require.Equal(t, "L1", events[0].GetError().GetLedger())
}

// TestCompareIndexes_DeletedInReplayFlagged pins the deleted-ledger
// follow-up guard: a stored entry survives a DeleteLedger that was
// replayed AND whose deferred Pebble cleanup has already completed
// (ledger NOT in pendingCleanupLedgers). This is tampering regardless of
// archive state — the per-key replayActivity guard cannot catch it when
// the original CreateIndex log lived in an archived chapter, because the
// DeleteLedger cascade iterates `expectedIndexes` and that map never
// held the archived key. The dedicated `deletedInReplay` set closes the
// gap.
func TestCompareIndexes_DeletedInReplayFlagged(t *testing.T) {
	t.Parallel()

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	key := domain.IndexKey{LedgerName: "L1", Canonical: indexes.Canonical(id)}

	checker, store := indexCheckerFor(t, map[domain.IndexKey]*commonpb.Index{
		key: {Id: id, Ledger: "L1"},
	})

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	// archives present + DeleteLedger replayed + cleanup completed (no
	// entry in pending) + replayActivity empty (CreateIndex was archived,
	// cascade couldn't mark the key) → tampering must surface.
	deleted := map[string]struct{}{"L1": {}}

	var events []*servicepb.CheckStoreEvent
	checker.compareIndexes(reader, map[domain.IndexKey]*commonpb.Index{}, nil, deleted, true, nil, func(e *servicepb.CheckStoreEvent) { events = append(events, e) })

	require.Len(t, events, 1)
	require.Equal(t,
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
		events[0].GetError().GetErrorType())
	require.Contains(t, events[0].GetError().GetMessage(), "surviving a replayed DeleteLedger")
	require.Equal(t, "L1", events[0].GetError().GetLedger())
}
