package processing

import (
	"sync"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// noopSink drops every absorbed (order, log) pair. Used by
// ProcessOrders tests that don't care about the cross-order signal side
// of the contract — the per-log dispatch is tested separately in
// internal/infra/state/write_set_absorb_test.go.
type noopSink struct{}

func (noopSink) Absorb(_ *raftcmdpb.Order, _ *commonpb.Log) {}

// kindStub is the generic test-side Accessor implementation: a per-key
// Get table plus optional Get / Put / Delete hooks. Tests register the
// stub once via setup<Kind>Stub(mockStore) (which wires
// mockStore.EXPECT().<Kind>().Return(stub).AnyTimes()), then declare
// expectations through the returned stub.
//
// Get resolves in this order: getHook → gets table → (zero, nil).
// Put/Delete fire their hooks when set; no-op otherwise.
//
// For tests that need to verify "exactly N calls" semantics, drop the
// helper and wire mockStore.EXPECT().<Kind>().Return(...) directly.
type kindStub[K AccessorKey, V any, R any] struct {
	gets map[K]struct {
		val R
		err error
	}
	getHook    func(K) (R, error)
	putHook    func(K, V)
	putHooks   map[K]func(K, V)
	deleteHook func(K)
	// expectedPuts tracks the keys that callers asserted MUST be written
	// by the end of the test. Each entry flips to true on Put, and the
	// registered t.Cleanup() fails the test if the entry is still false
	// at teardown. Mirrors the per-call strictness of the pre-refactor
	// EXPECT().PutX(key, ...).Times(1) gomock pattern.
	expectedPuts map[K]bool
	// expectedDeletes is the Delete equivalent of expectedPuts.
	expectedDeletes map[K]bool
}

func (s *kindStub[K, V, R]) Get(k K) (R, error) {
	if s.getHook != nil {
		return s.getHook(k)
	}
	if e, ok := s.gets[k]; ok {
		return e.val, e.err
	}

	// Match production rawAccessor.Get: an unprogrammed key surfaces as
	// (zero, domain.ErrNotFound), not (zero, nil). (nil, nil) is the
	// most dangerous shape to hand a processor — call sites that branch
	// on `err != nil` would slip past, masking unexpected reads. Test
	// suites must register every key the processor reads, or accept the
	// ErrNotFound branch via errors.Is.
	var zero R

	return zero, domain.ErrNotFound
}

func (s *kindStub[K, V, R]) Put(k K, v V) {
	if _, ok := s.expectedPuts[k]; ok {
		s.expectedPuts[k] = true
	}
	if hook, ok := s.putHooks[k]; ok {
		hook(k, v)

		return
	}
	if s.putHook != nil {
		s.putHook(k, v)
	}
}

// expectPut asserts that Put is called for k by the end of the test
// and, optionally, dispatches to a per-key hook when it fires. Multiple
// expectPut calls for distinct keys layer cleanly: each Put flips its
// expectedPuts entry and dispatches its own hook. Registers a
// t.Cleanup() that fails the test if Put was never invoked for k —
// restoring the per-call strictness of the pre-refactor
// EXPECT().PutX(key, ...).Times(1) pattern (NumaryBot finding on PR
// #569: expectPutX helpers never verified the key the processor
// actually wrote).
func (s *kindStub[K, V, R]) expectPut(t *testing.T, k K, hook func(K, V)) {
	t.Helper()
	if s.expectedPuts == nil {
		s.expectedPuts = make(map[K]bool)
	}
	s.expectedPuts[k] = false
	t.Cleanup(func() {
		if !s.expectedPuts[k] {
			t.Errorf("expected Put(%v, ...) was never called", k)
		}
	})
	if hook != nil {
		if s.putHooks == nil {
			s.putHooks = make(map[K]func(K, V))
		}
		s.putHooks[k] = hook
	}
}

func (s *kindStub[K, V, R]) Delete(k K) error {
	if _, ok := s.expectedDeletes[k]; ok {
		s.expectedDeletes[k] = true
	}
	if s.deleteHook != nil {
		s.deleteHook(k)
	}

	return nil
}

// Mutate mirrors the production rawAccessor semantics for the test stub:
// dispatches to Get for the reader, then uses the reader's Mutate() to
// obtain a mutable clone (via runtime interface assertion, since R is
// unconstrained here). Tests that program `gets[k]` see the same clone
// path the production code exercises.
func (s *kindStub[K, V, R]) Mutate(k K) (V, error) {
	r, err := s.Get(k)
	if err != nil {
		var zero V

		return zero, err
	}

	if m, ok := any(r).(interface{ Mutate() V }); ok {
		return m.Mutate(), nil
	}

	var zero V

	return zero, nil
}

// expectDelete asserts that Delete is called for k by the end of the
// test. Registers a t.Cleanup() that fails the test on miss — restoring
// the per-key Delete coverage that the bare `.AnyTimes()` no-op
// previously dropped (NumaryBot finding on PR #569: expectDeleteX
// helpers never verified the key the processor actually deleted).
func (s *kindStub[K, V, R]) expectDelete(t *testing.T, k K) {
	t.Helper()
	if s.expectedDeletes == nil {
		s.expectedDeletes = make(map[K]bool)
	}
	s.expectedDeletes[k] = false
	t.Cleanup(func() {
		if !s.expectedDeletes[k] {
			t.Errorf("expected Delete(%v) was never called", k)
		}
	})
}

func (s *kindStub[K, V, R]) expectGet(k K, v R, err error) {
	if s.gets == nil {
		s.gets = make(map[K]struct {
			val R
			err error
		})
	}
	s.gets[k] = struct {
		val R
		err error
	}{v, err}
}

func (s *kindStub[K, V, R]) onGet(hook func(K) (R, error)) { s.getHook = hook }
func (s *kindStub[K, V, R]) onPut(hook func(K, V))         { s.putHook = hook }

// Per-kind setup wrappers — register one mockStore.EXPECT().<Kind>().AnyTimes()
// and return the typed stub the test will configure.

func setupVolumesStub(mockStore *MockScope) *kindStub[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader] {
	s := &kindStub[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader]{}
	mockStore.EXPECT().Volumes().Return(s).AnyTimes()

	return s
}

func setupLedgersStub(mockStore *MockScope) *kindStub[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader] {
	s := &kindStub[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader]{}
	mockStore.EXPECT().Ledgers().Return(s).AnyTimes()

	return s
}

func setupBoundariesStub(mockStore *MockScope) *kindStub[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader] {
	s := &kindStub[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader]{}
	mockStore.EXPECT().Boundaries().Return(s).AnyTimes()

	return s
}

func setupIndexesStub(mockStore *MockScope) *kindStub[domain.IndexKey, *commonpb.Index, commonpb.IndexReader] {
	s := &kindStub[domain.IndexKey, *commonpb.Index, commonpb.IndexReader]{}
	mockStore.EXPECT().Indexes().Return(s).AnyTimes()

	return s
}

func setupPreparedQueriesStub(mockStore *MockScope) *kindStub[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader] {
	s := &kindStub[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader]{}
	mockStore.EXPECT().PreparedQueries().Return(s).AnyTimes()

	return s
}

// Legacy expect helpers — back-compat with tests written before the
// shared-stub pattern. Each helper participates in a per-(mockStore,
// kind) shared kindStub registered with mockStore.EXPECT().<Kind>()
// .Return(stub).AnyTimes() **once** at the first call. Subsequent
// calls for the same (mockStore, kind) reuse the same stub, so
// multi-key tests dispatch correctly through the gets map (paul-nicolas
// review on #569: the prior implementation re-registered an
// AnyTimes() expectation per call, and gomock matched every Volumes()
// call to the first one — multi-key tests silently returned the same
// value for every key, hiding source/destination swap bugs).
//
// The Put helpers accept an optional variadic Put hook so tests can
// continue to express the `expectPutX(...).Do(func(...) {...})` shape
// as `expectPutX(..., func(...) {...})`. The hook receives the
// raw accessor Put args (so e.g. Ledger Put hook signature is
// `func(string, *commonpb.LedgerInfo)` to match the pre-refactor shape:
// the helper translates domain.LedgerKey → name). Hooks accumulate via
// chaining — only the most recent hook fires per kind per mockStore;
// tests that need per-key Put assertion should migrate to the
// setupXStub pattern.

type mockStubs struct {
	mu sync.Mutex

	volumes                   *kindStub[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader]
	volumesCall               *gomock.Call
	ledgers                   *kindStub[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader]
	ledgersCall               *gomock.Call
	boundaries                *kindStub[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader]
	boundariesCall            *gomock.Call
	accountMetadata           *kindStub[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]
	accountMetadataCall       *gomock.Call
	transactionStates         *kindStub[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader]
	transactionStatesCall     *gomock.Call
	transactionReferences     *kindStub[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader]
	transactionReferencesCall *gomock.Call
	indexes                   *kindStub[domain.IndexKey, *commonpb.Index, commonpb.IndexReader]
	indexesCall               *gomock.Call
}

var (
	stubRegistryMu sync.Mutex
	stubRegistry   = map[*MockScope]*mockStubs{}
)

// stubsFor returns the per-mockStore stub registry, creating it on
// demand. Tests typically allocate one *MockScope per test, so the
// registry entry's lifetime is the test's lifetime. Cross-test cleanup
// is implicit (the GC reclaims when the *MockScope is dropped); the
// registry map grows over a test binary's run but each entry is small.
func stubsFor(mockStore *MockScope) *mockStubs {
	stubRegistryMu.Lock()
	defer stubRegistryMu.Unlock()

	s, ok := stubRegistry[mockStore]
	if !ok {
		s = &mockStubs{}
		stubRegistry[mockStore] = s
	}

	return s
}

func (m *mockStubs) volumesStubFor(mockStore *MockScope) (*kindStub[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader], *gomock.Call) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.volumes == nil {
		m.volumes = &kindStub[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader]{}
		m.volumesCall = mockStore.EXPECT().Volumes().Return(m.volumes).AnyTimes()
	}

	return m.volumes, m.volumesCall
}

func (m *mockStubs) ledgersStubFor(mockStore *MockScope) (*kindStub[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader], *gomock.Call) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ledgers == nil {
		m.ledgers = &kindStub[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader]{}
		m.ledgersCall = mockStore.EXPECT().Ledgers().Return(m.ledgers).AnyTimes()
	}

	return m.ledgers, m.ledgersCall
}

func (m *mockStubs) boundariesStubFor(mockStore *MockScope) (*kindStub[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader], *gomock.Call) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.boundaries == nil {
		m.boundaries = &kindStub[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader]{}
		m.boundariesCall = mockStore.EXPECT().Boundaries().Return(m.boundaries).AnyTimes()
	}

	return m.boundaries, m.boundariesCall
}

func (m *mockStubs) accountMetadataStubFor(mockStore *MockScope) (*kindStub[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader], *gomock.Call) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.accountMetadata == nil {
		m.accountMetadata = &kindStub[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]{}
		m.accountMetadataCall = mockStore.EXPECT().AccountMetadata().Return(m.accountMetadata).AnyTimes()
	}

	return m.accountMetadata, m.accountMetadataCall
}

func (m *mockStubs) transactionStatesStubFor(mockStore *MockScope) (*kindStub[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader], *gomock.Call) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.transactionStates == nil {
		m.transactionStates = &kindStub[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader]{}
		m.transactionStatesCall = mockStore.EXPECT().TransactionStates().Return(m.transactionStates).AnyTimes()
	}

	return m.transactionStates, m.transactionStatesCall
}

func (m *mockStubs) transactionReferencesStubFor(mockStore *MockScope) (*kindStub[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader], *gomock.Call) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.transactionReferences == nil {
		m.transactionReferences = &kindStub[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader]{}
		m.transactionReferencesCall = mockStore.EXPECT().TransactionReferences().Return(m.transactionReferences).AnyTimes()
	}

	return m.transactionReferences, m.transactionReferencesCall
}

func (m *mockStubs) indexesStubFor(mockStore *MockScope) (*kindStub[domain.IndexKey, *commonpb.Index, commonpb.IndexReader], *gomock.Call) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.indexes == nil {
		m.indexes = &kindStub[domain.IndexKey, *commonpb.Index, commonpb.IndexReader]{}
		m.indexesCall = mockStore.EXPECT().Indexes().Return(m.indexes).AnyTimes()
	}

	return m.indexes, m.indexesCall
}

func expectGetVolume(mockStore *MockScope, key domain.VolumeKey, value raftcmdpb.VolumePairReader, err error) *gomock.Call {
	stub, call := stubsFor(mockStore).volumesStubFor(mockStore)
	stub.expectGet(key, value, err)

	return call
}

func expectPutVolume(t *testing.T, mockStore *MockScope, key domain.VolumeKey, _ *raftcmdpb.VolumePair, hooks ...func(domain.VolumeKey, *raftcmdpb.VolumePair)) *gomock.Call {
	t.Helper()
	stub, call := stubsFor(mockStore).volumesStubFor(mockStore)
	var hook func(domain.VolumeKey, *raftcmdpb.VolumePair)
	if len(hooks) > 0 {
		hook = hooks[0]
	}
	stub.expectPut(t, key, hook)

	return call
}

func expectGetLedger(mockStore *MockScope, key domain.LedgerKey, value commonpb.LedgerInfoReader, err error) *gomock.Call {
	stub, call := stubsFor(mockStore).ledgersStubFor(mockStore)
	stub.expectGet(key, value, err)

	return call
}

func expectPutLedger(t *testing.T, mockStore *MockScope, key domain.LedgerKey, _ *commonpb.LedgerInfo, hooks ...func(string, *commonpb.LedgerInfo)) *gomock.Call {
	t.Helper()
	stub, call := stubsFor(mockStore).ledgersStubFor(mockStore)
	var hook func(domain.LedgerKey, *commonpb.LedgerInfo)
	if len(hooks) > 0 {
		hook = func(k domain.LedgerKey, v *commonpb.LedgerInfo) { hooks[0](k.Name, v) }
	}
	stub.expectPut(t, key, hook)

	return call
}

func expectGetBoundaries(mockStore *MockScope, key domain.LedgerKey, value raftcmdpb.LedgerBoundariesReader, err error) *gomock.Call {
	stub, call := stubsFor(mockStore).boundariesStubFor(mockStore)
	stub.expectGet(key, value, err)

	return call
}

func expectPutBoundaries(t *testing.T, mockStore *MockScope, key domain.LedgerKey, _ *raftcmdpb.LedgerBoundaries, hooks ...func(string, *raftcmdpb.LedgerBoundaries)) *gomock.Call {
	t.Helper()
	stub, call := stubsFor(mockStore).boundariesStubFor(mockStore)
	var hook func(domain.LedgerKey, *raftcmdpb.LedgerBoundaries)
	if len(hooks) > 0 {
		hook = func(k domain.LedgerKey, v *raftcmdpb.LedgerBoundaries) { hooks[0](k.Name, v) }
	}
	stub.expectPut(t, key, hook)

	return call
}

func expectGetAccountMetadata(mockStore *MockScope, key domain.MetadataKey, value commonpb.MetadataValueReader, err error) *gomock.Call {
	stub, call := stubsFor(mockStore).accountMetadataStubFor(mockStore)
	stub.expectGet(key, value, err)

	return call
}

func expectPutAccountMetadata(t *testing.T, mockStore *MockScope, key domain.MetadataKey, _ *commonpb.MetadataValue, hooks ...func(domain.MetadataKey, *commonpb.MetadataValue)) *gomock.Call {
	t.Helper()
	stub, call := stubsFor(mockStore).accountMetadataStubFor(mockStore)
	var hook func(domain.MetadataKey, *commonpb.MetadataValue)
	if len(hooks) > 0 {
		hook = hooks[0]
	}
	stub.expectPut(t, key, hook)

	return call
}

func expectDeleteAccountMetadata(t *testing.T, mockStore *MockScope, key domain.MetadataKey) *gomock.Call {
	t.Helper()
	stub, call := stubsFor(mockStore).accountMetadataStubFor(mockStore)
	stub.expectDelete(t, key)

	return call
}

func expectGetTransactionState(mockStore *MockScope, key domain.TransactionKey, value commonpb.TransactionStateReader, err error) *gomock.Call {
	stub, call := stubsFor(mockStore).transactionStatesStubFor(mockStore)
	stub.expectGet(key, value, err)

	return call
}

func expectPutTransactionState(t *testing.T, mockStore *MockScope, key domain.TransactionKey, _ *commonpb.TransactionState, hooks ...func(domain.TransactionKey, *commonpb.TransactionState)) *gomock.Call {
	t.Helper()
	stub, call := stubsFor(mockStore).transactionStatesStubFor(mockStore)
	var hook func(domain.TransactionKey, *commonpb.TransactionState)
	if len(hooks) > 0 {
		hook = hooks[0]
	}
	stub.expectPut(t, key, hook)

	return call
}

func expectPutTransactionReference(t *testing.T, mockStore *MockScope, key domain.TransactionReferenceKey, _ *commonpb.TransactionReferenceValue, hooks ...func(domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue)) *gomock.Call {
	t.Helper()
	stub, call := stubsFor(mockStore).transactionReferencesStubFor(mockStore)
	var hook func(domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue)
	if len(hooks) > 0 {
		hook = hooks[0]
	}
	stub.expectPut(t, key, hook)

	return call
}

func expectGetIndex(mockStore *MockScope, key domain.IndexKey, value commonpb.IndexReader, err error) *gomock.Call {
	stub, call := stubsFor(mockStore).indexesStubFor(mockStore)
	stub.expectGet(key, value, err)

	return call
}

func expectPutIndex(t *testing.T, mockStore *MockScope, key domain.IndexKey, _ *commonpb.Index, hooks ...func(domain.IndexKey, *commonpb.Index)) *gomock.Call {
	t.Helper()
	stub, call := stubsFor(mockStore).indexesStubFor(mockStore)
	var hook func(domain.IndexKey, *commonpb.Index)
	if len(hooks) > 0 {
		hook = hooks[0]
	}
	stub.expectPut(t, key, hook)

	return call
}

func expectDeleteIndex(t *testing.T, mockStore *MockScope, key domain.IndexKey) *gomock.Call {
	t.Helper()
	stub, call := stubsFor(mockStore).indexesStubFor(mockStore)
	stub.expectDelete(t, key)

	return call
}

// requestToOrder converts a servicepb.Request to a raftcmdpb.Order for test purposes.
func requestToOrder(req *servicepb.Request) *raftcmdpb.Order {
	order := &raftcmdpb.Order{}

	switch reqType := req.GetType().(type) {
	case *servicepb.Request_CreateLedger:
		order.Type = &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: reqType.CreateLedger.GetName(),
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{},
				},
			},
		}
	case *servicepb.Request_DeleteLedger:
		order.Type = &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: reqType.DeleteLedger.GetName(),
				Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedger{
					DeleteLedger: &raftcmdpb.DeleteLedgerOrder{},
				},
			},
		}
	case *servicepb.Request_Apply:
		applyOrder := &raftcmdpb.LedgerApplyOrder{}
		switch data := reqType.Apply.GetAction().GetData().(type) {
		case *servicepb.LedgerAction_CreateTransaction:
			applyOrder.Data = &raftcmdpb.LedgerApplyOrder_CreateTransaction{
				CreateTransaction: &raftcmdpb.CreateTransactionOrder{
					Postings:        data.CreateTransaction.GetPostings(),
					Script:          data.CreateTransaction.GetScript(),
					Timestamp:       data.CreateTransaction.GetTimestamp(),
					Reference:       data.CreateTransaction.GetReference(),
					Metadata:        data.CreateTransaction.GetMetadata(),
					AccountMetadata: data.CreateTransaction.GetAccountMetadata(),
					Force:           data.CreateTransaction.GetForce(),
				},
			}
		case *servicepb.LedgerAction_AddMetadata:
			applyOrder.Data = &raftcmdpb.LedgerApplyOrder_AddMetadata{
				AddMetadata: &raftcmdpb.SaveMetadataOrder{
					Target:   data.AddMetadata.GetTarget(),
					Metadata: data.AddMetadata.GetMetadata(),
				},
			}
		case *servicepb.LedgerAction_DeleteMetadata:
			applyOrder.Data = &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
				DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
					Target: data.DeleteMetadata.GetTarget(),
					Key:    data.DeleteMetadata.GetKey(),
				},
			}
		case *servicepb.LedgerAction_RevertTransaction:
			applyOrder.Data = &raftcmdpb.LedgerApplyOrder_RevertTransaction{
				RevertTransaction: &raftcmdpb.RevertTransactionOrder{
					TransactionId:   data.RevertTransaction.GetTransactionId(),
					Force:           data.RevertTransaction.GetForce(),
					AtEffectiveDate: data.RevertTransaction.GetAtEffectiveDate(),
					Metadata:        data.RevertTransaction.GetMetadata(),
				},
			}
		}

		order.Type = &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger:  reqType.Apply.GetLedger(),
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: applyOrder},
			},
		}
	}

	return order
}

// constantScopeFactory yields the same Scope for every NewScope call,
// regardless of coverage_bits / production_bits. Used by mock-based
// tests where the scope is a gomock — the bits are not exercised.
type constantScopeFactory struct{ scope Scope }

func (f constantScopeFactory) NewScope(_ []byte) (Scope, error) { return f.scope, nil }
func (f constantScopeFactory) NewProposalScope() (Scope, error) { return f.scope, nil }

// mockFactory returns a ScopeFactory that always yields the given scope.
func mockFactory(s Scope) ScopeFactory {
	return constantScopeFactory{scope: s}
}
