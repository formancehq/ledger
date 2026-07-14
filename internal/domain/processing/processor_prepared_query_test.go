package processing

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestProcessCreatePreparedQuery_TreatsNotFoundAsMiss pins the contract the
// pre-refactor WriteSet.GetPreparedQuery enforced inline ("Treat a cache miss
// as 'doesn't exist'"): a fresh prepared-query creation against a populated
// ledger must succeed even though Scope.PreparedQueries().Get returns
// (nil, domain.ErrNotFound) — that's the documented Accessor contract.
//
// The legacy helper silently normalised ErrNotFound → (nil, nil) so the
// processor's `if err != nil` branch never fired. With the generic accessor
// surfacing ErrNotFound unchanged, the lookup helper inside
// processor_prepared_query.go is responsible for the same normalisation;
// this test catches a regression if that helper goes away.
func TestProcessCreatePreparedQuery_TreatsNotFoundAsMiss(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger"}).AsReader(), nil)

	pq := setupPreparedQueriesStub(mockStore)
	pq.onGet(func(_ domain.PreparedQueryKey) (commonpb.PreparedQueryReader, error) {
		return nil, domain.ErrNotFound
	})

	order := &raftcmdpb.CreatePreparedQueryOrder{
		Query: &commonpb.PreparedQuery{Name: "q1", Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS},
	}

	payload, derr := processCreatePreparedQuery("test-ledger", order, &Context{Scope: mockStore})
	require.Nil(t, derr, "create on an empty cache slot must succeed — ErrNotFound is the documented miss signal, not a storage failure")
	require.NotNil(t, payload)
	require.Equal(t, "q1", payload.GetCreatedPreparedQuery().GetQuery().GetName())
}

// TestProcessUpdatePreparedQuery_NotFoundReturnsTypedError pins the opposite
// direction: a target-absent update must surface ErrPreparedQueryNotFound
// (not the raw ErrNotFound or an ErrStorageOperation wrap), matching the
// pre-refactor behaviour callers rely on.
func TestProcessUpdatePreparedQuery_NotFoundReturnsTypedError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger"}).AsReader(), nil)

	pq := setupPreparedQueriesStub(mockStore)
	pq.onGet(func(_ domain.PreparedQueryKey) (commonpb.PreparedQueryReader, error) {
		return nil, domain.ErrNotFound
	})

	order := &raftcmdpb.UpdatePreparedQueryOrder{Name: "missing"}
	_, derr := processUpdatePreparedQuery("test-ledger", order, &Context{Scope: mockStore})
	require.NotNil(t, derr)

	var notFound *domain.ErrPreparedQueryNotFound
	require.ErrorAs(t, derr, &notFound, "update on a missing PQ must surface ErrPreparedQueryNotFound, not ErrStorageOperation")
}

// TestProcessUpdatePreparedQuery_RejectsFilterInvalidForStoredTarget pins the
// EN-1504 guarantee that an update validates its new filter against the
// *stored* target (read from the cache, not the request — an update carries no
// target). Admission cannot catch this because it never loads the existing
// query; the FSM is the only layer that sees the stored target. Here the query
// was created on ACCOUNTS, and the update tries to smuggle in a
// transaction-only `reference` condition, which must be rejected.
func TestProcessUpdatePreparedQuery_RejectsFilterInvalidForStoredTarget(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger"}).AsReader(), nil)

	pq := setupPreparedQueriesStub(mockStore)
	pq.onGet(func(_ domain.PreparedQueryKey) (commonpb.PreparedQueryReader, error) {
		return (&commonpb.PreparedQuery{
			Name:   "q1",
			Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		}).AsReader(), nil
	})
	pq.onPut(func(domain.PreparedQueryKey, *commonpb.PreparedQuery) {
		t.Fatal("Put must not be called when the new filter is invalid for the stored target")
	})

	newFilter := &commonpb.QueryFilter{}
	require.NoError(t, json.Unmarshal([]byte(`{"$match":{"reference":"r"}}`), newFilter))

	order := &raftcmdpb.UpdatePreparedQueryOrder{Name: "q1", Filter: newFilter}
	_, derr := processUpdatePreparedQuery("test-ledger", order, &Context{Scope: mockStore})
	require.NotNil(t, derr, "a transaction-only condition must be rejected on an ACCOUNTS prepared query")

	var business *domain.BusinessError
	require.ErrorAs(t, derr, &business)
	require.Contains(t, derr.Error(), "accounts")
}

// TestProcessUpdatePreparedQuery_RejectsNilFilter guards against silently erasing
// a stored prepared query's filter: an update replaces the filter, and a nil
// filter passes ValidateFilterForTarget (nil == "no filter"), so without an
// explicit guard `updated.Filter = nil` would persist and drop the query's
// definition. The FSM must reject a nil filter (and never call Put).
func TestProcessUpdatePreparedQuery_RejectsNilFilter(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger"}).AsReader(), nil)

	pq := setupPreparedQueriesStub(mockStore)
	pq.onGet(func(_ domain.PreparedQueryKey) (commonpb.PreparedQueryReader, error) {
		return (&commonpb.PreparedQuery{
			Name:   "q1",
			Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			Filter: &commonpb.QueryFilter{},
		}).AsReader(), nil
	})
	pq.onPut(func(domain.PreparedQueryKey, *commonpb.PreparedQuery) {
		t.Fatal("Put must not be called when the update carries a nil filter")
	})

	order := &raftcmdpb.UpdatePreparedQueryOrder{Name: "q1"} // Filter left nil
	_, derr := processUpdatePreparedQuery("test-ledger", order, &Context{Scope: mockStore})
	require.NotNil(t, derr, "a nil filter must be rejected, not persisted over the stored filter")
	require.ErrorIs(t, derr, domain.ErrPreparedQueryFilterRequired)
}

// TestProcessUpdatePreparedQuery_RejectsNonExecutableStoredTarget covers a
// legacy escape hatch: CLI/gRPC creation used to accept non-executable targets
// (e.g. AUDIT) before EN-1504, so such a query can already sit in storage. An
// update must not write its filter back — ExecutePreparedQuery cannot hydrate
// AUDIT — so the stored-target executability gate fires before condition
// validity is ever evaluated. Delete stays allowed; only the rewrite is blocked.
// (LOGS is executable as of EN-1503, so it no longer belongs in this class —
// AUDIT is the remaining non-executable target.)
func TestProcessUpdatePreparedQuery_RejectsNonExecutableStoredTarget(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger"}).AsReader(), nil)

	pq := setupPreparedQueriesStub(mockStore)
	pq.onGet(func(_ domain.PreparedQueryKey) (commonpb.PreparedQueryReader, error) {
		return (&commonpb.PreparedQuery{
			Name:   "legacy-audit",
			Target: commonpb.QueryTarget_QUERY_TARGET_AUDIT,
		}).AsReader(), nil
	})
	pq.onPut(func(domain.PreparedQueryKey, *commonpb.PreparedQuery) {
		t.Fatal("Put must not be called when the stored target is not executable")
	})

	// The stored-target executability gate fires before condition validity is
	// even evaluated, so the filter's own validity is irrelevant here — any
	// structurally decodable filter must be rejected on the non-executable
	// stored target.
	newFilter := &commonpb.QueryFilter{}
	require.NoError(t, json.Unmarshal([]byte(`{"$exists":{"metadata":"x"}}`), newFilter))

	order := &raftcmdpb.UpdatePreparedQueryOrder{Name: "legacy-audit", Filter: newFilter}
	_, derr := processUpdatePreparedQuery("test-ledger", order, &Context{Scope: mockStore})
	require.NotNil(t, derr)
	require.ErrorIs(t, derr, domain.ErrPreparedQueryTargetUnsupported)
}
