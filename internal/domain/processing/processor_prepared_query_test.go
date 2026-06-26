package processing

import (
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
		Query: &commonpb.PreparedQuery{Name: "q1"},
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
