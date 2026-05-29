package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestReadLastAppliedIndex(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Initial value should be 0
	lastIndex, err := query.ReadLastAppliedIndex(s)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastIndex)

	// Create batch with index 5
	batch := s.NewBatch()
	require.NoError(t, state.SaveLedger(batch, &commonpb.LedgerInfo{
		Name: "test",
	}))
	require.NoError(t, state.SetAppliedIndex(batch, 5))
	require.NoError(t, batch.Commit())

	// Verify last applied index updated
	lastIndex, err = query.ReadLastAppliedIndex(s)
	require.NoError(t, err)
	require.Equal(t, uint64(5), lastIndex)

	// Create another batch with index 10
	batch = s.NewBatch()
	require.NoError(t, state.SaveLedger(batch, &commonpb.LedgerInfo{
		Name: "test2",
	}))
	require.NoError(t, state.SetAppliedIndex(batch, 10))
	require.NoError(t, batch.Commit())

	lastIndex, err = query.ReadLastAppliedIndex(s)
	require.NoError(t, err)
	require.Equal(t, uint64(10), lastIndex)
}
