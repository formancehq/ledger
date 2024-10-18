//go:build it

package ledgerstore

import (
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"testing"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/logging"

	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
)

func TestLogsList(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	for i := 1; i <= 3; i++ {
		newLog := ledger.NewLog(ledger.CreatedTransaction{
			Transaction:     ledger.NewTransaction(),
			AccountMetadata: ledger.AccountMetadata{},
		})
		newLog.Date = now.Add(-time.Duration(i) * time.Hour)

		err := store.newStore.InsertLog(ctx, &newLog)
		require.NoError(t, err)
	}

	cursor, err := store.GetLogs(ctx, ledgercontroller.NewListLogsQuery(ledgercontroller.NewPaginatedQueryOptions[any](nil)))
	require.NoError(t, err)
	require.Equal(t, bunpaginate.QueryDefaultPageSize, cursor.PageSize)

	require.Equal(t, 3, len(cursor.Data))
	require.EqualValues(t, 3, cursor.Data[0].ID)

	cursor, err = store.GetLogs(ctx, ledgercontroller.NewListLogsQuery(ledgercontroller.NewPaginatedQueryOptions[any](nil).WithPageSize(1)))
	require.NoError(t, err)
	// Should get only the first log.
	require.Equal(t, 1, cursor.PageSize)
	require.EqualValues(t, 3, cursor.Data[0].ID)

	cursor, err = store.GetLogs(ctx, ledgercontroller.NewListLogsQuery(ledgercontroller.NewPaginatedQueryOptions[any](nil).
		WithQueryBuilder(query.And(
			query.Gte("date", now.Add(-2*time.Hour)),
			query.Lt("date", now.Add(-time.Hour)),
		)).
		WithPageSize(10),
	))
	require.NoError(t, err)
	require.Equal(t, 10, cursor.PageSize)
	// Should get only the second log, as StartTime is inclusive and EndTime exclusive.
	require.Len(t, cursor.Data, 1)
	require.EqualValues(t, 2, cursor.Data[0].ID)
}
