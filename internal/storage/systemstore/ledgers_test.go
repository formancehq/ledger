package systemstore

import (
	"fmt"
	"testing"

	"github.com/formancehq/stack/libs/go-libs/time"

	"github.com/formancehq/stack/libs/go-libs/bun/bunconnect"
	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/stretchr/testify/require"
)

func newSystemStore(t *testing.T) *Store {
	t.Parallel()
	t.Helper()
	ctx := logging.TestingContext()

	pgServer := pgtesting.NewPostgresDatabase(t)

	store, err := Connect(ctx, bunconnect.ConnectionOptions{
		DatabaseSourceName: pgServer.ConnString(),
		Debug:              testing.Verbose(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	require.NoError(t, Migrate(ctx, store.DB()))

	return store
}

func TestListLedgers(t *testing.T) {
	ctx := logging.TestingContext()
	store := newSystemStore(t)

	ledgers := make([]Ledger, 0)
	pageSize := uint64(2)
	count := uint64(10)
	now := time.Now()
	for i := uint64(0); i < count; i++ {
		ledger := Ledger{
			Name:    fmt.Sprintf("ledger%d", i),
			AddedAt: now.Add(time.Duration(i) * time.Second),
		}
		ledgers = append(ledgers, ledger)
		_, err := store.RegisterLedger(ctx, &ledger)
		require.NoError(t, err)
	}

	cursor, err := store.ListLedgers(ctx, NewListLedgersQuery(pageSize))
	require.NoError(t, err)
	require.Len(t, cursor.Data, int(pageSize))
	require.Equal(t, ledgers[:pageSize], cursor.Data)

	for i := pageSize; i < count; i += pageSize {
		query := ListLedgersQuery{}
		require.NoError(t, bunpaginate.UnmarshalCursor(cursor.Next, &query))

		cursor, err = store.ListLedgers(ctx, query)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 2)
		require.Equal(t, ledgers[i:i+pageSize], cursor.Data)
	}
}
