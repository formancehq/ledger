//go:build it

package systemstore

import (
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/bun/bunpaginate"

	"github.com/formancehq/go-libs/logging"
	"github.com/stretchr/testify/require"
)

func newSystemStore(t *testing.T) *Store {
	t.Parallel()
	t.Helper()
	ctx := logging.TestingContext()

	pgServer := srv.NewDatabase(t)

	store, err := Connect(ctx, bunconnect.ConnectionOptions{
		DatabaseSourceName: pgServer.ConnString(),
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
		m := map[string]string{}
		if i%2 == 0 {
			m["foo"] = "bar"
		}
		ledger := Ledger{
			Name:     fmt.Sprintf("ledger%d", i),
			AddedAt:  now.Add(time.Duration(i) * time.Second),
			Metadata: m,
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

func TestUpdateLedgerMetadata(t *testing.T) {
	ctx := logging.TestingContext()
	store := newSystemStore(t)

	ledger := &Ledger{
		Name:    uuid.NewString(),
		AddedAt: time.Now(),
	}
	_, err := store.RegisterLedger(ctx, ledger)
	require.NoError(t, err)

	addedMetadata := map[string]string{
		"foo": "bar",
	}
	err = store.UpdateLedgerMetadata(ctx, ledger.Name, addedMetadata)
	require.NoError(t, err)

	ledgerFromDB, err := store.GetLedger(ctx, ledger.Name)
	require.NoError(t, err)
	require.Equal(t, addedMetadata, ledgerFromDB.Metadata)
}

func TestDeleteLedgerMetadata(t *testing.T) {
	ctx := logging.TestingContext()
	store := newSystemStore(t)

	ledger := &Ledger{
		Name:    uuid.NewString(),
		AddedAt: time.Now(),
		Metadata: map[string]string{
			"foo": "bar",
		},
	}
	_, err := store.RegisterLedger(ctx, ledger)
	require.NoError(t, err)

	err = store.DeleteLedgerMetadata(ctx, ledger.Name, "foo")
	require.NoError(t, err)

	ledgerFromDB, err := store.GetLedger(ctx, ledger.Name)
	require.NoError(t, err)
	require.Equal(t, map[string]string{}, ledgerFromDB.Metadata)
}
