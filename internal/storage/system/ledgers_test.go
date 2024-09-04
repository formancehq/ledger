//go:build it

package system

import (
	"fmt"
	"github.com/formancehq/go-libs/bun/bundebug"
	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/uptrace/bun"
	"testing"

	"github.com/google/uuid"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/bun/bunpaginate"

	"github.com/formancehq/go-libs/logging"
	"github.com/stretchr/testify/require"
)

func newSystemStore(t *testing.T) *Store {
	t.Helper()

	ctx := logging.TestingContext()

	hooks := make([]bun.QueryHook, 0)
	if testing.Verbose() {
		hooks = append(hooks, bundebug.NewQueryHook())
	}

	pgServer := srv.NewDatabase(t)
	db, err := bunconnect.OpenSQLDB(ctx, pgServer.ConnectionOptions(), hooks...)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	store := New(db)

	require.NoError(t, Migrate(ctx, store.DB()))

	return store
}

func TestLedgersCreate(t *testing.T) {
	ctx := logging.TestingContext()
	store := newSystemStore(t)

	l := ledger.Ledger{
		Name:    "foo",
		AddedAt: time.Now(),
	}
	_, err := store.CreateLedger(ctx, &l)
	require.NoError(t, err)
	require.Equal(t, 1, l.ID)
}

func TestLedgersList(t *testing.T) {
	ctx := logging.TestingContext()
	store := newSystemStore(t)

	ledgers := make([]ledger.Ledger, 0)
	pageSize := uint64(2)
	count := uint64(10)
	now := time.Now()
	for i := uint64(0); i < count; i++ {
		m := metadata.Metadata{}
		if i%2 == 0 {
			m["foo"] = "bar"
		}
		l := ledger.Ledger{
			Name:    fmt.Sprintf("ledger%d", i),
			AddedAt: now.Add(time.Duration(i) * time.Second),
			Configuration: ledger.Configuration{
				Metadata: m,
			},
		}
		_, err := store.CreateLedger(ctx, &l)
		require.NoError(t, err)

		ledgers = append(ledgers, l)
	}

	cursor, err := store.ListLedgers(ctx, ledgercontroller.NewListLedgersQuery(pageSize))
	require.NoError(t, err)
	require.Len(t, cursor.Data, int(pageSize))
	require.Equal(t, ledgers[:pageSize], cursor.Data)

	for i := pageSize; i < count; i += pageSize {
		query := ledgercontroller.ListLedgersQuery{}
		require.NoError(t, bunpaginate.UnmarshalCursor(cursor.Next, &query))

		cursor, err = store.ListLedgers(ctx, query)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 2)
		require.Equal(t, ledgers[i:i+pageSize], cursor.Data)
	}
}

func TestLedgerUpdateMetadata(t *testing.T) {
	ctx := logging.TestingContext()
	store := newSystemStore(t)

	l := ledger.Ledger{
		Name:    uuid.NewString(),
		AddedAt: time.Now(),
	}
	_, err := store.CreateLedger(ctx, &l)
	require.NoError(t, err)

	addedMetadata := metadata.Metadata{
		"foo": "bar",
	}
	err = store.UpdateLedgerMetadata(ctx, l.Name, addedMetadata)
	require.NoError(t, err)

	ledgerFromDB, err := store.GetLedger(ctx, l.Name)
	require.NoError(t, err)
	require.Equal(t, addedMetadata, ledgerFromDB.Metadata)
}

func TestLedgerDeleteMetadata(t *testing.T) {
	ctx := logging.TestingContext()
	store := newSystemStore(t)

	l := ledger.Ledger{
		Name:    uuid.NewString(),
		AddedAt: time.Now(),
		Configuration: ledger.Configuration{
			Metadata: map[string]string{
				"foo": "bar",
			},
		},
	}
	_, err := store.CreateLedger(ctx, &l)
	require.NoError(t, err)

	err = store.DeleteLedgerMetadata(ctx, l.Name, "foo")
	require.NoError(t, err)

	ledgerFromDB, err := store.GetLedger(ctx, l.Name)
	require.NoError(t, err)
	require.Equal(t, metadata.Metadata{}, ledgerFromDB.Metadata)
}
