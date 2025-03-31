//go:build it

package system

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/bun/bundebug"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/testing/docker"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/google/uuid"
	"github.com/uptrace/bun"
	"golang.org/x/sync/errgroup"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/stretchr/testify/require"
)

func TestLedgersCreate(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	store := newStore(t)

	const count = 30
	grp, ctx := errgroup.WithContext(ctx)
	createdLedgersChan := make(chan ledger.Ledger, count)

	for i := range count {
		grp.Go(func() error {
			l := ledger.MustNewWithDefault(fmt.Sprintf("ledger%d", i))

			ctx, cancel := context.WithDeadline(ctx, time.Now().Add(40*time.Second))
			defer cancel()

			err := store.CreateLedger(ctx, &l)
			if err != nil {
				return err
			}
			createdLedgersChan <- l

			return nil
		})
	}

	require.NoError(t, grp.Wait())

	close(createdLedgersChan)

	createdLedgers := make([]ledger.Ledger, 0)
	for createdLedger := range createdLedgersChan {
		createdLedgers = append(createdLedgers, createdLedger)
	}

	slices.SortStableFunc(createdLedgers, func(a, b ledger.Ledger) int {
		return a.ID - b.ID
	})

	for i, createdLedger := range createdLedgers {
		require.Equal(t, i+1, createdLedger.ID)
		require.NotEmpty(t, createdLedger.AddedAt)
	}
}

func TestLedgersList(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	store := newStore(t)

	ledgers := make([]ledger.Ledger, 0)
	pageSize := uint64(2)
	count := uint64(10)
	for i := uint64(0); i < count; i++ {
		m := metadata.Metadata{}
		if i%2 == 0 {
			m["foo"] = "bar"
		}
		l := ledger.MustNewWithDefault(fmt.Sprintf("ledger%d", i)).WithMetadata(m)
		err := store.CreateLedger(ctx, &l)
		require.NoError(t, err)

		ledgers = append(ledgers, l)
	}

	cursor, err := store.Ledgers().Paginate(ctx, ledgercontroller.NewListLedgersQuery(pageSize))
	require.NoError(t, err)
	require.Len(t, cursor.Data, int(pageSize))
	require.Equal(t, ledgers[:pageSize], cursor.Data)

	for i := pageSize; i < count; i += pageSize {
		query := common.ColumnPaginatedQuery[any]{}
		require.NoError(t, bunpaginate.UnmarshalCursor(cursor.Next, &query))

		cursor, err = store.Ledgers().Paginate(ctx, query)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 2)
		require.Equal(t, ledgers[i:i+pageSize], cursor.Data)
	}
}

func TestLedgerUpdateMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	store := newStore(t)

	l := ledger.MustNewWithDefault(uuid.NewString())
	err := store.CreateLedger(ctx, &l)
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
	t.Parallel()

	ctx := logging.TestingContext()
	store := newStore(t)

	l := ledger.MustNewWithDefault(uuid.NewString()).WithMetadata(metadata.Metadata{
		"foo": "bar",
	})

	err := store.CreateLedger(ctx, &l)
	require.NoError(t, err)

	err = store.DeleteLedgerMetadata(ctx, l.Name, "foo")
	require.NoError(t, err)

	ledgerFromDB, err := store.GetLedger(ctx, l.Name)
	require.NoError(t, err)
	require.Equal(t, metadata.Metadata{}, ledgerFromDB.Metadata)
}

func newStore(t docker.T) Store {
	t.Helper()

	ctx := logging.TestingContext()
	pgDatabase := srv.NewDatabase(t)

	hooks := make([]bun.QueryHook, 0)
	debugHook := bundebug.NewQueryHook()
	debugHook.Debug = os.Getenv("DEBUG") == "true"
	hooks = append(hooks, debugHook)
	db, err := bunconnect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions(), hooks...)
	require.NoError(t, err)

	ret := New(db)
	require.NoError(t, ret.Migrate(ctx))

	return ret
}
