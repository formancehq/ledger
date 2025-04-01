//go:build it

package ledger_test

import (
	"database/sql"
	"github.com/formancehq/go-libs/v2/bun/bundebug"
	. "github.com/formancehq/go-libs/v2/testing/utils"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"math/big"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/testing/docker"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/google/go-cmp/cmp"

	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var (
	srv           = NewDeferred[*pgtesting.PostgresServer]()
	defaultBunDB  = NewDeferred[*bun.DB]()
	defaultDriver = NewDeferred[*driver.Driver]()
)

func TestMain(m *testing.M) {
	WithTestMain(func(t *TestingTForMain) int {
		srv.LoadAsync(func() *pgtesting.PostgresServer {
			ret := pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()),
				pgtesting.WithExtension("pgcrypto"),
			)

			defaultBunDB.LoadAsync(func() *bun.DB {
				db, err := sql.Open("pgx", ret.GetDSN())
				require.NoError(t, err)

				bunDB := bun.NewDB(db, pgdialect.New(), bun.WithDiscardUnknownColumns())
				hook := bundebug.NewQueryHook()
				hook.Debug = os.Getenv("DEBUG") == "true"
				bunDB.AddQueryHook(hook)
				bunDB.SetMaxOpenConns(100)

				require.NoError(t, systemstore.Migrate(logging.TestingContext(), bunDB))

				err = bucket.GetMigrator(bunDB, "_default").Up(logging.TestingContext())
				require.NoError(t, err)

				defaultDriver.SetValue(driver.New(
					bunDB,
					ledgerstore.NewFactory(bunDB),
					bucket.NewDefaultFactory(),
					systemstore.NewStoreFactory(),
				))

				return bunDB
			})
			return ret
		})

		return m.Run()
	})
}

type T interface {
	require.TestingT
	Helper()
	Cleanup(func())
}

func newLedgerStore(t T, opts ...func(cfg *ledger.Configuration)) *ledgerstore.Store {
	t.Helper()

	<-defaultBunDB.Done()
	<-defaultDriver.Done()

	ledgerName := uuid.NewString()[:8]
	ctx := logging.TestingContext()

	ledgerConfiguration := ledger.NewDefaultConfiguration()
	for _, opt := range opts {
		opt(&ledgerConfiguration)
	}

	l, err := ledger.New(ledgerName, ledgerConfiguration)
	require.NoError(t, err)

	err = bucket.GetMigrator(defaultBunDB.GetValue(), "_default").Up(ctx)
	require.NoError(t, err)

	store, err := defaultDriver.GetValue().CreateLedger(ctx, l)
	require.NoError(t, err)

	return store
}

func bigIntComparer(v1 *big.Int, v2 *big.Int) bool {
	return v1.String() == v2.String()
}

func RequireEqual(t *testing.T, expected, actual any) {
	t.Helper()
	if diff := cmp.Diff(expected, actual, cmp.Comparer(bigIntComparer)); diff != "" {
		require.Failf(t, "Content not matching", diff)
	}
}
