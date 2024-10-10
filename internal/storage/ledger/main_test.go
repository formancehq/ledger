//go:build it

package ledger_test

import (
	"database/sql"
	systemstore "github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/bun/bundebug"
	"github.com/formancehq/go-libs/testing/docker"
	. "github.com/formancehq/go-libs/testing/utils"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/google/go-cmp/cmp"

	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var (
	srv   = NewDeferred[*pgtesting.PostgresServer]()
	bunDB = NewDeferred[*bun.DB]()
)

func TestMain(m *testing.M) {
	WithTestMain(func(t *TestingTForMain) int {
		srv.LoadAsync(func() *pgtesting.PostgresServer {
			ret := pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()), pgtesting.WithExtension("pgcrypto"))

			bunDB.LoadAsync(func() *bun.DB {
				db, err := sql.Open("postgres", ret.GetDSN())
				require.NoError(t, err)

				bunDB := bun.NewDB(db, pgdialect.New())
				if testing.Verbose() {
					bunDB.AddQueryHook(bundebug.NewQueryHook())
				}

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

func newLedgerStore(t T) *ledgerstore.Store {
	t.Helper()

	ledgerName := uuid.NewString()[:8]
	ctx := logging.TestingContext()

	Wait(srv, bunDB)

	pgDatabase := srv.GetValue().NewDatabase(t)

	hooks := make([]bun.QueryHook, 0)
	if testing.Verbose() {
		hooks = append(hooks, bundebug.NewQueryHook())
	}

	db, err := bunconnect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions(), hooks...)
	require.NoError(t, err)

	require.NoError(t, systemstore.Migrate(ctx, db))

	l := ledger.MustNewWithDefault(ledgerName)
	l.Bucket = ledgerName

	b := bucket.New(db, ledgerName)
	require.NoError(t, b.Migrate(ctx))
	require.NoError(t, ledgerstore.Migrate(ctx, db, l))

	return ledgerstore.New(db, l)
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
