//go:build it

package ledger_test

import (
	"database/sql"
	. "github.com/formancehq/go-libs/v2/testing/utils"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"go.opentelemetry.io/otel/trace/noop"
	"math/big"
	"sync/atomic"
	"testing"

	"github.com/formancehq/go-libs/v2/bun/bundebug"
	"github.com/formancehq/go-libs/v2/testing/docker"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/google/go-cmp/cmp"

	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var (
	srv         = NewDeferred[*pgtesting.PostgresServer]()
	bunDB       = NewDeferred[*bun.DB]()
	ledgerCount = atomic.Int64{}
)

func TestMain(m *testing.M) {
	WithTestMain(func(t *TestingTForMain) int {
		srv.LoadAsync(func() *pgtesting.PostgresServer {
			ret := pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()), pgtesting.WithExtension("pgcrypto"))

			bunDB.LoadAsync(func() *bun.DB {
				db, err := sql.Open("pgx", ret.GetDSN())
				require.NoError(t, err)

				bunDB := bun.NewDB(db, pgdialect.New(), bun.WithDiscardUnknownColumns())
				if testing.Verbose() {
					bunDB.AddQueryHook(bundebug.NewQueryHook())
				}

				require.NoError(t, driver.Migrate(logging.TestingContext(), bunDB))

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

	l := ledger.MustNewWithDefault(ledgerName)
	l.Bucket = ledgerName
	l.ID = int(ledgerCount.Add(1))

	b := bucket.New(bunDB.GetValue(), ledgerName)
	require.NoError(t, b.Migrate(ctx, noop.Tracer{}))
	require.NoError(t, b.AddLedger(ctx, l, bunDB.GetValue()))

	return ledgerstore.New(bunDB.GetValue(), b, l)
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
