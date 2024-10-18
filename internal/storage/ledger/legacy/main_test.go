//go:build it

package ledgerstore

import (
	"database/sql"
	"github.com/formancehq/go-libs/v2/bun/bundebug"
	"github.com/formancehq/go-libs/v2/testing/docker"
	"github.com/formancehq/go-libs/v2/testing/utils"
	"github.com/formancehq/ledger/internal/storage/bucket"
	systemstore "github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"go.opentelemetry.io/otel/trace/noop"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/bun/bunconnect"

	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var (
	srv   *pgtesting.PostgresServer
	bunDB *bun.DB
)

func TestMain(m *testing.M) {
	utils.WithTestMain(func(t *utils.TestingTForMain) int {
		srv = pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))

		db, err := sql.Open("pgx", srv.GetDSN())
		if err != nil {
			logging.Error(err)
			os.Exit(1)
		}

		bunDB = bun.NewDB(db, pgdialect.New())

		return m.Run()
	})
}

type T interface {
	require.TestingT
	Helper()
	Cleanup(func())
}

type testStore struct {
	*Store
	newStore *ledgerstore.Store
}

func newLedgerStore(t T) *testStore {
	t.Helper()

	ledgerName := uuid.NewString()[:8]
	ctx := logging.TestingContext()

	pgDatabase := srv.NewDatabase(t)

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
	require.NoError(t, b.Migrate(ctx, noop.Tracer{}))
	require.NoError(t, b.AddLedger(ctx, l, db))

	return &testStore{
		Store:    New(db, l.Name, l.Name),
		newStore: ledgerstore.New(db, b, l),
	}
}
