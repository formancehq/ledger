//go:build it

package legacy_test

import (
	"github.com/formancehq/go-libs/v2/bun/bundebug"
	"github.com/formancehq/go-libs/v2/testing/docker"
	"github.com/formancehq/go-libs/v2/testing/utils"
	"github.com/formancehq/ledger/internal/storage/bucket"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/formancehq/ledger/internal/storage/ledger/legacy"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"go.opentelemetry.io/otel/trace/noop"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/bun/bunconnect"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var (
	srv *pgtesting.PostgresServer
)

func TestMain(m *testing.M) {
	utils.WithTestMain(func(t *utils.TestingTForMain) int {
		srv = pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))

		return m.Run()
	})
}

type T interface {
	require.TestingT
	Helper()
	Cleanup(func())
}

type testStore struct {
	*legacy.Store
	newStore *ledgerstore.Store
}

func newLedgerStore(t T) *testStore {
	t.Helper()

	ledgerName := uuid.NewString()[:8]
	ctx := logging.TestingContext()

	pgDatabase := srv.NewDatabase(t)

	hooks := make([]bun.QueryHook, 0)
	if os.Getenv("DEBUG") == "true" {
		hooks = append(hooks, bundebug.NewQueryHook())
	}

	db, err := bunconnect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions(), hooks...)
	require.NoError(t, err)

	require.NoError(t, systemstore.Migrate(ctx, db))

	l := ledger.MustNewWithDefault(ledgerName)

	b := bucket.NewDefault(db, noop.Tracer{}, ledger.DefaultBucket)
	require.NoError(t, b.Migrate(ctx))
	require.NoError(t, b.AddLedger(ctx, l))

	return &testStore{
		Store:    legacy.New(db, ledger.DefaultBucket, l.Name),
		newStore: ledgerstore.New(db, b, l),
	}
}
