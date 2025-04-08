//go:build it

package driver_test

import (
	"database/sql"
	. "github.com/formancehq/go-libs/v3/testing/utils"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v3/bun/bundebug"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/stretchr/testify/require"
)

var (
	srv *pgtesting.PostgresServer
	db  *bun.DB
)

func TestMain(m *testing.M) {
	WithTestMain(func(t *TestingTForMain) int {
		srv = pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()), pgtesting.WithExtension("pgcrypto"))
		sqlDB, err := sql.Open("pgx", srv.GetDSN())
		require.NoError(t, err)

		db = bun.NewDB(sqlDB, pgdialect.New(), bun.WithDiscardUnknownColumns())
		if os.Getenv("DEBUG") == "true" {
			queryHook := bundebug.NewQueryHook()
			queryHook.Debug = true
			db.AddQueryHook(queryHook)
		}
		require.NoError(t, systemstore.Migrate(logging.TestingContext(), db))

		return m.Run()
	})
}
