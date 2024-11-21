//go:build it

package bucket_test

import (
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/system"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/extra/bundebug"
	"testing"
)

func TestMigrations(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	pgDatabase := srv.NewDatabase(t)
	db, err := bunconnect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions())
	require.NoError(t, err)

	require.NoError(t, system.Migrate(ctx, db))
	if testing.Verbose() {
		db.AddQueryHook(bundebug.NewQueryHook())
	}

	bucketName := uuid.NewString()[:8]
	migrator := bucket.GetMigrator(db, bucketName)

	err = migrations.TestMigrations(ctx, bucket.MigrationsFS, migrator)
	require.NoError(t, err)
}
