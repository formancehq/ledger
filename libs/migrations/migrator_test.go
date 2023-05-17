package migrations

import (
	"context"
	"database/sql"
	"testing"

	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/extra/bundebug"
)

func TestMigrations(t *testing.T) {
	require.NoError(t, pgtesting.CreatePostgresServer())
	defer func() {
		require.NoError(t, pgtesting.DestroyPostgresServer())
	}()

	migrator := NewMigrator()
	migrator.RegisterMigrations(
		Migration{
			Up: func(tx bun.Tx) error {
				_, err := tx.Exec(`CREATE TABLE "foo" (id varchar)`)
				return err
			},
		},
	)

	db := pgtesting.NewPostgresDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnString())
	require.NoError(t, err)

	bunDB := bun.NewDB(sqlDB, pgdialect.New())
	if testing.Verbose() {
		bunDB.AddQueryHook(bundebug.NewQueryHook(
			bundebug.WithVerbose(true),
			bundebug.FromEnv("BUNDEBUG"),
		))
	}

	require.NoError(t, migrator.Up(context.Background(), bunDB))
	version, err := migrator.GetDBVersion(context.Background(), bunDB)
	require.NoError(t, err)
	require.EqualValues(t, 1, version)
}
