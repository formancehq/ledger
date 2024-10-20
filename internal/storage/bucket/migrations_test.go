//go:build it

package bucket_test

import (
	"errors"
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/extra/bundebug"
	"io/fs"
	"testing"
)

func TestMigrations(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	pgDatabase := srv.NewDatabase(t)
	db, err := bunconnect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions())
	require.NoError(t, err)

	require.NoError(t, driver.Migrate(ctx, db))
	if testing.Verbose() {
		db.AddQueryHook(bundebug.NewQueryHook())
	}

	bucketName := uuid.NewString()[:8]
	migrator := bucket.GetMigrator(bucketName)

	_, err = bucket.WalkMigrations(func(entry fs.DirEntry) (*struct{}, error) {
		before, err := bucket.TemplateSQLFile(bucketName, entry.Name(), "tests_before.sql")
		if !errors.Is(err, fs.ErrNotExist) {
			require.NoError(t, err)
		}
		if err == nil {
			_, err = db.ExecContext(ctx, before)
			require.NoError(t, err, "executing pre migration script: %s", entry.Name())
		}

		if err := migrator.UpByOne(ctx, db); err != nil {
			if !errors.Is(err, migrations.ErrAlreadyUpToDate) {
				require.Fail(t, err.Error())
			}
		}

		after, err := bucket.TemplateSQLFile(bucketName, entry.Name(), "tests_after.sql")
		if !errors.Is(err, fs.ErrNotExist) {
			require.NoError(t, err)
		}
		if err == nil {
			_, err = db.ExecContext(ctx, after)
			require.NoErrorf(t, err, "executing post migration script: %s", entry.Name())
		}

		return pointer.For(struct{}{}), nil
	})
	require.NoError(t, err)

	//moves := make([]map[string]any, 0)
	//err = db.NewSelect().
	//	ModelTableExpr(`"`+bucketName+`".moves`).
	//	Scan(ctx, &moves)
	//require.NoError(t, err)
	//
	//rows, err := db.NewSelect().
	//	ModelTableExpr(`"`+bucketName+`".transactions`).
	//	Column("seq", "id", "post_commit_volumes", "ledger").
	//	Order("id desc").
	//	Where("ledger = 'ledger0'").
	//	Rows(ctx)
	//require.NoError(t, err)
	//
	//data, _ := xsql.Pretty(rows)
	//fmt.Println(data)
}
