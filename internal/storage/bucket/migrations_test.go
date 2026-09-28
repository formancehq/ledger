//go:build it

package bucket_test

import (
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"testing"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	debug "github.com/uptrace/bun/extra/bundebug"
	"go.opentelemetry.io/otel/trace/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/connect"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/storage/migrations"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/formancehq/ledger/internal/storage/system"
)

func TestMigrationsPreservePermanentTables(t *testing.T) {
	t.Parallel()

	migrationNames, err := bucket.WalkMigrations(bucket.MigrationsFS, func(entry fs.DirEntry) (*string, error) {
		return pointer.For(entry.Name()), nil
	})
	require.NoError(t, err)

	for _, testCase := range []struct {
		migration string
		table     string
	}{
		{migration: "11-make-stateless", table: "tmp_volumes"},
		{migration: "17-moves-fill-transaction-id", table: "transactions_ids"},
		{migration: "18-transactions-fill-inserted-at", table: "logs_transactions"},
		{migration: "20-accounts-volumes-fill-history", table: "tmp_volumes"},
		{migration: "42-fix-missing-inserted-at-in-log-data", table: "logs_view"},
	} {
		for _, leftover := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/leftover=%t", testCase.migration, leftover), func(t *testing.T) {
				t.Parallel()

				ctx := logging.ContextWithLogger(t.Context(), logging.Testing())
				pgDatabase := srv.NewDatabase(t)
				options := pgDatabase.ConnectionOptions()
				options.MaxOpenConns = 1
				options.MaxIdleConns = 1
				db, err := connect.OpenSQLDB(ctx, options)
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, db.Close())
				})
				require.NoError(t, system.Migrate(ctx, db))

				const schema = "testbucket"
				migrator := bucket.GetMigrator(db, schema)
				migrationIndex := slices.Index(migrationNames, testCase.migration)
				require.NotEqual(t, -1, migrationIndex)
				for range migrationIndex {
					require.NoError(t, migrator.UpByOne(ctx))
				}

				persistentTable := bun.Ident(schema + "." + testCase.table)
				_, err = db.ExecContext(ctx, "CREATE TABLE ? (sentinel text NOT NULL)", persistentTable)
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, "INSERT INTO ? VALUES (?)", persistentTable, "preserve me")
				require.NoError(t, err)
				if leftover {
					_, err = db.ExecContext(ctx, "CREATE TEMPORARY TABLE ? (stale boolean)", bun.Ident(testCase.table))
					require.NoError(t, err)
				}

				require.NoError(t, migrator.UpByOne(ctx))

				var values []string
				require.NoError(t, db.NewSelect().TableExpr("?", persistentTable).Column("sentinel").Scan(ctx, &values))
				require.Equal(t, []string{"preserve me"}, values)

				var temporaryTableRemoved bool
				require.NoError(t, db.NewRaw("SELECT to_regclass(?) IS NULL", "pg_temp."+testCase.table).Scan(ctx, &temporaryTableRemoved))
				require.True(t, temporaryTableRemoved)
			})
		}
	}
}

func TestMigrations(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	pgDatabase := srv.NewDatabase(t)
	db, err := connect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions())
	require.NoError(t, err)

	require.NoError(t, system.Migrate(ctx, db))
	if testing.Verbose() {
		db.AddQueryHook(debug.NewQueryHook())
	}

	bucketName := uuid.NewString()[:8]
	migrator := bucket.GetMigrator(db, bucketName)
	ledgers := make([]ledger.Ledger, 0)

	for i := 0; i < 5; i++ {
		l, err := ledger.New(fmt.Sprintf("ledger%d", i), ledger.Configuration{
			Bucket: bucketName,
		})
		require.NoError(t, err)
		require.NoError(t, system.New(db).CreateLedger(ctx, l))

		ledgers = append(ledgers, *l)
	}

	_, err = bucket.WalkMigrations(bucket.MigrationsFS, func(entry fs.DirEntry) (*struct{}, error) {
		before, err := bucket.TemplateSQLFile(bucket.MigrationsFS, migrator.GetSchema(), entry.Name(), "up_tests_before.sql", nil)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		if err == nil {
			_, err = db.ExecContext(ctx, before)
			if err != nil {
				return nil, fmt.Errorf("executing pre migration script (%s): %w", entry.Name(), err)
			}
		}

		if err := migrator.UpByOne(ctx); err != nil {
			switch {
			case errors.Is(err, migrations.ErrAlreadyUpToDate):
				return nil, nil
			default:
				return nil, err
			}
		}

		after, err := bucket.TemplateSQLFile(bucket.MigrationsFS, migrator.GetSchema(), entry.Name(), "up_tests_after.sql", nil)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		if err == nil {
			_, err = db.ExecContext(ctx, after)
			if err != nil {
				return nil, fmt.Errorf("executing post migration script (%s): %w", entry.Name(), err)
			}
		}

		return pointer.For(struct{}{}), nil
	})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		store := ledgerstore.New(db, bucket.NewDefault(noop.Tracer{}, bucketName), ledgers[i])

		require.NoError(t, common.Iterate(
			ctx,
			common.InitialPaginatedQuery[any]{
				PageSize: 100,
				Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
			},
			store.Logs().Paginate,
			func(cursor *paginate.Cursor[ledger.Log]) error {
				return nil
			},
		))
	}
}
