//go:build it

package bucket_test

import (
	"errors"
	"fmt"
	"io/fs"
	"testing"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/extra/bundebug"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/go-libs/v4/bun/bunconnect"
	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/migrations"
	"github.com/formancehq/go-libs/v4/pointer"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/formancehq/ledger/internal/storage/system"
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
				Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
			},
			store.Logs().Paginate,
			func(cursor *bunpaginate.Cursor[ledger.Log]) error {
				return nil
			},
		))
	}
}
