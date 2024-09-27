package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/shomali11/xsql"

	"github.com/formancehq/go-libs/platform/postgres"

	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/migrations"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

type Store struct {
	db     bun.IDB
	ledger ledger.Ledger
}

func (s *Store) Name() string {
	return s.ledger.Name
}

func (s *Store) GetDB() bun.IDB {
	return s.db
}

func (s *Store) GetPrefixedRelationName(v string) string {
	return fmt.Sprintf(`"%s".%s`, s.ledger.Bucket, v)
}

func (s *Store) WithDB(db bun.IDB) *Store {
	return &Store{
		ledger: s.ledger,
		db:     db,
	}
}

// todo: merge with bucket migration info
// todo: add test
func (s *Store) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return getMigrator(s.ledger).GetMigrations(ctx, s.db)
}

func (s *Store) IsUpToDate(ctx context.Context) (bool, error) {
	bucketUpToDate, err := tracing.TraceWithLatency(ctx, "CheckBucketSchema", func(ctx context.Context) (bool, error) {
		return bucket.New(s.db, s.ledger.Bucket).IsUpToDate(ctx)
	})
	if err != nil {
		return false, errors.Wrap(err, "failed to check if bucket is up to date")
	}
	if !bucketUpToDate {
		logging.FromContext(ctx).Errorf("bucket %s is not up to date", s.ledger.Bucket)
		return false, nil
	}

	ret, err := tracing.TraceWithLatency(ctx, "CheckLedgerSchema", func(ctx context.Context) (bool, error) {
		return getMigrator(s.ledger).IsUpToDate(ctx, s.db)
	})
	if err != nil && errors.Is(err, migrations.ErrMissingVersionTable) {
		logging.FromContext(ctx).Errorf("ledger %s is not up to date", s.ledger.Name)
		return false, nil
	}

	return ret, err
}

func (s *Store) validateAddressFilter(operator string, value any) error {
	if operator != "$match" {
		return errors.New("'address' column can only be used with $match")
	}
	if value, ok := value.(string); !ok {
		return fmt.Errorf("invalid 'address' filter")
	} else {
		if isSegmentedAddress(value) && !s.ledger.HasFeature(ledger.FeatureIndexAddressSegments, "ON") {
			return fmt.Errorf("feature %s must be 'ON' to use segments address", ledger.FeatureIndexAddressSegments)
		}
	}

	return nil
}

// dev util
func (s *Store) dumpTables(ctx context.Context, tables ...string) {
	for _, table := range tables {
		s.dumpQuery(
			ctx,
			s.db.NewSelect().
				ModelTableExpr(s.GetPrefixedRelationName(table)),
		)
	}
}

func (s *Store) dumpQuery(ctx context.Context, query *bun.SelectQuery) {
	fmt.Println(query)
	rows, err := query.Rows(ctx)
	if err != nil {
		panic(err)
	}
	s.dumpRows(rows)
}

func (s *Store) dumpRows(rows *sql.Rows) {
	data, err := xsql.Pretty(rows)
	if err != nil {
		panic(err)
	}
	fmt.Println(data)
	if err := rows.Close(); err != nil {
		panic(err)
	}
}

func (s *Store) LockLedger(ctx context.Context) error {
	_, err := s.db.NewRaw(`lock table ` + s.GetPrefixedRelationName("logs")).Exec(ctx)
	return postgres.ResolveError(err)
}

func New(db bun.IDB, ledger ledger.Ledger) *Store {
	return &Store{
		db:     db,
		ledger: ledger,
	}
}
