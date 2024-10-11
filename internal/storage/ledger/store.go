package ledger

import (
	"context"
	"fmt"

	"github.com/formancehq/go-libs/platform/postgres"

	"github.com/formancehq/ledger/internal/tracing"

	"errors"
	"github.com/formancehq/go-libs/migrations"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/uptrace/bun"
)

type Store struct {
	db     bun.IDB
	ledger ledger.Ledger
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
		return false, fmt.Errorf("failed to check if bucket is up to date: %w", err)
	}
	if !bucketUpToDate {
		return false, nil
	}

	ret, err := tracing.TraceWithLatency(ctx, "CheckLedgerSchema", func(ctx context.Context) (bool, error) {
		return getMigrator(s.ledger).IsUpToDate(ctx, s.db)
	})
	if err != nil && errors.Is(err, migrations.ErrMissingVersionTable) {
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
	} else if isSegmentedAddress(value) && !s.ledger.HasFeature(ledger.FeatureIndexAddressSegments, "ON") {
		return fmt.Errorf("feature %s must be 'ON' to use segments address", ledger.FeatureIndexAddressSegments)
	}

	return nil
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
