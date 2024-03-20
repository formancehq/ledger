package systemstore

import (
	"context"

	"github.com/formancehq/stack/libs/go-libs/time"

	storageerrors "github.com/formancehq/ledger/internal/storage/sqlutils"

	"github.com/uptrace/bun"
)

type configuration struct {
	bun.BaseModel `bun:"_system.configuration,alias:configuration"`

	Key     string    `bun:"key,type:varchar(255),pk"` // Primary key
	Value   string    `bun:"value,type:text"`
	AddedAt time.Time `bun:"addedAt,type:timestamp"`
}

func (s *Store) GetConfiguration(ctx context.Context, key string) (string, error) {
	query := s.db.NewSelect().
		Model((*configuration)(nil)).
		Column("value").
		Where("key = ?", key).
		Limit(1).
		String()

	row := s.db.QueryRowContext(ctx, query)
	if row.Err() != nil {
		return "", storageerrors.PostgresError(row.Err())
	}
	var value string
	if err := row.Scan(&value); err != nil {
		return "", storageerrors.PostgresError(err)
	}

	return value, nil
}

func (s *Store) InsertConfiguration(ctx context.Context, key, value string) error {
	config := &configuration{
		Key:     key,
		Value:   value,
		AddedAt: time.Now(),
	}

	_, err := s.db.NewInsert().
		Model(config).
		Exec(ctx)

	return storageerrors.PostgresError(err)
}
