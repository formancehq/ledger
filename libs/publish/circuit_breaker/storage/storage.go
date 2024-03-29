package storage

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

type Store interface {
	Insert(ctx context.Context, topic string, data []byte, metadata map[string]string) error
	List(ctx context.Context) ([]*CircuitBreakerModel, error)
	Delete(ctx context.Context, ids []uint64) error
}

type CircuitBreakerModel struct {
	bun.BaseModel `bun:"circuit_breaker"`

	ID        uint64            `bun:"id,pk,autoincrement"`
	CreatedAt time.Time         `bun:",notnull"`
	Topic     string            `bun:",notnull"`
	Data      []byte            `bun:",notnull"`
	Metadata  map[string]string `bun:",type:jsonb"`
}

type Storage struct {
	db           *bun.DB
	schema       string
	storageLimit int
}

func New(schema string, db *bun.DB, storageLimit int) *Storage {
	return &Storage{
		schema:       schema,
		db:           db,
		storageLimit: storageLimit,
	}
}

func (s *Storage) Insert(ctx context.Context, topic string, data []byte, metadata map[string]string) error {
	_, err := s.db.NewInsert().
		Model(&CircuitBreakerModel{
			CreatedAt: time.Now().UTC(),
			Topic:     topic,
			Data:      data,
			Metadata:  metadata,
		},
		).Exec(ctx)

	return err
}

func (s *Storage) List(ctx context.Context) ([]*CircuitBreakerModel, error) {
	var models []*CircuitBreakerModel
	err := s.db.NewSelect().
		Model(&models).
		Order("created_at ASC").
		Limit(s.storageLimit).
		Scan(ctx)

	return models, err
}

func (s *Storage) Delete(ctx context.Context, ids []uint64) error {
	models := make([]*CircuitBreakerModel, 0, len(ids))
	for _, id := range ids {
		models = append(models, &CircuitBreakerModel{ID: id})
	}

	_, err := s.db.NewDelete().
		Model(&models).
		WherePK().
		Exec(ctx)

	return err
}
