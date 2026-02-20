package ledger

import (
	"context"
	"database/sql"
	"errors"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
	"github.com/formancehq/go-libs/v4/platform/postgres"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
)

func (s *Store) InsertSchema(ctx context.Context, schema *ledger.Schema) error {
	_, err := s.db.NewInsert().
		Model(schema).
		Value("ledger", "?", s.ledger.Name).
		ModelTableExpr(s.GetPrefixedRelationName("schemas")).
		Returning("created_at").
		Exec(ctx)
	return postgres.ResolveError(err)
}

func (s *Store) FindSchema(ctx context.Context, version string) (*ledger.Schema, error) {
	schema := &ledger.Schema{}
	err := s.db.NewSelect().
		Model(schema).
		ModelTableExpr(s.GetPrefixedRelationName("schemas")).
		Where("version = ?", version).
		Where("ledger = ?", s.ledger.Name).
		Scan(ctx)
	if err != nil {
		return nil, postgres.ResolveError(err)
	}

	return schema, nil
}

func (s *Store) FindLatestSchemaVersion(ctx context.Context) (*string, error) {
	schema := &ledger.Schema{}
	err := s.db.NewSelect().
		Model(schema).
		ModelTableExpr(s.GetPrefixedRelationName("schemas")).
		Where("ledger = ?", s.ledger.Name).
		Order("created_at DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		} else {
			return nil, postgres.ResolveError(err)
		}
	}
	return &schema.Version, nil
}

func (s *Store) FindSchemas(ctx context.Context, query common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Schema], error) {
	return s.Schemas().Paginate(ctx, query)
}
