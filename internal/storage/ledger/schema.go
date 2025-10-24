package ledger

import (
	"context"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/platform/postgres"
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

func (s *Store) FindSchemas(ctx context.Context, query common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Schema], error) {
	return s.Schemas().Paginate(ctx, query)
}
