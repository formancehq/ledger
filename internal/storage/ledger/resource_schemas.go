package ledger

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/uptrace/bun"
)

type schemasResourceHandler struct {
	store *Store
}

func (h schemasResourceHandler) Schema() common.EntitySchema {
	return common.EntitySchema{
		Fields: map[string]common.Field{
			"version":    common.NewStringField().Paginated(),
			"created_at": common.NewDateField().Paginated(),
		},
	}
}

func (h schemasResourceHandler) BuildDataset(opts common.RepositoryHandlerBuildContext[any]) (*bun.SelectQuery, error) {
	q := h.store.db.NewSelect().
		ModelTableExpr(h.store.GetPrefixedRelationName("schemas")).
		Where("ledger = ?", h.store.ledger.Name)

	if opts.PIT != nil && !opts.PIT.IsZero() {
		q = q.Where("created_at <= ?", opts.PIT)
	}

	return q, nil
}

func (h schemasResourceHandler) Project(_ common.ResourceQuery[any], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error) {
	return selectQuery.ColumnExpr("*"), nil
}

func (h schemasResourceHandler) ResolveFilter(_ common.ResourceQuery[any], operator, property string, value any) (string, []any, error) {
	switch property {
	case "version", "created_at":
		return fmt.Sprintf("%s %s ?", property, common.ConvertOperatorToSQL(operator)), []any{value}, nil
	default:
		return "", nil, fmt.Errorf("unknown key '%s' when building query", property)
	}
}

func (h schemasResourceHandler) Expand(_ common.ResourceQuery[any], _ string) (*bun.SelectQuery, *common.JoinCondition, error) {
	return nil, nil, errors.New("no expand supported")
}

var _ common.RepositoryHandler[any] = schemasResourceHandler{}
