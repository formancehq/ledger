package ledger

import (
	"errors"
	"fmt"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/uptrace/bun"
)

type logsResourceHandler struct {
	store *Store
}

func (h logsResourceHandler) Schema() common.EntitySchema {
	return common.EntitySchema{
		Fields: map[string]common.Field{
			"date": common.NewDateField().Paginated(),
			"id":   common.NewNumericField().Paginated(),
		},
	}
}

func (h logsResourceHandler) BuildDataset(opts common.RepositoryHandlerBuildContext[any]) (*bun.SelectQuery, error) {
	ret := h.store.db.NewSelect().
		ModelTableExpr(h.store.GetPrefixedRelationName("logs")).
		ColumnExpr("*")
	ret = h.store.applyLedgerFilter(opts.Ctx, ret, "logs")
	return ret, nil
}

func (h logsResourceHandler) ResolveFilter(_ common.ResourceQuery[any], operator, property string, value any) (string, []any, error) {
	switch property {
	case "date", "id":
		return fmt.Sprintf("%s %s ?", property, common.ConvertOperatorToSQL(operator)), []any{value}, nil
	default:
		return "", nil, fmt.Errorf("unknown key '%s' when building query", property)
	}
}

func (h logsResourceHandler) Expand(_ common.ResourceQuery[any], _ string) (*bun.SelectQuery, *common.JoinCondition, error) {
	return nil, nil, errors.New("no expand supported")
}

func (h logsResourceHandler) Project(_ common.ResourceQuery[any], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error) {
	return selectQuery.ColumnExpr("*"), nil
}

var _ common.RepositoryHandler[any] = logsResourceHandler{}
