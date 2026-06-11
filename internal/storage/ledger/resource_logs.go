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

func (h logsResourceHandler) BuildDataset(_ common.RepositoryHandlerBuildContext[any]) (*bun.SelectQuery, error) {
	return h.store.newScopedSelect().
		ModelTableExpr(h.store.GetPrefixedRelationName("logs")).
		ColumnExpr("*"), nil
}

func (h logsResourceHandler) ResolveFilter(_ common.ResourceQuery[any], operator, property string, value any) (string, []any, error) {
	switch property {
	case "date":
		value, err := common.NormalizeDateFilterValue(value)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("date %s ?", common.ConvertOperatorToSQL(operator)), []any{value}, nil
	case "id":
		return fmt.Sprintf("id %s ?", common.ConvertOperatorToSQL(operator)), []any{value}, nil
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
