package ledger

import (
	"errors"
	"fmt"
	"github.com/formancehq/ledger/internal/storage/resources"
	"github.com/uptrace/bun"
)

type logsResourceHandler struct {
	store *Store
}

func (h logsResourceHandler) Filters() []resources.Filter {
	return []resources.Filter{
		{
			// todo: add validators
			Name: "date",
		},
		{
			Name: "id",
		},
	}
}

func (h logsResourceHandler) BuildDataset(_ resources.RepositoryHandlerBuildContext[any]) (*bun.SelectQuery, error) {
	return h.store.db.NewSelect().
		ModelTableExpr(h.store.GetPrefixedRelationName("logs")).
		ColumnExpr("*").
		Where("ledger = ?", h.store.ledger.Name), nil
}

func (h logsResourceHandler) ResolveFilter(_ resources.ResourceQuery[any], operator, property string, value any) (string, []any, error) {
	switch {
	case property == "date" || property == "id":
		return fmt.Sprintf("%s %s ?", property, resources.ConvertOperatorToSQL(operator)), []any{value}, nil
	default:
		return "", nil, fmt.Errorf("unknown key '%s' when building query", property)
	}
}

func (h logsResourceHandler) Expand(_ resources.ResourceQuery[any], _ string) (*bun.SelectQuery, *resources.JoinCondition, error) {
	return nil, nil, errors.New("no expand supported")
}

func (h logsResourceHandler) Project(query resources.ResourceQuery[any], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error) {
	return selectQuery.ColumnExpr("*"), nil
}

var _ resources.RepositoryHandler[any] = logsResourceHandler{}
