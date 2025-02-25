package ledger

import (
	"errors"
	"fmt"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/uptrace/bun"
)

type logsResourceHandler struct{}

func (h logsResourceHandler) filters() []filter {
	return []filter{
		{
			// todo: add validators
			name: "date",
		},
		{
			name: "id",
		},
	}
}

func (h logsResourceHandler) buildDataset(store *Store, _ repositoryHandlerBuildContext[any]) (*bun.SelectQuery, error) {
	return store.db.NewSelect().
		ModelTableExpr(store.GetPrefixedRelationName("logs")).
		ColumnExpr("*").
		Where("ledger = ?", store.ledger.Name), nil
}

func (h logsResourceHandler) resolveFilter(_ *Store, _ ledgercontroller.ResourceQuery[any], operator, property string, value any) (string, []any, error) {
	switch {
	case property == "date" || property == "id":
		return fmt.Sprintf("%s %s ?", property, convertOperatorToSQL(operator)), []any{value}, nil
	default:
		return "", nil, fmt.Errorf("unknown key '%s' when building query", property)
	}
}

func (h logsResourceHandler) expand(_ *Store, _ ledgercontroller.ResourceQuery[any], _ string) (*bun.SelectQuery, *joinCondition, error) {
	return nil, nil, errors.New("no expand supported")
}

func (h logsResourceHandler) project(store *Store, query ledgercontroller.ResourceQuery[any], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error) {
	return selectQuery.ColumnExpr("*"), nil
}

var _ repositoryHandler[any] = logsResourceHandler{}
