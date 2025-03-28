package system

import (
	"errors"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/resources"
	"github.com/uptrace/bun"
	"regexp"
)

var (
	featuresRegex = regexp.MustCompile(`features\[(.+)]`)
)

type ledgersResourceHandler struct {
	store *DefaultStore
}

func (h ledgersResourceHandler) Filters() []resources.Filter {
	return []resources.Filter{
		{
			Name: "bucket",
			Validators: []resources.PropertyValidator{
				resources.AcceptOperators("$match"),
			},
		},
		{
			Name: `features\[.*]`,
			Validators: []resources.PropertyValidator{
				resources.AcceptOperators("$match"),
			},
		},
	}
}

func (h ledgersResourceHandler) BuildDataset(opts resources.RepositoryHandlerBuildContext[any]) (*bun.SelectQuery, error) {
	return h.store.db.NewSelect().
		Model(&ledger.Ledger{}).
		Column("*"), nil
}

func (h ledgersResourceHandler) ResolveFilter(opts resources.ResourceQuery[any], operator, property string, value any) (string, []any, error) {
	switch {
	case property == "bucket":
		return "bucket = ?", []any{value}, nil
	case featuresRegex.Match([]byte(property)):
		match := featuresRegex.FindAllStringSubmatch(property, 3)

		return "features @> ?", []any{map[string]any{
			match[0][1]: value,
		}}, nil
	default:
		return "", nil, resources.NewErrInvalidQuery("invalid filter property %s", property)
	}
}

func (h ledgersResourceHandler) Project(query resources.ResourceQuery[any], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error) {
	return selectQuery.ColumnExpr("*"), nil
}

func (h ledgersResourceHandler) Expand(opts resources.ResourceQuery[any], property string) (*bun.SelectQuery, *resources.JoinCondition, error) {
	return nil, nil, errors.New("no expansion available")
}

var _ resources.RepositoryHandler[any] = ledgersResourceHandler{}
