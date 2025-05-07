package system

import (
	"errors"
	"regexp"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/uptrace/bun"
)

var (
	featuresRegex = regexp.MustCompile(`features\[(.+)]`)
)

type ledgersResourceHandler struct {
	store *DefaultStore
}

func (h ledgersResourceHandler) Filters() []common.Filter {
	return []common.Filter{
		{
			Name: "bucket",
			Validators: []common.PropertyValidator{
				common.AcceptOperators("$match"),
			},
		},
		{
			Name: `features\[.*]`,
			Validators: []common.PropertyValidator{
				common.AcceptOperators("$match"),
			},
		},
		{
			Name: `metadata\[.*]`,
			Validators: []common.PropertyValidator{
				common.AcceptOperators("$match"),
			},
		},
		{
			Name: `name`,
			Validators: []common.PropertyValidator{
				common.AcceptOperators("$match", "$like"),
			},
		},
	}
}

func (h ledgersResourceHandler) BuildDataset(opts common.RepositoryHandlerBuildContext[any]) (*bun.SelectQuery, error) {
	return h.store.db.NewSelect().
		Model(&ledger.Ledger{}).
		Join("LEFT JOIN _system.buckets ON _system.ledgers.bucket = _system.buckets.name").
		Where("_system.buckets.deleted_at IS NULL").
		Column("_system.ledgers.*"), nil
}

func (h ledgersResourceHandler) ResolveFilter(opts common.ResourceQuery[any], operator, property string, value any) (string, []any, error) {
	switch {
	case property == "bucket":
		return "bucket = ?", []any{value}, nil
	case featuresRegex.Match([]byte(property)):
		match := featuresRegex.FindAllStringSubmatch(property, 3)

		return "features @> ?", []any{map[string]any{
			match[0][1]: value,
		}}, nil
	case common.MetadataRegex.Match([]byte(property)):
		match := common.MetadataRegex.FindAllStringSubmatch(property, 3)

		return "metadata @> ?", []any{map[string]any{
			match[0][1]: value,
		}}, nil

	case property == "metadata":
		return "metadata -> ? is not null", []any{value}, nil
	case property == "name":
		return "name " + common.ConvertOperatorToSQL(operator) + " ?", []any{value}, nil
	default:
		return "", nil, common.NewErrInvalidQuery("invalid filter property %s", property)
	}
}

func (h ledgersResourceHandler) Project(query common.ResourceQuery[any], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error) {
	return selectQuery.ColumnExpr("*"), nil
}

func (h ledgersResourceHandler) Expand(opts common.ResourceQuery[any], property string) (*bun.SelectQuery, *common.JoinCondition, error) {
	return nil, nil, errors.New("no expansion available")
}

var _ common.RepositoryHandler[any] = ledgersResourceHandler{}
