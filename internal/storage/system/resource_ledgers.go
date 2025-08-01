package system

import (
	"errors"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/uptrace/bun"
	"regexp"
)

var (
	featuresRegex = regexp.MustCompile(`features\[(.+)]`)
)

type ledgersResourceHandler struct {
	store *DefaultStore
}

func (h ledgersResourceHandler) Schema() common.EntitySchema {
	return common.EntitySchema{
		Fields: map[string]common.Field{
			"bucket":   common.NewStringField(),
			"features": common.NewStringMapField(),
			"metadata": common.NewStringMapField(),
			"name":     common.NewStringField(),
			"id":       common.NewNumericField().Paginated(),
		},
	}
}

func (h ledgersResourceHandler) BuildDataset(_ common.RepositoryHandlerBuildContext[ListLedgersQueryPayload]) (*bun.SelectQuery, error) {
	return h.store.db.NewSelect().
		Model(&ledger.Ledger{}).
		Column("*"), nil
}

func (h ledgersResourceHandler) ResolveFilter(_ common.ResourceQuery[ListLedgersQueryPayload], operator, property string, value any) (string, []any, error) {
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

func (h ledgersResourceHandler) Project(_ common.ResourceQuery[ListLedgersQueryPayload], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error) {
	return selectQuery.ColumnExpr("*"), nil
}

func (h ledgersResourceHandler) Expand(_ common.ResourceQuery[ListLedgersQueryPayload], _ string) (*bun.SelectQuery, *common.JoinCondition, error) {
	return nil, nil, errors.New("no expansion available")
}

var _ common.RepositoryHandler[ListLedgersQueryPayload] = ledgersResourceHandler{}
