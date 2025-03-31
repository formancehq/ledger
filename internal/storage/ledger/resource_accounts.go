package ledger

import (
	"fmt"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/stoewer/go-strcase"
	"github.com/uptrace/bun"
)

type accountsResourceHandler struct {
	store *Store
}

func (h accountsResourceHandler) Filters() []common.Filter {
	return []common.Filter{
		{
			Name: "address",
			Validators: []common.PropertyValidator{
				common.PropertyValidatorFunc(func(operator string, key string, value any) error {
					return validateAddressFilter(h.store.ledger, operator, value)
				}),
			},
		},
		{
			Name: "first_usage",
			Validators: []common.PropertyValidator{
				common.AcceptOperators("$lt", "$gt", "$lte", "$gte", "$match"),
			},
		},
		{
			Name: `balance(\[.*])?`,
			Validators: []common.PropertyValidator{
				common.AcceptOperators("$lt", "$gt", "$lte", "$gte", "$match"),
			},
		},
		{
			Name: "metadata",
			Validators: []common.PropertyValidator{
				common.AcceptOperators("$exists"),
			},
		},
		{
			Name: `metadata\[.*]`,
			Validators: []common.PropertyValidator{
				common.AcceptOperators("$match"),
			},
		},
	}
}

func (h accountsResourceHandler) BuildDataset(opts common.RepositoryHandlerBuildContext[any]) (*bun.SelectQuery, error) {
	ret := h.store.db.NewSelect()

	// Build the query
	ret = ret.
		ModelTableExpr(h.store.GetPrefixedRelationName("accounts")).
		Column("address", "address_array", "first_usage", "insertion_date", "updated_at").
		Where("ledger = ?", h.store.ledger.Name)

	if opts.PIT != nil && !opts.PIT.IsZero() {
		ret = ret.Where("accounts.first_usage <= ?", opts.PIT)
	}

	if h.store.ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") && opts.PIT != nil && !opts.PIT.IsZero() {
		selectDistinctAccountMetadataHistories := h.store.db.NewSelect().
			DistinctOn("accounts_address").
			ModelTableExpr(h.store.GetPrefixedRelationName("accounts_metadata")).
			Where("ledger = ?", h.store.ledger.Name).
			Column("accounts_address").
			ColumnExpr("first_value(metadata) over (partition by accounts_address order by revision desc) as metadata").
			Where("date <= ?", opts.PIT)

		ret = ret.
			Join(
				`left join (?) accounts_metadata on accounts_metadata.accounts_address = accounts.address`,
				selectDistinctAccountMetadataHistories,
			).
			ColumnExpr("coalesce(accounts_metadata.metadata, '{}'::jsonb) as metadata")
	} else {
		ret = ret.ColumnExpr("accounts.metadata")
	}

	return ret, nil
}

func (h accountsResourceHandler) ResolveFilter(opts common.ResourceQuery[any], operator, property string, value any) (string, []any, error) {
	switch {
	case property == "address":
		return filterAccountAddress(value.(string), "address"), nil, nil
	case property == "first_usage":
		return fmt.Sprintf("first_usage %s ?", common.ConvertOperatorToSQL(operator)), []any{value}, nil
	case balanceRegex.MatchString(property) || property == "balance":

		selectBalance := h.store.db.NewSelect().
			Where("accounts_address = dataset.address").
			Where("ledger = ?", h.store.ledger.Name)

		if opts.PIT != nil && !opts.PIT.IsZero() {
			if !h.store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
				return "", nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistory)
			}
			selectBalance = selectBalance.
				ModelTableExpr(h.store.GetPrefixedRelationName("moves")).
				DistinctOn("asset").
				ColumnExpr("first_value((post_commit_volumes).inputs - (post_commit_volumes).outputs) over (partition by (accounts_address, asset) order by seq desc) as balance").
				Where("insertion_date <= ?", opts.PIT)
		} else {
			selectBalance = selectBalance.
				ModelTableExpr(h.store.GetPrefixedRelationName("accounts_volumes")).
				ColumnExpr("input - output as balance")
		}

		if balanceRegex.MatchString(property) {
			selectBalance = selectBalance.Where("asset = ?", balanceRegex.FindAllStringSubmatch(property, 2)[0][1])
		}

		return h.store.db.NewSelect().
			TableExpr("(?) balance", selectBalance).
			ColumnExpr(fmt.Sprintf("balance %s ?", common.ConvertOperatorToSQL(operator)), value).
			String(), nil, nil
	case property == "metadata":
		return "metadata -> ? is not null", []any{value}, nil

	case common.MetadataRegex.Match([]byte(property)):
		match := common.MetadataRegex.FindAllStringSubmatch(property, 3)

		return "metadata @> ?", []any{map[string]any{
			match[0][1]: value,
		}}, nil
	default:
		return "", nil, common.NewErrInvalidQuery("invalid filter property %s", property)
	}
}

func (h accountsResourceHandler) Project(query common.ResourceQuery[any], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error) {
	return selectQuery.ColumnExpr("*"), nil
}

func (h accountsResourceHandler) Expand(opts common.ResourceQuery[any], property string) (*bun.SelectQuery, *common.JoinCondition, error) {
	switch property {
	case "volumes":
		if !h.store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
			return nil, nil, common.NewErrInvalidQuery("feature %s must be 'ON' to use volumes", features.FeatureMovesHistory)
		}
	case "effectiveVolumes":
		if !h.store.ledger.HasFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
			return nil, nil, common.NewErrInvalidQuery("feature %s must be 'SYNC' to use effectiveVolumes", features.FeatureMovesHistoryPostCommitEffectiveVolumes)
		}
	}

	selectRowsQuery := h.store.db.NewSelect().
		Where("accounts_address in (select address from dataset)")
	if opts.UsePIT() {
		selectRowsQuery = selectRowsQuery.
			ModelTableExpr(h.store.GetPrefixedRelationName("moves")).
			DistinctOn("accounts_address, asset").
			Column("accounts_address", "asset").
			Where("ledger = ?", h.store.ledger.Name)
		if property == "volumes" {
			selectRowsQuery = selectRowsQuery.
				ColumnExpr("first_value(post_commit_volumes) over (partition by (accounts_address, asset) order by seq desc) as volumes").
				Where("insertion_date <= ?", opts.PIT)
		} else {
			selectRowsQuery = selectRowsQuery.
				ColumnExpr("first_value(post_commit_volumes) over (partition by (accounts_address, asset) order by effective_date desc) as volumes").
				Where("effective_date <= ?", opts.PIT)
		}
	} else {
		selectRowsQuery = selectRowsQuery.
			ModelTableExpr(h.store.GetPrefixedRelationName("accounts_volumes")).
			Column("asset", "accounts_address").
			ColumnExpr("(input, output)::"+h.store.GetPrefixedRelationName("volumes")+" as volumes").
			Where("ledger = ?", h.store.ledger.Name)
	}

	return h.store.db.NewSelect().
			With("rows", selectRowsQuery).
			ModelTableExpr("rows").
			Column("accounts_address").
			ColumnExpr("public.aggregate_objects(json_build_object(asset, json_build_object('input', (volumes).inputs, 'output', (volumes).outputs))::jsonb) as " + strcase.SnakeCase(property)).
			Group("accounts_address"), &common.JoinCondition{
			Left:  "address",
			Right: "accounts_address",
		}, nil
}

var _ common.RepositoryHandler[any] = accountsResourceHandler{}
