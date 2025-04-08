package ledger

import (
	"fmt"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/stoewer/go-strcase"
	"github.com/uptrace/bun"
)

type accountsResourceHandler struct{}

func (h accountsResourceHandler) filters() []filter {
	return []filter{
		{
			name: "address",
			validators: []propertyValidator{
				propertyValidatorFunc(func(l ledger.Ledger, operator string, key string, value any) error {
					return validateAddressFilter(l, operator, value)
				}),
			},
		},
		{
			name: "first_usage",
			validators: []propertyValidator{
				acceptOperators("$lt", "$gt", "$lte", "$gte", "$match"),
			},
		},
		{
			name: `balance(\[.*])?`,
			validators: []propertyValidator{
				acceptOperators("$lt", "$gt", "$lte", "$gte", "$match"),
			},
		},
		{
			name: "metadata",
			validators: []propertyValidator{
				acceptOperators("$exists"),
			},
		},
		{
			name: `metadata\[.*]`,
			validators: []propertyValidator{
				acceptOperators("$match"),
			},
		},
	}
}

func (h accountsResourceHandler) buildDataset(store *Store, opts repositoryHandlerBuildContext[any]) (*bun.SelectQuery, error) {
	ret := store.db.NewSelect()

	// Build the query
	ret = ret.
		ModelTableExpr(store.GetPrefixedRelationName("accounts")).
		Column("address", "address_array", "first_usage", "insertion_date", "updated_at").
		Where("ledger = ?", store.ledger.Name)

	if opts.PIT != nil && !opts.PIT.IsZero() {
		ret = ret.Where("accounts.first_usage <= ?", opts.PIT)
	}

	if store.ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") && opts.PIT != nil && !opts.PIT.IsZero() {
		selectDistinctAccountMetadataHistories := store.db.NewSelect().
			DistinctOn("accounts_address").
			ModelTableExpr(store.GetPrefixedRelationName("accounts_metadata")).
			Where("ledger = ?", store.ledger.Name).
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

func (h accountsResourceHandler) resolveFilter(store *Store, opts ledgercontroller.ResourceQuery[any], operator, property string, value any) (string, []any, error) {
	switch {
	case property == "address":
		return filterAccountAddress(value.(string), "address"), nil, nil
	case property == "first_usage":
		return fmt.Sprintf("first_usage %s ?", convertOperatorToSQL(operator)), []any{value}, nil
	case balanceRegex.MatchString(property) || property == "balance":

		selectBalance := store.db.NewSelect().
			Where("accounts_address = dataset.address").
			Where("ledger = ?", store.ledger.Name)

		if opts.PIT != nil && !opts.PIT.IsZero() {
			if !store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
				return "", nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistory)
			}
			selectBalance = selectBalance.
				ModelTableExpr(store.GetPrefixedRelationName("moves")).
				DistinctOn("asset").
				ColumnExpr("first_value((post_commit_volumes).inputs - (post_commit_volumes).outputs) over (partition by (accounts_address, asset) order by seq desc) as balance").
				Where("insertion_date <= ?", opts.PIT)
		} else {
			selectBalance = selectBalance.
				ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
				ColumnExpr("input - output as balance")
		}

		if balanceRegex.MatchString(property) {
			selectBalance = selectBalance.Where("asset = ?", balanceRegex.FindAllStringSubmatch(property, 2)[0][1])
		}

		return store.db.NewSelect().
			TableExpr("(?) balance", selectBalance).
			ColumnExpr(fmt.Sprintf("balance %s ?", convertOperatorToSQL(operator)), value).
			String(), nil, nil
	case property == "metadata":
		return "metadata -> ? is not null", []any{value}, nil

	case metadataRegex.Match([]byte(property)):
		match := metadataRegex.FindAllStringSubmatch(property, 3)

		return "metadata @> ?", []any{map[string]any{
			match[0][1]: value,
		}}, nil
	default:
		return "", nil, ledgercontroller.NewErrInvalidQuery("invalid filter property %s", property)
	}
}

func (h accountsResourceHandler) project(store *Store, query ledgercontroller.ResourceQuery[any], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error) {
	return selectQuery.ColumnExpr("*"), nil
}

func (h accountsResourceHandler) expand(store *Store, opts ledgercontroller.ResourceQuery[any], property string) (*bun.SelectQuery, *joinCondition, error) {
	switch property {
	case "volumes":
		if !store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
			return nil, nil, ledgercontroller.NewErrInvalidQuery("feature %s must be 'ON' to use volumes", features.FeatureMovesHistory)
		}
	case "effectiveVolumes":
		if !store.ledger.HasFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
			return nil, nil, ledgercontroller.NewErrInvalidQuery("feature %s must be 'SYNC' to use effectiveVolumes", features.FeatureMovesHistoryPostCommitEffectiveVolumes)
		}
	}

	selectRowsQuery := store.db.NewSelect().
		Where("accounts_address in (select address from dataset)")
	if opts.UsePIT() {
		selectRowsQuery = selectRowsQuery.
			ModelTableExpr(store.GetPrefixedRelationName("moves")).
			DistinctOn("accounts_address, asset").
			Column("accounts_address", "asset").
			Where("ledger = ?", store.ledger.Name)
		if property == "volumes" {
			selectRowsQuery = selectRowsQuery.
				ColumnExpr("first_value(post_commit_volumes) over (partition by (accounts_address, asset) order by seq desc) as volumes").
				Where("insertion_date <= ?", opts.PIT)
		} else {
			selectRowsQuery = selectRowsQuery.
				ColumnExpr("first_value(post_commit_volumes) over (partition by (accounts_address, asset) order by effective_date desc, seq desc) as volumes").
				Where("effective_date <= ?", opts.PIT)
		}
	} else {
		selectRowsQuery = selectRowsQuery.
			ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
			Column("asset", "accounts_address").
			ColumnExpr("(input, output)::"+store.GetPrefixedRelationName("volumes")+" as volumes").
			Where("ledger = ?", store.ledger.Name)
	}

	return store.db.NewSelect().
			With("rows", selectRowsQuery).
			ModelTableExpr("rows").
			Column("accounts_address").
			ColumnExpr("public.aggregate_objects(json_build_object(asset, json_build_object('input', (volumes).inputs, 'output', (volumes).outputs))::jsonb) as " + strcase.SnakeCase(property)).
			Group("accounts_address"), &joinCondition{
			left:  "address",
			right: "accounts_address",
		}, nil
}

var _ repositoryHandler[any] = accountsResourceHandler{}
