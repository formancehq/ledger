package ledger

import (
	"errors"
	"fmt"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/uptrace/bun"
)

type aggregatedBalancesResourceRepositoryHandler struct{}

func (h aggregatedBalancesResourceRepositoryHandler) filters() []filter {
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
			name: "metadata",
			matchers: []func(string) bool{
				func(key string) bool {
					return key == "metadata" || metadataRegex.Match([]byte(key))
				},
			},
			validators: []propertyValidator{
				propertyValidatorFunc(func(l ledger.Ledger, operator string, key string, value any) error {
					if key == "metadata" {
						if operator != "$exists" {
							return fmt.Errorf("unsupported operator %s for metadata", operator)
						}
						return nil
					}
					if operator != "$match" {
						return fmt.Errorf("unsupported operator %s for metadata", operator)
					}
					return nil
				}),
			},
		},
	}
}

func (h aggregatedBalancesResourceRepositoryHandler) buildDataset(store *Store, query repositoryHandlerBuildContext[ledgercontroller.GetAggregatedVolumesOptions]) (*bun.SelectQuery, error) {

	if query.UsePIT() {
		ret := store.db.NewSelect().
			ModelTableExpr(store.GetPrefixedRelationName("moves")).
			DistinctOn("accounts_address, asset").
			Column("accounts_address", "asset").
			Where("ledger = ?", store.ledger.Name)
		if query.Opts.UseInsertionDate {
			if !store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
				return nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistory)
			}

			ret = ret.
				ColumnExpr("first_value(post_commit_volumes) over (partition by (accounts_address, asset) order by seq desc) as volumes").
				Where("insertion_date <= ?", query.PIT)
		} else {
			if !store.ledger.HasFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
				return nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes)
			}

			ret = ret.
				ColumnExpr("first_value(post_commit_effective_volumes) over (partition by (accounts_address, asset) order by effective_date desc, seq desc) as volumes").
				Where("effective_date <= ?", query.PIT)
		}

		if query.useFilter("address", isPartialAddress) {
			subQuery := store.db.NewSelect().
				TableExpr(store.GetPrefixedRelationName("accounts")).
				Column("address_array").
				Where("accounts.address = accounts_address").
				Where("ledger = ?", store.ledger.Name)

			ret = ret.
				ColumnExpr("accounts.address_array as accounts_address_array").
				Join(`join lateral (?) accounts on true`, subQuery)
		}

		if query.useFilter("metadata") {
			subQuery := store.db.NewSelect().
				DistinctOn("accounts_address").
				ModelTableExpr(store.GetPrefixedRelationName("accounts_metadata")).
				ColumnExpr("first_value(metadata) over (partition by accounts_address order by revision desc) as metadata").
				Where("ledger = ?", store.ledger.Name).
				Where("accounts_metadata.accounts_address = moves.accounts_address").
				Where("date <= ?", query.PIT)

			ret = ret.
				Join(`left join lateral (?) accounts_metadata on true`, subQuery).
				Column("metadata")
		}

		return ret, nil
	} else {
		ret := store.db.NewSelect().
			ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
			Column("asset", "accounts_address").
			ColumnExpr("(input, output)::"+store.GetPrefixedRelationName("volumes")+" as volumes").
			Where("ledger = ?", store.ledger.Name)

		if query.useFilter("metadata") || query.useFilter("address", isPartialAddress) {
			subQuery := store.db.NewSelect().
				TableExpr(store.GetPrefixedRelationName("accounts")).
				Column("address").
				Where("ledger = ?", store.ledger.Name).
				Where("accounts.address = accounts_address")

			if query.useFilter("address") {
				subQuery = subQuery.ColumnExpr("address_array as accounts_address_array")
				ret = ret.Column("accounts_address_array")
			}
			if query.useFilter("metadata") {
				subQuery = subQuery.ColumnExpr("metadata")
				ret = ret.Column("metadata")
			}

			ret = ret.
				Join(`join lateral (?) accounts on true`, subQuery)
		}

		return ret, nil
	}
}

func (h aggregatedBalancesResourceRepositoryHandler) resolveFilter(store *Store, query ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions], operator, property string, value any) (string, []any, error) {
	switch {
	case property == "address":
		return filterAccountAddress(value.(string), "accounts_address"), nil, nil
	case metadataRegex.Match([]byte(property)) || property == "metadata":
		if property == "metadata" {
			return "metadata -> ? is not null", []any{value}, nil
		} else {
			match := metadataRegex.FindAllStringSubmatch(property, 3)

			return "metadata @> ?", []any{map[string]any{
				match[0][1]: value,
			}}, nil
		}
	default:
		return "", nil, ledgercontroller.NewErrInvalidQuery("unknown key '%s' when building query", property)
	}
}

func (h aggregatedBalancesResourceRepositoryHandler) expand(_ *Store, _ ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions], property string) (*bun.SelectQuery, *joinCondition, error) {
	return nil, nil, errors.New("no expand available for aggregated balances")
}

func (h aggregatedBalancesResourceRepositoryHandler) project(
	store *Store,
	_ ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions],
	selectQuery *bun.SelectQuery,
) (*bun.SelectQuery, error) {
	sumVolumesForAsset := store.db.NewSelect().
		TableExpr("(?) values", selectQuery).
		Group("asset").
		Column("asset").
		ColumnExpr("json_build_object('input', sum(((volumes).inputs)::numeric), 'output', sum(((volumes).outputs)::numeric)) as volumes")

	return store.db.NewSelect().
		TableExpr("(?) values", sumVolumesForAsset).
		ColumnExpr("public.aggregate_objects(json_build_object(asset, volumes)::jsonb) as aggregated"), nil
}

var _ repositoryHandler[ledgercontroller.GetAggregatedVolumesOptions] = aggregatedBalancesResourceRepositoryHandler{}
