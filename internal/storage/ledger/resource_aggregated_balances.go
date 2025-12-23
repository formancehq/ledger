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
		needAddressSegments := query.useFilter("address", isPartialAddress)

		dateFilterColumn := "effective_date"
		if query.Opts.UseInsertionDate {
			if !store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
				return nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistory)
			}
			dateFilterColumn = "insertion_date"
		} else {
			if !store.ledger.HasFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
				return nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes)
			}
		}

		// Optimization: when filtering on partial addresses, first identify eligible accounts
		// then INNER JOIN with moves. This is more efficient than LATERAL JOIN + filtering after.
		useAddressOptimization := needAddressSegments && !query.useFilter("metadata")
		if useAddressOptimization {
			// Build eligible accounts subquery with address filters pre-applied
			eligibleAccounts := store.newScopedSelect().
				TableExpr(store.GetPrefixedRelationName("accounts")).
				Column("address", "address_array")

			where, args, err := filterInvolvedAccounts(query.Builder, "address")
			if err != nil {
				return nil, err
			}
			if len(args) > 0 {
				eligibleAccounts = eligibleAccounts.Where(where, args...)
			} else {
				eligibleAccounts = eligibleAccounts.Where(where)
			}

			ret := store.newScopedSelect().
				ModelTableExpr(store.GetPrefixedRelationName("moves")+" moves").
				DistinctOn("moves.accounts_address, moves.asset").
				Column("moves.accounts_address", "moves.asset").
				ColumnExpr("eligible_accounts.address_array as accounts_address_array").
				Join("inner join (?) eligible_accounts on eligible_accounts.address = moves.accounts_address", eligibleAccounts).
				Where("moves."+dateFilterColumn+" <= ?", query.PIT)

			if query.Opts.UseInsertionDate {
				ret = ret.ColumnExpr("first_value(moves.post_commit_volumes) over (partition by (moves.accounts_address, moves.asset) order by moves.seq desc) as volumes")
			} else {
				ret = ret.ColumnExpr("first_value(moves.post_commit_effective_volumes) over (partition by (moves.accounts_address, moves.asset) order by moves.effective_date desc, moves.seq desc) as volumes")
			}

			if query.useFilter("metadata") {
				subQuery := store.newScopedSelect().
					DistinctOn("accounts_address").
					ModelTableExpr(store.GetPrefixedRelationName("accounts_metadata")).
					ColumnExpr("first_value(metadata) over (partition by accounts_address order by revision desc) as metadata").
					Where("accounts_metadata.accounts_address = moves.accounts_address").
					Where("date <= ?", query.PIT)

				ret = ret.
					Join(`left join lateral (?) accounts_metadata on true`, subQuery).
					Column("metadata")
			}

			return ret, nil
		} else {
			ret := store.newScopedSelect().
				ModelTableExpr(store.GetPrefixedRelationName("moves")+" moves").
				DistinctOn("moves.accounts_address, moves.asset").
				Column("moves.accounts_address", "moves.asset").
				Where("moves."+dateFilterColumn+" <= ?", query.PIT)

			if query.Opts.UseInsertionDate {
				ret = ret.ColumnExpr("first_value(moves.post_commit_volumes) over (partition by (moves.accounts_address, moves.asset) order by moves.seq desc) as volumes")
			} else {
				ret = ret.ColumnExpr("first_value(moves.post_commit_effective_volumes) over (partition by (moves.accounts_address, moves.asset) order by moves.effective_date desc, moves.seq desc) as volumes")
			}

			// When we have partial address filters with other filters (like metadata),
			// we need to join with accounts to get the address_array for filtering
			if needAddressSegments {
				subQuery := store.newScopedSelect().
					TableExpr(store.GetPrefixedRelationName("accounts")).
					Column("address_array").
					Where("accounts.address = moves.accounts_address")

				ret = ret.
					ColumnExpr("accounts.address_array as accounts_address_array").
					Join(`join lateral (?) accounts on true`, subQuery)
			}

			if query.useFilter("metadata") {
				subQuery := store.newScopedSelect().
					DistinctOn("accounts_address").
					ModelTableExpr(store.GetPrefixedRelationName("accounts_metadata")).
					ColumnExpr("first_value(metadata) over (partition by accounts_address order by revision desc) as metadata").
					Where("accounts_metadata.accounts_address = moves.accounts_address").
					Where("date <= ?", query.PIT)

				ret = ret.
					Join(`left join lateral (?) accounts_metadata on true`, subQuery).
					Column("metadata")
			}

			return ret, nil
		}
	} else {
		ret := store.newScopedSelect().
			ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
			Column("asset", "accounts_address").
			ColumnExpr("(input, output)::" + store.GetPrefixedRelationName("volumes") + " as volumes")

		if query.useFilter("metadata") || query.useFilter("address", isPartialAddress) {
			subQuery := store.newScopedSelect().
				TableExpr(store.GetPrefixedRelationName("accounts")).
				Column("address").
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
