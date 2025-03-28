package ledger

import (
	"errors"
	"fmt"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/uptrace/bun"
)

type aggregatedBalancesResourceRepositoryHandler struct {
	store *Store
}

func (h aggregatedBalancesResourceRepositoryHandler) Filters() []common.Filter {
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
			Name: "metadata",
			Matchers: []func(string) bool{
				func(key string) bool {
					return key == "metadata" || common.MetadataRegex.Match([]byte(key))
				},
			},
			Validators: []common.PropertyValidator{
				common.PropertyValidatorFunc(func(operator string, key string, value any) error {
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

func (h aggregatedBalancesResourceRepositoryHandler) BuildDataset(query common.RepositoryHandlerBuildContext[ledgercontroller.GetAggregatedVolumesOptions]) (*bun.SelectQuery, error) {

	if query.UsePIT() {
		ret := h.store.db.NewSelect().
			ModelTableExpr(h.store.GetPrefixedRelationName("moves")).
			DistinctOn("accounts_address, asset").
			Column("accounts_address", "asset").
			Where("ledger = ?", h.store.ledger.Name)
		if query.Opts.UseInsertionDate {
			if !h.store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
				return nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistory)
			}

			ret = ret.
				ColumnExpr("first_value(post_commit_volumes) over (partition by (accounts_address, asset) order by seq desc) as volumes").
				Where("insertion_date <= ?", query.PIT)
		} else {
			if !h.store.ledger.HasFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
				return nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes)
			}

			ret = ret.
				ColumnExpr("first_value(post_commit_effective_volumes) over (partition by (accounts_address, asset) order by effective_date desc, seq desc) as volumes").
				Where("effective_date <= ?", query.PIT)
		}

		if query.UseFilter("address", isPartialAddress) {
			subQuery := h.store.db.NewSelect().
				TableExpr(h.store.GetPrefixedRelationName("accounts")).
				Column("address_array").
				Where("accounts.address = accounts_address").
				Where("ledger = ?", h.store.ledger.Name)

			ret = ret.
				ColumnExpr("accounts.address_array as accounts_address_array").
				Join(`join lateral (?) accounts on true`, subQuery)
		}

		if query.UseFilter("metadata") {
			subQuery := h.store.db.NewSelect().
				DistinctOn("accounts_address").
				ModelTableExpr(h.store.GetPrefixedRelationName("accounts_metadata")).
				ColumnExpr("first_value(metadata) over (partition by accounts_address order by revision desc) as metadata").
				Where("ledger = ?", h.store.ledger.Name).
				Where("accounts_metadata.accounts_address = moves.accounts_address").
				Where("date <= ?", query.PIT)

			ret = ret.
				Join(`left join lateral (?) accounts_metadata on true`, subQuery).
				Column("metadata")
		}

		return ret, nil
	} else {
		ret := h.store.db.NewSelect().
			ModelTableExpr(h.store.GetPrefixedRelationName("accounts_volumes")).
			Column("asset", "accounts_address").
			ColumnExpr("(input, output)::"+h.store.GetPrefixedRelationName("volumes")+" as volumes").
			Where("ledger = ?", h.store.ledger.Name)

		if query.UseFilter("metadata") || query.UseFilter("address", isPartialAddress) {
			subQuery := h.store.db.NewSelect().
				TableExpr(h.store.GetPrefixedRelationName("accounts")).
				Column("address").
				Where("ledger = ?", h.store.ledger.Name).
				Where("accounts.address = accounts_address")

			if query.UseFilter("address") {
				subQuery = subQuery.ColumnExpr("address_array as accounts_address_array")
				ret = ret.Column("accounts_address_array")
			}
			if query.UseFilter("metadata") {
				subQuery = subQuery.ColumnExpr("metadata")
				ret = ret.Column("metadata")
			}

			ret = ret.
				Join(`join lateral (?) accounts on true`, subQuery)
		}

		return ret, nil
	}
}

func (h aggregatedBalancesResourceRepositoryHandler) ResolveFilter(_ common.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions], operator, property string, value any) (string, []any, error) {
	switch {
	case property == "address":
		return filterAccountAddress(value.(string), "accounts_address"), nil, nil
	case common.MetadataRegex.Match([]byte(property)) || property == "metadata":
		if property == "metadata" {
			return "metadata -> ? is not null", []any{value}, nil
		} else {
			match := common.MetadataRegex.FindAllStringSubmatch(property, 3)

			return "metadata @> ?", []any{map[string]any{
				match[0][1]: value,
			}}, nil
		}
	default:
		return "", nil, common.NewErrInvalidQuery("unknown key '%s' when building query", property)
	}
}

func (h aggregatedBalancesResourceRepositoryHandler) Expand(_ common.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions], property string) (*bun.SelectQuery, *common.JoinCondition, error) {
	return nil, nil, errors.New("no expand available for aggregated balances")
}

func (h aggregatedBalancesResourceRepositoryHandler) Project(
	_ common.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions],
	selectQuery *bun.SelectQuery,
) (*bun.SelectQuery, error) {
	sumVolumesForAsset := h.store.db.NewSelect().
		TableExpr("(?) values", selectQuery).
		Group("asset").
		Column("asset").
		ColumnExpr("json_build_object('input', sum(((volumes).inputs)::numeric), 'output', sum(((volumes).outputs)::numeric)) as volumes")

	return h.store.db.NewSelect().
		TableExpr("(?) values", sumVolumesForAsset).
		ColumnExpr("public.aggregate_objects(json_build_object(asset, volumes)::jsonb) as aggregated"), nil
}

var _ common.RepositoryHandler[ledgercontroller.GetAggregatedVolumesOptions] = aggregatedBalancesResourceRepositoryHandler{}
