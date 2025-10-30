package ledger

import (
	"errors"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/uptrace/bun"
)

type aggregatedBalancesResourceRepositoryHandler struct {
	store *Store
}

func (h aggregatedBalancesResourceRepositoryHandler) Schema() common.EntitySchema {
	return common.EntitySchema{
		Fields: map[string]common.Field{
			"address":  common.NewStringField().Paginated(),
			"metadata": common.NewStringMapField(),
		},
	}
}

func (h aggregatedBalancesResourceRepositoryHandler) BuildDataset(query common.RepositoryHandlerBuildContext[GetAggregatedVolumesOptions]) (*bun.SelectQuery, error) {

	if query.UsePIT() {
		ret := h.store.db.NewSelect().
			ModelTableExpr(h.store.GetPrefixedRelationName("moves")).
			DistinctOn("accounts_address, asset").
			Column("accounts_address", "asset")
		ret = h.store.applyLedgerFilter(query.Ctx, ret, "moves")
		if query.Opts.UseInsertionDate {
			if !h.store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
				return nil, NewErrMissingFeature(features.FeatureMovesHistory)
			}

			ret = ret.
				ColumnExpr("first_value(post_commit_volumes) over (partition by (accounts_address, asset) order by seq desc) as volumes").
				Where("insertion_date <= ?", query.PIT)
		} else {
			if !h.store.ledger.HasFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
				return nil, NewErrMissingFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes)
			}

			ret = ret.
				ColumnExpr("first_value(post_commit_effective_volumes) over (partition by (accounts_address, asset) order by effective_date desc, seq desc) as volumes").
				Where("effective_date <= ?", query.PIT)
		}

		if query.UseFilter("address", func(value any) bool {
			return isPartialAddress(value.(string))
		}) {
			subQuery := h.store.db.NewSelect().
				TableExpr(h.store.GetPrefixedRelationName("accounts")).
				Column("address_array").
				Where("accounts.address = accounts_address")
			subQuery = h.store.applyLedgerFilter(query.Ctx, subQuery, "accounts")

			ret = ret.
				ColumnExpr("accounts.address_array as accounts_address_array").
				Join(`join lateral (?) accounts on true`, subQuery)
		}

		if query.UseFilter("metadata") {
			subQuery := h.store.db.NewSelect().
				DistinctOn("accounts_address").
				ModelTableExpr(h.store.GetPrefixedRelationName("accounts_metadata")).
				ColumnExpr("first_value(metadata) over (partition by accounts_address order by revision desc) as metadata").
				Where("accounts_metadata.accounts_address = moves.accounts_address").
				Where("date <= ?", query.PIT)
			subQuery = h.store.applyLedgerFilter(query.Ctx, subQuery, "accounts_metadata")

			ret = ret.
				Join(`left join lateral (?) accounts_metadata on true`, subQuery).
				Column("metadata")
		}

		return ret, nil
	} else {
		ret := h.store.db.NewSelect().
			ModelTableExpr(h.store.GetPrefixedRelationName("accounts_volumes")).
			Column("asset", "accounts_address").
			ColumnExpr("(input, output)::"+h.store.GetPrefixedRelationName("volumes")+" as volumes")
		ret = h.store.applyLedgerFilter(query.Ctx, ret, "accounts_volumes")

		if query.UseFilter("metadata") || query.UseFilter("address", func(value any) bool {
			return isPartialAddress(value.(string))
		}) {
			subQuery := h.store.db.NewSelect().
				TableExpr(h.store.GetPrefixedRelationName("accounts")).
				Column("address").
				Where("accounts.address = accounts_address")
			subQuery = h.store.applyLedgerFilter(query.Ctx, subQuery, "accounts")

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

func (h aggregatedBalancesResourceRepositoryHandler) ResolveFilter(_ common.ResourceQuery[GetAggregatedVolumesOptions], _, property string, value any) (string, []any, error) {
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

func (h aggregatedBalancesResourceRepositoryHandler) Expand(_ common.ResourceQuery[GetAggregatedVolumesOptions], property string) (*bun.SelectQuery, *common.JoinCondition, error) {
	return nil, nil, errors.New("no expand available for aggregated balances")
}

func (h aggregatedBalancesResourceRepositoryHandler) Project(
	_ common.ResourceQuery[GetAggregatedVolumesOptions],
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

var _ common.RepositoryHandler[GetAggregatedVolumesOptions] = aggregatedBalancesResourceRepositoryHandler{}
