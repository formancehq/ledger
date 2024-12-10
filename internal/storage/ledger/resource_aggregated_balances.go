package ledger

import (
	"errors"
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

func (h aggregatedBalancesResourceRepositoryHandler) buildDataset(store *Store, ledger ledger.Ledger, query ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]) (*bun.SelectQuery, error) {

	if query.PIT != nil && !query.PIT.IsZero() {
		ret := store.db.NewSelect().
			ModelTableExpr(store.GetPrefixedRelationName("moves")).
			DistinctOn("accounts_address, asset").
			Column("accounts_address", "asset").
			Where("ledger = ?", ledger.Name)
		if query.Opts.UseInsertionDate {
			if !ledger.HasFeature(features.FeatureMovesHistory, "ON") {
				return nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistory)
			}

			return ret.
				ColumnExpr("first_value(post_commit_volumes) over (partition by (accounts_address, asset) order by seq desc) as volumes").
				Where("insertion_date <= ?", query.PIT), nil
		} else {
			if !ledger.HasFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
				return nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes)
			}

			return ret.
				ColumnExpr("first_value(post_commit_effective_volumes) over (partition by (accounts_address, asset) order by effective_date desc, seq desc) as volumes").
				Where("effective_date <= ?", query.PIT), nil
		}
	} else {
		return store.db.NewSelect().
			ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
			Column("asset", "accounts_address").
			ColumnExpr("(input, output)::"+store.GetPrefixedRelationName("volumes")+" as volumes").
			Where("ledger = ?", ledger.Name), nil
	}
}

func (h aggregatedBalancesResourceRepositoryHandler) resolveFilter(store *Store, ledger ledger.Ledger, query ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions], operator, property string, value any) (string, []any, error) {
	switch {
	case property == "address":
		address := value.(string)
		if isPartialAddress(address) {
			return store.db.NewSelect().
				TableExpr(store.GetPrefixedRelationName("accounts")).
				ColumnExpr("true").
				Where(filterAccountAddress(address, "address")).
				Where("address = dataset.accounts_address").
				String(), []any{}, nil
		}

		return "accounts_address = ?", []any{address}, nil
	case metadataRegex.Match([]byte(property)) || property == "metadata":
		var selectMetadata *bun.SelectQuery
		if ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") && query.PIT != nil && !query.PIT.IsZero() {
			selectMetadata = store.db.NewSelect().
				DistinctOn("accounts_address").
				ModelTableExpr(store.GetPrefixedRelationName("accounts_metadata")).
				Where("accounts_address = dataset.accounts_address").
				Order("accounts_address", "revision desc")

			if query.PIT != nil && !query.PIT.IsZero() {
				selectMetadata = selectMetadata.Where("date <= ?", query.PIT)
			}
		} else {
			selectMetadata = store.db.NewSelect().
				ModelTableExpr(store.GetPrefixedRelationName("accounts")).
				Where("address = dataset.accounts_address")
		}
		selectMetadata = selectMetadata.
			Where("ledger = ?", ledger.Name).
			Column("metadata").
			Limit(1)

		switch {
		case metadataRegex.Match([]byte(property)):
			match := metadataRegex.FindAllStringSubmatch(property, 3)

			return "(?) @> ?", []any{selectMetadata, map[string]any{
				match[0][1]: value,
			}}, nil

		case property == "metadata":
			return "(?) -> ? is not null", []any{selectMetadata, value}, nil
		default:
			panic("unreachable")
		}
	default:
		return "", nil, ledgercontroller.NewErrInvalidQuery("unknown key '%s' when building query", property)
	}
}

func (h aggregatedBalancesResourceRepositoryHandler) expand(_ *Store, _ ledger.Ledger, _ ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions], property string) (*bun.SelectQuery, *joinCondition, error) {
	return nil, nil, errors.New("no expand available for aggregated balances")
}

func (h aggregatedBalancesResourceRepositoryHandler) aggregate(
	store *Store,
	_ ledger.Ledger,
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
		ColumnExpr("aggregate_objects(json_build_object(asset, volumes)::jsonb) as aggregated"), nil
}

var _ repositoryHandler[ledgercontroller.GetAggregatedVolumesOptions] = aggregatedBalancesResourceRepositoryHandler{}
