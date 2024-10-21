package legacy

import (
	"context"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/uptrace/bun"
)

func (store *Store) GetAggregatedBalances(ctx context.Context, q ledgercontroller.GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {

	var (
		needMetadata bool
		subQuery     string
		args         []any
		err          error
	)
	if q.QueryBuilder != nil {
		subQuery, args, err = q.QueryBuilder.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
			switch {
			case key == "address":
				if operator != "$match" {
					return "", nil, newErrInvalidQuery("'address' column can only be used with $match")
				}

				switch address := value.(type) {
				case string:
					return filterAccountAddress(address, "accounts_address"), nil, nil
				default:
					return "", nil, newErrInvalidQuery("unexpected type %T for column 'address'", address)
				}
			case metadataRegex.Match([]byte(key)):
				if operator != "$match" {
					return "", nil, newErrInvalidQuery("'metadata' column can only be used with $match")
				}
				match := metadataRegex.FindAllStringSubmatch(key, 3)
				needMetadata = true
				key := "accounts.metadata"
				if q.PIT != nil {
					key = "am.metadata"
				}

				return key + " @> ?", []any{map[string]any{
					match[0][1]: value,
				}}, nil

			case key == "metadata":
				if operator != "$exists" {
					return "", nil, newErrInvalidQuery("'metadata' key filter can only be used with $exists")
				}
				needMetadata = true
				key := "accounts.metadata"
				if q.PIT != nil && !q.PIT.IsZero() {
					key = "am.metadata"
				}

				return fmt.Sprintf("%s -> ? IS NOT NULL", key), []any{value}, nil
			default:
				return "", nil, newErrInvalidQuery("unknown key '%s' when building query", key)
			}
		}))
		if err != nil {
			return nil, err
		}
	}

	type Temp struct {
		Aggregated ledger.VolumesByAssets `bun:"aggregated,type:jsonb"`
	}
	ret, err := fetch[*Temp](store, false, ctx,
		func(selectQuery *bun.SelectQuery) *bun.SelectQuery {
			pitColumn := "effective_date"
			if q.UseInsertionDate {
				pitColumn = "insertion_date"
			}
			moves := store.db.
				NewSelect().
				ModelTableExpr(store.GetPrefixedRelationName("moves")).
				DistinctOn("moves.accounts_address, moves.asset").
				Where("moves.ledger = ?", store.name).
				Apply(filterPIT(q.PIT, pitColumn))

			if q.UseInsertionDate {
				moves = moves.
					ColumnExpr("accounts_address").
					ColumnExpr("asset").
					ColumnExpr("first_value(moves.post_commit_volumes) over (partition by moves.accounts_address, moves.asset order by seq desc) as post_commit_volumes")
			} else {
				moves = moves.
					ColumnExpr("accounts_address").
					ColumnExpr("asset").
					ColumnExpr("first_value(moves.post_commit_effective_volumes) over (partition by moves.accounts_address, moves.asset order by effective_date desc, seq desc) as post_commit_effective_volumes")
			}

			if needMetadata {
				if q.PIT != nil {
					moves = moves.Join(`join lateral (	
						select metadata
						from `+store.GetPrefixedRelationName("accounts_metadata")+` am 
						where am.accounts_seq = moves.accounts_seq and (? is null or date <= ?)
						order by revision desc 
						limit 1
					) am on true`, q.PIT, q.PIT)
				} else {
					moves = moves.Join(`join lateral (	
						select metadata
						from ` + store.GetPrefixedRelationName("accounts") + ` a 
						where a.seq = moves.accounts_seq
					) accounts on true`)
				}
			}
			if subQuery != "" {
				moves = moves.Where(subQuery, args...)
			}

			volumesColumn := "post_commit_effective_volumes"
			if q.UseInsertionDate {
				volumesColumn = "post_commit_volumes"
			}

			finalQuery := selectQuery.
				With("moves", moves).
				With(
					"data",
					selectQuery.NewSelect().
						TableExpr("moves").
						ColumnExpr(fmt.Sprintf(store.GetPrefixedRelationName("volumes_to_jsonb")+`((moves.asset, (sum((moves.%s).inputs), sum((moves.%s).outputs))::%s)) as aggregated`, volumesColumn, volumesColumn, store.GetPrefixedRelationName("volumes"))).
						Group("moves.asset"),
				).
				TableExpr("data").
				ColumnExpr("aggregate_objects(data.aggregated) as aggregated")

			return finalQuery
		})
	if err != nil && !errors.Is(err, postgres.ErrNotFound) {
		return nil, err
	}
	if errors.Is(err, postgres.ErrNotFound) {
		return ledger.BalancesByAssets{}, nil
	}

	return ret.Aggregated.Balances(), nil
}
