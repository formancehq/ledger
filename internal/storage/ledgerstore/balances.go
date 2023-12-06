package ledgerstore

import (
	"context"
	"errors"
	"math/big"

	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"

	"github.com/formancehq/ledger/internal/storage/sqlutils"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/stack/libs/go-libs/query"
	"github.com/uptrace/bun"
)

// todo: should return a cursor?
func (store *Store) GetAggregatedBalances(ctx context.Context, q GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {

	var (
		needMetadata bool
		subQuery     string
		args         []any
		err          error
	)
	if q.Options.QueryBuilder != nil {
		subQuery, args, err = q.Options.QueryBuilder.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
			switch {
			case key == "address":
				// TODO: Should allow comparison operator only if segments not used
				if operator != "$match" {
					return "", nil, newErrInvalidQuery("'address' column can only be used with $match")
				}

				switch address := value.(type) {
				case string:
					return filterAccountAddress(address, "account_address"), nil, nil
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
				if q.Options.Options.PIT != nil {
					key = "am.metadata"
				}

				return key + " @> ?", []any{map[string]any{
					match[0][1]: value,
				}}, nil
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
	ret, err := fetch[*Temp](store, ctx,
		func(selectQuery *bun.SelectQuery) *bun.SelectQuery {
			moves := store.bucket.db.
				NewSelect().
				Table(MovesTableName).
				ColumnExpr("distinct on (moves.account_address, moves.asset) moves.*").
				Order("account_address", "asset", "moves.seq desc").
				Where("moves.ledger = ?", store.name).
				Apply(filterPIT(q.Options.Options.PIT, "insertion_date")) // todo(gfyrag): expose capability to use effective_date

			if needMetadata {
				if q.Options.Options.PIT != nil {
					moves = moves.Join(`join lateral (	
						select metadata
						from accounts_metadata am 
						where am.accounts_seq = moves.accounts_seq and (? is null or date <= ?)
						order by revision desc 
						limit 1
					) am on true`, q.Options.Options.PIT, q.Options.Options.PIT)
				} else {
					moves = moves.Join(`join lateral (	
						select metadata
						from accounts a 
						where a.seq = moves.accounts_seq
					) accounts on true`)
				}
			}
			if subQuery != "" {
				moves = moves.Where(subQuery, args...)
			}

			asJsonb := selectQuery.NewSelect().
				TableExpr("moves").
				ColumnExpr("volumes_to_jsonb((moves.asset, (sum((moves.post_commit_volumes).inputs), sum((moves.post_commit_volumes).outputs))::volumes)) as aggregated").
				Group("moves.asset")

			return selectQuery.
				With("moves", moves).
				With("data", asJsonb).
				TableExpr("data").
				ColumnExpr("aggregate_objects(data.aggregated) as aggregated")
		})
	if err != nil && !errors.Is(err, sqlutils.ErrNotFound) {
		return nil, err
	}
	if errors.Is(err, sqlutils.ErrNotFound) {
		return ledger.BalancesByAssets{}, nil
	}

	return ret.Aggregated.Balances(), nil
}

func (store *Store) GetBalance(ctx context.Context, address, asset string) (*big.Int, error) {
	type Temp struct {
		Balance *big.Int `bun:"balance,type:numeric"`
	}
	v, err := fetch[*Temp](store, ctx, func(query *bun.SelectQuery) *bun.SelectQuery {
		return query.TableExpr("get_account_balance(?, ?, ?) as balance", store.name, address, asset)
	})
	if err != nil {
		return nil, err
	}

	return v.Balance, nil
}

type GetAggregatedBalanceQuery bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions[PITFilter]]

func NewGetAggregatedBalancesQuery(options PaginatedQueryOptions[PITFilter]) GetAggregatedBalanceQuery {
	return GetAggregatedBalanceQuery{
		PageSize: options.PageSize,
		Order:    bunpaginate.OrderAsc,
		Options:  options,
	}
}
