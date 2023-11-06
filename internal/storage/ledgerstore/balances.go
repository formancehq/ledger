package ledgerstore

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger/internal/storage"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/paginate"
	"github.com/formancehq/stack/libs/go-libs/query"
	"github.com/uptrace/bun"
)

// todo: should return a cursor?
func (store *Store) GetAggregatedBalances(ctx context.Context, q *GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {

	type Temp struct {
		Aggregated ledger.VolumesByAssets `bun:"aggregated,type:jsonb"`
	}
	ret, err := fetchAndMap[*Temp, ledger.BalancesByAssets](store, ctx,
		func(temp *Temp) ledger.BalancesByAssets {
			return temp.Aggregated.Balances()
		},
		func(selectQuery *bun.SelectQuery) *bun.SelectQuery {
			moves := store.db.
				NewSelect().
				Table(MovesTableName).
				ColumnExpr("distinct on (moves.account_address, moves.asset) moves.*").
				Order("account_address", "asset", "moves.seq desc").
				Apply(filterPIT(q.Options.Options.PIT, "insertion_date")) // todo(gfyrag): expose capability to use effective_date

			if q.Options.QueryBuilder != nil {
				joinOnMetadataAdded := false
				subQuery, args, err := q.Options.QueryBuilder.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
					switch {
					case key == "address":
						// TODO: Should allow comparison operator only if segments not used
						if operator != "$match" {
							return "", nil, errors.New("'address' column can only be used with $match")
						}
						switch address := value.(type) {
						case string:
							return filterAccountAddress(address, "account_address"), nil, nil
						default:
							return "", nil, fmt.Errorf("unexpected type %T for column 'address'", address)
						}
					case metadataRegex.Match([]byte(key)):
						if operator != "$match" {
							return "", nil, errors.New("'metadata' column can only be used with $match")
						}
						match := metadataRegex.FindAllStringSubmatch(key, 3)
						if !joinOnMetadataAdded {
							moves = moves.Join(`left join lateral (	
								select metadata
								from accounts_metadata am 
								where am.address = moves.account_address and (? is null or date <= ?)
								order by revision desc 
								limit 1
							) am on true`, q.Options.Options.PIT, q.Options.Options.PIT)
							joinOnMetadataAdded = true
						}

						return "am.metadata @> ?", []any{map[string]any{
							match[0][1]: value,
						}}, nil
					default:
						return "", nil, fmt.Errorf("unknown key '%s' when building query", key)
					}
				}))
				if err != nil {
					panic(err)
				}
				moves = moves.Where(subQuery, args...)
			}

			return selectQuery.
				With("moves", moves).
				TableExpr("moves").
				ColumnExpr("volumes_to_jsonb((moves.asset, (sum((moves.post_commit_volumes).inputs), sum((moves.post_commit_volumes).outputs))::volumes)) as aggregated").
				Group("moves.asset")
		})
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}
	if errors.Is(err, storage.ErrNotFound) {
		return ledger.BalancesByAssets{}, nil
	}

	return ret, nil
}

func (store *Store) GetBalance(ctx context.Context, address, asset string) (*big.Int, error) {
	type Temp struct {
		Balance *big.Int `bun:"balance,type:numeric"`
	}
	return fetchAndMap[*Temp, *big.Int](store, ctx, func(temp *Temp) *big.Int {
		return temp.Balance
	}, func(query *bun.SelectQuery) *bun.SelectQuery {
		return query.TableExpr("get_account_balance(?, ?) as balance", address, asset)
	})
}

type GetAggregatedBalanceQuery paginate.OffsetPaginatedQuery[PaginatedQueryOptions[PITFilter]]

func NewGetAggregatedBalancesQuery(options PaginatedQueryOptions[PITFilter]) *GetAggregatedBalanceQuery {
	return &GetAggregatedBalanceQuery{
		PageSize: options.PageSize,
		Order:    paginate.OrderAsc,
		Options:  options,
	}
}
