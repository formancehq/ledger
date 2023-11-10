package ledgerstore

import (
	"context"
	"errors"
	"math/big"

	"github.com/formancehq/ledger/internal/storage/sqlutils"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/paginate"
	"github.com/formancehq/stack/libs/go-libs/query"
	"github.com/uptrace/bun"
)

// todo: should return a cursor?
func (store *Store) GetAggregatedBalances(ctx context.Context, q *GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {

	var (
		needJoinMetadata bool
		subQuery         string
		args             []any
		err              error
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
				needJoinMetadata = true

				return "am.metadata @> ?", []any{map[string]any{
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
			moves := store.db.
				NewSelect().
				Table(MovesTableName).
				ColumnExpr("distinct on (moves.account_address, moves.asset) moves.*").
				Order("account_address", "asset", "moves.seq desc").
				Apply(filterPIT(q.Options.Options.PIT, "insertion_date")) // todo(gfyrag): expose capability to use effective_date

			if needJoinMetadata {
				moves = moves.Join(`left join lateral (	
					select metadata
					from accounts_metadata am 
					where am.address = moves.account_address and (? is null or date <= ?)
					order by revision desc 
					limit 1
				) am on true`, q.Options.Options.PIT, q.Options.Options.PIT)
			}
			if subQuery != "" {
				moves = moves.Where(subQuery, args...)
			}

			return selectQuery.
				With("moves", moves).
				TableExpr("moves").
				ColumnExpr("volumes_to_jsonb((moves.asset, (sum((moves.post_commit_volumes).inputs), sum((moves.post_commit_volumes).outputs))::volumes)) as aggregated").
				Group("moves.asset")
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
		return query.TableExpr("get_account_balance(?, ?) as balance", address, asset)
	})
	if err != nil {
		return nil, err
	}

	return v.Balance, nil
}

type GetAggregatedBalanceQuery paginate.OffsetPaginatedQuery[PaginatedQueryOptions[PITFilter]]

func NewGetAggregatedBalancesQuery(options PaginatedQueryOptions[PITFilter]) *GetAggregatedBalanceQuery {
	return &GetAggregatedBalanceQuery{
		PageSize: options.PageSize,
		Order:    paginate.OrderAsc,
		Options:  options,
	}
}
