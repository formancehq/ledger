package ledger

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/platform/postgres"
	"github.com/formancehq/go-libs/time"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
)

type Balances struct {
	bun.BaseModel `bun:"balances"`

	Ledger  string   `bun:"ledger,type:varchar"`
	Account string   `bun:"account,type:varchar"`
	Asset   string   `bun:"asset,type:varchar"`
	Balance *big.Int `bun:"balance,type:numeric"`
}

func (s *Store) SelectAggregatedBalances(date *time.Time, useInsertionDate bool, builder query.Builder) *bun.SelectQuery {

	ret := s.db.NewSelect()
	var needMetadata bool

	if builder != nil {
		if err := builder.Walk(func(operator string, key string, value any) error {
			switch {
			case key == "address":
				return s.validateAddressFilter(operator, value)
			case key == "metadata":
				needMetadata = true
				if operator != "$exists" {
					return ledgercontroller.NewErrInvalidQuery("'metadata' key filter can only be used with $exists")
				}
			case metadataRegex.Match([]byte(key)):
				needMetadata = true
				if operator != "$match" {
					return ledgercontroller.NewErrInvalidQuery("'account' column can only be used with $match")
				}
			default:
				return ledgercontroller.NewErrInvalidQuery("unknown key '%s' when building query", key)
			}
			return nil
		}); err != nil {
			return ret.Err(err)
		}
	}

	var selectMoves *bun.SelectQuery
	if useInsertionDate {
		if !s.ledger.HasFeature(ledger.FeaturePostCommitVolumes, "SYNC") {
			return ret.Err(ledgercontroller.NewErrMissingFeature(ledger.FeaturePostCommitVolumes))
		}
		selectMoves = s.db.NewSelect().
			TableExpr("(?) moves", s.SelectDistinctMovesBySeq(date)).
			Column("asset", "account_address", "account_address_array").
			ColumnExpr("post_commit_volumes as volumes")
	} else {
		if !s.ledger.HasFeature(ledger.FeaturePostCommitEffectiveVolumes, "SYNC") {
			return ret.Err(ledgercontroller.NewErrMissingFeature(ledger.FeaturePostCommitEffectiveVolumes))
		}
		selectMoves = s.db.NewSelect().
			TableExpr("(?) moves", s.SelectDistinctMovesByEffectiveDate(date)).
			ColumnExpr("moves.post_commit_effective_volumes as volumes").
			Column("asset", "account_address", "account_address_array")
	}

	if needMetadata {
		if s.ledger.HasFeature(ledger.FeatureAccountMetadataHistories, "SYNC") && date != nil && !date.IsZero() {
			selectMoves = selectMoves.
				Join(
					`left join (?) accounts_metadata on accounts_metadata.accounts_seq = moves.accounts_seq`,
					s.selectDistinctAccountMetadataHistories(date),
				).
				ColumnExpr("coalesce(accounts_metadata.metadata, '{}'::jsonb) as metadata")
		} else {
			selectMoves = selectMoves.
				Join(
					`join (?) accounts on accounts.seq = moves.accounts_seq`,
					s.db.NewSelect().ModelTableExpr(s.GetPrefixedRelationName("accounts")),
				)
		}
	}

	if builder != nil {
		where, args, err := builder.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
			switch {
			case key == "address":
				return filterAccountAddress(value.(string), "account_address"), nil, nil
			case metadataRegex.Match([]byte(key)):
				match := metadataRegex.FindAllStringSubmatch(key, 3)

				key := "accounts.metadata"
				if s.ledger.HasFeature(ledger.FeatureAccountMetadataHistories, "SYNC") && date != nil && !date.IsZero() {
					key = "accounts_metadata.metadata"
				}

				return key + " @> ?", []any{map[string]any{
					match[0][1]: value,
				}}, nil

			case key == "metadata":
				key := "accounts.metadata"
				if s.ledger.HasFeature(ledger.FeatureAccountMetadataHistories, "SYNC") && date != nil && !date.IsZero() {
					key = "am.metadata"
				}

				return fmt.Sprintf("%s -> ? IS NOT NULL", key), []any{value}, nil
			default:
				return "", nil, ledgercontroller.NewErrInvalidQuery("unknown key '%s' when building query", key)
			}
		}))
		if err != nil {
			return ret.Err(errors.Wrap(err, "building where clause"))
		}
		selectMoves = selectMoves.Where(where, args...)
	}

	return s.db.NewSelect().
		ModelTableExpr("(?) moves", selectMoves).
		ColumnExpr(`to_json(array_agg(json_build_object('asset', moves.asset, 'inputs', (moves.volumes).inputs, 'outputs', (moves.volumes).outputs))) as aggregated`)
}

func (s *Store) GetAggregatedBalances(ctx context.Context, q ledgercontroller.GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {
	type AggregatedVolumes struct {
		Aggregated Volumes `bun:"aggregated,type:jsonb"`
	}
	aggregatedVolumes := AggregatedVolumes{}
	if err := s.SelectAggregatedBalances(q.PIT, q.UseInsertionDate, q.QueryBuilder).
		Model(&aggregatedVolumes).
		Scan(ctx); err != nil {
		return nil, err
	}

	return aggregatedVolumes.Aggregated.toCore().Balances(), nil
}

func (s *Store) GetBalances(ctx context.Context, query ledgercontroller.BalanceQuery) (ledgercontroller.Balances, error) {
	return tracing.TraceWithLatency(ctx, "GetBalances", func(ctx context.Context) (ledgercontroller.Balances, error) {
		conditions := make([]string, 0)
		args := make([]any, 0)
		for account, assets := range query {
			for _, asset := range assets {
				conditions = append(conditions, "account = ? and asset = ?")
				args = append(args, account, asset)
			}
		}

		balances := make([]Balances, 0)
		err := s.db.NewSelect().
			Model(&balances).
			ModelTableExpr(s.GetPrefixedRelationName("balances")).
			Where("("+strings.Join(conditions, ") OR (")+")", args...).
			For("UPDATE").
			// notes(gfyrag): keep order, it ensures consistent locking order and limit deadlocks
			Order("account", "asset").
			Scan(ctx)
		if err != nil {
			return nil, err
		}

		ret := ledgercontroller.Balances{}
		for _, balance := range balances {
			if _, ok := ret[balance.Account]; !ok {
				ret[balance.Account] = map[string]*big.Int{}
			}
			ret[balance.Account][balance.Asset] = balance.Balance
		}

		// fill empty balances with 0 value
		for account, assets := range query {
			if _, ok := ret[account]; !ok {
				ret[account] = map[string]*big.Int{}
			}
			for _, asset := range assets {
				if _, ok := ret[account][asset]; !ok {
					ret[account][asset] = big.NewInt(0)
				}
			}
		}

		return ret, nil
	})
}

func (s *Store) updateBalances(ctx context.Context, diff map[string]map[string]*big.Int) (map[string]map[string]*big.Int, error) {
	return tracing.TraceWithLatency(ctx, "UpdateBalances", func(ctx context.Context) (map[string]map[string]*big.Int, error) {

		balances := make([]Balances, 0)
		for account, forAccount := range diff {
			for asset, amount := range forAccount {
				balances = append(balances, Balances{
					Ledger:  s.ledger.Name,
					Account: account,
					Asset:   asset,
					Balance: amount,
				})
			}
		}

		_, err := s.db.NewInsert().
			Model(&balances).
			ModelTableExpr(s.GetPrefixedRelationName("balances")).
			On("conflict (ledger, account, asset) do update").
			Set("balance = balances.balance + excluded.balance").
			Returning("balance").
			Exec(ctx)
		if err != nil {
			return nil, postgres.ResolveError(err)
		}

		ret := make(map[string]map[string]*big.Int)
		for _, balance := range balances {
			if _, ok := ret[balance.Account]; !ok {
				ret[balance.Account] = map[string]*big.Int{}
			}
			ret[balance.Account][balance.Asset] = balance.Balance
		}

		return ret, err
	})
}
