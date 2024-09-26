package ledger

import (
	"context"
	"math/big"
	"strings"

	"github.com/formancehq/go-libs/platform/postgres"
	"github.com/pkg/errors"

	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/query"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/uptrace/bun"
)

func (s *Store) selectAccountWithVolumes(date *time.Time, useInsertionDate bool, builder query.Builder) *bun.SelectQuery {

	ret := s.db.NewSelect()
	var (
		needMetadata       bool
		needAddressSegment bool
	)

	if builder != nil {
		if err := builder.Walk(func(operator string, key string, value any) error {
			switch {
			case key == "address":
				if err := s.validateAddressFilter(operator, value); err != nil {
					return err
				}
				if !needAddressSegment {
					needAddressSegment = isSegmentedAddress(value.(string)) // cast is safe, the type has been validated by validatedAddressFilter
				}

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

	if needAddressSegment && !s.ledger.HasFeature(ledger.FeatureIndexAddressSegments, "ON") {
		return ret.Err(ledgercontroller.NewErrMissingFeature(ledger.FeatureIndexAddressSegments))
	}

	var selectAccountsWithVolumes *bun.SelectQuery
	if date != nil && !date.IsZero() {
		if useInsertionDate {
			if !s.ledger.HasFeature(ledger.FeatureMovesHistory, "ON") {
				return ret.Err(ledgercontroller.NewErrMissingFeature(ledger.FeatureMovesHistory))
			}
			selectAccountsWithVolumes = s.db.NewSelect().
				TableExpr("(?) moves", s.SelectDistinctMovesBySeq(date)).
				Column("asset", "accounts_address").
				ColumnExpr("post_commit_volumes as volumes")
		} else {
			if !s.ledger.HasFeature(ledger.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
				return ret.Err(ledgercontroller.NewErrMissingFeature(ledger.FeatureMovesHistoryPostCommitEffectiveVolumes))
			}
			selectAccountsWithVolumes = s.db.NewSelect().
				TableExpr("(?) moves", s.SelectDistinctMovesByEffectiveDate(date)).
				Column("asset", "accounts_address").
				ColumnExpr("moves.post_commit_effective_volumes as volumes")
		}
	} else {
		selectAccountsWithVolumes = s.db.NewSelect().
			ModelTableExpr(s.GetPrefixedRelationName("accounts_volumes")).
			Column("asset", "accounts_address").
			ColumnExpr("json_build_object('input', input, 'output', output) as volumes").
			Where("ledger = ?", s.ledger.Name)
	}

	selectAccountsWithVolumes = s.db.NewSelect().
		ColumnExpr("*").
		TableExpr("(?) accounts_volumes", selectAccountsWithVolumes)

	if needMetadata {
		if s.ledger.HasFeature(ledger.FeatureAccountMetadataHistory, "SYNC") && date != nil && !date.IsZero() {
			selectAccountsWithVolumes = selectAccountsWithVolumes.
				Join(
					`left join (?) accounts_metadata on accounts_metadata.accounts_address = accounts_volumes.accounts_address`,
					s.selectDistinctAccountMetadataHistories(date),
				).
				ColumnExpr("coalesce(accounts_metadata.metadata, '{}'::jsonb) as metadata")

			if needAddressSegment {
				selectAccountsWithVolumes = selectAccountsWithVolumes.
					Join("join " + s.GetPrefixedRelationName("accounts") + " on accounts.address = accounts_volumes.accounts_address").
					Column("accounts.address_array")
			}
		} else {
			selectAccountsWithVolumes = selectAccountsWithVolumes.
				Join(
					`join (?) accounts on accounts.address = accounts_volumes.accounts_address`,
					s.db.NewSelect().ModelTableExpr(s.GetPrefixedRelationName("accounts")),
				)

			if needAddressSegment {
				selectAccountsWithVolumes = selectAccountsWithVolumes.Column("accounts.address_array")
			}
		}
	} else {
		if needAddressSegment {
			selectAccountsWithVolumes = s.db.NewSelect().
				TableExpr(
					"(?) accounts",
					selectAccountsWithVolumes.
						Join("join "+s.GetPrefixedRelationName("accounts")+" accounts on accounts.address = accounts_volumes.accounts_address"),
				).
				ColumnExpr("*")
		}
	}

	if builder != nil {
		where, args, err := builder.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
			switch {
			case key == "address":
				return filterAccountAddress(value.(string), "accounts.address"), nil, nil
			case metadataRegex.Match([]byte(key)):
				match := metadataRegex.FindAllStringSubmatch(key, 3)

				return "metadata @> ?", []any{map[string]any{
					match[0][1]: value,
				}}, nil

			case key == "metadata":
				return "metadata -> ? is not null", []any{value}, nil
			default:
				return "", nil, ledgercontroller.NewErrInvalidQuery("unknown key '%s' when building query", key)
			}
		}))
		if err != nil {
			return ret.Err(errors.Wrap(err, "building where clause"))
		}
		selectAccountsWithVolumes = selectAccountsWithVolumes.Where(where, args...)
	}

	return selectAccountsWithVolumes
}

func (s *Store) SelectAggregatedBalances(date *time.Time, useInsertionDate bool, builder query.Builder) *bun.SelectQuery {
	return s.db.NewSelect().
		ModelTableExpr("(?) accounts", s.selectAccountWithVolumes(date, useInsertionDate, builder)).
		ColumnExpr(`to_json(array_agg(json_build_object('asset', accounts.asset, 'input', (accounts.volumes->>'input')::numeric, 'output', (accounts.volumes->>'output')::numeric))) as aggregated`)
}

func (s *Store) GetAggregatedBalances(ctx context.Context, q ledgercontroller.GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {
	type AggregatedVolumes struct {
		Aggregated AggregatedAccountVolumes `bun:"aggregated,type:jsonb"`
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
				conditions = append(conditions, "accounts_address = ? and asset = ?")
				args = append(args, account, asset)
			}
		}

		accountsVolumes := make([]AccountsVolumes, 0)
		for account, assets := range query {
			for _, asset := range assets {
				accountsVolumes = append(accountsVolumes, AccountsVolumes{
					Ledger:  s.ledger.Name,
					Account: account,
					Asset:   asset,
					Input:   new(big.Int),
					Output:  new(big.Int),
				})
			}
		}

		err := s.db.NewSelect().
			With(
				"ins",
				// try to insert volumes with 0 values
				// this way, if the account has a 0 balance at this point, it will be locked also
				s.db.NewInsert().
					Model(&accountsVolumes).
					ModelTableExpr(s.GetPrefixedRelationName("accounts_volumes")).
					On("conflict do nothing"),
			).
			Model(&accountsVolumes).
			ModelTableExpr(s.GetPrefixedRelationName("accounts_volumes")).
			Column("accounts_address", "asset", "input", "output").
			Where("("+strings.Join(conditions, ") OR (")+")", args...).
			For("update").
			// notes(gfyrag): keep order, it ensures consistent locking order and limit deadlocks
			Order("accounts_address", "asset").
			Scan(ctx)
		if err != nil {
			return nil, postgres.ResolveError(err)
		}

		ret := ledgercontroller.Balances{}
		for _, volumes := range accountsVolumes {
			if _, ok := ret[volumes.Account]; !ok {
				ret[volumes.Account] = map[string]*big.Int{}
			}
			ret[volumes.Account][volumes.Asset] = new(big.Int).Sub(volumes.Input, volumes.Output)
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

/**
SELECT to_json(array_agg(json_build_object('asset', accounts.asset, 'input', (accounts.volumes->>'input')::numeric, 'output', (accounts.volumes->>'output')::numeric))) AS aggregated
FROM (
	SELECT *
   	FROM (
		SELECT *, accounts.address_array
      	FROM (
			SELECT "asset", "accounts_address", post_commit_volumes AS volumes
         	FROM (
				SELECT DISTINCT ON (accounts_address, asset)
					"accounts_address",
                    "asset",
                    first_value(post_commit_volumes) OVER (PARTITION BY (accounts_address, asset) ORDER BY seq DESC) AS post_commit_volumes
            	FROM (
					SELECT *
               		FROM "87b28082".moves
               		WHERE (ledger = '87b28082') AND (insertion_date <= '2024-09-26T14:45:10.568382Z')
	                ORDER BY "seq" DESC
				) moves
            	WHERE (ledger = '87b28082') AND (insertion_date <= '2024-09-26T14:45:10.568382Z')
			) moves
		) accounts_volumes
      	JOIN "87b28082".accounts accounts ON accounts.address = accounts_volumes.accounts_address
	) accounts
	WHERE (jsonb_array_length(accounts.address_array) = 2
	AND accounts.address_array @@ ('$[0] == "users"')::jsonpath)
) accounts
*/
