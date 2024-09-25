package ledger

import (
	"context"
	"github.com/pkg/errors"
	"math/big"
	"strings"

	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/query"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/uptrace/bun"
)

type Balances struct {
	bun.BaseModel `bun:"accounts_volumes"`

	Ledger  string   `bun:"ledger,type:varchar"`
	Account string   `bun:"account,type:varchar"`
	Asset   string   `bun:"asset,type:varchar"`
	Balance *big.Int `bun:"balance,type:numeric"`
}

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
			if !s.ledger.HasFeature(ledger.FeaturePostCommitVolumes, "SYNC") {
				return ret.Err(ledgercontroller.NewErrMissingFeature(ledger.FeaturePostCommitVolumes))
			}
			selectAccountsWithVolumes = s.db.NewSelect().
				TableExpr("(?) moves", s.SelectDistinctMovesBySeq(date)).
				Column("asset", "accounts_seq", "account_address", "account_address_array").
				ColumnExpr("post_commit_volumes as volumes")
		} else {
			if !s.ledger.HasFeature(ledger.FeaturePostCommitEffectiveVolumes, "SYNC") {
				return ret.Err(ledgercontroller.NewErrMissingFeature(ledger.FeaturePostCommitEffectiveVolumes))
			}
			selectAccountsWithVolumes = s.db.NewSelect().
				TableExpr("(?) moves", s.SelectDistinctMovesByEffectiveDate(date)).
				Column("asset", "accounts_seq", "account_address", "account_address_array").
				ColumnExpr("moves.post_commit_effective_volumes as volumes")
		}
	} else {
		selectAccountsWithVolumes = s.db.NewSelect().
			ModelTableExpr(s.GetPrefixedRelationName("accounts_volumes")).
			Column("asset", "accounts_seq").
			ColumnExpr("account as account_address").
			ColumnExpr("(inputs, outputs)::"+s.GetPrefixedRelationName("volumes")+" as volumes").
			Where("ledger = ?", s.ledger.Name)
	}

	selectAccountsWithVolumes = s.db.NewSelect().
		ColumnExpr("*").
		TableExpr("(?) accounts_volumes", selectAccountsWithVolumes)

	if needMetadata {
		if s.ledger.HasFeature(ledger.FeatureAccountMetadataHistories, "SYNC") && date != nil && !date.IsZero() {
			selectAccountsWithVolumes = selectAccountsWithVolumes.
				Join(
					`left join (?) accounts_metadata on accounts_metadata.accounts_seq = accounts_volumes.accounts_seq`,
					s.selectDistinctAccountMetadataHistories(date),
				).
				ColumnExpr("coalesce(accounts_metadata.metadata, '{}'::jsonb) as metadata")

			if needAddressSegment {
				selectAccountsWithVolumes = selectAccountsWithVolumes.
					Join("join " + s.GetPrefixedRelationName("accounts") + " on accounts.seq = accounts_volumes.accounts_seq").
					Column("accounts.address_array")
			}
		} else {
			selectAccountsWithVolumes = selectAccountsWithVolumes.
				Join(
					`join (?) accounts on accounts.seq = accounts_volumes.accounts_seq`,
					s.db.NewSelect().ModelTableExpr(s.GetPrefixedRelationName("accounts")),
				)

			if needAddressSegment {
				selectAccountsWithVolumes = selectAccountsWithVolumes.Column("accounts.address_array")
			}
		}
	} else {
		if needAddressSegment {
			if date == nil || date.IsZero() { // account_address_array already resolved by moves if pit is specified
				selectAccountsWithVolumes = s.db.NewSelect().
					TableExpr(
						"(?) accounts",
						selectAccountsWithVolumes.
							Join("join "+s.GetPrefixedRelationName("accounts")+" accounts on accounts.seq = accounts_volumes.accounts_seq").
							ColumnExpr("accounts.address_array as account_address_array"),
					).
					ColumnExpr("*")
			}
		}
	}

	if builder != nil {
		where, args, err := builder.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
			switch {
			case key == "address":
				return filterAccountAddress(value.(string), "account_address"), nil, nil
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
		ColumnExpr(`to_json(array_agg(json_build_object('asset', accounts.asset, 'inputs', (accounts.volumes).inputs, 'outputs', (accounts.volumes).outputs))) as aggregated`)
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
				conditions = append(conditions, "account = ? and asset = ?")
				args = append(args, account, asset)
			}
		}

		balances := make([]Balances, 0)
		err := s.db.NewSelect().
			Model(&balances).
			ModelTableExpr(s.GetPrefixedRelationName("accounts_volumes")).
			ColumnExpr("account, asset").
			ColumnExpr("inputs - outputs as balance").
			Where("("+strings.Join(conditions, ") OR (")+")", args...).
			For("update").
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

/**

SELECT *
FROM (
	SELECT *, accounts.address_array AS account_address_array
   	FROM (
		SELECT
			"asset",
            "accounts_seq",
            "account_address",
            "account_address_array",
            post_commit_volumes AS volumes
      	FROM (
			SELECT DISTINCT ON (accounts_seq, account_address, asset)
				"accounts_seq",
                "account_address",
                "asset",
				first_value(account_address_array) OVER (PARTITION BY (accounts_seq, account_address, asset) ORDER BY seq DESC) AS account_address_array,
				first_value(post_commit_volumes) OVER (PARTITION BY (accounts_seq, account_address, asset) ORDER BY seq DESC) AS post_commit_volumes
         	FROM (
				SELECT *
            	FROM "7c44551f".moves
            	WHERE (ledger = '7c44551f') AND (insertion_date <= '2024-09-25T12:01:13.895812Z')
	            ORDER BY "seq" DESC
			) moves
         	WHERE (ledger = '7c44551f') AND (insertion_date <= '2024-09-25T12:01:13.895812Z')) moves
	) accounts_volumes
   	JOIN "7c44551f".accounts accounts ON accounts.seq = accounts_volumes.accounts_seq
) accounts
WHERE (jsonb_array_length(account_address_array) = 2 AND account_address_array @@ ('$[0] == "users"')::jsonpath)

*/
