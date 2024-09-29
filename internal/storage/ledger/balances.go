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

func (s *Store) selectAccountWithAssetAndVolumes(date *time.Time, useInsertionDate bool, builder query.Builder) *bun.SelectQuery {

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
					// Cast is safe, the type has been validated by validatedAddressFilter
					needAddressSegment = isSegmentedAddress(value.(string))
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

func (s *Store) selectAccountWithAggregatedVolumes(date *time.Time, useInsertionDate bool, alias string) *bun.SelectQuery {
	selectAccountWithAssetAndVolumes := s.selectAccountWithAssetAndVolumes(date, useInsertionDate, nil)
	return s.db.NewSelect().
		TableExpr("(?) values", selectAccountWithAssetAndVolumes).
		Group("accounts_address").
		Column("accounts_address").
		ColumnExpr("aggregate_objects(json_build_object(asset, volumes)::jsonb) as " + alias)
}

func (s *Store) SelectAggregatedBalances(date *time.Time, useInsertionDate bool, builder query.Builder) *bun.SelectQuery {

	selectAccountsWithVolumes := s.selectAccountWithAssetAndVolumes(date, useInsertionDate, builder)
	sumVolumesForAsset := s.db.NewSelect().
		TableExpr("(?) values", selectAccountsWithVolumes).
		Group("asset").
		Column("asset").
		ColumnExpr("json_build_object('input', sum((volumes->>'input')::numeric), 'output', sum((volumes->>'output')::numeric)) as volumes")

	return s.db.NewSelect().
		TableExpr("(?) values", sumVolumesForAsset).
		ColumnExpr("aggregate_objects(json_build_object(asset, volumes)::jsonb) as aggregated")
}

func (s *Store) GetAggregatedBalances(ctx context.Context, q ledgercontroller.GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {
	type AggregatedVolumes struct {
		Aggregated ledger.VolumesByAssets `bun:"aggregated,type:jsonb"`
	}

	aggregatedVolumes := AggregatedVolumes{}
	if err := s.db.NewSelect().
		ModelTableExpr("(?) aggregated_volumes", s.SelectAggregatedBalances(q.PIT, q.UseInsertionDate, q.QueryBuilder)).
		Model(&aggregatedVolumes).
		Scan(ctx); err != nil {
		return nil, err
	}

	return aggregatedVolumes.Aggregated.Balances(), nil
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

		type AccountsVolumesWithLedger struct {
			ledger.AccountsVolumes `bun:",extend"`
			Ledger                 string `bun:"ledger,type:varchar"`
		}

		accountsVolumes := make([]AccountsVolumesWithLedger, 0)
		for account, assets := range query {
			for _, asset := range assets {
				accountsVolumes = append(accountsVolumes, AccountsVolumesWithLedger{
					Ledger: s.ledger.Name,
					AccountsVolumes: ledger.AccountsVolumes{
						Account: account,
						Asset:   asset,
						Input:   new(big.Int),
						Output:  new(big.Int),
					},
				})
			}
		}

		err := s.db.NewSelect().
			With(
				"ins",
				// Try to insert volumes with 0 values.
				// This way, if the account has a 0 balance at this point, it will be locked as any other accounts.
				// It the complete sql transaction fail, the account volumes will not be inserted.
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
			// notes(gfyrag): Keep order, it ensures consistent locking order and limit deadlocks
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

		// Fill empty balances with 0 value
		// todo: still required as we insert balances earlier
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
