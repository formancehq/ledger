package ledger

import (
	"context"
	"fmt"
	"github.com/formancehq/ledger/pkg/features"
	"math/big"
	"strings"

	"github.com/formancehq/go-libs/v2/platform/postgres"

	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/uptrace/bun"
)

func (s *Store) selectAccountWithAssetAndVolumes(date *time.Time, useInsertionDate bool, builder query.Builder) (*bun.SelectQuery, error) {

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
			return nil, err
		}
	}

	if needAddressSegment && !s.ledger.HasFeature(features.FeatureIndexAddressSegments, "ON") {
		return nil, ledgercontroller.NewErrMissingFeature(features.FeatureIndexAddressSegments)
	}

	var selectAccountsWithVolumes *bun.SelectQuery
	if date != nil && !date.IsZero() {
		if useInsertionDate {
			if !s.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
				return nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistory)
			}
			selectDistinctMovesBySeq, err := s.SelectDistinctMovesBySeq(date)
			if err != nil {
				return nil, err
			}
			selectAccountsWithVolumes = s.db.NewSelect().
				TableExpr("(?) moves", selectDistinctMovesBySeq).
				Column("asset", "accounts_address").
				ColumnExpr("post_commit_volumes as volumes")
		} else {
			if !s.ledger.HasFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
				return nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes)
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
			ColumnExpr("(input, output)::"+s.GetPrefixedRelationName("volumes")+" as volumes").
			Where("ledger = ?", s.ledger.Name)
	}

	selectAccountsWithVolumes = s.db.NewSelect().
		ColumnExpr("*").
		TableExpr("(?) accounts_volumes", selectAccountsWithVolumes)

	needAccount := needAddressSegment
	if needMetadata {
		if s.ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") && date != nil && !date.IsZero() {
			selectAccountsWithVolumes = selectAccountsWithVolumes.
				Join(
					`left join (?) accounts_metadata on accounts_metadata.accounts_address = accounts_volumes.accounts_address`,
					s.selectDistinctAccountMetadataHistories(date),
				)
		} else {
			needAccount = true
		}
	}

	if needAccount {
		selectAccountsWithVolumes = s.db.NewSelect().
			TableExpr(
				"(?) accounts",
				selectAccountsWithVolumes.
					Join("join "+s.GetPrefixedRelationName("accounts")+" accounts on accounts.address = accounts_volumes.accounts_address and ledger = ?", s.ledger.Name),
			).
			ColumnExpr("address, asset, volumes, metadata").
			ColumnExpr("accounts.address_array as accounts_address_array")
	}

	finalQuery := s.db.NewSelect().
		TableExpr("(?) accounts", selectAccountsWithVolumes)

	if builder != nil {
		where, args, err := builder.Build(query.ContextFn(func(key, _ string, value any) (string, []any, error) {
			switch {
			case key == "address":
				return filterAccountAddress(value.(string), "accounts_address"), nil, nil
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
			return nil, fmt.Errorf("building where clause: %w", err)
		}
		finalQuery = finalQuery.Where(where, args...)
	}

	return finalQuery, nil
}

func (s *Store) selectAccountWithAggregatedVolumes(date *time.Time, useInsertionDate bool, alias string) (*bun.SelectQuery, error) {
	selectAccountWithAssetAndVolumes, err := s.selectAccountWithAssetAndVolumes(date, useInsertionDate, nil)
	if err != nil {
		return nil, err
	}
	return s.db.NewSelect().
		TableExpr("(?) values", selectAccountWithAssetAndVolumes).
		Group("accounts_address").
		Column("accounts_address").
		ColumnExpr("public.aggregate_objects(json_build_object(asset, json_build_object('input', (volumes).inputs, 'output', (volumes).outputs))::jsonb) as " + alias), nil
}

func (s *Store) SelectAggregatedBalances(date *time.Time, useInsertionDate bool, builder query.Builder) (*bun.SelectQuery, error) {

	selectAccountsWithVolumes, err := s.selectAccountWithAssetAndVolumes(date, useInsertionDate, builder)
	if err != nil {
		return nil, err
	}
	sumVolumesForAsset := s.db.NewSelect().
		TableExpr("(?) values", selectAccountsWithVolumes).
		Group("asset").
		Column("asset").
		ColumnExpr("json_build_object('input', sum(((volumes).inputs)::numeric), 'output', sum(((volumes).outputs)::numeric)) as volumes")

	return s.db.NewSelect().
		TableExpr("(?) values", sumVolumesForAsset).
		ColumnExpr("aggregate_objects(json_build_object(asset, volumes)::jsonb) as aggregated"), nil
}

func (s *Store) GetAggregatedBalances(ctx context.Context, q ledgercontroller.GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {
	type AggregatedVolumes struct {
		Aggregated ledger.VolumesByAssets `bun:"aggregated,type:jsonb"`
	}

	selectAggregatedBalances, err := s.SelectAggregatedBalances(q.PIT, q.UseInsertionDate, q.QueryBuilder)
	if err != nil {
		return nil, err
	}

	aggregatedVolumes := AggregatedVolumes{}
	if err := s.db.NewSelect().
		ModelTableExpr("(?) aggregated_volumes", selectAggregatedBalances).
		Model(&aggregatedVolumes).
		Scan(ctx); err != nil {
		return nil, err
	}

	return aggregatedVolumes.Aggregated.Balances(), nil
}

func (s *Store) GetBalances(ctx context.Context, query ledgercontroller.BalanceQuery) (ledgercontroller.Balances, error) {
	return tracing.TraceWithMetric(
		ctx,
		"GetBalances",
		s.tracer,
		s.getBalancesHistogram,
		func(ctx context.Context) (ledgercontroller.Balances, error) {
			conditions := make([]string, 0)
			args := make([]any, 0)
			for account, assets := range query {
				for _, asset := range assets {
					conditions = append(conditions, "ledger = ? and accounts_address = ? and asset = ?")
					args = append(args, s.ledger.Name, account, asset)
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

			// Try to insert volumes using last move (to keep compat with previous version) or 0 values.
			// This way, if the account has a 0 balance at this point, it will be locked as any other accounts.
			// If the complete sql transaction fails, the account volumes will not be inserted.
			selectMoves := s.db.NewSelect().
				ModelTableExpr(s.GetPrefixedRelationName("moves")).
				DistinctOn("accounts_address, asset").
				Column("accounts_address", "asset").
				ColumnExpr("first_value(post_commit_volumes) over (partition by accounts_address, asset order by seq desc) as post_commit_volumes").
				ColumnExpr("first_value(ledger) over (partition by accounts_address, asset order by seq desc) as ledger").
				Where("("+strings.Join(conditions, ") OR (")+")", args...)

			zeroValuesAndMoves := s.db.NewSelect().
				TableExpr("(?) data", selectMoves).
				Column("ledger", "accounts_address", "asset").
				ColumnExpr("(post_commit_volumes).inputs as input").
				ColumnExpr("(post_commit_volumes).outputs as output").
				UnionAll(
					s.db.NewSelect().
						TableExpr(
							"(?) data",
							s.db.NewSelect().NewValues(&accountsVolumes),
						).
						Column("*"),
				)

			zeroValueOrMoves := s.db.NewSelect().
				TableExpr("(?) data", zeroValuesAndMoves).
				Column("ledger", "accounts_address", "asset", "input", "output").
				DistinctOn("ledger, accounts_address, asset")

			insertDefaultValue := s.db.NewInsert().
				TableExpr(s.GetPrefixedRelationName("accounts_volumes")).
				TableExpr("(" + zeroValueOrMoves.String() + ") data").
				On("conflict (ledger, accounts_address, asset) do nothing").
				Returning("ledger, accounts_address, asset, input, output")

			selectExistingValues := s.db.NewSelect().
				ModelTableExpr(s.GetPrefixedRelationName("accounts_volumes")).
				Column("ledger", "accounts_address", "asset", "input", "output").
				Where("("+strings.Join(conditions, ") OR (")+")", args...).
				For("update").
				// notes(gfyrag): Keep order, it ensures consistent locking order and limit deadlocks
				Order("accounts_address", "asset")

			finalQuery := s.db.NewSelect().
				With("inserted", insertDefaultValue).
				With("existing", selectExistingValues).
				ModelTableExpr(
					"(?) accounts_volumes",
					s.db.NewSelect().
						ModelTableExpr("inserted").
						UnionAll(s.db.NewSelect().ModelTableExpr("existing")),
				).
				Model(&accountsVolumes)

			err := finalQuery.Scan(ctx)
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
		},
	)
}
