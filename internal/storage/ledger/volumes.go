package ledger

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/platform/postgres"

	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	lquery "github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/uptrace/bun"
)

func (s *Store) UpdateVolumes(ctx context.Context, accountVolumes ...ledger.AccountsVolumes) (ledger.PostCommitVolumes, error) {
	return tracing.TraceWithMetric(
		ctx,
		"UpdateBalances",
		s.tracer,
		s.updateBalancesHistogram,
		func(ctx context.Context) (ledger.PostCommitVolumes, error) {

			type AccountsVolumesWithLedger struct {
				ledger.AccountsVolumes `bun:",extend"`
				Ledger                 string `bun:"ledger,type:varchar"`
			}

			accountsVolumesWithLedger := collectionutils.Map(accountVolumes, func(from ledger.AccountsVolumes) AccountsVolumesWithLedger {
				return AccountsVolumesWithLedger{
					AccountsVolumes: from,
					Ledger:          s.ledger.Name,
				}
			})

			_, err := s.db.NewInsert().
				Model(&accountsVolumesWithLedger).
				ModelTableExpr(s.GetPrefixedRelationName("accounts_volumes")).
				On("conflict (ledger, accounts_address, asset) do update").
				Set("input = accounts_volumes.input + excluded.input").
				Set("output = accounts_volumes.output + excluded.output").
				Returning("input, output").
				Exec(ctx)
			if err != nil {
				return nil, postgres.ResolveError(err)
			}

			ret := ledger.PostCommitVolumes{}
			for _, volumes := range accountVolumes {
				if _, ok := ret[volumes.Account]; !ok {
					ret[volumes.Account] = map[string]ledger.Volumes{}
				}
				ret[volumes.Account][volumes.Asset] = ledger.Volumes{
					Input:  volumes.Input,
					Output: volumes.Output,
				}
			}

			return ret, err
		},
	)
}

func (s *Store) selectVolumes(oot, pit *time.Time, useInsertionDate bool, groupLevel int, q lquery.Builder) *bun.SelectQuery {
	ret := s.db.NewSelect()

	if !s.ledger.HasFeature(ledger.FeatureMovesHistory, "ON") {
		return ret.Err(ledgercontroller.NewErrMissingFeature(ledger.FeatureMovesHistory))
	}

	var (
		useMetadata        bool
		needSegmentAddress bool
	)
	if q != nil {
		err := q.Walk(func(operator, key string, value any) error {
			switch {
			case key == "account" || key == "address":
				if err := s.validateAddressFilter(operator, value); err != nil {
					return err
				}
				if !needSegmentAddress {
					needSegmentAddress = isSegmentedAddress(value.(string)) // Safe cast
				}
			case metadataRegex.Match([]byte(key)):
				if operator != "$match" {
					return ledgercontroller.NewErrInvalidQuery("'metadata' column can only be used with $match")
				}
				useMetadata = true
			case key == "metadata":
				if operator != "$exists" {
					return ledgercontroller.NewErrInvalidQuery("'metadata' key filter can only be used with $exists")
				}
				useMetadata = true
			case balanceRegex.Match([]byte(key)):
			default:
				return ledgercontroller.NewErrInvalidQuery("unknown key '%s' when building query", key)
			}
			return nil
		})
		if err != nil {
			return ret.Err(err)
		}
	}

	selectVolumes := s.db.NewSelect().
		ColumnExpr("accounts_address as address").
		Column("asset").
		ColumnExpr("sum(case when not is_source then amount else 0 end) as input").
		ColumnExpr("sum(case when is_source then amount else 0 end) as output").
		ColumnExpr("sum(case when not is_source then amount else -amount end) as balance").
		ModelTableExpr(s.GetPrefixedRelationName("moves")).
		GroupExpr("accounts_address, asset")

	dateFilterColumn := "effective_date"
	if useInsertionDate {
		dateFilterColumn = "insertion_date"
	}

	if pit != nil && !pit.IsZero() {
		selectVolumes = selectVolumes.Where(dateFilterColumn+" <= ?", pit)
	}
	if oot != nil && !oot.IsZero() {
		selectVolumes = selectVolumes.Where(dateFilterColumn+" >= ?", oot)
	}

	ret = ret.
		ModelTableExpr("(?) volumes", selectVolumes).
		Column("address", "asset", "input", "output", "balance")

	if needSegmentAddress {
		selectAccount := s.db.NewSelect().
			ModelTableExpr(s.GetPrefixedRelationName("accounts")).
			Where("ledger = ? and address = volumes.address", s.ledger.Name).
			Column("address_array")
		if useMetadata && (pit == nil || pit.IsZero()) {
			selectAccount = selectAccount.Column("metadata")
		}

		ret = ret.
			Join("join lateral (?) accounts on true", selectAccount).
			Column("accounts.address_array")
		if useMetadata && (pit == nil || pit.IsZero()) {
			ret = ret.Column("accounts.metadata")
		}
	}

	if useMetadata {
		switch {
		case needSegmentAddress && (pit == nil || pit.IsZero()):
			// nothing to do, already handled earlier
		case !needSegmentAddress && (pit == nil || pit.IsZero()):
			selectAccount := s.db.NewSelect().
				ModelTableExpr(s.GetPrefixedRelationName("accounts")).
				Where("ledger = ? and address = volumes.address", s.ledger.Name).
				Column("metadata")

			ret = ret.
				Join("join lateral (?) accounts on true", selectAccount).
				Column("accounts.metadata")
		case pit != nil && !pit.IsZero():
			selectAccountMetadata := s.db.NewSelect().
				Column("metadata").
				ModelTableExpr(s.GetPrefixedRelationName("accounts_metadata")).
				Where("ledger = ? and accounts_address = volumes.address and date <= ?", s.ledger.Name, pit)

			ret = ret.
				Join("join lateral (?) accounts_metadata on true", selectAccountMetadata).
				Column("accounts_metadata.metadata")
		}
	}

	if q != nil {
		where, args, err := q.Build(lquery.ContextFn(func(key, operator string, value any) (string, []any, error) {

			switch {
			case key == "account" || key == "address":
				return filterAccountAddress(value.(string), "address"), nil, nil
			case metadataRegex.Match([]byte(key)):
				match := metadataRegex.FindAllStringSubmatch(key, 3)
				return "metadata @> ?", []any{map[string]any{
					match[0][1]: value,
				}}, nil
			case key == "metadata":
				return "metadata -> ? is not null", []any{value}, nil
			case balanceRegex.Match([]byte(key)):
				match := balanceRegex.FindAllStringSubmatch(key, 2)
				return `balance ` + convertOperatorToSQL(operator) + ` ? and asset = ?`, []any{value, match[0][1]}, nil
			default:
				return "", nil, ledgercontroller.NewErrInvalidQuery("unknown key '%s' when building query", key)
			}
		}))
		if err != nil {
			return ret.Err(err)
		}
		ret = ret.Where(where, args...)
	}

	globalQuery := s.db.NewSelect()
	globalQuery = globalQuery.
		With("query", ret).
		ModelTableExpr("query")

	if groupLevel > 0 {
		globalQuery = globalQuery.
			ColumnExpr(fmt.Sprintf(`(array_to_string((string_to_array(address, ':'))[1:LEAST(array_length(string_to_array(address, ':'),1),%d)],':')) as account`, groupLevel)).
			ColumnExpr("asset").
			ColumnExpr("sum(input) as input").
			ColumnExpr("sum(output) as output").
			ColumnExpr("sum(balance) as balance").
			GroupExpr("account, asset")
	} else {
		globalQuery = globalQuery.ColumnExpr("address as account, asset, input, output, balance")
	}

	return globalQuery
}

func (s *Store) GetVolumesWithBalances(ctx context.Context, q ledgercontroller.GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	return tracing.TraceWithMetric(
		ctx,
		"GetVolumesWithBalances",
		s.tracer,
		s.getVolumesWithBalancesHistogram,
		func(ctx context.Context) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
			return bunpaginate.UsingOffset[ledgercontroller.PaginatedQueryOptions[ledgercontroller.FiltersForVolumes], ledger.VolumesWithBalanceByAssetByAccount](
				ctx,
				s.selectVolumes(
					q.Options.Options.OOT,
					q.Options.Options.PIT,
					q.Options.Options.UseInsertionDate,
					q.Options.Options.GroupLvl,
					q.Options.QueryBuilder,
				),
				bunpaginate.OffsetPaginatedQuery[ledgercontroller.PaginatedQueryOptions[ledgercontroller.FiltersForVolumes]](q),
			)
		},
	)
}
