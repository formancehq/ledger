package ledger

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/platform/postgres"
	"math/big"

	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	lquery "github.com/formancehq/go-libs/query"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/uptrace/bun"
)

type AccountsVolumes struct {
	bun.BaseModel `bun:"accounts_volumes"`

	Ledger      string   `bun:"ledger,type:varchar"`
	Account     string   `bun:"account,type:varchar"`
	Asset       string   `bun:"asset,type:varchar"`
	Input       *big.Int `bun:"input,type:numeric"`
	Output      *big.Int `bun:"output,type:numeric"`
	AccountsSeq int      `bun:"accounts_seq,type:int"`
}

func (s *Store) updateVolumes(ctx context.Context, accountVolumes ...AccountsVolumes) (ledger.PostCommitVolumes, error) {
	return tracing.TraceWithLatency(ctx, "UpdateBalances", func(ctx context.Context) (ledger.PostCommitVolumes, error) {

		_, err := s.db.NewInsert().
			Model(&accountVolumes).
			ModelTableExpr(s.GetPrefixedRelationName("accounts_volumes")).
			On("conflict (ledger, account, asset) do update").
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
	})
}

func (s *Store) selectVolumes(oot, pit *time.Time, useInsertionDate bool, groupLevel int, q lquery.Builder) *bun.SelectQuery {
	ret := s.db.NewSelect()

	if !s.ledger.HasFeature(ledger.FeatureMovesHistory, "ON") {
		return ret.Err(ledgercontroller.NewErrMissingFeature(ledger.FeatureMovesHistory))
	}

	var useMetadata bool
	if q != nil {
		err := q.Walk(func(operator, key string, value any) error {
			switch {
			case key == "account" || key == "address":
				return s.validateAddressFilter(operator, value)
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

	ret = ret.
		Column("account_address_array").
		Column("account_address").
		Column("asset").
		ColumnExpr("sum(case when not is_source then amount else 0 end) as input").
		ColumnExpr("sum(case when is_source then amount else 0 end) as output").
		ColumnExpr("sum(case when not is_source then amount else -amount end) as balance").
		ModelTableExpr(s.GetPrefixedRelationName("moves"))

	// todo: handle with pit by using accounts_metadata
	if useMetadata {
		ret = ret.
			Join(
				"join lateral (?) accounts on true",
				s.db.NewSelect().
					Column("metadata").
					ModelTableExpr(s.GetPrefixedRelationName("accounts")).
					Where("accounts.seq = moves.accounts_seq"),
			).
			ColumnExpr("accounts.metadata as metadata").
			Group("accounts.metadata")
	}

	dateFilterColumn := "effective_date"
	if useInsertionDate {
		dateFilterColumn = "insertion_date"
	}

	if pit != nil && !pit.IsZero() {
		ret = ret.Where(dateFilterColumn+" <= ?", pit)
	}
	if oot != nil && !oot.IsZero() {
		ret = ret.Where(dateFilterColumn+" >= ?", oot)
	}

	ret = ret.GroupExpr("account_address, account_address_array, asset")

	globalQuery := s.db.NewSelect()
	globalQuery = globalQuery.
		With("query", ret).
		ModelTableExpr("query")

	if groupLevel > 0 {
		globalQuery = globalQuery.
			ColumnExpr(fmt.Sprintf(`(array_to_string((string_to_array(account_address, ':'))[1:LEAST(array_length(string_to_array(account_address, ':'),1),%d)],':')) as account`, groupLevel)).
			ColumnExpr("asset").
			ColumnExpr("sum(input) as input").
			ColumnExpr("sum(output) as output").
			ColumnExpr("sum(balance) as balance").
			GroupExpr("account, asset")
	} else {
		globalQuery = globalQuery.ColumnExpr("account_address as account, asset, input, output, balance")
	}

	if useMetadata {
		globalQuery = globalQuery.Column("metadata")
	}

	if q != nil {
		where, args, err := q.Build(lquery.ContextFn(func(key, operator string, value any) (string, []any, error) {

			switch {
			case key == "account" || key == "address":
				return filterAccountAddress(value.(string), "account_address"), nil, nil
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
		globalQuery = globalQuery.Where(where, args...)
	}

	return globalQuery
}

func (s *Store) GetVolumesWithBalances(ctx context.Context, q ledgercontroller.GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	return tracing.TraceWithLatency(ctx, "GetVolumesWithBalances", func(ctx context.Context) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
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
	})
}
