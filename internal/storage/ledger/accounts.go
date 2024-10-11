package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"

	. "github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/platform/postgres"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
)

var (
	balanceRegex = regexp.MustCompile(`balance\[(.*)]`)
)

func convertOperatorToSQL(operator string) string {
	switch operator {
	case "$match":
		return "="
	case "$lt":
		return "<"
	case "$gt":
		return ">"
	case "$lte":
		return "<="
	case "$gte":
		return ">="
	}
	panic("unreachable")
}

func (s *Store) selectBalance(date *time.Time) *bun.SelectQuery {

	if date != nil && !date.IsZero() {
		sortedMoves := s.SortMovesBySeq(date).
			ColumnExpr("(post_commit_volumes->>'input')::numeric - (post_commit_volumes->>'output')::numeric as balance").
			Limit(1)

		return s.db.NewSelect().
			ModelTableExpr("(?) moves", sortedMoves).
			ColumnExpr("accounts_address, asset")
	}

	return s.db.NewSelect().
		ModelTableExpr(s.GetPrefixedRelationName("accounts_volumes")).
		ColumnExpr("input - output as balance")
}

func (s *Store) selectDistinctAccountMetadataHistories(date *time.Time) *bun.SelectQuery {
	ret := s.db.NewSelect().
		DistinctOn("accounts_address").
		ModelTableExpr(s.GetPrefixedRelationName("accounts_metadata")).
		Where("ledger = ?", s.ledger.Name).
		Column("accounts_address", "metadata").
		Order("accounts_address", "revision desc")

	if date != nil && !date.IsZero() {
		ret = ret.Where("date <= ?", date)
	}

	return ret
}

func (s *Store) selectAccounts(date *time.Time, expandVolumes, expandEffectiveVolumes bool, qb query.Builder) *bun.SelectQuery {

	ret := s.db.NewSelect()

	needVolumes := expandVolumes
	if qb != nil {
		// Analyze filters to check for errors and find potentially additional table to load
		if err := qb.Walk(func(operator, key string, value any) error {
			switch {
			// Balances requires pvc, force load in this case
			case balanceRegex.MatchString(key):
				needVolumes = true
			case key == "address":
				return s.validateAddressFilter(operator, value)
			case key == "metadata":
				if operator != "$exists" {
					return ledgercontroller.NewErrInvalidQuery("'metadata' key filter can only be used with $exists")
				}
			case metadataRegex.MatchString(key):
				if operator != "$match" {
					return ledgercontroller.NewErrInvalidQuery("'metadata' key filter can only be used with $match")
				}
			case key == "first_usage":
			default:
				return ledgercontroller.NewErrInvalidQuery("unknown key '%s' when building query", key)
			}

			return nil
		}); err != nil {
			return ret.Err(fmt.Errorf("failed to check filters: %w", err))
		}
	}

	// Build the query
	ret = ret.
		ModelTableExpr(s.GetPrefixedRelationName("accounts")).
		Column("accounts.address", "accounts.first_usage").
		Where("ledger = ?", s.ledger.Name).
		Order("accounts.address")

	if date != nil && !date.IsZero() {
		ret = ret.Where("accounts.first_usage <= ?", date)
	}

	if s.ledger.HasFeature(ledger.FeatureAccountMetadataHistory, "SYNC") && date != nil && !date.IsZero() {
		ret = ret.
			Join(
				`left join (?) accounts_metadata on accounts_metadata.accounts_address = accounts.address`,
				s.selectDistinctAccountMetadataHistories(date),
			).
			ColumnExpr("coalesce(accounts_metadata.metadata, '{}'::jsonb) as metadata")
	} else {
		ret = ret.ColumnExpr("accounts.metadata")
	}

	if s.ledger.HasFeature(ledger.FeatureMovesHistory, "ON") && needVolumes {
		ret = ret.Join(
			`left join (?) volumes on volumes.accounts_address = accounts.address`,
			s.selectAccountWithAggregatedVolumes(date, true, "volumes"),
		).Column("volumes.*")
	}

	if s.ledger.HasFeature(ledger.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") && expandEffectiveVolumes {
		ret = ret.Join(
			`left join (?) effective_volumes on effective_volumes.accounts_address = accounts.address`,
			s.selectAccountWithAggregatedVolumes(date, false, "effective_volumes"),
		).Column("effective_volumes.*")
	}

	if qb != nil {
		// Convert filters to where clause
		where, args, err := qb.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
			switch {
			case key == "address":
				return filterAccountAddress(value.(string), "accounts.address"), nil, nil
			case key == "first_usage":
				return fmt.Sprintf("first_usage %s ?", convertOperatorToSQL(operator)), []any{value}, nil
			case balanceRegex.Match([]byte(key)):
				match := balanceRegex.FindAllStringSubmatch(key, 2)
				asset := match[0][1]

				return s.db.NewSelect().
					TableExpr(
						"(?) balance",
						s.selectBalance(date).
							Where("asset = ? and accounts_address = accounts.address", asset),
					).
					ColumnExpr(fmt.Sprintf("balance %s ?", convertOperatorToSQL(operator)), value).
					String(), nil, nil

			case key == "balance":
				return s.db.NewSelect().
					TableExpr(
						"(?) balance",
						s.selectBalance(date).
							Where("accounts_address = accounts.address"),
					).
					ColumnExpr(fmt.Sprintf("balance %s ?", convertOperatorToSQL(operator)), value).
					String(), nil, nil

			case key == "metadata":
				if s.ledger.HasFeature(ledger.FeatureAccountMetadataHistory, "SYNC") && date != nil && !date.IsZero() {
					key = "accounts_metadata.metadata"
				}

				return key + " -> ? is not null", []any{value}, nil

			case metadataRegex.Match([]byte(key)):
				match := metadataRegex.FindAllStringSubmatch(key, 3)
				if s.ledger.HasFeature(ledger.FeatureAccountMetadataHistory, "SYNC") && date != nil && !date.IsZero() {
					key = "accounts_metadata.metadata"
				} else {
					key = "metadata"
				}

				return key + " @> ?", []any{map[string]any{
					match[0][1]: value,
				}}, nil
			}

			panic("unreachable")
		}))
		if err != nil {
			return ret.Err(fmt.Errorf("evaluating filters: %w", err))
		}
		if len(args) > 0 {
			ret = ret.Where(where, args...)
		} else {
			ret = ret.Where(where)
		}
	}

	return ret
}

func (s *Store) ListAccounts(ctx context.Context, q ledgercontroller.ListAccountsQuery) (*Cursor[ledger.Account], error) {
	return tracing.TraceWithMetric(
		ctx,
		"ListAccounts",
		s.listAccountsHistogram,
		func(ctx context.Context) (*Cursor[ledger.Account], error) {
			ret, err := UsingOffset[ledgercontroller.PaginatedQueryOptions[ledgercontroller.PITFilterWithVolumes], ledger.Account](
				ctx,
				s.selectAccounts(
					q.Options.Options.PIT,
					q.Options.Options.ExpandVolumes,
					q.Options.Options.ExpandEffectiveVolumes,
					q.Options.QueryBuilder,
				),
				OffsetPaginatedQuery[ledgercontroller.PaginatedQueryOptions[ledgercontroller.PITFilterWithVolumes]](q),
			)

			if err != nil {
				return nil, err
			}

			return ret, nil
		},
	)
}

func (s *Store) GetAccount(ctx context.Context, q ledgercontroller.GetAccountQuery) (*ledger.Account, error) {
	return tracing.TraceWithMetric(
		ctx,
		"GetAccount",
		s.getAccountHistogram,
		func(ctx context.Context) (*ledger.Account, error) {
			ret := &ledger.Account{}
			if err := s.selectAccounts(q.PIT, q.ExpandVolumes, q.ExpandEffectiveVolumes, nil).
				Model(ret).
				Where("accounts.address = ?", q.Addr).
				Limit(1).
				Scan(ctx); err != nil {
				return nil, postgres.ResolveError(err)
			}

			return ret, nil
		},
	)
}

func (s *Store) CountAccounts(ctx context.Context, q ledgercontroller.ListAccountsQuery) (int, error) {
	return tracing.TraceWithMetric(
		ctx,
		"CountAccounts",
		s.countAccountsHistogram,
		func(ctx context.Context) (int, error) {
			return s.db.NewSelect().
				TableExpr("(?) data", s.selectAccounts(
					q.Options.Options.PIT,
					q.Options.Options.ExpandVolumes,
					q.Options.Options.ExpandEffectiveVolumes,
					q.Options.QueryBuilder,
				)).
				Count(ctx)
		},
	)
}

func (s *Store) UpdateAccountsMetadata(ctx context.Context, m map[string]metadata.Metadata) error {
	_, err := tracing.TraceWithMetric(
		ctx,
		"UpdateAccountsMetadata",
		s.updateAccountsMetadataHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			type AccountWithLedger struct {
				ledger.Account `bun:",extend"`
				Ledger         string `bun:"ledger,type:varchar"`
			}

			accounts := make([]AccountWithLedger, 0)
			for account, accountMetadata := range m {
				accounts = append(accounts, AccountWithLedger{
					Ledger: s.ledger.Name,
					Account: ledger.Account{
						Address:  account,
						Metadata: accountMetadata,
					},
				})
			}

			_, err := s.db.NewInsert().
				Model(&accounts).
				ModelTableExpr(s.GetPrefixedRelationName("accounts")).
				On("CONFLICT (ledger, address) DO UPDATE").
				Set("metadata = excluded.metadata || accounts.metadata").
				Where("not accounts.metadata @> excluded.metadata").
				Exec(ctx)
			return postgres.ResolveError(err)
		}),
	)
	return err
}

func (s *Store) DeleteAccountMetadata(ctx context.Context, account, key string) error {
	_, err := tracing.TraceWithMetric(
		ctx,
		"DeleteAccountMetadata",
		s.deleteAccountMetadataHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			_, err := s.db.NewUpdate().
				ModelTableExpr(s.GetPrefixedRelationName("accounts")).
				Set("metadata = metadata - ?", key).
				Where("address = ?", account).
				Where("ledger = ?", s.ledger.Name).
				Exec(ctx)
			return postgres.ResolveError(err)
		}),
	)
	return err
}

func (s *Store) UpsertAccount(ctx context.Context, account *ledger.Account) (bool, error) {
	return tracing.TraceWithMetric(
		ctx,
		"UpsertAccount",
		s.upsertAccountHistogram,
		func(ctx context.Context) (bool, error) {
			upserted := false
			err := s.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
				ret, err := tx.NewInsert().
					Model(account).
					ModelTableExpr(s.GetPrefixedRelationName("accounts")).
					On("conflict (ledger, address) do update").
					Set("first_usage = case when ? < excluded.first_usage then ? else excluded.first_usage end", account.FirstUsage, account.FirstUsage).
					Set("metadata = accounts.metadata || excluded.metadata").
					Set("updated_at = ?", account.UpdatedAt).
					Value("ledger", "?", s.ledger.Name).
					Returning("*").
					Where("(? < accounts.first_usage) or not accounts.metadata @> excluded.metadata", account.FirstUsage).
					Exec(ctx)
				if err != nil {
					return err
				}
				rowsModified, err := ret.RowsAffected()
				if err != nil {
					return err
				}
				upserted = rowsModified > 0
				return nil
			})
			return upserted, postgres.ResolveError(err)
		},
		func(ctx context.Context, upserted bool) {
			trace.SpanFromContext(ctx).SetAttributes(
				attribute.String("address", account.Address),
				attribute.Bool("upserted", upserted),
			)
		},
	)
}
