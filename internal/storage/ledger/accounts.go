package ledger

import (
	"context"
	"fmt"
	. "github.com/formancehq/go-libs/v2/bun/bunpaginate"
	. "github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/ledger/pkg/features"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"regexp"

	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	"github.com/formancehq/go-libs/v2/time"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/query"
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

func (s *Store) selectBalance(date *time.Time) (*bun.SelectQuery, error) {

	if date != nil && !date.IsZero() {
		selectDistinctMovesBySeq, err := s.SelectDistinctMovesBySeq(date)
		if err != nil {
			return nil, err
		}
		sortedMoves := selectDistinctMovesBySeq.
			ColumnExpr("(post_commit_volumes).inputs - (post_commit_volumes).outputs as balance")

		return s.db.NewSelect().
			ModelTableExpr("(?) moves", sortedMoves).
			Where("ledger = ?", s.ledger.Name).
			ColumnExpr("accounts_address, asset, balance"), nil
	}

	return s.db.NewSelect().
		ModelTableExpr(s.GetPrefixedRelationName("accounts_volumes")).
		Where("ledger = ?", s.ledger.Name).
		ColumnExpr("input - output as balance"), nil
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

func (s *Store) selectAccounts(date *time.Time, expandVolumes, expandEffectiveVolumes bool, qb query.Builder) (*bun.SelectQuery, error) {

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
			case key == "first_usage" || key == "balance":
			default:
				return ledgercontroller.NewErrInvalidQuery("unknown key '%s' when building query", key)
			}

			return nil
		}); err != nil {
			return nil, fmt.Errorf("failed to check filters: %w", err)
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

	if s.ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") && date != nil && !date.IsZero() {
		ret = ret.
			Join(
				`left join (?) accounts_metadata on accounts_metadata.accounts_address = accounts.address`,
				s.selectDistinctAccountMetadataHistories(date),
			).
			ColumnExpr("coalesce(accounts_metadata.metadata, '{}'::jsonb) as metadata")
	} else {
		ret = ret.ColumnExpr("accounts.metadata")
	}

	if s.ledger.HasFeature(features.FeatureMovesHistory, "ON") && needVolumes {
		selectAccountWithAggregatedVolumes, err := s.selectAccountWithAggregatedVolumes(date, true, "volumes")
		if err != nil {
			return nil, err
		}
		ret = ret.Join(
			`left join (?) volumes on volumes.accounts_address = accounts.address`,
			selectAccountWithAggregatedVolumes,
		).Column("volumes.*")
	}

	if s.ledger.HasFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") && expandEffectiveVolumes {
		selectAccountWithAggregatedVolumes, err := s.selectAccountWithAggregatedVolumes(date, false, "effective_volumes")
		if err != nil {
			return nil, err
		}
		ret = ret.Join(
			`left join (?) effective_volumes on effective_volumes.accounts_address = accounts.address`,
			selectAccountWithAggregatedVolumes,
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

				selectBalance, err := s.selectBalance(date)
				if err != nil {
					return "", nil, err
				}

				return s.db.NewSelect().
					TableExpr(
						"(?) balance",
						selectBalance.
							Where("asset = ? and accounts_address = accounts.address", asset),
					).
					ColumnExpr(fmt.Sprintf("balance %s ?", convertOperatorToSQL(operator)), value).
					String(), nil, nil

			case key == "balance":
				selectBalance, err := s.selectBalance(date)
				if err != nil {
					return "", nil, err
				}

				return s.db.NewSelect().
					TableExpr(
						"(?) balance",
						selectBalance.
							Where("accounts_address = accounts.address"),
					).
					ColumnExpr(fmt.Sprintf("balance %s ?", convertOperatorToSQL(operator)), value).
					String(), nil, nil

			case key == "metadata":
				if s.ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") && date != nil && !date.IsZero() {
					key = "accounts_metadata.metadata"
				}

				return key + " -> ? is not null", []any{value}, nil

			case metadataRegex.Match([]byte(key)):
				match := metadataRegex.FindAllStringSubmatch(key, 3)
				if s.ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") && date != nil && !date.IsZero() {
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
			return nil, fmt.Errorf("evaluating filters: %w", err)
		}
		if len(args) > 0 {
			ret = ret.Where(where, args...)
		} else {
			ret = ret.Where(where)
		}
	}

	return ret, nil
}

func (s *Store) ListAccounts(ctx context.Context, q ledgercontroller.ListAccountsQuery) (*Cursor[ledger.Account], error) {
	selectAccounts, err := s.selectAccounts(
		q.Options.Options.PIT,
		q.Options.Options.ExpandVolumes,
		q.Options.Options.ExpandEffectiveVolumes,
		q.Options.QueryBuilder,
	)
	if err != nil {
		return nil, err
	}
	return tracing.TraceWithMetric(
		ctx,
		"ListAccounts",
		s.tracer,
		s.listAccountsHistogram,
		func(ctx context.Context) (*Cursor[ledger.Account], error) {
			ret, err := UsingOffset[ledgercontroller.PaginatedQueryOptions[ledgercontroller.PITFilterWithVolumes], ledger.Account](
				ctx,
				selectAccounts,
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
		s.tracer,
		s.getAccountHistogram,
		func(ctx context.Context) (*ledger.Account, error) {
			ret := &ledger.Account{}
			selectAccounts, err := s.selectAccounts(q.PIT, q.ExpandVolumes, q.ExpandEffectiveVolumes, nil)
			if err != nil {
				return nil, err
			}
			if err := selectAccounts.
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
		s.tracer,
		s.countAccountsHistogram,
		func(ctx context.Context) (int, error) {
			selectAccounts, err := s.selectAccounts(
				q.Options.Options.PIT,
				q.Options.Options.ExpandVolumes,
				q.Options.Options.ExpandEffectiveVolumes,
				q.Options.QueryBuilder,
			)
			if err != nil {
				return 0, err
			}
			return s.db.NewSelect().
				TableExpr("(?) data", selectAccounts).
				Count(ctx)
		},
	)
}

func (s *Store) UpdateAccountsMetadata(ctx context.Context, m map[string]metadata.Metadata) error {
	_, err := tracing.TraceWithMetric(
		ctx,
		"UpdateAccountsMetadata",
		s.tracer,
		s.updateAccountsMetadataHistogram,
		tracing.NoResult(func(ctx context.Context) error {

			span := trace.SpanFromContext(ctx)
			span.SetAttributes(attribute.StringSlice("accounts", Keys(m)))

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

			ret, err := s.db.NewInsert().
				Model(&accounts).
				ModelTableExpr(s.GetPrefixedRelationName("accounts")).
				On("CONFLICT (ledger, address) DO UPDATE").
				Set("metadata = excluded.metadata || accounts.metadata").
				Where("not accounts.metadata @> excluded.metadata").
				Exec(ctx)

			if err != nil {
				return postgres.ResolveError(err)
			}

			rowsAffected, err := ret.RowsAffected()
			if err != nil {
				return err
			}

			span.SetAttributes(attribute.Int("upserted", int(rowsAffected)))

			return nil
		}),
	)
	return err
}

func (s *Store) DeleteAccountMetadata(ctx context.Context, account, key string) error {
	_, err := tracing.TraceWithMetric(
		ctx,
		"DeleteAccountMetadata",
		s.tracer,
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

func (s *Store) UpsertAccounts(ctx context.Context, accounts ...*ledger.Account) error {
	return tracing.SkipResult(tracing.TraceWithMetric(
		ctx,
		"UpsertAccounts",
		s.tracer,
		s.upsertAccountsHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			span := trace.SpanFromContext(ctx)
			span.SetAttributes(attribute.StringSlice("accounts", Map(accounts, (*ledger.Account).GetAddress)))

			ret, err := s.db.NewInsert().
				Model(&accounts).
				ModelTableExpr(s.GetPrefixedRelationName("accounts")).
				On("conflict (ledger, address) do update").
				Set("first_usage = case when excluded.first_usage < accounts.first_usage then excluded.first_usage else accounts.first_usage end").
				Set("metadata = accounts.metadata || excluded.metadata").
				Set("updated_at = excluded.updated_at").
				Value("ledger", "?", s.ledger.Name).
				Returning("*").
				Where("(excluded.first_usage < accounts.first_usage) or not accounts.metadata @> excluded.metadata").
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("upserting accounts: %w", postgres.ResolveError(err))
			}

			rowsAffected, err := ret.RowsAffected()
			if err != nil {
				return err
			}
			span.SetAttributes(attribute.Int("upserted", int(rowsAffected)))

			return nil
		}),
	))
}
