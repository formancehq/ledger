package ledger

import (
	"context"
	"fmt"
	. "github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/ledger/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"regexp"

	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
)

var (
	balanceRegex = regexp.MustCompile(`balance\[(.*)]`)
)

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
				Set("updated_at = excluded.updated_at").
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
