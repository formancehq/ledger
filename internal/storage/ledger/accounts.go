package ledger

import (
	"context"
	"fmt"
	. "github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/ledger/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"regexp"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
)

var (
	balanceRegex = regexp.MustCompile(`balance\[(.*)]`)
)

func (store *Store) UpdateAccountsMetadata(ctx context.Context, m map[string]metadata.Metadata) error {
	_, err := tracing.TraceWithMetric(
		ctx,
		"UpdateAccountsMetadata",
		store.tracer,
		store.updateAccountsMetadataHistogram,
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
					Ledger: store.ledger.Name,
					Account: ledger.Account{
						Address:  account,
						Metadata: accountMetadata,
					},
				})
			}

			ret, err := store.db.NewInsert().
				Model(&accounts).
				ModelTableExpr(store.GetPrefixedRelationName("accounts")).
				On("conflict (ledger, address) do update").
				Set("metadata = accounts.metadata || excluded.metadata").
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

func (store *Store) DeleteAccountMetadata(ctx context.Context, account, key string) error {
	_, err := tracing.TraceWithMetric(
		ctx,
		"DeleteAccountMetadata",
		store.tracer,
		store.deleteAccountMetadataHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			_, err := store.db.NewUpdate().
				ModelTableExpr(store.GetPrefixedRelationName("accounts")).
				Set("metadata = metadata - ?", key).
				Where("address = ?", account).
				Where("ledger = ?", store.ledger.Name).
				Exec(ctx)
			return postgres.ResolveError(err)
		}),
	)
	return err
}

func (store *Store) UpsertAccounts(ctx context.Context, accounts ...*ledger.Account) error {
	return tracing.SkipResult(tracing.TraceWithMetric(
		ctx,
		"UpsertAccounts",
		store.tracer,
		store.upsertAccountsHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			span := trace.SpanFromContext(ctx)
			span.SetAttributes(attribute.StringSlice("accounts", Map(accounts, (*ledger.Account).GetAddress)))

			ret, err := store.db.NewInsert().
				Model(&accounts).
				ModelTableExpr(store.GetPrefixedRelationName("accounts")).
				On("conflict (ledger, address) do update").
				Set("first_usage = case when excluded.first_usage < accounts.first_usage then excluded.first_usage else accounts.first_usage end").
				Set("metadata = accounts.metadata || excluded.metadata").
				Set("updated_at = excluded.updated_at").
				Value("ledger", "?", store.ledger.Name).
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
