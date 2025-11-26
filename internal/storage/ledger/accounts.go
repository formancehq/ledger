package ledger

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	. "github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/tracing"
)

var (
	balanceRegex = regexp.MustCompile(`balance\[(.*)]`)
)

func (store *Store) UpdateAccountsMetadata(ctx context.Context, m map[string]metadata.Metadata, at time.Time) error {
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
				Ledger         string   `bun:"ledger,type:varchar"`
				AddressArray   []string `bun:"address_array,type:jsonb"`
			}

			accounts := make([]AccountWithLedger, 0)
			for account, accountMetadata := range m {
				accounts = append(accounts, AccountWithLedger{
					Ledger: store.ledger.Name,
					Account: ledger.Account{
						Address:       account,
						Metadata:      accountMetadata,
						FirstUsage:    at,
						InsertionDate: at,
						UpdatedAt:     at,
					},
					AddressArray: strings.Split(account, ":"),
				})
			}

			ret, err := store.db.NewInsert().
				Model(&accounts).
				ModelTableExpr(store.GetPrefixedRelationName("accounts")).
				On("conflict (ledger, address) do update").
				Set("metadata = accounts.metadata || excluded.metadata").
				Set("updated_at = excluded.updated_at").
				Set("first_usage = case when excluded.first_usage < accounts.first_usage then excluded.first_usage else accounts.first_usage end").
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

func (store *Store) UpsertAccounts(ctx context.Context, schema *ledger.Schema, accounts ...*ledger.Account) error {
	return tracing.SkipResult(tracing.TraceWithMetric(
		ctx,
		"UpsertAccounts",
		store.tracer,
		store.upsertAccountsHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			span := trace.SpanFromContext(ctx)
			span.SetAttributes(attribute.StringSlice("accounts", Map(accounts, (*ledger.Account).GetAddress)))

			type account struct {
				*ledger.Account `bun:",extend"`
				AddressArray    []string          `bun:"address_array,type:jsonb"`
				DefaultMetadata metadata.Metadata `bun:"default_metadata,type:jsonb"`
			}

			rows := Map(accounts, func(from *ledger.Account) account {
				// Set default metadata from schema
				defaultMetadata := metadata.Metadata{}
				if schema != nil {
					accountSchema, _ := schema.Chart.FindAccountSchema(from.Address)
					if accountSchema != nil {
						for key, value := range accountSchema.Metadata {
							if value.Default != nil {
								defaultMetadata[key] = *value.Default
							}
						}
					}
				}

				if from.Metadata == nil {
					from.Metadata = metadata.Metadata{}
				}

				return account{
					Account:         from,
					AddressArray:    strings.Split(from.Address, ":"),
					DefaultMetadata: defaultMetadata,
				}
			})

			err := store.db.NewRaw(`
				WITH
					data_batch (address, metadata, first_usage, insertion_date, updated_at, address_array, default_metadata)
						AS (?0),
					existing_accounts AS (
						SELECT a.address
						FROM ?1.accounts a
						JOIN data_batch d
							ON a.address = d.address
							AND a.ledger = ?2
					),
					updated_rows AS (
						-- If present: update
						UPDATE ?1.accounts a
						SET
							metadata = a.metadata || d.metadata,
							first_usage = LEAST(d.first_usage, a.first_usage),
							updated_at = COALESCE(d.updated_at, ?1.transaction_date())
						FROM data_batch d
						WHERE a.address = d.address and ledger = ?2 and (d.first_usage < a.first_usage or not a.metadata @> d.metadata)
						RETURNING a.address, a.metadata, a.first_usage, a.updated_at, a.insertion_date
					),
					inserted_rows AS (
						-- If not present: insert
						INSERT INTO ?1.accounts (address, metadata, first_usage, updated_at, insertion_date, ledger, address_array)
						SELECT
							d.address,
							d.default_metadata || d.metadata,
							COALESCE(d.first_usage, ?1.transaction_date()),
							COALESCE(d.updated_at, ?1.transaction_date()),
							COALESCE(d.insertion_date, ?1.transaction_date()),
							?2,
							d.address_array
						FROM data_batch d
						WHERE d.address NOT IN (SELECT address FROM existing_accounts)
						RETURNING address, metadata, first_usage, updated_at, insertion_date
					)
				SELECT * FROM updated_rows
				UNION ALL SELECT * FROM inserted_rows`,
				store.db.NewValues(&rows),
				bun.Ident(store.ledger.Bucket),
				store.ledger.Name,
			).Scan(ctx, &rows)

			if err != nil {
				return fmt.Errorf("upserting accounts: %w", postgres.ResolveError(err))
			}

			// rowsAffected, err := returned.RowsAffected()
			// if err != nil {
			// 	return err
			// }
			// span.SetAttributes(attribute.Int("upserted", int(rowsAffected)))

			return nil
		}),
	))
}
