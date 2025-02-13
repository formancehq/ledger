package ledger

import (
	"context"
	. "github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/ledger/internal/tracing"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"regexp"
	"strings"
	"time"

	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/platform/postgres"
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

			type Account struct {
				bun.BaseModel `bun:"table:accounts"`

				Address          string                 `json:"address" bun:"address"`
				Metadata         metadata.Metadata      `json:"metadata" bun:"metadata,type:jsonb,default:'{}'"`
				FirstUsage       time.Time              `json:"-" bun:"first_usage,type:timestamp,nullzero"`
				InsertionDate    time.Time              `json:"-" bun:"insertion_date,type:timestamp,nullzero"`
				UpdatedAt        time.Time              `json:"-" bun:"updated_at,type:timestamp,nullzero"`
				Volumes          ledger.VolumesByAssets `json:"volumes,omitempty" bun:"volumes,scanonly"`
				EffectiveVolumes ledger.VolumesByAssets `json:"effectiveVolumes,omitempty" bun:"effective_volumes,scanonly"`
			}

			accounts := Map(accounts, func(from *ledger.Account) Account {
				return Account{
					Address:          from.Address,
					Metadata:         from.Metadata,
					FirstUsage:       from.FirstUsage.Time,
					InsertionDate:    from.InsertionDate.Time,
					UpdatedAt:        from.UpdatedAt.Time,
					Volumes:          from.Volumes,
					EffectiveVolumes: from.EffectiveVolumes,
				}
			})

			conditionsForUpdates := make([]string, 0)
			argsForUpdate := make([]any, 0)
			for _, account := range accounts {
				conditionsForUpdates = append(conditionsForUpdates, "accounts.ledger = ? and accounts.address = ? and (accounts.first_usage > ? or not accounts.metadata @> ?)")
				argsForUpdate = append(argsForUpdate, store.ledger.Name, account.Address, account.FirstUsage, account.Metadata)
			}

			query := store.db.NewInsert().
				With("accounts_to_upsert", store.db.NewValues(&accounts)).
				With("existing_accounts_to_update", store.db.NewSelect().
					Model(&ledger.Account{}).
					ModelTableExpr("accounts_to_upsert").
					ColumnExpr("accounts_to_upsert.*").
					Join("join "+store.GetPrefixedRelationName("accounts")+" on accounts.address = accounts_to_upsert.address ").
					Where("("+strings.Join(conditionsForUpdates, ") OR (")+")", argsForUpdate...),
				).
				With("not_existing_accounts", store.db.NewSelect().
					Model(&ledger.Account{}).
					ModelTableExpr("accounts_to_upsert").
					ColumnExpr("accounts_to_upsert.*").
					Join("left join "+store.GetPrefixedRelationName("accounts")+" on accounts.address = accounts_to_upsert.address and ledger = ?", store.ledger.Name).
					Where("accounts.address is null"),
				).
				With("_accounts", store.db.NewSelect().
					Table("existing_accounts_to_update").
					Column("*").
					ColumnExpr("? as ledger", store.ledger.Name).
					UnionAll(
						store.db.NewSelect().
							Table("not_existing_accounts").
							Column("*").
							ColumnExpr("? as ledger", store.ledger.Name),
					),
				).
				Table("_accounts").
				Model(&accounts).
				ModelTableExpr(store.GetPrefixedRelationName("accounts")).
				Column("ledger", "address", "metadata", "first_usage", "insertion_date", "updated_at").
				On("conflict (ledger, address) do update").
				Set("first_usage = case when excluded.first_usage < accounts.first_usage then excluded.first_usage else accounts.first_usage end").
				Set("metadata = accounts.metadata || excluded.metadata").
				Set("updated_at = excluded.updated_at")

			_, err := query.Exec(ctx)
			if err != nil {
				return postgres.ResolveError(err)
			}

			return nil
		}),
	))
}
