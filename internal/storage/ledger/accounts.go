package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"regexp"
	"strings"

	. "github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/platform/postgres"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
)

type AggregatedAccountVolume struct {
	ledger.Volumes
	Asset string `bun:"asset"`
}

type AggregatedAccountVolumes []AggregatedAccountVolume

func (volumes AggregatedAccountVolumes) toCore() ledger.VolumesByAssets {
	if volumes == nil {
		return ledger.VolumesByAssets{}
	}

	ret := ledger.VolumesByAssets{}
	for _, volume := range volumes {
		if volumesForAsset, ok := ret[volume.Asset]; !ok {
			ret[volume.Asset] = ledger.Volumes{
				Input:  new(big.Int).Set(volume.Input),
				Output: new(big.Int).Set(volume.Output),
			}
		} else {
			volumesForAsset.Input = volumesForAsset.Input.Add(volumesForAsset.Input, volume.Input)
			volumesForAsset.Output = volumesForAsset.Output.Add(volumesForAsset.Output, volume.Output)

			ret[volume.Asset] = volumesForAsset
		}
	}

	return ret
}

type Account struct {
	bun.BaseModel `bun:"table:accounts"`

	Ledger        string            `bun:"ledger"`
	Address       string            `bun:"address"`
	AddressArray  []string          `bun:"address_array"`
	Metadata      metadata.Metadata `bun:"metadata,type:jsonb"`
	InsertionDate time.Time         `bun:"insertion_date"`
	UpdatedAt     time.Time         `bun:"updated_at"`
	FirstUsage    time.Time         `bun:"first_usage"`

	PostCommitVolumes          AggregatedAccountVolumes `bun:"pcv,scanonly"`
	PostCommitEffectiveVolumes AggregatedAccountVolumes `bun:"pcev,scanonly"`
	Seq                        int                      `bun:"seq,scanonly"`
}

func (account Account) toCore() ledger.ExpandedAccount {
	return ledger.ExpandedAccount{
		Account: ledger.Account{
			Address:       account.Address,
			Metadata:      account.Metadata,
			FirstUsage:    account.FirstUsage,
			InsertionDate: account.InsertionDate,
			UpdatedAt:     account.UpdatedAt,
		},
		Volumes:          account.PostCommitVolumes.toCore(),
		EffectiveVolumes: account.PostCommitEffectiveVolumes.toCore(),
	}
}

var (
	balanceRegex = regexp.MustCompile("balance\\[(.*)\\]")
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
	return s.SortMovesBySeq(date).
		ColumnExpr("(post_commit_volumes->>'input')::numeric - (post_commit_volumes->>'output')::numeric as balance").
		Limit(1)
}

func (s *Store) selectDistinctAccountMetadataHistories(date *time.Time) *bun.SelectQuery {
	ret := s.db.NewSelect().
		DistinctOn("accounts_seq").
		ModelTableExpr(s.GetPrefixedRelationName("accounts_metadata")).
		Where("ledger = ?", s.ledger.Name).
		Column("accounts_seq", "metadata").
		Order("accounts_seq", "revision desc")

	if date != nil && !date.IsZero() {
		ret = ret.Where("date <= ?", date)
	}

	return ret
}

func (s *Store) selectAccounts(date *time.Time, expandVolumes, expandEffectiveVolumes bool, qb query.Builder) *bun.SelectQuery {

	ret := s.db.NewSelect()

	needPCV := expandVolumes
	if qb != nil {
		// analyze filters to check for errors and find potentially additional table to load
		if err := qb.Walk(func(operator, key string, value any) error {
			switch {
			// balances requires pvc, force load in this case
			case balanceRegex.Match([]byte(key)):
				needPCV = true
			case key == "address":
				return s.validateAddressFilter(operator, value)
			case key == "metadata":
				if operator != "$exists" {
					return ledgercontroller.NewErrInvalidQuery("'metadata' key filter can only be used with $exists")
				}
			case metadataRegex.Match([]byte(key)):
				if operator != "$match" {
					return ledgercontroller.NewErrInvalidQuery("'metadata' key filter can only be used with $match")
				}
			default:
				return ledgercontroller.NewErrInvalidQuery("unknown key '%s' when building query", key)
			}

			return nil
		}); err != nil {
			return ret.Err(errors.Wrap(err, "failed to check filters"))
		}
	}

	if needPCV && !s.ledger.HasFeature(ledger.FeatureMovesHistoryPostCommitVolumes, "SYNC") {
		return ret.Err(ledgercontroller.NewErrMissingFeature(ledger.FeatureMovesHistoryPostCommitVolumes))
	}

	// build the query
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
				`left join (?) accounts_metadata on accounts_metadata.accounts_seq = accounts.seq`,
				s.selectDistinctAccountMetadataHistories(date),
			).
			ColumnExpr("coalesce(accounts_metadata.metadata, '{}'::jsonb) as metadata")
	} else {
		ret = ret.ColumnExpr("accounts.metadata")
	}

	// todo: should join on histories only if pit is specified
	// otherwise the accounts_volumes table is enough
	if s.ledger.HasFeature(ledger.FeatureMovesHistoryPostCommitVolumes, "SYNC") && needPCV {
		ret = ret.
			Join(
				`left join (?) pcv on pcv.accounts_seq = accounts.seq`,
				s.db.NewSelect().
					TableExpr("(?) v", s.SelectDistinctMovesBySeq(date)).
					Column("accounts_seq").
					ColumnExpr(`to_json(array_agg(json_build_object('asset', v.asset, 'input', (v.post_commit_volumes->>'input')::numeric, 'output', (v.post_commit_volumes->>'output')::numeric))) as pcv`).
					Group("accounts_seq"),
			).
			ColumnExpr("pcv.*")
	}

	if s.ledger.HasFeature(ledger.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") && expandEffectiveVolumes {
		ret = ret.
			Join(
				`left join (?) pcev on pcev.accounts_seq = accounts.seq`,
				s.db.NewSelect().
					TableExpr("(?) v", s.SelectDistinctMovesByEffectiveDate(date)).
					Column("accounts_seq").
					ColumnExpr(`to_json(array_agg(json_build_object('asset', v.asset, 'input', (v.post_commit_effective_volumes->>'input')::numeric, 'output', (v.post_commit_effective_volumes->>'output')::numeric))) as pcev`).
					Group("accounts_seq"),
			).
			ColumnExpr("pcev.*")
	}

	if qb != nil {
		// convert filters to where clause
		where, args, err := qb.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
			switch {
			case key == "address":
				return filterAccountAddress(value.(string), "accounts.address"), nil, nil

			case balanceRegex.Match([]byte(key)):
				match := balanceRegex.FindAllStringSubmatch(key, 2)
				asset := match[0][1]

				// todo: use moves only if feature is enabled
				return s.db.NewSelect().
					TableExpr(
						"(?) balance",
						s.selectBalance(date).
							Where("asset = ? and moves.accounts_seq = accounts.seq", asset),
					).
					ColumnExpr(fmt.Sprintf("balance %s ?", convertOperatorToSQL(operator)), value).
					String(), nil, nil

			case key == "balance":
				return s.db.NewSelect().
					TableExpr(
						"(?) balance",
						s.selectBalance(date).
							Where("moves.accounts_seq = accounts.seq"),
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
			return ret.Err(errors.Wrap(err, "evaluating filters"))
		}
		if len(args) > 0 {
			ret = ret.Where(where, args...)
		} else {
			ret = ret.Where(where)
		}
	}

	return ret
}

func (s *Store) ListAccounts(ctx context.Context, q ledgercontroller.ListAccountsQuery) (*Cursor[ledger.ExpandedAccount], error) {
	return tracing.TraceWithLatency(ctx, "ListAccounts", func(ctx context.Context) (*Cursor[ledger.ExpandedAccount], error) {
		ret, err := UsingOffset[ledgercontroller.PaginatedQueryOptions[ledgercontroller.PITFilterWithVolumes], Account](
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

		return MapCursor(ret, Account.toCore), nil
	})
}

func (s *Store) GetAccount(ctx context.Context, q ledgercontroller.GetAccountQuery) (*ledger.ExpandedAccount, error) {
	return tracing.TraceWithLatency(ctx, "GetAccount", func(ctx context.Context) (*ledger.ExpandedAccount, error) {
		ret := &Account{}
		if err := s.selectAccounts(q.PIT, q.ExpandVolumes, q.ExpandEffectiveVolumes, nil).
			Model(ret).
			Where("accounts.address = ?", q.Addr).
			Limit(1).
			Scan(ctx); err != nil {
			return nil, postgres.ResolveError(err)
		}

		return pointer.For(ret.toCore()), nil
	})
}

func (s *Store) CountAccounts(ctx context.Context, q ledgercontroller.ListAccountsQuery) (int, error) {
	return tracing.TraceWithLatency(ctx, "CountAccounts", func(ctx context.Context) (int, error) {
		return s.db.NewSelect().
			TableExpr("(?) data", s.selectAccounts(
				q.Options.Options.PIT,
				q.Options.Options.ExpandVolumes,
				q.Options.Options.ExpandEffectiveVolumes,
				q.Options.QueryBuilder,
			)).
			Count(ctx)
	})
}

func (s *Store) UpdateAccountsMetadata(ctx context.Context, m map[string]metadata.Metadata) error {
	_, err := tracing.TraceWithLatency(ctx, "UpdateAccountsMetadata", tracing.NoResult(func(ctx context.Context) error {
		now := time.Now()
		accounts := make([]Account, 0)
		for account, accountMetadata := range m {
			accounts = append(accounts, Account{
				Ledger:        s.ledger.Name,
				Address:       account,
				AddressArray:  strings.Split(account, ":"),
				Metadata:      accountMetadata,
				InsertionDate: now,
				UpdatedAt:     now,
				FirstUsage:    now,
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
	}))
	return err
}

func (s *Store) DeleteAccountMetadata(ctx context.Context, account, key string) error {
	_, err := tracing.TraceWithLatency(ctx, "DeleteAccountMetadata", tracing.NoResult(func(ctx context.Context) error {
		_, err := s.db.NewUpdate().
			ModelTableExpr(s.GetPrefixedRelationName("accounts")).
			Set("metadata = metadata - ?", key).
			Where("address = ?", account).
			Where("ledger = ?", s.ledger.Name).
			Exec(ctx)
		return postgres.ResolveError(err)
	}))
	return err
}

func (s *Store) UpsertAccount(ctx context.Context, account *ledger.Account) error {
	mappedAccount := &Account{
		Ledger:        s.ledger.Name,
		AddressArray:  strings.Split(account.Address, ":"),
		Address:       account.Address,
		FirstUsage:    account.FirstUsage,
		InsertionDate: account.InsertionDate,
		UpdatedAt:     account.UpdatedAt,
		Metadata:      account.Metadata,
	}
	_, err := s.upsertAccount(ctx, mappedAccount)
	if err != nil {
		return err
	}

	account.FirstUsage = mappedAccount.FirstUsage
	account.InsertionDate = mappedAccount.InsertionDate
	account.UpdatedAt = mappedAccount.UpdatedAt
	account.Metadata = mappedAccount.Metadata

	return nil
}

func (s *Store) upsertAccount(ctx context.Context, account *Account) (bool, error) {
	var rollbacked = errors.New("rollbacked")
	upserted, err := tracing.TraceWithLatency(ctx, "UpsertAccount", func(ctx context.Context) (bool, error) {
		type upsertedEntity struct {
			Account  `bun:",extend"`
			Upserted bool `bun:"upserted"`
		}
		upserted := &upsertedEntity{}

		err := s.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
			if err := tx.NewSelect().
				With(
					"ins",
					tx.NewInsert().
						Model(account).
						ModelTableExpr(s.GetPrefixedRelationName("accounts")).
						On("conflict (ledger, address) do update").
						Set("first_usage = case when ? < excluded.first_usage then ? else excluded.first_usage end", account.FirstUsage, account.FirstUsage).
						Set("metadata = accounts.metadata || excluded.metadata").
						Set("updated_at = ?", account.UpdatedAt).
						Returning("*").
						Where("(? < accounts.first_usage) or not accounts.metadata @> excluded.metadata", account.FirstUsage),
				).
				ModelTableExpr(
					"(?) account",
					tx.NewSelect().
						ModelTableExpr("ins").
						ColumnExpr("ins.*, true as upserted").
						UnionAll(
							tx.NewSelect().
								ModelTableExpr(s.GetPrefixedRelationName("accounts")).
								ColumnExpr("*, false as upserted").
								Where("address = ? and ledger = ?", account.Address, s.ledger.Name).
								Limit(1),
						),
				).
				Model(upserted).
				ColumnExpr("*").
				Limit(1).
				Scan(ctx); err != nil {
				return err
			}

			account.Seq = upserted.Seq
			account.FirstUsage = upserted.FirstUsage
			account.InsertionDate = upserted.InsertionDate
			account.UpdatedAt = upserted.UpdatedAt
			account.Metadata = upserted.Metadata

			if !upserted.Upserted {
				// by roll-backing the transaction, we release the lock, allowing a concurrent transaction
				// to use the table
				// but at this point, we have fill the Account model with the account sequence in the bucket
				if err := tx.Rollback(); err != nil {
					return err
				}
				return rollbacked
			}

			return nil
		})
		if err != nil && !errors.Is(err, rollbacked) {
			return false, errors.Wrap(err, "upserting account")
		}

		return upserted.Upserted, nil
	}, func(ctx context.Context, upserted bool) {
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String("address", account.Address),
			attribute.Bool("upserted", upserted),
		)
	})
	if err != nil && !errors.Is(err, rollbacked) {
		return false, errors.Wrap(err, "failed to upsert account")
	} else if upserted {
		logging.FromContext(ctx).Debugf("account upserted")
	} else {
		logging.FromContext(ctx).Debugf("account not modified")
	}

	return upserted, nil
}
