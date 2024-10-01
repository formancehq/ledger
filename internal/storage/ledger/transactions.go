package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"slices"
	"strings"

	"github.com/formancehq/ledger/internal/tracing"

	"errors"
	. "github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/platform/postgres"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/pointer"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/bun/bunpaginate"

	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
)

var (
	metadataRegex = regexp.MustCompile(`metadata\[(.+)]`)
)

func (s *Store) selectDistinctTransactionMetadataHistories(date *time.Time) *bun.SelectQuery {
	ret := s.db.NewSelect().
		DistinctOn("transactions_id").
		ModelTableExpr(s.GetPrefixedRelationName("transactions_metadata")).
		Where("ledger = ?", s.ledger.Name).
		Column("transactions_id", "metadata").
		Order("transactions_id", "revision desc")

	if date != nil && !date.IsZero() {
		ret = ret.Where("date <= ?", date)
	}

	return ret
}

func (s *Store) selectTransactions(date *time.Time, expandVolumes, expandEffectiveVolumes bool, q query.Builder) *bun.SelectQuery {

	ret := s.db.NewSelect()
	if expandEffectiveVolumes && !s.ledger.HasFeature(ledger.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
		return ret.Err(ledgercontroller.NewErrMissingFeature(ledger.FeatureMovesHistoryPostCommitEffectiveVolumes))
	}

	if q != nil {
		if err := q.Walk(func(operator, key string, value any) error {
			switch {
			case key == "reverted":
				if operator != "$match" {
					return ledgercontroller.NewErrInvalidQuery("'reverted' column can only be used with $match")
				}
				switch value.(type) {
				case bool:
					return nil
				default:
					return ledgercontroller.NewErrInvalidQuery("'reverted' can only be used with bool value")
				}
			case key == "account":
				return s.validateAddressFilter(operator, value)
			case key == "source":
				return s.validateAddressFilter(operator, value)
			case key == "destination":
				return s.validateAddressFilter(operator, value)
			case key == "timestamp":
			case metadataRegex.Match([]byte(key)):
				if operator != "$match" {
					return ledgercontroller.NewErrInvalidQuery("'metadata[xxx]' column can only be used with $match")
				}
			case key == "metadata":
				if operator != "$exists" {
					return ledgercontroller.NewErrInvalidQuery("'metadata' key filter can only be used with $exists")
				}
			default:
				return ledgercontroller.NewErrInvalidQuery("unknown key '%s' when building query", key)
			}

			return nil
		}); err != nil {
			return ret.Err(err)
		}
	}

	ret = ret.
		ModelTableExpr(s.GetPrefixedRelationName("transactions")).
		Column(
			"ledger",
			"id",
			"timestamp",
			"reference",
			"inserted_at",
			"updated_at",
			"postings",
			"sources",
			"destinations",
			"sources_arrays",
			"destinations_arrays",
			"reverted_at",
			"post_commit_volumes",
		).
		Where("ledger = ?", s.ledger.Name)

	if date != nil && !date.IsZero() {
		ret = ret.Where("timestamp <= ?", date)
	}

	if s.ledger.HasFeature(ledger.FeatureAccountMetadataHistory, "SYNC") && date != nil && !date.IsZero() {
		ret = ret.
			Join(
				`left join (?) transactions_metadata on transactions_metadata.transactions_id = transactions.id`,
				s.selectDistinctTransactionMetadataHistories(date),
			).
			ColumnExpr("coalesce(transactions_metadata.metadata, '{}'::jsonb) as metadata")
	} else {
		ret = ret.ColumnExpr("metadata")
	}

	if s.ledger.HasFeature(ledger.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") && expandEffectiveVolumes {
		ret = ret.
			Join(
				`join (?) pcev on pcev.transactions_id = transactions.id`,
				s.db.NewSelect().
					Column("transactions_id").
					ColumnExpr("aggregate_objects(pcev::jsonb) as post_commit_effective_volumes").
					TableExpr(
						"(?) data",
						s.db.NewSelect().
							DistinctOn("transactions_id, accounts_address, asset").
							ModelTableExpr(s.GetPrefixedRelationName("moves")).
							Column("transactions_id").
							ColumnExpr(`
								json_build_object(
									moves.accounts_address,
									json_build_object(
										moves.asset,
										first_value(moves.post_commit_effective_volumes) over (partition by (transactions_id, accounts_address, asset) order by seq desc)
									)
								) as pcev
							`),
					).
					Group("transactions_id"),
			).
			ColumnExpr("pcev.*")
	}

	// Create a parent query which set reverted_at to null if the date passed as argument is before
	ret = s.db.NewSelect().
		ModelTableExpr("(?) transactions", ret).
		Column(
			"ledger",
			"id",
			"timestamp",
			"reference",
			"inserted_at",
			"updated_at",
			"postings",
			"sources",
			"destinations",
			"sources_arrays",
			"destinations_arrays",
			"metadata",
		)
	if expandVolumes {
		ret = ret.Column("post_commit_volumes")
	}
	if expandEffectiveVolumes {
		ret = ret.Column("post_commit_effective_volumes")
	}
	if date != nil && !date.IsZero() {
		ret = ret.ColumnExpr("(case when transactions.reverted_at <= ? then transactions.reverted_at else null end) as reverted_at", date)
	} else {
		ret = ret.Column("reverted_at")
	}

	if q != nil {
		where, args, err := q.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
			switch {
			case key == "reference" || key == "timestamp":
				return fmt.Sprintf("%s %s ?", key, query.DefaultComparisonOperatorsMapping[operator]), []any{value}, nil
			case key == "reverted":
				ret := "reverted_at is"
				if value.(bool) {
					ret += " not"
				}
				return ret + " null", nil, nil
			case key == "account":
				return filterAccountAddressOnTransactions(value.(string), true, true), nil, nil
			case key == "source":
				return filterAccountAddressOnTransactions(value.(string), true, false), nil, nil
			case key == "destination":
				return filterAccountAddressOnTransactions(value.(string), false, true), nil, nil
			case metadataRegex.Match([]byte(key)):
				match := metadataRegex.FindAllStringSubmatch(key, 3)

				return "metadata @> ?", []any{map[string]any{
					match[0][1]: value,
				}}, nil

			case key == "metadata":
				return "metadata -> ? is not null", []any{value}, nil
			case key == "timestamp":
				return fmt.Sprintf("timestamp %s ?", convertOperatorToSQL(operator)), []any{value}, nil
			default:
				return "", nil, ledgercontroller.NewErrInvalidQuery("unknown key '%s' when building query", key)
			}
		}))
		if err != nil {
			return ret.Err(err)
		}

		if len(args) > 0 {
			ret = ret.Where(where, args...)
		} else {
			ret = ret.Where(where)
		}
	}

	return ret
}

func (s *Store) CommitTransaction(ctx context.Context, tx *ledger.Transaction) error {

	sqlQueries := Map(tx.InvolvedAccounts(), func(from string) string {
		return fmt.Sprintf("select pg_advisory_xact_lock(hashtext('%s'))", fmt.Sprintf("%s_%s", s.ledger.Name, from))
	})

	_, err := s.db.NewRaw(strings.Join(sqlQueries, ";")).Exec(ctx)
	if err != nil {
		return postgres.ResolveError(err)
	}

	if tx.InsertedAt.IsZero() {
		tx.InsertedAt = time.Now()
	}

	for _, address := range tx.InvolvedAccounts() {
		_, err := s.UpsertAccount(ctx, &ledger.Account{
			Address:       address,
			FirstUsage:    tx.Timestamp,
			InsertionDate: tx.InsertedAt,
			UpdatedAt:     tx.InsertedAt,
			Metadata:      make(metadata.Metadata),
		})
		if err != nil {
			return fmt.Errorf("upserting account: %w", err)
		}
	}

	postCommitVolumes, err := s.UpdateVolumes(ctx, tx.VolumeUpdates()...)
	if err != nil {
		return fmt.Errorf("failed to update balances: %w", err)
	}
	tx.PostCommitVolumes = postCommitVolumes.Copy()

	err = s.InsertTransaction(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to insert transaction: %w", err)
	}

	if s.ledger.HasFeature(ledger.FeatureMovesHistory, "ON") {
		moves := ledger.Moves{}
		postings := tx.Postings
		slices.Reverse(postings)

		for _, posting := range postings {
			moves = append(moves, &ledger.Move{
				Ledger:            s.ledger.Name,
				Account:           posting.Destination,
				Amount:            (*bunpaginate.BigInt)(posting.Amount),
				Asset:             posting.Asset,
				InsertionDate:     tx.InsertedAt,
				EffectiveDate:     tx.Timestamp,
				PostCommitVolumes: pointer.For(postCommitVolumes[posting.Destination][posting.Asset].Copy()),
				TransactionID:     tx.ID,
			})
			postCommitVolumes.AddInput(posting.Destination, posting.Asset, new(big.Int).Neg(posting.Amount))

			moves = append(moves, &ledger.Move{
				Ledger:            s.ledger.Name,
				IsSource:          true,
				Account:           posting.Source,
				Amount:            (*bunpaginate.BigInt)(posting.Amount),
				Asset:             posting.Asset,
				InsertionDate:     tx.InsertedAt,
				EffectiveDate:     tx.Timestamp,
				PostCommitVolumes: pointer.For(postCommitVolumes[posting.Source][posting.Asset].Copy()),
				TransactionID:     tx.ID,
			})
			postCommitVolumes.AddOutput(posting.Source, posting.Asset, new(big.Int).Neg(posting.Amount))
		}

		slices.Reverse(moves)

		if err := s.InsertMoves(ctx, moves...); err != nil {
			return fmt.Errorf("failed to insert moves: %w", err)
		}

		if s.ledger.HasFeature(ledger.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
			tx.PostCommitEffectiveVolumes = moves.ComputePostCommitEffectiveVolumes()
		}
	}

	return nil
}

func (s *Store) ListTransactions(ctx context.Context, q ledgercontroller.ListTransactionsQuery) (*bunpaginate.Cursor[ledger.Transaction], error) {
	return tracing.Trace(ctx, "ListTransactions", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Transaction], error) {
		cursor, err := bunpaginate.UsingColumn[ledgercontroller.PaginatedQueryOptions[ledgercontroller.PITFilterWithVolumes], ledger.Transaction](
			ctx,
			s.selectTransactions(
				q.Options.Options.PIT,
				q.Options.Options.ExpandVolumes,
				q.Options.Options.ExpandEffectiveVolumes,
				q.Options.QueryBuilder,
			),
			bunpaginate.ColumnPaginatedQuery[ledgercontroller.PaginatedQueryOptions[ledgercontroller.PITFilterWithVolumes]](q),
		)
		if err != nil {
			return nil, err
		}

		return cursor, nil
	})
}

func (s *Store) CountTransactions(ctx context.Context, q ledgercontroller.ListTransactionsQuery) (int, error) {
	return tracing.TraceWithLatency(ctx, "CountTransactions", func(ctx context.Context) (int, error) {
		return s.db.NewSelect().
			TableExpr("(?) data", s.selectTransactions(
				q.Options.Options.PIT,
				q.Options.Options.ExpandVolumes,
				q.Options.Options.ExpandEffectiveVolumes,
				q.Options.QueryBuilder,
			)).
			Count(ctx)
	})
}

func (s *Store) GetTransaction(ctx context.Context, filter ledgercontroller.GetTransactionQuery) (*ledger.Transaction, error) {
	return tracing.TraceWithLatency(ctx, "GetTransaction", func(ctx context.Context) (*ledger.Transaction, error) {

		ret := &ledger.Transaction{}
		if err := s.selectTransactions(
			filter.PIT,
			filter.ExpandVolumes,
			filter.ExpandEffectiveVolumes,
			nil,
		).
			Where("transactions.id = ?", filter.ID).
			Limit(1).
			Model(ret).
			Scan(ctx); err != nil {
			return nil, postgres.ResolveError(err)
		}

		return ret, nil
	})
}

func (s *Store) InsertTransaction(ctx context.Context, tx *ledger.Transaction) error {
	_, err := tracing.TraceWithLatency(ctx, "InsertTransaction", func(ctx context.Context) (*ledger.Transaction, error) {
		_, err := s.db.NewInsert().
			Model(tx).
			ModelTableExpr(s.GetPrefixedRelationName("transactions")).
			Value("id", "nextval(?)", s.GetPrefixedRelationName(fmt.Sprintf(`"transaction_id_%d"`, s.ledger.ID))).
			Value("ledger", "?", s.ledger.Name).
			Returning("id, timestamp, inserted_at").
			Exec(ctx)
		if err != nil {
			err = postgres.ResolveError(err)
			switch {
			case errors.Is(err, postgres.ErrConstraintsFailed{}):
				if err.(postgres.ErrConstraintsFailed).GetConstraint() == "transactions_reference" {
					return nil, ledgercontroller.NewErrTransactionReferenceConflict(tx.Reference)
				}
			default:
				return nil, err
			}
		}

		return tx, nil
	}, func(ctx context.Context, tx *ledger.Transaction) {
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.Int("id", tx.ID),
			attribute.String("timestamp", tx.Timestamp.Format(time.RFC3339Nano)),
		)
	})

	return err
}

// updateTxWithRetrieve try to apply to provided update query and check (if the update return no rows modified), that the row exists
func (s *Store) updateTxWithRetrieve(ctx context.Context, id int, query *bun.UpdateQuery) (*ledger.Transaction, bool, error) {
	type modifiedEntity struct {
		ledger.Transaction `bun:",extend"`
		Modified           bool `bun:"modified"`
	}
	me := &modifiedEntity{}

	err := s.db.NewSelect().
		With("upd", query).
		ModelTableExpr(
			"(?) transactions",
			s.db.NewSelect().
				ColumnExpr("upd.*, true as modified").
				ModelTableExpr("upd").
				UnionAll(
					s.db.NewSelect().
						ModelTableExpr(s.GetPrefixedRelationName("transactions")).
						ColumnExpr("*, false as modified").
						Where("id = ? and ledger = ?", id, s.ledger.Name).
						Limit(1),
				),
		).
		Model(me).
		ColumnExpr("*").
		Limit(1).
		Scan(ctx)
	if err != nil {
		return nil, false, postgres.ResolveError(err)
	}

	return &me.Transaction, me.Modified, nil
}

func (s *Store) RevertTransaction(ctx context.Context, id int) (tx *ledger.Transaction, modified bool, err error) {
	_, err = tracing.TraceWithLatency(ctx, "RevertTransaction", func(ctx context.Context) (*ledger.Transaction, error) {
		now := time.Now()
		tx, modified, err = s.updateTxWithRetrieve(
			ctx,
			id,
			s.db.NewUpdate().
				Model(&ledger.Transaction{}).
				ModelTableExpr(s.GetPrefixedRelationName("transactions")).
				Where("id = ?", id).
				Where("reverted_at is null").
				Where("ledger = ?", s.ledger.Name).
				Set("reverted_at = ?", now).
				Set("updated_at = ?", now).
				Returning("*"),
		)
		return nil, err
	})
	if err != nil {
		return nil, false, err
	}
	return tx, modified, err
}

func (s *Store) UpdateTransactionMetadata(ctx context.Context, id int, m metadata.Metadata) (tx *ledger.Transaction, modified bool, err error) {
	_, err = tracing.TraceWithLatency(ctx, "UpdateTransactionMetadata", func(ctx context.Context) (*ledger.Transaction, error) {
		tx, modified, err = s.updateTxWithRetrieve(
			ctx,
			id,
			s.db.NewUpdate().
				Model(&ledger.Transaction{}).
				ModelTableExpr(s.GetPrefixedRelationName("transactions")).
				Where("id = ?", id).
				Where("ledger = ?", s.ledger.Name).
				Set("metadata = metadata || ?", m).
				Set("updated_at = ?", time.Now()).
				Where("not (metadata @> ?)", m).
				Returning("*"),
		)
		return nil, err
	})
	if err != nil {
		return nil, false, err
	}
	return tx, modified, err
}

func (s *Store) DeleteTransactionMetadata(ctx context.Context, id int, key string) (tx *ledger.Transaction, modified bool, err error) {
	_, err = tracing.TraceWithLatency(ctx, "DeleteTransactionMetadata", func(ctx context.Context) (*ledger.Transaction, error) {
		tx, modified, err = s.updateTxWithRetrieve(
			ctx,
			id,
			s.db.NewUpdate().
				Model(&ledger.Transaction{}).
				ModelTableExpr(s.GetPrefixedRelationName("transactions")).
				Set("metadata = metadata - ?", key).
				Set("updated_at = ?", time.Now()).
				Where("id = ?", id).
				Where("ledger = ?", s.ledger.Name).
				Where("metadata -> ? is not null", key).
				Returning("*"),
		)
		return nil, err
	})
	if err != nil {
		return nil, false, err
	}
	return tx, modified, err
}

func filterAccountAddressOnTransactions(address string, source, destination bool) string {
	src := strings.Split(address, ":")

	needSegmentCheck := false
	for _, segment := range src {
		needSegmentCheck = segment == ""
		if needSegmentCheck {
			break
		}
	}

	if needSegmentCheck {
		m := map[string]any{
			fmt.Sprint(len(src)): nil,
		}
		parts := make([]string, 0)

		for i, segment := range src {
			if len(segment) == 0 {
				continue
			}
			m[fmt.Sprint(i)] = segment
		}

		data, err := json.Marshal([]any{m})
		if err != nil {
			panic(err)
		}

		if source {
			parts = append(parts, fmt.Sprintf("sources_arrays @> '%s'", string(data)))
		}
		if destination {
			parts = append(parts, fmt.Sprintf("destinations_arrays @> '%s'", string(data)))
		}
		return strings.Join(parts, " or ")
	} else {
		data, err := json.Marshal([]string{address})
		if err != nil {
			panic(err)
		}

		parts := make([]string, 0)
		if source {
			parts = append(parts, fmt.Sprintf("sources @> '%s'", string(data)))
		}
		if destination {
			parts = append(parts, fmt.Sprintf("destinations @> '%s'", string(data)))
		}
		return strings.Join(parts, " or ")
	}
}
