package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	. "github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/ledger/pkg/features"
	"math/big"
	"slices"
	"strings"

	"github.com/formancehq/ledger/internal/tracing"

	"errors"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/v2/pointer"

	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"

	"github.com/formancehq/go-libs/v2/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
)

func (store *Store) CommitTransaction(ctx context.Context, tx *ledger.Transaction, accountMetadata map[string]metadata.Metadata) error {
	if accountMetadata == nil {
		accountMetadata = make(map[string]metadata.Metadata)
	}

	postCommitVolumes, err := store.UpdateVolumes(ctx, tx.VolumeUpdates()...)
	if err != nil {
		return fmt.Errorf("failed to update balances: %w", err)
	}
	tx.PostCommitVolumes = postCommitVolumes.Copy()

	err = store.InsertTransaction(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to insert transaction: %w", err)
	}

	accountsToUpsert := tx.InvolvedAccounts()
	accountsToUpsert = append(accountsToUpsert, Keys(accountMetadata)...)

	slices.Sort(accountsToUpsert)
	accountsToUpsert = slices.Compact(accountsToUpsert)

	err = store.UpsertAccounts(ctx, Map(accountsToUpsert, func(address string) *ledger.Account {
		return &ledger.Account{
			Address:       address,
			FirstUsage:    tx.Timestamp,
			Metadata:      accountMetadata[address],
			InsertionDate: tx.InsertedAt,
			UpdatedAt:     tx.InsertedAt,
		}
	})...)
	if err != nil {
		return fmt.Errorf("upserting accounts: %w", err)
	}

	if store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
		moves := ledger.Moves{}
		postings := make([]ledger.Posting, len(tx.Postings))
		copy(postings, tx.Postings)
		slices.Reverse(postings)

		for _, posting := range postings {
			moves = append(moves, &ledger.Move{
				Account:           posting.Destination,
				Amount:            (*bunpaginate.BigInt)(posting.Amount),
				Asset:             posting.Asset,
				InsertionDate:     tx.InsertedAt,
				EffectiveDate:     tx.Timestamp,
				PostCommitVolumes: pointer.For(postCommitVolumes[posting.Destination][posting.Asset].Copy()),
				TransactionID:     *tx.ID,
			})
			postCommitVolumes.AddInput(posting.Destination, posting.Asset, new(big.Int).Neg(posting.Amount))

			moves = append(moves, &ledger.Move{
				IsSource:          true,
				Account:           posting.Source,
				Amount:            (*bunpaginate.BigInt)(posting.Amount),
				Asset:             posting.Asset,
				InsertionDate:     tx.InsertedAt,
				EffectiveDate:     tx.Timestamp,
				PostCommitVolumes: pointer.For(postCommitVolumes[posting.Source][posting.Asset].Copy()),
				TransactionID:     *tx.ID,
			})
			postCommitVolumes.AddOutput(posting.Source, posting.Asset, new(big.Int).Neg(posting.Amount))
		}

		slices.Reverse(moves)

		if err := store.InsertMoves(ctx, moves...); err != nil {
			return fmt.Errorf("failed to insert moves: %w", err)
		}

		if store.ledger.HasFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
			tx.PostCommitEffectiveVolumes = moves.ComputePostCommitEffectiveVolumes()
		}
	}

	return nil
}

func (store *Store) InsertTransaction(ctx context.Context, tx *ledger.Transaction) error {
	return tracing.SkipResult(tracing.TraceWithMetric(
		ctx,
		"InsertTransaction",
		store.tracer,
		store.insertTransactionHistogram,
		func(ctx context.Context) (*ledger.Transaction, error) {
			query := store.db.NewInsert().
				Model(tx).
				ModelTableExpr(store.GetPrefixedRelationName("transactions")).
				Value("ledger", "?", store.ledger.Name).
				Returning("id, timestamp, inserted_at")

			if tx.ID == nil {
				query = query.Value("id", "nextval(?)", store.GetPrefixedRelationName(fmt.Sprintf(`"transaction_id_%d"`, store.ledger.ID)))
			}

			_, err := query.Exec(ctx)
			if err != nil {
				err = postgres.ResolveError(err)
				switch {
				case errors.Is(err, postgres.ErrConstraintsFailed{}):
					if err.(postgres.ErrConstraintsFailed).GetConstraint() == "transactions_reference" {
						return nil, ledgercontroller.NewErrTransactionReferenceConflict(tx.Reference)
					}
					if err.(postgres.ErrConstraintsFailed).GetConstraint() == "transactions_ledger" {
						return nil, ledgercontroller.NewErrConcurrentTransaction(*tx.ID)
					}

					return nil, err
				default:
					return nil, err
				}
			}

			return tx, nil
		},
		func(ctx context.Context, tx *ledger.Transaction) {
			trace.SpanFromContext(ctx).SetAttributes(
				attribute.Int("id", *tx.ID),
				attribute.String("timestamp", tx.Timestamp.Format(time.RFC3339Nano)),
			)
		},
	))
}

// updateTxWithRetrieve try to apply to provided update query and check (if the update return no rows modified), that the row exists
func (store *Store) updateTxWithRetrieve(ctx context.Context, id int, query *bun.UpdateQuery) (*ledger.Transaction, bool, error) {
	type modifiedEntity struct {
		ledger.Transaction `bun:",extend"`
		Modified           bool `bun:"modified"`
	}
	me := &modifiedEntity{}

	err := store.db.NewSelect().
		With("upd", query).
		ModelTableExpr(
			"(?) transactions",
			store.db.NewSelect().
				ColumnExpr("upd.*, true as modified").
				ModelTableExpr("upd").
				UnionAll(
					store.db.NewSelect().
						ModelTableExpr(store.GetPrefixedRelationName("transactions")).
						ColumnExpr("*, false as modified").
						Where("id = ? and ledger = ?", id, store.ledger.Name).
						Limit(1),
				),
		).
		Model(me).
		ColumnExpr("*").
		Limit(1).
		Scan(ctx)

	return &me.Transaction, me.Modified, postgres.ResolveError(err)
}

func (store *Store) RevertTransaction(ctx context.Context, id int, at time.Time) (tx *ledger.Transaction, modified bool, err error) {
	_, err = tracing.TraceWithMetric(
		ctx,
		"RevertTransaction",
		store.tracer,
		store.revertTransactionHistogram,
		func(ctx context.Context) (*ledger.Transaction, error) {
			query := store.db.NewUpdate().
				Model(&ledger.Transaction{}).
				ModelTableExpr(store.GetPrefixedRelationName("transactions")).
				Where("id = ?", id).
				Where("reverted_at is null").
				Where("ledger = ?", store.ledger.Name).
				Returning("*")
			if at.IsZero() {
				query = query.
					Set("reverted_at = (now() at time zone 'utc')").
					Set("updated_at = (now() at time zone 'utc')")
			} else {
				query = query.
					Set("reverted_at = ?", at).
					Set("updated_at = ?", at)
			}

			tx, modified, err = store.updateTxWithRetrieve(ctx, id, query)
			return nil, err
		},
	)
	return tx, modified, err
}

func (store *Store) UpdateTransactionMetadata(ctx context.Context, id int, m metadata.Metadata) (tx *ledger.Transaction, modified bool, err error) {
	_, err = tracing.TraceWithMetric(
		ctx,
		"UpdateTransactionMetadata",
		store.tracer,
		store.updateTransactionMetadataHistogram,
		func(ctx context.Context) (*ledger.Transaction, error) {
			tx, modified, err = store.updateTxWithRetrieve(
				ctx,
				id,
				store.db.NewUpdate().
					Model(&ledger.Transaction{}).
					ModelTableExpr(store.GetPrefixedRelationName("transactions")).
					Where("id = ?", id).
					Where("ledger = ?", store.ledger.Name).
					Set("metadata = metadata || ?", m).
					Set("updated_at = (now() at time zone 'utc')").
					Where("not (metadata @> ?)", m).
					Returning("*"),
			)
			return nil, postgres.ResolveError(err)
		},
	)
	return tx, modified, err
}

func (store *Store) DeleteTransactionMetadata(ctx context.Context, id int, key string) (tx *ledger.Transaction, modified bool, err error) {
	_, err = tracing.TraceWithMetric(
		ctx,
		"DeleteTransactionMetadata",
		store.tracer,
		store.deleteTransactionMetadataHistogram,
		func(ctx context.Context) (*ledger.Transaction, error) {
			tx, modified, err = store.updateTxWithRetrieve(
				ctx,
				id,
				store.db.NewUpdate().
					Model(&ledger.Transaction{}).
					ModelTableExpr(store.GetPrefixedRelationName("transactions")).
					Set("metadata = metadata - ?", key).
					Set("updated_at = (now() at time zone 'utc')").
					Where("id = ?", id).
					Where("ledger = ?", store.ledger.Name).
					Where("metadata -> ? is not null", key).
					Returning("*"),
			)
			return nil, postgres.ResolveError(err)
		},
	)
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
	}

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
