package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/formancehq/ledger/internal/tracing"

	. "github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/platform/postgres"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/pkg/errors"
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
	metadataRegex = regexp.MustCompile("metadata\\[(.+)\\]")
)

type Transaction struct {
	bun.BaseModel `bun:"table:transactions,alias:transactions"`

	Ledger            string            `bun:"ledger,type:varchar"`
	ID                int               `bun:"id,type:numeric"`
	Seq               int               `bun:"seq,scanonly"`
	Timestamp         *time.Time        `bun:"timestamp,type:timestamp without time zone"`
	Reference         string            `bun:"reference,type:varchar,unique,nullzero"`
	Postings          []ledger.Posting  `bun:"postings,type:jsonb"`
	Metadata          metadata.Metadata `bun:"metadata,type:jsonb,default:'{}'"`
	RevertedAt        *time.Time        `bun:"reverted_at"`
	InsertedAt        *time.Time        `bun:"inserted_at"`
	Sources           []string          `bun:"sources,type:jsonb"`
	Destinations      []string          `bun:"destinations,type:jsonb"`
	SourcesArray      []map[string]any  `bun:"sources_arrays,type:jsonb"`
	DestinationsArray []map[string]any  `bun:"destinations_arrays,type:jsonb"`
}

func (t Transaction) toCore() ledger.Transaction {
	return ledger.Transaction{
		TransactionData: ledger.TransactionData{
			Reference:  t.Reference,
			Metadata:   t.Metadata,
			Timestamp:  *t.Timestamp,
			Postings:   t.Postings,
			InsertedAt: *t.InsertedAt,
		},
		ID:       t.ID,
		Reverted: t.RevertedAt != nil && !t.RevertedAt.IsZero(),
	}
}

type TransactionPostCommitVolume struct {
	AggregatedAccountVolume
	Account string `bun:"account"`
}

type TransactionsPostCommitVolumes []TransactionPostCommitVolume

func (p TransactionsPostCommitVolumes) toCore() ledger.PostCommitVolumes {
	ret := ledger.PostCommitVolumes{}
	for _, volumes := range p {
		if _, ok := ret[volumes.Account]; !ok {
			ret[volumes.Account] = map[string]ledger.Volumes{}
		}
		if v, ok := ret[volumes.Account][volumes.Asset]; !ok {
			ret[volumes.Account][volumes.Asset] = ledger.Volumes{
				Inputs:  volumes.Inputs,
				Outputs: volumes.Outputs,
			}
		} else {
			v.Inputs = v.Inputs.Add(v.Inputs, volumes.Inputs)
			v.Outputs = v.Outputs.Add(v.Outputs, volumes.Outputs)

			ret[volumes.Account][volumes.Asset] = v
		}

	}
	return ret
}

type ExpandedTransaction struct {
	Transaction `bun:",extend"`

	PostCommitEffectiveVolumes TransactionsPostCommitVolumes `bun:"post_commit_effective_volumes,type:jsonb,scanonly"`
	PostCommitVolumes          TransactionsPostCommitVolumes `bun:"post_commit_volumes,type:jsonb,scanonly"`
}

func (t ExpandedTransaction) toCore() ledger.ExpandedTransaction {
	return ledger.ExpandedTransaction{
		Transaction:                t.Transaction.toCore(),
		PostCommitEffectiveVolumes: t.PostCommitEffectiveVolumes.toCore(),
		PostCommitVolumes:          t.PostCommitVolumes.toCore(),
	}
}

func (s *Store) selectDistinctTransactionMetadataHistories(date *time.Time) *bun.SelectQuery {
	ret := s.db.NewSelect().
		DistinctOn("transactions_seq").
		ModelTableExpr(s.GetPrefixedRelationName("transactions_metadata")).
		Where("ledger = ?", s.ledger.Name).
		Column("transactions_seq", "metadata").
		Order("transactions_seq", "revision desc")

	if date != nil && !date.IsZero() {
		ret = ret.Where("date <= ?", date)
	}

	return ret
}

func (s *Store) selectTransactions(date *time.Time, expandVolumes, expandEffectiveVolumes bool, q query.Builder) *bun.SelectQuery {

	ret := s.db.NewSelect()
	if expandVolumes && !s.ledger.HasFeature(ledger.FeaturePostCommitVolumes, "SYNC") {
		return ret.Err(ledgercontroller.NewErrMissingFeature(ledger.FeaturePostCommitVolumes))
	}
	if expandEffectiveVolumes && !s.ledger.HasFeature(ledger.FeaturePostCommitEffectiveVolumes, "SYNC") {
		return ret.Err(ledgercontroller.NewErrMissingFeature(ledger.FeaturePostCommitEffectiveVolumes))
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
			"seq",
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
		).
		Where("ledger = ?", s.ledger.Name)

	if date != nil && !date.IsZero() {
		ret = ret.Where("timestamp <= ?", date)
	}

	if s.ledger.HasFeature(ledger.FeatureAccountMetadataHistories, "SYNC") && date != nil && !date.IsZero() {
		ret = ret.
			Join(
				`left join (?) transactions_metadata on transactions_metadata.transactions_seq = transactions.seq`,
				s.selectDistinctTransactionMetadataHistories(date),
			).
			ColumnExpr("coalesce(transactions_metadata.metadata, '{}'::jsonb) as metadata")
	} else {
		ret = ret.ColumnExpr("metadata")
	}

	if s.ledger.HasFeature(ledger.FeaturePostCommitVolumes, "SYNC") && expandVolumes {
		ret = ret.
			Join(
				`join (?) pcv on pcv.transactions_seq = transactions.seq`,
				s.db.NewSelect().
					TableExpr(s.GetPrefixedRelationName("moves")).
					Column("transactions_seq").
					ColumnExpr(`to_json(array_agg(json_build_object('account', moves.account_address,	'asset', moves.asset, 'inputs', (moves.post_commit_volumes).inputs, 'outputs', (moves.post_commit_volumes).outputs))) as post_commit_volumes`).
					Group("transactions_seq"),
			).
			ColumnExpr("pcv.*")
	}

	if s.ledger.HasFeature(ledger.FeaturePostCommitEffectiveVolumes, "SYNC") && expandEffectiveVolumes {
		ret = ret.
			Join(
				`join (?) pcev on pcev.transactions_seq = transactions.seq`,
				// todo: need to take only the last move for each account/asset
				s.db.NewSelect().
					TableExpr(s.GetPrefixedRelationName("moves")).
					Column("transactions_seq").
					ColumnExpr(`to_json(array_agg(json_build_object('account', moves.account_address,	'asset', moves.asset, 'inputs', (moves.post_commit_effective_volumes).inputs, 'outputs', (moves.post_commit_effective_volumes).outputs))) as post_commit_effective_volumes`).
					Group("transactions_seq"),
			).
			ColumnExpr("pcev.*")
	}

	// create a parent query which set reverted_at to null if the date passed as argument is before
	ret = s.db.NewSelect().
		ModelTableExpr("(?) transactions", ret).
		Column(
			"seq",
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

	sources := Map(tx.Postings, ledger.Posting.GetSource)
	destinations := Map(tx.Postings, ledger.Posting.GetDestination)
	mappedTx := &Transaction{
		Ledger:   s.ledger.Name,
		Postings: tx.Postings,
		Metadata: tx.Metadata,
		Timestamp: func() *time.Time {
			if tx.Timestamp.IsZero() {
				return nil
			}
			return &tx.Timestamp
		}(),
		Reference: tx.Reference,
		InsertedAt: func() *time.Time {
			if tx.InsertedAt.IsZero() {
				return nil
			}
			return &tx.InsertedAt
		}(),
		Sources:           sources,
		Destinations:      destinations,
		SourcesArray:      Map(sources, convertAddrToIndexedJSONB),
		DestinationsArray: Map(destinations, convertAddrToIndexedJSONB),
	}

	sqlQueries := Map(tx.InvolvedAccounts(), func(from string) string {
		return fmt.Sprintf("select pg_advisory_xact_lock(hashtext('%s'))", fmt.Sprintf("%s%s", s.ledger.Name, from))
	})

	_, err := s.db.NewRaw(strings.Join(sqlQueries, ";")).Exec(ctx)
	if err != nil {
		return postgres.ResolveError(err)
	}

	err = s.insertTransaction(ctx, mappedTx)
	if err != nil {
		return errors.Wrap(err, "failed to insert transaction")
	}

	accounts := map[string]Account{}
	for _, address := range tx.InvolvedAccounts() {
		account := Account{
			Ledger:        s.ledger.Name,
			AddressArray:  strings.Split(address, ":"),
			Address:       address,
			FirstUsage:    *mappedTx.Timestamp,
			InsertionDate: *mappedTx.InsertedAt,
			UpdatedAt:     *mappedTx.InsertedAt,
			Metadata:      make(metadata.Metadata),
		}
		_, err := s.upsertAccount(ctx, &account)
		if err != nil {
			return errors.Wrap(err, "upserting account")
		}

		accounts[address] = account
	}

	// notes(gfyrag): keep upserting of accounts separated as some account can be created (and locked),
	// while some other will not (the underlying SAVEPOINT of the storage is ROLLBACK if no rows are touched)
	// maybe it could be handled by the storage
	moves := Moves{}
	for _, p := range tx.Postings {
		moves = append(moves, []*Move{
			{
				Ledger:              s.ledger.Name,
				IsSource:            true,
				Account:             p.Source,
				AccountAddressArray: strings.Split(p.Source, ":"),
				Amount:              (*bunpaginate.BigInt)(p.Amount),
				Asset:               p.Asset,
				InsertionDate:       *mappedTx.InsertedAt,
				EffectiveDate:       *mappedTx.Timestamp,
				TransactionSeq:      mappedTx.Seq,
				AccountSeq:          accounts[p.Source].Seq,
			},
			{
				Ledger:              s.ledger.Name,
				Account:             p.Destination,
				AccountAddressArray: strings.Split(p.Destination, ":"),
				Amount:              (*bunpaginate.BigInt)(p.Amount),
				Asset:               p.Asset,
				InsertionDate:       *mappedTx.InsertedAt,
				EffectiveDate:       *mappedTx.Timestamp,
				TransactionSeq:      mappedTx.Seq,
				AccountSeq:          accounts[p.Destination].Seq,
			},
		}...)
	}

	if err := s.insertMoves(ctx, moves...); err != nil {
		return errors.Wrap(err, "failed to insert moves")
	}

	_, err = s.updateBalances(ctx, moves.BalanceUpdates())
	if err != nil {
		return errors.Wrap(err, "failed to update balances")
	}

	tx.ID = mappedTx.ID
	tx.InsertedAt = *mappedTx.InsertedAt
	tx.Timestamp = *mappedTx.Timestamp

	return nil
}

func (s *Store) ListTransactions(ctx context.Context, q ledgercontroller.ListTransactionsQuery) (*bunpaginate.Cursor[ledger.ExpandedTransaction], error) {
	return tracing.Trace(ctx, "ListTransactions", func(ctx context.Context) (*bunpaginate.Cursor[ledger.ExpandedTransaction], error) {
		cursor, err := bunpaginate.UsingColumn[ledgercontroller.PaginatedQueryOptions[ledgercontroller.PITFilterWithVolumes], ExpandedTransaction](
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

		return bunpaginate.MapCursor(cursor, ExpandedTransaction.toCore), nil
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

func (s *Store) GetTransaction(ctx context.Context, filter ledgercontroller.GetTransactionQuery) (*ledger.ExpandedTransaction, error) {
	return tracing.TraceWithLatency(ctx, "GetTransaction", func(ctx context.Context) (*ledger.ExpandedTransaction, error) {
		ret := &ExpandedTransaction{}
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

		return pointer.For(ret.toCore()), nil
	})
}

func (s *Store) insertTransaction(ctx context.Context, tx *Transaction) error {
	_, err := tracing.TraceWithLatency(ctx, "InsertTransaction", func(ctx context.Context) (*Transaction, error) {
		_, err := s.db.NewInsert().
			Model(tx).
			ModelTableExpr(s.GetPrefixedRelationName("transactions")).
			Value("id", "nextval(?)", s.GetPrefixedRelationName(fmt.Sprintf(`"transaction_id_%d"`, s.ledger.ID))).
			Returning("*").
			Exec(ctx)
		if err != nil {
			err = postgres.ResolveError(err)
			switch {
			case errors.Is(err, postgres.ErrConstraintsFailed{}):
				if err.(postgres.ErrConstraintsFailed).GetConstraint() == "transactions_reference" {
					return nil, ledgercontroller.NewErrReferenceConflict(tx.Reference)
				}
			default:
				return nil, err
			}
		}

		return tx, nil
	}, func(ctx context.Context, tx *Transaction) {
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.Int("id", tx.ID),
			attribute.String("timestamp", tx.Timestamp.Format(time.RFC3339Nano)),
		)
	})

	return err
}

// updateTxWithRetrieve try to apply to provided query and check (if the update return no rows modified), that the row exists
func (s *Store) updateTxWithRetrieve(ctx context.Context, id int, query *bun.UpdateQuery) (*ledger.Transaction, bool, error) {
	type modifiedEntity struct {
		Transaction `bun:",extend"`
		Modified    bool `bun:"modified"`
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

	return pointer.For(me.toCore()), me.Modified, nil
}

func (s *Store) RevertTransaction(ctx context.Context, id int) (tx *ledger.Transaction, modified bool, err error) {
	_, err = tracing.TraceWithLatency(ctx, "RevertTransaction", func(ctx context.Context) (*ledger.Transaction, error) {
		now := time.Now()
		tx, modified, err = s.updateTxWithRetrieve(
			ctx,
			id,
			s.db.NewUpdate().
				Model(&Transaction{}).
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
				Model(&Transaction{}).
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
				Model(&Transaction{}).
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
