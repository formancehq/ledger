package ledger

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	storageerrors "github.com/formancehq/ledger/pkg/storage/sqlstorage/errors"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/pagination"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/extra/bunbig"
)

const (
	TransactionsTableName = "transactions"
	PostingsTableName     = "postings"
)

type Transaction struct {
	bun.BaseModel `bun:"transactions,alias:transactions"`

	ID        uint64            `bun:"id,type:bigint,pk"`
	Timestamp core.Time         `bun:"timestamp,type:timestamptz"`
	Reference string            `bun:"reference,type:varchar,unique,nullzero"`
	Postings  []Posting         `bun:"rel:has-many,join:id=txid"`
	Metadata  metadata.Metadata `bun:"metadata,type:jsonb,default:'{}'"`
	//TODO(gfyrag): change to bytea
	PreCommitVolumes  core.AccountsAssetsVolumes `bun:"pre_commit_volumes,type:bytea"`
	PostCommitVolumes core.AccountsAssetsVolumes `bun:"post_commit_volumes,type:bytea"`
}

func (t Transaction) toCore() core.ExpandedTransaction {
	postings := core.Postings{}
	for _, p := range t.Postings {
		postings = append(postings, p.toCore())
	}
	return core.ExpandedTransaction{
		Transaction: core.Transaction{
			TransactionData: core.TransactionData{
				Postings:  postings,
				Reference: t.Reference,
				Metadata:  t.Metadata,
				Timestamp: t.Timestamp,
			},
			ID: t.ID,
		},
		PreCommitVolumes:  t.PreCommitVolumes,
		PostCommitVolumes: t.PostCommitVolumes,
	}
}

type account string

var _ driver.Valuer = account("")

func (m1 account) Value() (driver.Value, error) {
	ret, err := json.Marshal(strings.Split(string(m1), ":"))
	if err != nil {
		return nil, err
	}
	return string(ret), nil
}

// Scan - Implement the database/sql scanner interface
func (m1 *account) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	v, err := driver.String.ConvertValue(value)
	if err != nil {
		return err
	}

	array := make([]string, 0)
	switch vv := v.(type) {
	case []uint8:
		err = json.Unmarshal(vv, &array)
	case string:
		err = json.Unmarshal([]byte(vv), &array)
	default:
		panic("not handled type")
	}
	if err != nil {
		return err
	}
	*m1 = account(strings.Join(array, ":"))
	return nil
}

type Posting struct {
	bun.BaseModel `bun:"postings,alias:postings"`

	Transaction   *Transaction `bun:"rel:belongs-to,join:txid=id"`
	TransactionID uint64       `bun:"txid,type:bigint"`
	Amount        *bunbig.Int  `bun:"amount,type:bigint"`
	Asset         string       `bun:"asset,type:string"`
	Source        account      `bun:"source,type:jsonb"`
	Destination   account      `bun:"destination,type:jsonb"`
}

func (p Posting) toCore() core.Posting {
	return core.Posting{
		Source:      string(p.Source),
		Destination: string(p.Destination),
		Amount:      (*big.Int)(p.Amount),
		Asset:       p.Asset,
	}
}

func (s *Store) buildTransactionsQuery(p storage.TransactionsQueryFilters, models *[]Transaction) *bun.SelectQuery {
	sb := s.schema.
		NewSelect(TransactionsTableName).
		Model(models).
		Join(fmt.Sprintf("JOIN %s", s.schema.Table(PostingsTableName))).
		JoinOn("postings.txid = transactions.id").
		Relation("Postings", func(sb *bun.SelectQuery) *bun.SelectQuery {
			return sb.With("postings", s.schema.NewSelect(PostingsTableName))
		}).
		Distinct()

	if p.Source != "" {
		src := strings.Split(p.Source, ":")
		sb.Where(fmt.Sprintf("jsonb_array_length(postings.source) = %d", len(src)))

		for i, segment := range src {
			if segment == ".*" || segment == "*" || segment == "" {
				continue
			}

			sb.Where(fmt.Sprintf("postings.source @@ ('$[%d] == \"' || ?::text || '\"')::jsonpath", i), segment)
		}
	}
	if p.Destination != "" {
		dst := strings.Split(p.Destination, ":")
		sb.Where(fmt.Sprintf("jsonb_array_length(postings.destination) = %d", len(dst)))
		for i, segment := range dst {
			if segment == ".*" || segment == "*" || segment == "" {
				continue
			}

			sb.Where(fmt.Sprintf("postings.destination @@ ('$[%d] == \"' || ?::text || '\"')::jsonpath", i), segment)
		}
	}
	if p.Account != "" {
		dst := strings.Split(p.Account, ":")
		sb.Where(fmt.Sprintf("(jsonb_array_length(postings.destination) = %d OR jsonb_array_length(postings.source) = %d)", len(dst), len(dst)))
		for i, segment := range dst {
			if segment == ".*" || segment == "*" || segment == "" {
				continue
			}

			sb.Where(fmt.Sprintf("(postings.source @@ ('$[%d] == \"' || ?0::text || '\"')::jsonpath OR postings.destination @@ ('$[%d] == \"' || ?0::text || '\"')::jsonpath)", i, i), segment)
		}
	}

	if p.Reference != "" {
		sb.Where("reference = ?", p.Reference)
	}
	if !p.StartTime.IsZero() {
		sb.Where("timestamp >= ?", p.StartTime.UTC())
	}
	if !p.EndTime.IsZero() {
		sb.Where("timestamp < ?", p.EndTime.UTC())
	}
	if p.AfterTxID > 0 {
		sb.Where("id > ?", p.AfterTxID)
	}

	for key, value := range p.Metadata {
		sb.Where(s.schema.Table(
			fmt.Sprintf("%s(metadata, ?, '%s')",
				SQLCustomFuncMetaCompare, strings.ReplaceAll(key, ".", "', '")),
		), value)
	}

	return sb
}

func (s *Store) GetTransactions(ctx context.Context, q storage.TransactionsQuery) (*api.Cursor[core.ExpandedTransaction], error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_transactions")
	defer recordMetrics()

	cursor, err := pagination.UsingColumn[storage.TransactionsQueryFilters, Transaction](ctx,
		s.buildTransactionsQuery, storage.ColumnPaginatedQuery[storage.TransactionsQueryFilters](q),
	)
	if err != nil {
		return nil, err
	}

	return api.MapCursor(cursor, Transaction.toCore), nil
}

func (s *Store) CountTransactions(ctx context.Context, q storage.TransactionsQuery) (uint64, error) {
	if !s.isInitialized {
		return 0, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "count_transactions")
	defer recordMetrics()

	models := make([]Transaction, 0)
	count, err := s.buildTransactionsQuery(q.Filters, &models).Count(ctx)

	return uint64(count), storageerrors.PostgresError(err)
}

func (s *Store) GetTransaction(ctx context.Context, txId uint64) (*core.ExpandedTransaction, error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_transaction")
	defer recordMetrics()

	tx := &Transaction{}
	err := s.schema.NewSelect(TransactionsTableName).
		Model(tx).
		Relation("Postings", func(query *bun.SelectQuery) *bun.SelectQuery {
			return query.With("postings", s.schema.NewSelect(PostingsTableName))
		}).
		Where("id = ?", txId).
		OrderExpr("id DESC").
		Scan(ctx)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}
	coreTx := tx.toCore()

	return &coreTx, nil

}

func (s *Store) insertTransactions(ctx context.Context, txs ...core.ExpandedTransaction) error {
	ts := make([]Transaction, len(txs))
	ps := make([]Posting, 0)

	for i, tx := range txs {
		ts[i].ID = tx.ID
		ts[i].Timestamp = tx.Timestamp
		ts[i].Metadata = tx.Metadata
		ts[i].PreCommitVolumes = tx.PreCommitVolumes
		ts[i].PostCommitVolumes = tx.PostCommitVolumes
		ts[i].Reference = ""
		if tx.Reference != "" {
			cp := tx.Reference
			ts[i].Reference = cp
		}

		for _, p := range tx.Postings {
			ps = append(ps, Posting{
				TransactionID: tx.ID,
				Amount:        (*bunbig.Int)(p.Amount),
				Asset:         p.Asset,
				Source:        account(p.Source),
				Destination:   account(p.Destination),
			})
		}
	}

	_, err := s.schema.NewInsert(TransactionsTableName).
		Model(&ts).
		On("CONFLICT (id) DO NOTHING").
		Exec(ctx)
	if err != nil {
		return storageerrors.PostgresError(err)
	}

	_, err = s.schema.NewInsert(PostingsTableName).
		Model(&ps).
		// TODO(polo/gfyrag): Current postings table does not have
		// unique indexes in txid and posting_index. It means that if we insert
		// a posting with same txid and same posting index, it will be
		// duplicated. We should fix this in the future.
		// Why this index was removed ?
		// On("CONFLICT (txid, posting_index) DO NOTHING").
		Exec(ctx)

	return storageerrors.PostgresError(err)
}

func (s *Store) InsertTransactions(ctx context.Context, txs ...core.ExpandedTransaction) error {
	if !s.isInitialized {
		return storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "insert_transactions")
	defer recordMetrics()

	return storageerrors.PostgresError(s.insertTransactions(ctx, txs...))
}

func (s *Store) UpdateTransactionMetadata(ctx context.Context, id uint64, metadata metadata.Metadata) error {
	if !s.isInitialized {
		return storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "update_transaction_metadata")
	defer recordMetrics()

	metadataData, err := json.Marshal(metadata)
	if err != nil {
		return errors.Wrap(err, "failed to marshal metadata")

	}

	_, err = s.schema.NewUpdate(TransactionsTableName).
		Model((*Transaction)(nil)).
		Set("metadata = metadata || ?", string(metadataData)).
		Where("id = ?", id).
		Exec(ctx)

	return storageerrors.PostgresError(err)
}

func (s *Store) UpdateTransactionsMetadata(ctx context.Context, transactionsWithMetadata ...core.TransactionWithMetadata) error {
	if !s.isInitialized {
		return storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "update_transactions_metadata")
	defer recordMetrics()

	txs := make([]*Transaction, 0, len(transactionsWithMetadata))
	for _, tx := range transactionsWithMetadata {
		txs = append(txs, &Transaction{
			ID:       tx.ID,
			Metadata: tx.Metadata,
		})
	}

	values := s.schema.NewValues(&txs)

	_, err := s.schema.NewUpdate(TransactionsTableName).
		With("_data", values).
		Model((*Transaction)(nil)).
		TableExpr("_data").
		Set("metadata = transactions.metadata || _data.metadata").
		Where(fmt.Sprintf("%s.id = _data.id", TransactionsTableName)).
		Exec(ctx)

	return storageerrors.PostgresError(err)
}
