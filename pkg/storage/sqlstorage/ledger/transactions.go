package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	storageerrors "github.com/formancehq/ledger/pkg/storage/sqlstorage/errors"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/pagination"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const (
	TransactionsTableName = "transactions"
	PostingsTableName     = "postings"
)

// this regexp is used to distinguish between deprecated regex queries for
// source, destination and account params and the new wildcard query
// which allows segmented address pattern matching, e.g; "foo:bar:*"
var addressQueryRegexp = regexp.MustCompile(`^(\w+|\*|\.\*)(:(\w+|\*|\.\*))*$`)

type Transaction struct {
	bun.BaseModel `bun:"transactions,alias:transactions"`

	ID                string          `bun:"id,type:uuid,unique"`
	Timestamp         core.Time       `bun:"timestamp,type:timestamptz"`
	Reference         string          `bun:"reference,type:varchar,unique,nullzero"`
	Hash              string          `bun:"hash,type:varchar"`
	Postings          json.RawMessage `bun:"postings,type:jsonb"`
	Metadata          json.RawMessage `bun:"metadata,type:jsonb,default:'{}'"`
	PreCommitVolumes  json.RawMessage `bun:"pre_commit_volumes,type:jsonb"`
	PostCommitVolumes json.RawMessage `bun:"post_commit_volumes,type:jsonb"`
}

type Postings struct {
	bun.BaseModel `bun:"postings,alias:postings"`

	TxID         string          `bun:"txid,type:uuid"`
	PostingIndex int             `bun:"posting_index,type:integer"`
	Source       json.RawMessage `bun:"source,type:jsonb"`
	Destination  json.RawMessage `bun:"destination,type:jsonb"`
}

func (s *Store) buildTransactionsQuery(p storage.TransactionsQuery) *bun.SelectQuery {
	sb := s.schema.NewSelect(TransactionsTableName).Model((*Transaction)(nil))

	sb.
		Column("id", "timestamp", "reference", "metadata", "postings", "pre_commit_volumes", "post_commit_volumes").
		OrderExpr("timestamp DESC").
		Distinct()
	if p.Filters.Source != "" || p.Filters.Destination != "" || p.Filters.Account != "" {
		// new wildcard handling
		sb.Join(fmt.Sprintf(
			"JOIN %s postings",
			s.schema.Table(PostingsTableName),
		)).JoinOn(fmt.Sprintf("postings.txid = %s.id", TransactionsTableName))
	}
	if p.Filters.Source != "" {
		if !addressQueryRegexp.MatchString(p.Filters.Source) {
			// deprecated regex handling
			sb.Where(fmt.Sprintf("%s(postings, ?)", s.schema.Table("use_account_as_source")), p.Filters.Source)
		} else {
			// new wildcard handling
			src := strings.Split(p.Filters.Source, ":")
			sb.Where(fmt.Sprintf("jsonb_array_length(postings.source) = %d", len(src)))

			for i, segment := range src {
				if segment == ".*" || segment == "*" || segment == "" {
					continue
				}

				sb.Where(fmt.Sprintf("postings.source @@ ('$[%d] == \"' || ?::text || '\"')::jsonpath", i), segment)
			}
		}
	}
	if p.Filters.Destination != "" {
		if !addressQueryRegexp.MatchString(p.Filters.Destination) {
			// deprecated regex handling
			sb.Where(fmt.Sprintf("%s(postings, ?)", s.schema.Table("use_account_as_destination")), p.Filters.Destination)
		} else {
			// new wildcard handling
			dst := strings.Split(p.Filters.Destination, ":")
			sb.Where(fmt.Sprintf("jsonb_array_length(postings.destination) = %d", len(dst)))
			for i, segment := range dst {
				if segment == ".*" || segment == "*" || segment == "" {
					continue
				}

				sb.Where(fmt.Sprintf("postings.destination @@ ('$[%d] == \"' || ?::text || '\"')::jsonpath", i), segment)
			}
		}
	}
	if p.Filters.Account != "" {
		if !addressQueryRegexp.MatchString(p.Filters.Account) {
			// deprecated regex handling
			sb.Where(fmt.Sprintf("%s(postings, ?)", s.schema.Table("use_account")), p.Filters.Account)
		} else {
			// new wildcard handling
			dst := strings.Split(p.Filters.Account, ":")
			sb.Where(fmt.Sprintf("(jsonb_array_length(postings.destination) = %d OR jsonb_array_length(postings.source) = %d)", len(dst), len(dst)))
			for i, segment := range dst {
				if segment == ".*" || segment == "*" || segment == "" {
					continue
				}

				sb.Where(fmt.Sprintf("(postings.source @@ ('$[%d] == \"' || ?0::text || '\"')::jsonpath OR postings.destination @@ ('$[%d] == \"' || ?0::text || '\"')::jsonpath)", i, i), segment)
			}
		}
	}
	if p.Filters.Reference != "" {
		sb.Where("reference = ?", p.Filters.Reference)
	}
	if !p.Filters.StartTime.IsZero() {
		sb.Where("timestamp >= ?", p.Filters.StartTime.UTC())
	}
	if !p.Filters.EndTime.IsZero() {
		sb.Where("timestamp < ?", p.Filters.EndTime.UTC())
	}
	if p.Filters.AfterTxID > 0 {
		sb.Where("id > ?", p.Filters.AfterTxID)
	}

	for key, value := range p.Filters.Metadata {
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

	return pagination.UsingOffset(ctx, s.buildTransactionsQuery(q), storage.OffsetPaginatedQuery[storage.TransactionsQueryFilters](q),
		func(tx *core.ExpandedTransaction, scanner interface{ Scan(args ...any) error }) error {
			//TODO(gfyrag): try to use sql.Scan on ExpandedTransaction
			var ref sql.NullString
			if err := scanner.Scan(&tx.ID, &tx.Timestamp, &ref, &tx.Metadata, &tx.Postings, &tx.PreCommitVolumes, &tx.PostCommitVolumes); err != nil {
				return err
			}

			tx.Reference = ref.String
			if tx.Metadata == nil {
				tx.Metadata = metadata.Metadata{}
			}
			tx.Timestamp = tx.Timestamp.UTC()

			return nil
		})
}

func (s *Store) CountTransactions(ctx context.Context, q storage.TransactionsQuery) (uint64, error) {
	if !s.isInitialized {
		return 0, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "count_transactions")
	defer recordMetrics()

	count, err := s.buildTransactionsQuery(q).Count(ctx)
	return uint64(count), storageerrors.PostgresError(err)
}

func (s *Store) GetTransaction(ctx context.Context, txId string) (*core.ExpandedTransaction, error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_transaction")
	defer recordMetrics()

	sb := s.schema.NewSelect(TransactionsTableName).
		Model((*Transaction)(nil)).
		Column("id", "timestamp", "reference", "metadata", "postings", "pre_commit_volumes", "post_commit_volumes").
		Where("id = ?", txId).
		OrderExpr("id DESC")

	row := s.schema.QueryRowContext(ctx, sb.String())
	if row.Err() != nil {
		return nil, storageerrors.PostgresError(row.Err())
	}

	tx := core.ExpandedTransaction{
		Transaction: core.Transaction{
			TransactionData: core.TransactionData{
				Postings: core.Postings{},
				Metadata: metadata.Metadata{},
			},
		},
		PreCommitVolumes:  core.AccountsAssetsVolumes{},
		PostCommitVolumes: core.AccountsAssetsVolumes{},
	}

	var ref sql.NullString
	if err := row.Scan(&tx.ID, &tx.Timestamp, &ref, &tx.Metadata, &tx.Postings, &tx.PreCommitVolumes, &tx.PostCommitVolumes); err != nil {
		return nil, storageerrors.PostgresError(err)
	}
	tx.Timestamp = tx.Timestamp.UTC()
	tx.Reference = ref.String

	return &tx, nil
}

func (s *Store) insertTransactions(ctx context.Context, txs ...core.ExpandedTransaction) error {
	ts := make([]Transaction, len(txs))
	ps := make([]Postings, 0)

	for i, tx := range txs {
		postingsData, err := json.Marshal(tx.Postings)
		if err != nil {
			return errors.Wrap(err, "failed to marshal postings")
		}

		metadataData := []byte("{}")
		if tx.Metadata != nil {
			metadataData, err = json.Marshal(tx.Metadata)
			if err != nil {
				return errors.Wrap(err, "failed to marshal metadata")
			}
		}

		preCommitVolumesData, err := json.Marshal(tx.PreCommitVolumes)
		if err != nil {
			return errors.Wrap(err, "failed to marshal pre-commit volumes")
		}

		postCommitVolumesData, err := json.Marshal(tx.PostCommitVolumes)
		if err != nil {
			return errors.Wrap(err, "failed to marshal post-commit volumes")
		}

		ts[i].ID = tx.ID
		ts[i].Timestamp = tx.Timestamp
		ts[i].Postings = postingsData
		ts[i].Metadata = metadataData
		ts[i].PreCommitVolumes = preCommitVolumesData
		ts[i].PostCommitVolumes = postCommitVolumesData
		ts[i].Reference = ""
		if tx.Reference != "" {
			cp := tx.Reference
			ts[i].Reference = cp
		}

		for i, p := range tx.Postings {
			sourcesBy, err := json.Marshal(strings.Split(p.Source, ":"))
			if err != nil {
				return errors.Wrap(err, "failed to marshal source")
			}
			destinationsBy, err := json.Marshal(strings.Split(p.Destination, ":"))
			if err != nil {
				return errors.Wrap(err, "failed to marshal destination")
			}
			ps = append(ps, Postings{
				TxID:         tx.ID,
				PostingIndex: i,
				Source:       sourcesBy,
				Destination:  destinationsBy,
			})
		}
	}

	_, err := s.schema.NewInsert(PostingsTableName).
		Model(&ps).
		// TODO(polo/gfyrag): Current postings table does not have
		// unique indexes in txid and posting_index. It means that if we insert
		// a posting with same txid and same posting index, it will be
		// duplicated. We should fix this in the future.
		// Why this index was removed ?
		// On("CONFLICT (txid, posting_index) DO NOTHING").
		Exec(ctx)
	if err != nil {
		return storageerrors.PostgresError(err)
	}

	_, err = s.schema.NewInsert(TransactionsTableName).
		Model(&ts).
		On("CONFLICT (id) DO NOTHING").
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

func (s *Store) UpdateTransactionMetadata(ctx context.Context, id string, metadata metadata.Metadata) error {
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
		metadataData, err := json.Marshal(tx.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to marshal metadata")
		}

		txs = append(txs, &Transaction{
			ID:       tx.ID,
			Metadata: metadataData,
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
