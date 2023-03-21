package ledger

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/api"
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

type Transactions struct {
	bun.BaseModel `bun:"transactions,alias:transactions"`

	ID                uint64          `bun:"id,type:bigint,unique"`
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

	TxID         uint64          `bun:"txid,type:bigint"`
	PostingIndex int             `bun:"posting_index,type:integer"`
	Source       json.RawMessage `bun:"source,type:jsonb"`
	Destination  json.RawMessage `bun:"destination,type:jsonb"`
}

type TxsPaginationToken struct {
	AfterTxID         uint64            `json:"after"`
	ReferenceFilter   string            `json:"reference,omitempty"`
	AccountFilter     string            `json:"account,omitempty"`
	SourceFilter      string            `json:"source,omitempty"`
	DestinationFilter string            `json:"destination,omitempty"`
	StartTime         core.Time         `json:"startTime,omitempty"`
	EndTime           core.Time         `json:"endTime,omitempty"`
	MetadataFilter    map[string]string `json:"metadata,omitempty"`
	PageSize          uint              `json:"pageSize,omitempty"`
}

func (s *Store) buildTransactionsQuery(ctx context.Context, p storage.TransactionsQuery) (*bun.SelectQuery, TxsPaginationToken) {
	sb := s.schema.NewSelect(TransactionsTableName).Model((*Transactions)(nil))
	t := TxsPaginationToken{}

	var (
		destination = p.Filters.Destination
		source      = p.Filters.Source
		account     = p.Filters.Account
		reference   = p.Filters.Reference
		startTime   = p.Filters.StartTime
		endTime     = p.Filters.EndTime
		metadata    = p.Filters.Metadata
	)

	sb.Column("id", "timestamp", "reference", "metadata", "postings", "pre_commit_volumes", "post_commit_volumes").
		Distinct()
	if source != "" || destination != "" || account != "" {
		// new wildcard handling
		sb.Join(fmt.Sprintf(
			"JOIN %s postings",
			s.schema.Table(PostingsTableName),
		)).JoinOn(fmt.Sprintf("postings.txid = %s.id", TransactionsTableName))
	}
	if source != "" {
		if !addressQueryRegexp.MatchString(source) {
			// deprecated regex handling
			sb.Where(fmt.Sprintf("%s(postings, ?)", s.schema.Table("use_account_as_source")), source)
		} else {
			// new wildcard handling
			src := strings.Split(source, ":")
			sb.Where(fmt.Sprintf("jsonb_array_length(postings.source) = %d", len(src)))

			for i, segment := range src {
				if segment == ".*" || segment == "*" || segment == "" {
					continue
				}

				sb.Where(fmt.Sprintf("postings.source @@ ('$[%d] == \"' || ?::text || '\"')::jsonpath", i), segment)
			}
		}
		t.SourceFilter = source
	}
	if destination != "" {
		if !addressQueryRegexp.MatchString(destination) {
			// deprecated regex handling
			sb.Where(fmt.Sprintf("%s(postings, ?)", s.schema.Table("use_account_as_destination")), destination)
		} else {
			// new wildcard handling
			dst := strings.Split(destination, ":")
			sb.Where(fmt.Sprintf("jsonb_array_length(postings.destination) = %d", len(dst)))
			for i, segment := range dst {
				if segment == ".*" || segment == "*" || segment == "" {
					continue
				}

				sb.Where(fmt.Sprintf("postings.destination @@ ('$[%d] == \"' || ?::text || '\"')::jsonpath", i), segment)
			}
		}
		t.DestinationFilter = destination
	}
	if account != "" {
		if !addressQueryRegexp.MatchString(account) {
			// deprecated regex handling
			sb.Where(fmt.Sprintf("%s(postings, ?)", s.schema.Table("use_account")), account)
		} else {
			// new wildcard handling
			dst := strings.Split(account, ":")
			sb.Where(fmt.Sprintf("(jsonb_array_length(postings.destination) = %d OR jsonb_array_length(postings.source) = %d)", len(dst), len(dst)))
			for i, segment := range dst {
				if segment == ".*" || segment == "*" || segment == "" {
					continue
				}

				sb.Where(fmt.Sprintf("(postings.source @@ ('$[%d] == \"' || ?0::text || '\"')::jsonpath OR postings.destination @@ ('$[%d] == \"' || ?0::text || '\"')::jsonpath)", i, i), segment)
			}
		}
		t.AccountFilter = account
	}
	if reference != "" {
		sb.Where("reference = ?", reference)
		t.ReferenceFilter = reference
	}
	if !startTime.IsZero() {
		sb.Where("timestamp >= ?", startTime.UTC())
		t.StartTime = startTime
	}
	if !endTime.IsZero() {
		sb.Where("timestamp < ?", endTime.UTC())
		t.EndTime = endTime
	}

	for key, value := range metadata {
		sb.Where(s.schema.Table(
			fmt.Sprintf("%s(metadata, ?, '%s')",
				SQLCustomFuncMetaCompare, strings.ReplaceAll(key, ".", "', '")),
		), value)
	}
	t.MetadataFilter = metadata

	return sb, t
}

func (s *Store) GetTransactions(ctx context.Context, q storage.TransactionsQuery) (api.Cursor[core.ExpandedTransaction], error) {

	txs := make([]core.ExpandedTransaction, 0)

	if q.PageSize == 0 {
		return api.Cursor[core.ExpandedTransaction]{Data: txs}, nil
	}

	sb, t := s.buildTransactionsQuery(ctx, q)
	sb.OrderExpr("id DESC")
	if q.AfterTxID > 0 {
		sb.Where("id <= ?", q.AfterTxID)
	}

	// We fetch additional transactions to know if there are more before and/or after.
	sb.Limit(int(q.PageSize + 2))
	t.PageSize = q.PageSize

	rows, err := s.schema.QueryContext(ctx, sb.String())
	if err != nil {
		return api.Cursor[core.ExpandedTransaction]{}, s.error(err)
	}
	defer rows.Close()

	for rows.Next() {
		var ref sql.NullString
		tx := core.ExpandedTransaction{}
		if err := rows.Scan(
			&tx.ID,
			&tx.Timestamp,
			&ref,
			&tx.Metadata,
			&tx.Postings,
			&tx.PreCommitVolumes,
			&tx.PostCommitVolumes,
		); err != nil {
			return api.Cursor[core.ExpandedTransaction]{}, err
		}
		tx.Reference = ref.String
		if tx.Metadata == nil {
			tx.Metadata = core.Metadata{}
		}
		tx.Timestamp = tx.Timestamp.UTC()
		txs = append(txs, tx)
	}
	if rows.Err() != nil {
		return api.Cursor[core.ExpandedTransaction]{}, s.error(err)
	}

	var previous, next string

	// Page with transactions before
	if q.AfterTxID > 0 && len(txs) > 1 && txs[0].ID == q.AfterTxID {
		t.AfterTxID = txs[0].ID + uint64(q.PageSize)
		txs = txs[1:]
		raw, err := json.Marshal(t)
		if err != nil {
			return api.Cursor[core.ExpandedTransaction]{}, s.error(err)
		}
		previous = base64.RawURLEncoding.EncodeToString(raw)
	}

	// Page with transactions after
	if len(txs) > int(q.PageSize) {
		txs = txs[:q.PageSize]
		t.AfterTxID = txs[len(txs)-1].ID
		raw, err := json.Marshal(t)
		if err != nil {
			return api.Cursor[core.ExpandedTransaction]{}, s.error(err)
		}
		next = base64.RawURLEncoding.EncodeToString(raw)
	}

	hasMore := next != ""
	return api.Cursor[core.ExpandedTransaction]{
		PageSize: int(q.PageSize),
		HasMore:  hasMore,
		Previous: previous,
		Next:     next,
		Data:     txs,
	}, nil
}

func (s *Store) CountTransactions(ctx context.Context, q storage.TransactionsQuery) (uint64, error) {
	sb, _ := s.buildTransactionsQuery(ctx, q)
	count, err := sb.Count(ctx)
	return uint64(count), s.error(err)
}

func (s *Store) GetTransaction(ctx context.Context, txId uint64) (*core.ExpandedTransaction, error) {
	sb := s.schema.NewSelect(TransactionsTableName).
		Model((*Transactions)(nil)).
		Column("id", "timestamp", "reference", "metadata", "postings", "pre_commit_volumes", "post_commit_volumes").
		Where("id = ?", txId).
		OrderExpr("id DESC")

	row := s.schema.QueryRowContext(ctx, sb.String())
	if row.Err() != nil {
		return nil, s.error(row.Err())
	}

	tx := core.ExpandedTransaction{
		Transaction: core.Transaction{
			TransactionData: core.TransactionData{
				Postings: core.Postings{},
				Metadata: core.Metadata{},
			},
		},
		PreCommitVolumes:  core.AccountsAssetsVolumes{},
		PostCommitVolumes: core.AccountsAssetsVolumes{},
	}

	var ref sql.NullString
	if err := row.Scan(
		&tx.ID,
		&tx.Timestamp,
		&ref,
		&tx.Metadata,
		&tx.Postings,
		&tx.PreCommitVolumes,
		&tx.PostCommitVolumes,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	tx.Timestamp = tx.Timestamp.UTC()
	tx.Reference = ref.String

	return &tx, nil
}

func (s *Store) GetLastTransaction(ctx context.Context) (*core.ExpandedTransaction, error) {
	sb := s.schema.NewSelect(TransactionsTableName).
		Model((*Transactions)(nil)).
		Column("id", "timestamp", "reference", "metadata", "postings", "pre_commit_volumes", "post_commit_volumes").
		OrderExpr("id DESC").
		Limit(1)

	row := s.schema.QueryRowContext(ctx, sb.String())
	if row.Err() != nil {
		return nil, s.error(row.Err())
	}

	tx := core.ExpandedTransaction{
		Transaction: core.Transaction{
			TransactionData: core.TransactionData{
				Postings: core.Postings{},
				Metadata: core.Metadata{},
			},
		},
		PreCommitVolumes:  core.AccountsAssetsVolumes{},
		PostCommitVolumes: core.AccountsAssetsVolumes{},
	}

	var ref sql.NullString
	if err := row.Scan(
		&tx.ID,
		&tx.Timestamp,
		&ref,
		&tx.Metadata,
		&tx.Postings,
		&tx.PreCommitVolumes,
		&tx.PostCommitVolumes,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	tx.Timestamp = tx.Timestamp.UTC()
	tx.Reference = ref.String

	return &tx, nil
}

func (s *Store) insertTransactions(ctx context.Context, txs ...core.ExpandedTransaction) error {
	ts := make([]Transactions, len(txs))
	ps := make([]Postings, 0)

	for i, tx := range txs {
		postingsData, err := json.Marshal(tx.Postings)
		if err != nil {
			panic(err)
		}

		metadataData := []byte("{}")
		if tx.Metadata != nil {
			metadataData, err = json.Marshal(tx.Metadata)
			if err != nil {
				panic(err)
			}
		}

		preCommitVolumesData, err := json.Marshal(tx.PreCommitVolumes)
		if err != nil {
			panic(err)
		}

		postCommitVolumesData, err := json.Marshal(tx.PostCommitVolumes)
		if err != nil {
			panic(err)
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
				panic(err)
			}
			destinationsBy, err := json.Marshal(strings.Split(p.Destination, ":"))
			if err != nil {
				panic(err)
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
		return s.error(err)
	}

	_, err = s.schema.NewInsert(TransactionsTableName).
		Model(&ts).
		On("CONFLICT (id) DO NOTHING").
		Exec(ctx)
	if err != nil {
		return s.error(err)
	}

	return nil
}

func (s *Store) InsertTransactions(ctx context.Context, txs ...core.ExpandedTransaction) error {
	return s.insertTransactions(ctx, txs...)
}

func (s *Store) UpdateTransactionMetadata(ctx context.Context, id uint64, metadata core.Metadata) error {
	metadataData, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	_, err = s.schema.NewUpdate(TransactionsTableName).
		Model((*Transactions)(nil)).
		Set("metadata = metadata || ?", string(metadataData)).
		Where("id = ?", id).
		Exec(ctx)
	return err
}
