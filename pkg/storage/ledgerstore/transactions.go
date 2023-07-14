package ledgerstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/formancehq/ledger/pkg/core"
	storageerrors "github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/formancehq/stack/libs/go-libs/pointer"
	"github.com/uptrace/bun"
)

const (
	TransactionsTableName = "transactions"
	MovesTableName        = "moves"
)

type TransactionsQuery ColumnPaginatedQuery[TransactionsQueryFilters]

func NewTransactionsQuery() TransactionsQuery {
	return TransactionsQuery{
		PageSize: QueryDefaultPageSize,
		Column:   "id",
		Order:    OrderDesc,
		Filters: TransactionsQueryFilters{
			Metadata: metadata.Metadata{},
		},
	}
}

type TransactionsQueryFilters struct {
	AfterTxID   uint64            `json:"afterTxID,omitempty"`
	Reference   string            `json:"reference,omitempty"`
	Destination string            `json:"destination,omitempty"`
	Source      string            `json:"source,omitempty"`
	Account     string            `json:"account,omitempty"`
	EndTime     core.Time         `json:"endTime,omitempty"`
	StartTime   core.Time         `json:"startTime,omitempty"`
	Metadata    metadata.Metadata `json:"metadata,omitempty"`
}

func (a TransactionsQuery) WithPageSize(pageSize uint64) TransactionsQuery {
	if pageSize != 0 {
		a.PageSize = pageSize
	}

	return a
}

func (a TransactionsQuery) WithAfterTxID(after uint64) TransactionsQuery {
	a.Filters.AfterTxID = after

	return a
}

func (a TransactionsQuery) WithStartTimeFilter(start core.Time) TransactionsQuery {
	if !start.IsZero() {
		a.Filters.StartTime = start
	}

	return a
}

func (a TransactionsQuery) WithEndTimeFilter(end core.Time) TransactionsQuery {
	if !end.IsZero() {
		a.Filters.EndTime = end
	}

	return a
}

func (a TransactionsQuery) WithAccountFilter(account string) TransactionsQuery {
	a.Filters.Account = account

	return a
}

func (a TransactionsQuery) WithDestinationFilter(dest string) TransactionsQuery {
	a.Filters.Destination = dest

	return a
}

func (a TransactionsQuery) WithReferenceFilter(ref string) TransactionsQuery {
	a.Filters.Reference = ref

	return a
}

func (a TransactionsQuery) WithSourceFilter(source string) TransactionsQuery {
	a.Filters.Source = source

	return a
}

func (a TransactionsQuery) WithMetadataFilter(metadata metadata.Metadata) TransactionsQuery {
	a.Filters.Metadata = metadata

	return a
}

type Transaction struct {
	bun.BaseModel `bun:"transactions,alias:transactions"`

	ID        uint64            `bun:"id,type:bigint,pk"`
	Timestamp core.Time         `bun:"timestamp,type:timestamptz"`
	Reference string            `bun:"reference,type:varchar,unique,nullzero"`
	Moves     []Move            `bun:"rel:has-many,join:id=transaction_id"`
	Metadata  metadata.Metadata `bun:"metadata,type:jsonb,default:'{}'"`
}

func (t Transaction) toCore() core.ExpandedTransaction {
	//data, _ := json.MarshalIndent(t, "", "  ")
	//fmt.Println(string(data))
	ret := core.ExpandedTransaction{
		Transaction: core.Transaction{
			TransactionData: core.TransactionData{
				Reference: t.Reference,
				Metadata:  t.Metadata,
				Timestamp: t.Timestamp,
				Postings:  make(core.Postings, len(t.Moves)/2),
			},
			ID: t.ID,
		},
		PreCommitVolumes:  map[string]core.VolumesByAssets{},
		PostCommitVolumes: map[string]core.VolumesByAssets{},
	}
	for _, m := range t.Moves {
		ret.Postings[m.PostingIndex].Amount = (*big.Int)(m.Amount)
		ret.Postings[m.PostingIndex].Asset = m.Asset
		if m.IsSource {
			ret.Postings[m.PostingIndex].Source = m.Account
		} else {
			ret.Postings[m.PostingIndex].Destination = m.Account
		}
		if _, ok := ret.PostCommitVolumes[m.Account]; !ok {
			ret.PostCommitVolumes[m.Account] = map[string]*core.Volumes{}
			ret.PreCommitVolumes[m.Account] = map[string]*core.Volumes{}
		}
		if _, ok := ret.PostCommitVolumes[m.Account][m.Asset]; !ok {
			ret.PostCommitVolumes[m.Account][m.Asset] = core.NewEmptyVolumes()
			ret.PreCommitVolumes[m.Account][m.Asset] = core.NewEmptyVolumes()
		}

		ret.PostCommitVolumes[m.Account][m.Asset].Output = NewInt().Set(m.PostCommitOutputVolume).ToMathBig()
		ret.PostCommitVolumes[m.Account][m.Asset].Input = NewInt().Set(m.PostCommitInputVolume).ToMathBig()
		if m.IsSource {
			ret.PreCommitVolumes[m.Account][m.Asset].Output = NewInt().Sub(m.PostCommitOutputVolume, m.Amount).ToMathBig()
			ret.PreCommitVolumes[m.Account][m.Asset].Input = NewInt().Set(m.PostCommitInputVolume).ToMathBig()
		} else {
			ret.PreCommitVolumes[m.Account][m.Asset].Output = NewInt().Set(m.PostCommitOutputVolume).ToMathBig()
			ret.PreCommitVolumes[m.Account][m.Asset].Input = NewInt().Sub(m.PostCommitInputVolume, m.Amount).ToMathBig()
		}
	}
	return ret
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

type Move struct {
	bun.BaseModel `bun:"moves,alias:m"`

	TransactionID          uint64    `bun:"transaction_id,type:bigint" json:"transaction_id"`
	Amount                 *Int      `bun:"amount,type:bigint" json:"amount"`
	Asset                  string    `bun:"asset,type:varchar" json:"asset"`
	Account                string    `bun:"account,type:varchar" json:"account"`
	AccountArray           []string  `bun:"account_array,type:jsonb" json:"account_array"`
	PostingIndex           uint8     `bun:"posting_index,type:int8" json:"posting_index"`
	IsSource               bool      `bun:"is_source,type:bool" json:"is_source"`
	Timestamp              core.Time `bun:"timestamp,type:timestamp" json:"timestamp"`
	PostCommitInputVolume  *Int      `bun:"post_commit_input_value,type:numeric" json:"post_commit_input_value"`
	PostCommitOutputVolume *Int      `bun:"post_commit_output_value,type:numeric" json:"post_commit_output_value"`
}

func (s *Store) buildTransactionsQuery(p TransactionsQueryFilters, models *[]Transaction) *bun.SelectQuery {

	selectMatchingTransactions := s.schema.NewSelect(TransactionsTableName).
		ColumnExpr("distinct on(transactions.id) transactions.id as transaction_id")
	if p.Reference != "" {
		selectMatchingTransactions.Where("transactions.reference = ?", p.Reference)
	}
	if !p.StartTime.IsZero() {
		selectMatchingTransactions.Where("transactions.timestamp >= ?", p.StartTime)
	}
	if !p.EndTime.IsZero() {
		selectMatchingTransactions.Where("transactions.timestamp < ?", p.EndTime)
	}
	if p.AfterTxID != 0 {
		selectMatchingTransactions.Where("transactions.id > ?", p.AfterTxID)
	}
	if p.Metadata != nil && len(p.Metadata) > 0 {
		selectMatchingTransactions.Where("transactions.metadata @> ?", p.Metadata)
	}
	if p.Source != "" || p.Destination != "" || p.Account != "" {
		selectMatchingTransactions.Join(fmt.Sprintf("join %s m on transactions.id = m.transaction_id", s.schema.Table("moves")))
		if p.Source != "" {
			parts := strings.Split(p.Source, ":")
			selectMatchingTransactions.Where(fmt.Sprintf("m.is_source and jsonb_array_length(m.account_array) = %d", len(parts)))
			for index, segment := range parts {
				if len(segment) == 0 {
					continue
				}
				selectMatchingTransactions.Where(fmt.Sprintf(`m.account_array @@ ('$[%d] == "%s"')`, index, segment))
			}
		}
		if p.Destination != "" {
			parts := strings.Split(p.Destination, ":")
			selectMatchingTransactions.Where(fmt.Sprintf("not m.is_source and jsonb_array_length(m.account_array) = %d", len(parts)))
			for index, segment := range parts {
				if len(segment) == 0 {
					continue
				}
				selectMatchingTransactions.Where(fmt.Sprintf(`m.account_array @@ ('$[%d] == "%s"')`, index, segment))
			}
		}
		if p.Account != "" {
			parts := strings.Split(p.Account, ":")
			selectMatchingTransactions.Where(fmt.Sprintf("jsonb_array_length(m.account_array) = %d", len(parts)))
			for index, segment := range parts {
				if len(segment) == 0 {
					continue
				}
				selectMatchingTransactions.Where(fmt.Sprintf(`m.account_array @@ ('$[%d] == "%s"')`, index, segment))
			}
		}
	}

	return s.schema.NewSelect(TransactionsTableName).
		Model(models).
		Column("transactions.id", "transactions.reference", "transactions.metadata", "transactions.timestamp").
		ColumnExpr(`json_agg(json_build_object(
			'posting_index', m.posting_index,
			'transaction_id', m.transaction_id,
			'account', m.account,
			'account_array', m.account_array,
			'asset', m.asset,
			'post_commit_input_value', m.post_commit_input_value,
			'post_commit_output_value', m.post_commit_output_value,
			'timestamp', m.timestamp,
			'amount', m.amount,
			'is_source', m.is_source
		)) as moves`).
		Join(fmt.Sprintf("join %s m on transactions.id = m.transaction_id", s.schema.Table("moves"))).
		Join(fmt.Sprintf(`join (%s) ids on ids.transaction_id = transactions.id`, selectMatchingTransactions.String())).
		Group("transactions.id")
}

func (s *Store) GetTransactions(ctx context.Context, q TransactionsQuery) (*api.Cursor[core.ExpandedTransaction], error) {
	cursor, err := UsingColumn[TransactionsQueryFilters, Transaction](ctx,
		s.buildTransactionsQuery, ColumnPaginatedQuery[TransactionsQueryFilters](q),
	)
	if err != nil {
		return nil, err
	}

	return api.MapCursor(cursor, Transaction.toCore), nil
}

func (s *Store) CountTransactions(ctx context.Context, q TransactionsQuery) (uint64, error) {
	models := make([]Transaction, 0)
	count, err := s.buildTransactionsQuery(q.Filters, &models).Count(ctx)

	return uint64(count), storageerrors.PostgresError(err)
}

func (s *Store) GetTransaction(ctx context.Context, txId uint64) (*core.ExpandedTransaction, error) {
	tx := &Transaction{}
	err := s.schema.NewSelect(TransactionsTableName).
		Model(tx).
		Relation("Moves", func(query *bun.SelectQuery) *bun.SelectQuery {
			return query.With("moves", s.schema.NewSelect(MovesTableName))
		}).
		Where("id = ?", txId).
		OrderExpr("id DESC").
		Scan(ctx)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}

	return pointer.For(tx.toCore()), nil

}

func (s *Store) InsertTransactions(ctx context.Context, txs ...core.Transaction) error {

	ts := make([]Transaction, len(txs))

	for i, tx := range txs {
		ts[i].ID = tx.ID
		ts[i].Timestamp = tx.Timestamp
		ts[i].Metadata = tx.Metadata
		ts[i].Reference = ""
		if tx.Reference != "" {
			cp := tx.Reference
			ts[i].Reference = cp
		}
	}

	_, err := s.schema.NewInsert(TransactionsTableName).
		Model(&ts).
		On("CONFLICT (id) DO NOTHING").
		Exec(ctx)

	return storageerrors.PostgresError(err)
}

func (s *Store) InsertMoves(ctx context.Context, objects ...*core.Move) error {
	type moveValue struct {
		Move
		AmountInputBefore        *big.Int `bun:"amount_input_before,type:numeric"`
		AmountOutputBefore       *big.Int `bun:"amount_output_before,type:numeric"`
		AccumulatedPostingAmount *big.Int `bun:"accumulated_posting_amount,type:numeric"`
	}

	transactionIds := make([]uint64, 0)
	moves := make([]moveValue, 0)

	sort.Slice(objects, func(i, j int) bool {
		if objects[i].Timestamp.Equal(objects[j].Timestamp) {
			if objects[i].TransactionID == objects[j].TransactionID {
				if objects[i].PostingIndex == objects[j].PostingIndex {
					if objects[i].IsSource {
						return false
					}
				}
				return objects[i].PostingIndex < objects[j].PostingIndex
			}
			return objects[i].TransactionID < objects[j].TransactionID
		}
		return objects[i].Timestamp.Before(objects[j].Timestamp)
	})

	var (
		accumulatedAmounts                    core.AccountsAssetsVolumes
		actualTransactionID                   *uint64
		actualAccumulatedVolumesOnTransaction core.AccountsAssetsVolumes
	)

	for i := 0; i < len(objects); {

		if actualTransactionID == nil || objects[i].TransactionID != *actualTransactionID {
			actualTransactionID = &objects[i].TransactionID
			actualAccumulatedVolumesOnTransaction = core.AccountsAssetsVolumes{}
			transactionIds = append(transactionIds, *actualTransactionID)
		}

		for j := i; j < len(objects) && objects[j].TransactionID == *actualTransactionID; j++ {
			if objects[j].IsSource {
				actualAccumulatedVolumesOnTransaction.AddOutput(objects[j].Account, objects[j].Asset, objects[j].Amount)
			} else {
				actualAccumulatedVolumesOnTransaction.AddInput(objects[j].Account, objects[j].Asset, objects[j].Amount)
			}
		}

		j := i
		for ; j < len(objects) && objects[j].TransactionID == *actualTransactionID; j++ {
			if objects[j].IsSource {
				moves = append(moves, moveValue{
					Move: Move{
						TransactionID: *actualTransactionID,
						Amount:        (*Int)(objects[j].Amount),
						Asset:         objects[j].Asset,
						Account:       objects[j].Account,
						AccountArray:  strings.Split(objects[j].Account, ":"),
						PostingIndex:  objects[j].PostingIndex,
						IsSource:      true,
						Timestamp:     objects[j].Timestamp,
					},
					AmountOutputBefore:       accumulatedAmounts.GetVolumes(objects[j].Account, objects[j].Asset).Output,
					AmountInputBefore:        accumulatedAmounts.GetVolumes(objects[j].Account, objects[j].Asset).Input,
					AccumulatedPostingAmount: actualAccumulatedVolumesOnTransaction.GetVolumes(objects[j].Account, objects[j].Asset).Output,
				})
			} else {
				moves = append(moves, moveValue{
					Move: Move{
						TransactionID: *actualTransactionID,
						Amount:        (*Int)(objects[j].Amount),
						Asset:         objects[j].Asset,
						Account:       objects[j].Account,
						AccountArray:  strings.Split(objects[j].Account, ":"),
						PostingIndex:  objects[j].PostingIndex,
						IsSource:      false,
						Timestamp:     objects[j].Timestamp,
					},
					AmountOutputBefore:       accumulatedAmounts.GetVolumes(objects[j].Account, objects[j].Asset).Output,
					AmountInputBefore:        accumulatedAmounts.GetVolumes(objects[j].Account, objects[j].Asset).Input,
					AccumulatedPostingAmount: actualAccumulatedVolumesOnTransaction.GetVolumes(objects[j].Account, objects[j].Asset).Input,
				})
			}

			if objects[j].IsSource {
				accumulatedAmounts.AddOutput(objects[j].Account, objects[j].Asset, objects[j].Amount)
			} else {
				accumulatedAmounts.AddInput(objects[j].Account, objects[j].Asset, objects[j].Amount)
			}
		}

		i = j
	}

	type insertedMove struct {
		TransactionID uint64 `bun:"transaction_id"`
		PostingIndex  uint8  `bun:"posting_index"`
		IsSource      bool   `bun:"is_source"`
	}
	insertedMoves := make([]insertedMove, 0)

	tx, err := s.schema.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	err = tx.NewInsert(MovesTableName).
		With("cte1", s.schema.NewValues(&moves)).
		Column(
			"posting_index",
			"transaction_id",
			"account",
			"post_commit_input_value",
			"post_commit_output_value",
			"timestamp",
			"asset",
			"account_array",
			"amount",
			"is_source",
		).
		TableExpr(fmt.Sprintf(`
			(select cte1.posting_index, cte1.transaction_id::numeric, cte1.account, coalesce(
				(select post_commit_input_value
				from %s
				where account = cte1.account and asset = cte1.asset and timestamp <= cte1.timestamp
				order by timestamp desc, transaction_id desc
				limit 1)
			, 0) + cte1.amount_input_before + (case when not cte1.is_source then cte1.accumulated_posting_amount else 0 end) as post_commit_input_value, coalesce(
				(select post_commit_output_value
				from %s
				where account = cte1.account and asset = cte1.asset and timestamp <= cte1.timestamp
				order by timestamp desc, transaction_id desc
				limit 1)
			, 0) + cte1.amount_output_before + (case when cte1.is_source then cte1.accumulated_posting_amount else 0 end) as post_commit_output_value, cte1.timestamp, cte1.asset, cte1.account_array, cte1.amount, cte1.is_source
			from cte1) data
		`, s.schema.Table(MovesTableName), s.schema.Table(MovesTableName))).
		On("CONFLICT DO NOTHING").
		Returning("transaction_id, posting_index, is_source").
		Scan(ctx, &insertedMoves)
	if err != nil {
		return storageerrors.PostgresError(err)
	}

	if len(insertedMoves) != len(moves) { // Some conflict (maybe after a crash?), we need to filter already inserted moves
		ind := 0
	l:
		for _, move := range moves {
			for _, insertedMove := range insertedMoves {
				if move.TransactionID == insertedMove.TransactionID &&
					move.PostingIndex == insertedMove.PostingIndex &&
					move.IsSource == insertedMove.IsSource {
					ind++
					continue l
				}
			}
			if ind < len(moves)-1 {
				moves = append(moves[:ind], moves[ind+1:]...)
			} else {
				moves = moves[:ind]
			}
		}
	}

	if len(moves) > 0 {
		_, err = tx.NewUpdate(MovesTableName).
			With("cte1", s.schema.NewValues(&moves)).
			Set("post_commit_output_value = moves.post_commit_output_value + (case when cte1.is_source then cte1.amount else 0 end)").
			Set("post_commit_input_value = moves.post_commit_input_value + (case when not cte1.is_source then cte1.amount else 0 end)").
			Table("cte1").
			Where("moves.timestamp > cte1.timestamp and moves.account = cte1.account and moves.asset = cte1.asset and moves.transaction_id not in (?)", bun.In(transactionIds)).
			Exec(ctx)
		if err != nil {
			return storageerrors.PostgresError(err)
		}
	}

	return storageerrors.PostgresError(tx.Commit())
}

func (s *Store) UpdateTransactionsMetadata(ctx context.Context, transactionsWithMetadata ...core.TransactionWithMetadata) error {
	txs := make([]*Transaction, 0, len(transactionsWithMetadata))
	for _, tx := range transactionsWithMetadata {
		txs = append(txs, &Transaction{
			ID:       tx.ID,
			Metadata: tx.Metadata,
		})
	}

	_, err := s.schema.NewUpdate(TransactionsTableName).
		With("_data", s.schema.NewValues(&txs)).
		Model((*Transaction)(nil)).
		TableExpr("_data").
		Set("metadata = transactions.metadata || _data.metadata").
		Where(fmt.Sprintf("%s.id = _data.id", TransactionsTableName)).
		Exec(ctx)

	return storageerrors.PostgresError(err)
}
