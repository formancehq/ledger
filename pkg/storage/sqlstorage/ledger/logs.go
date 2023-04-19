package ledger

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	storageerrors "github.com/formancehq/ledger/pkg/storage/sqlstorage/errors"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/pagination"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const (
	LogTableName          = "logs_v2"
	LogIngestionTableName = "logs_ingestion"
)

type LogsV2 struct {
	bun.BaseModel `bun:"logs_v2,alias:logs_v2"`

	ID   uint64    `bun:"id,unique,type:bigint"`
	Type int16     `bun:"type,type:smallint"`
	Hash []byte    `bun:"hash,type:bytea"`
	Date core.Time `bun:"date,type:timestamptz"`
	Data []byte    `bun:"data,type:jsonb"`
}

func (log LogsV2) toCore() core.Log {
	payload, err := core.HydrateLog(core.LogType(log.Type), log.Data)
	if err != nil {
		panic(errors.Wrap(err, "hydrating log data"))
	}

	return core.Log{
		ID:   log.ID,
		Type: core.LogType(log.Type),
		Data: payload,
		Hash: log.Hash,
		Date: log.Date.UTC(),
	}
}

type LogsIngestion struct {
	bun.BaseModel `bun:"logs_ingestion,alias:logs_ingestion"`

	OnerowId bool   `bun:"onerow_id,pk,type:bool,default:true"`
	LogId    uint64 `bun:"log_id,type:bigint"`
}

type RawMessage json.RawMessage

func (j RawMessage) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return string(j), nil
}

func (s *Store) initLastLog(ctx context.Context) {
	s.once.Do(func() {
		var err error
		s.previousLog, err = s.GetLastLog(ctx)
		if err != nil && !storage.IsNotFoundError(err) {
			panic(errors.Wrap(err, "reading last log"))
		}
	})
}

func (s *Store) batchLogs(ctx context.Context, logs []*core.Log) error {
	recordMetrics := s.instrumentalized(ctx, "batch_logs")
	defer recordMetrics()

	s.initLastLog(ctx)

	txn, err := s.schema.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return storageerrors.PostgresError(err)
	}

	// Beware: COPY query is not supported by bun if the pgx driver is used.
	stmt, err := txn.Prepare(pq.CopyInSchema(
		s.schema.Name(),
		"logs_v2",
		"id", "type", "hash", "date", "data",
	))
	if err != nil {
		return storageerrors.PostgresError(err)
	}

	ls := make([]LogsV2, len(logs))
	for i, l := range logs {
		data, err := json.Marshal(l.Data)
		if err != nil {
			return errors.Wrap(err, "marshaling log data")
		}

		id := uint64(0)
		if s.previousLog != nil {
			id = s.previousLog.ID + 1
		}
		logs[i].ID = id
		logs[i].ComputeHash(s.previousLog)

		ls[i].ID = id
		ls[i].Type = int16(l.Type)
		ls[i].Hash = logs[i].Hash
		ls[i].Date = l.Date
		ls[i].Data = data

		s.previousLog = logs[i]
		_, err = stmt.Exec(ls[i].ID, ls[i].Type, ls[i].Hash, ls[i].Date, RawMessage(ls[i].Data))
		if err != nil {
			return storageerrors.PostgresError(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return storageerrors.PostgresError(err)
	}

	err = stmt.Close()
	if err != nil {
		return storageerrors.PostgresError(err)
	}

	return storageerrors.PostgresError(txn.Commit())
}

func (s *Store) AppendLog(ctx context.Context, log *core.Log) error {
	if !s.isInitialized {
		return storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "append_log")
	defer recordMetrics()

	return <-s.logsBatchWorker.WriteModels(ctx, log)
}

func (s *Store) GetLastLog(ctx context.Context) (*core.Log, error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_last_log")
	defer recordMetrics()

	raw := &LogsV2{}
	err := s.schema.NewSelect(LogTableName).
		Model(raw).
		OrderExpr("id desc").
		Limit(1).
		Scan(ctx)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}

	l := raw.toCore()
	return &l, nil
}

func (s *Store) GetLogs(ctx context.Context, q storage.LogsQuery) (*api.Cursor[core.Log], error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_logs")
	defer recordMetrics()

	cursor, err := pagination.UsingColumn[storage.LogsQueryFilters, LogsV2](ctx,
		s.buildLogsQuery,
		storage.ColumnPaginatedQuery[storage.LogsQueryFilters](q),
	)
	if err != nil {
		return nil, err
	}

	return api.MapCursor(cursor, LogsV2.toCore), nil
}

func (s *Store) buildLogsQuery(q storage.LogsQueryFilters, models *[]LogsV2) *bun.SelectQuery {
	sb := s.schema.NewSelect(LogTableName).
		Model(models).
		Column("id", "type", "hash", "date", "data")

	if !q.StartTime.IsZero() {
		sb.Where("date >= ?", q.StartTime.UTC())
	}
	if !q.EndTime.IsZero() {
		sb.Where("date < ?", q.EndTime.UTC())
	}

	return sb
}

func (s *Store) getNextLogID(ctx context.Context, sq interface {
	NewSelect(string) *bun.SelectQuery
}) (uint64, error) {
	var logID uint64
	err := sq.
		NewSelect(LogIngestionTableName).
		Model((*LogsIngestion)(nil)).
		Column("log_id").
		Limit(1).
		Scan(ctx, &logID)
	if err != nil {
		return 0, storageerrors.PostgresError(err)
	}

	return logID, nil
}

func (s *Store) GetNextLogID(ctx context.Context) (uint64, error) {
	if !s.isInitialized {
		return 0, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_next_log_id")
	defer recordMetrics()

	return s.getNextLogID(ctx, &s.schema)
}

func (s *Store) ReadLogsRange(ctx context.Context, idMin, idMax uint64) ([]core.Log, error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "read_logs_starting_from_id")
	defer recordMetrics()

	return s.readLogsRange(ctx, &s.schema, idMin, idMax)
}

func (s *Store) readLogsRange(ctx context.Context, exec interface {
	NewSelect(tableName string) *bun.SelectQuery
}, idMin, idMax uint64) ([]core.Log, error) {

	rawLogs := make([]LogsV2, 0)
	err := exec.
		NewSelect(LogTableName).
		Where("id >= ?", idMin).
		Where("id < ?", idMax).
		Model(&rawLogs).
		Scan(ctx)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}

	return collectionutils.Map(rawLogs, LogsV2.toCore), nil
}

func (s *Store) UpdateNextLogID(ctx context.Context, id uint64) error {
	if !s.isInitialized {
		return storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "update_next_log_id")
	defer recordMetrics()

	_, err := s.schema.
		NewInsert(LogIngestionTableName).
		Model(&LogsIngestion{
			LogId: id,
		}).
		On("CONFLICT (onerow_id) DO UPDATE").
		Set("log_id = EXCLUDED.log_id").
		Exec(ctx)

	return storageerrors.PostgresError(err)
}

func (s *Store) ReadLastLogWithType(ctx context.Context, logTypes ...core.LogType) (*core.Log, error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "read_last_log_with_type")
	defer recordMetrics()

	raw := &LogsV2{}
	err := s.schema.
		NewSelect(LogTableName).
		Where("type IN (?)", bun.In(logTypes)).
		OrderExpr("date DESC").
		Model(raw).
		Limit(1).
		Scan(ctx)

	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}
	ret := raw.toCore()

	return &ret, nil
}

func (s *Store) ReadLogForCreatedTransactionWithReference(ctx context.Context, reference string) (*core.Log, error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "read_log_for_created_transaction_with_reference")
	defer recordMetrics()

	raw := &LogsV2{}
	err := s.schema.NewSelect(LogTableName).
		Model(raw).
		OrderExpr("id desc").
		Limit(1).
		Where("data->'transaction'->>'reference' = ?", reference).
		Scan(ctx)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}

	l := raw.toCore()
	return &l, nil
}

func (s *Store) ReadLogForCreatedTransaction(ctx context.Context, txID uint64) (*core.Log, error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "read_log_for_created_transaction")
	defer recordMetrics()

	raw := &LogsV2{}
	err := s.schema.NewSelect(LogTableName).
		Model(raw).
		OrderExpr("id desc").
		Limit(1).
		Where("data->'transaction'->>'txid' = ?", fmt.Sprint(txID)).
		Scan(ctx)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}

	l := raw.toCore()
	return &l, nil
}

func (s *Store) ReadLogForRevertedTransaction(ctx context.Context, txID uint64) (*core.Log, error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "read_log_for_reverted_transaction")
	defer recordMetrics()

	raw := &LogsV2{}
	err := s.schema.NewSelect(LogTableName).
		Model(raw).
		OrderExpr("id desc").
		Limit(1).
		Where("data->>'revertedTransactionID' = ?", fmt.Sprint(txID)).
		Scan(ctx)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}

	l := raw.toCore()
	return &l, nil
}
