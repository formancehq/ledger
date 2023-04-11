package ledger

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	storageerrors "github.com/formancehq/ledger/pkg/storage/sqlstorage/errors"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/pagination"
	"github.com/formancehq/stack/libs/go-libs/api"
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

	ID        uint64    `bun:"id,unique,type:bigint"`
	Type      int16     `bun:"type,type:smallint"`
	Hash      string    `bun:"hash,type:varchar(256)"`
	Date      core.Time `bun:"date,type:timestamptz"`
	Data      []byte    `bun:"data,type:bytea"`
	Reference string    `bun:"reference,type:text"`
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

func (s *Store) batchLogs(ctx context.Context, logs []*core.Log) error {
	recordMetrics := s.instrumentalized(ctx, "batch_logs")
	defer recordMetrics()

	previousLog, err := s.GetLastLog(ctx)
	if err != nil && !storage.IsNotFoundError(err) {
		return errors.Wrap(err, "reading last log")
	}

	txn, err := s.schema.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return storageerrors.PostgresError(err)
	}

	// Beware: COPY query is not supported by bun if the pgx driver is used.
	stmt, err := txn.Prepare(pq.CopyInSchema(
		s.schema.Name(),
		"logs_v2",
		"id", "type", "hash", "date", "data", "reference",
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
		if previousLog != nil {
			id = previousLog.ID + 1
		}
		logs[i].ID = id
		logs[i].Hash = core.Hash(previousLog, &logs[i])

		ls[i].ID = id
		ls[i].Type = int16(l.Type)
		ls[i].Hash = logs[i].Hash
		ls[i].Date = l.Date
		ls[i].Data = data
		ls[i].Reference = l.Reference

		previousLog = logs[i]
		_, err = stmt.Exec(ls[i].ID, ls[i].Type, ls[i].Hash, ls[i].Date, RawMessage(ls[i].Data), ls[i].Reference)
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
		Column("id", "type", "hash", "date", "data", "reference").
		OrderExpr("id desc").
		Limit(1).
		Scan(ctx)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}

	payload, err := core.HydrateLog(core.LogType(raw.Type), raw.Data)
	if err != nil {
		return nil, errors.Wrap(err, "hydrating log data")
	}

	l := &core.Log{
		ID:        raw.ID,
		Type:      core.LogType(raw.Type),
		Data:      payload,
		Hash:      raw.Hash,
		Date:      raw.Date.UTC(),
		Reference: raw.Reference,
	}

	return l, nil
}

func (s *Store) GetLogs(ctx context.Context, q storage.LogsQuery) (*api.Cursor[core.Log], error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_logs")
	defer recordMetrics()

	return pagination.UsingColumn[storage.LogsQueryFilters, core.Log](ctx,
		s.buildLogsQuery(q.Filters),
		storage.ColumnPaginatedQuery[storage.LogsQueryFilters](q),
		func(log *core.Log, scanner interface{ Scan(args ...any) error }) (uint64, error) {
			var raw LogsV2
			err := scanner.Scan(&raw.ID, &raw.Type, &raw.Hash, &raw.Date, &raw.Data, &raw.Reference)
			if err != nil {
				return 0, err
			}

			payload, err := core.HydrateLog(core.LogType(raw.Type), raw.Data)
			if err != nil {
				return 0, errors.Wrap(err, "hydrating log data")
			}

			log.ID = raw.ID
			log.Type = core.LogType(raw.Type)
			log.Data = payload
			log.Hash = raw.Hash
			log.Date = raw.Date.UTC()
			log.Reference = raw.Reference

			return log.ID, nil
		})
}

func (s *Store) buildLogsQuery(q storage.LogsQueryFilters) *bun.SelectQuery {
	sb := s.schema.NewSelect(LogTableName).
		Model((*LogsV2)(nil)).
		Column("id", "type", "hash", "date", "data", "reference")

	if !q.StartTime.IsZero() {
		sb.Where("date >= ?", q.StartTime.UTC())
	}
	if !q.EndTime.IsZero() {
		sb.Where("date < ?", q.EndTime.UTC())
	}
	if q.AfterID > 0 {
		sb.Where("id < ?", q.AfterID)
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

	logs := make([]core.Log, len(rawLogs))
	for index, rawLog := range rawLogs {
		payload, err := core.HydrateLog(core.LogType(rawLog.Type), rawLog.Data)
		if err != nil {
			return nil, errors.Wrap(err, "hydrating log data")
		}
		logs[index] = core.Log{
			ID:        rawLog.ID,
			Type:      core.LogType(rawLog.Type),
			Hash:      rawLog.Hash,
			Date:      rawLog.Date,
			Data:      payload,
			Reference: rawLog.Reference,
		}
	}

	return logs, nil
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

func (s *Store) ReadLogWithReference(ctx context.Context, reference string) (*core.Log, error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "read_log_with_reference")
	defer recordMetrics()

	raw := &LogsV2{}
	err := s.schema.
		NewSelect(LogTableName).
		Where("reference = ?", reference).
		Model(raw).
		Limit(1).
		Scan(ctx)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}

	payload, err := core.HydrateLog(core.LogType(raw.Type), raw.Data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to hydrate log")
	}

	return &core.Log{
		ID:        raw.ID,
		Type:      core.LogType(raw.Type),
		Data:      payload,
		Hash:      raw.Hash,
		Date:      raw.Date,
		Reference: raw.Reference,
	}, nil
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

	payload, err := core.HydrateLog(core.LogType(raw.Type), raw.Data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to hydrate log")
	}

	return &core.Log{
		ID:        raw.ID,
		Type:      core.LogType(raw.Type),
		Data:      payload,
		Hash:      raw.Hash,
		Date:      raw.Date,
		Reference: raw.Reference,
	}, nil
}
