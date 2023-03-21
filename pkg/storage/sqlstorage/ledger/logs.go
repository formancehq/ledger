package ledger

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const (
	LogTableName          = "log"
	LogIngestionTableName = "logs_ingestion"
)

type Log struct {
	bun.BaseModel `bun:"log,alias:log"`

	ID        uint64          `bun:"id,unique,type:bigint"`
	Type      string          `bun:"type,type:varchar"`
	Hash      string          `bun:"hash,type:varchar"`
	Date      core.Time       `bun:"date,type:timestamptz"`
	Data      json.RawMessage `bun:"data,type:jsonb"`
	Reference string          `bun:"reference,type:varchar"`
}

type LogsIngestion struct {
	bun.BaseModel `bun:"logs_ingestion,alias:logs_ingestion"`

	OnerowId bool   `bun:"onerow_id,pk,type:bool,default:true"`
	LogId    uint64 `bun:"log_id,type:bigint"`
}

type LogsPaginationToken struct {
	AfterID   uint64    `json:"after"`
	PageSize  uint      `json:"pageSize,omitempty"`
	StartTime core.Time `json:"startTime,omitempty"`
	EndTime   core.Time `json:"endTime,omitempty"`
}

type RawMessage json.RawMessage

func (j RawMessage) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return string(j), nil
}

func (s *Store) batchLogs(ctx context.Context, logs []*core.Log) error {
	previousLog, err := s.GetLastLog(ctx)
	if err != nil {
		return errors.Wrap(err, "reading last log")
	}

	txn, err := s.schema.Begin()
	if err != nil {
		return err
	}

	// Beware: COPY query is not supported by bun if the pgx driver is used.
	stmt, err := txn.Prepare(pq.CopyInSchema(
		s.schema.Name(),
		"log",
		"id", "type", "hash", "date", "data", "reference",
	))
	if err != nil {
		return err
	}

	ls := make([]Log, len(logs))
	for i, l := range logs {
		data, err := json.Marshal(l.Data)
		if err != nil {
			panic(err)
		}

		id := uint64(0)
		if previousLog != nil {
			id = previousLog.ID + 1
		}
		logs[i].ID = id
		logs[i].Hash = core.Hash(previousLog, &logs[i])

		ls[i].ID = id
		ls[i].Type = l.Type
		ls[i].Hash = logs[i].Hash
		ls[i].Date = l.Date
		ls[i].Data = data
		ls[i].Reference = l.Reference

		previousLog = logs[i]
		_, err = stmt.Exec(ls[i].ID, ls[i].Type, ls[i].Hash, ls[i].Date, RawMessage(ls[i].Data), ls[i].Reference)
		if err != nil {
			return s.error(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return s.error(err)
	}

	err = stmt.Close()
	if err != nil {
		return s.error(err)
	}

	return s.error(txn.Commit())
}

func (s *Store) AppendLog(ctx context.Context, log *core.Log) error {
	return <-s.logsBatchWorker.WriteModels(ctx, log)
}

func (s *Store) GetLastLog(ctx context.Context) (*core.Log, error) {
	sb := s.schema.NewSelect(LogTableName).
		Model((*Log)(nil)).
		Column("id", "type", "hash", "date", "data", "reference").
		OrderExpr("id desc").
		Limit(1)

	l := core.Log{}
	data := sql.NullString{}
	row := s.schema.QueryRowContext(ctx, sb.String())
	if err := row.Scan(&l.ID, &l.Type, &l.Hash, &l.Date, &data, &l.Reference); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, errors.Wrap(err, "scanning log")
	}
	l.Date = l.Date.UTC()

	var err error
	l.Data, err = core.HydrateLog(l.Type, data.String)
	if err != nil {
		return nil, errors.Wrap(err, "hydrating log")
	}
	l.Date = l.Date.UTC()

	return &l, nil
}

func (s *Store) GetLogs(ctx context.Context, q *storage.LogsQuery) (api.Cursor[core.Log], error) {
	res := []core.Log{}

	if q.PageSize == 0 {
		return api.Cursor[core.Log]{Data: res}, nil
	}

	sb, t := s.buildLogsQuery(q)

	rows, err := s.schema.QueryContext(ctx, sb.String())
	if err != nil {
		return api.Cursor[core.Log]{}, s.error(err)
	}
	defer rows.Close()

	for rows.Next() {
		l := core.Log{}
		data := sql.NullString{}
		if err := rows.Scan(&l.ID, &l.Type, &l.Hash, &l.Date, &data, &l.Reference); err != nil {
			return api.Cursor[core.Log]{}, err
		}
		l.Date = l.Date.UTC()

		l.Data, err = core.HydrateLog(l.Type, data.String)
		if err != nil {
			return api.Cursor[core.Log]{}, errors.Wrap(err, "hydrating log")
		}
		l.Date = l.Date.UTC()
		res = append(res, l)
	}
	if rows.Err() != nil {
		return api.Cursor[core.Log]{}, s.error(rows.Err())
	}

	var previous, next string

	// Page with logs before
	if q.AfterID > 0 && len(res) > 1 && res[0].ID == q.AfterID {
		t.AfterID = res[0].ID + uint64(q.PageSize)
		res = res[1:]
		raw, err := json.Marshal(t)
		if err != nil {
			return api.Cursor[core.Log]{}, s.error(err)
		}
		previous = base64.RawURLEncoding.EncodeToString(raw)
	}

	// Page with logs after
	if len(res) > int(q.PageSize) {
		res = res[:q.PageSize]
		t.AfterID = res[len(res)-1].ID
		raw, err := json.Marshal(t)
		if err != nil {
			return api.Cursor[core.Log]{}, s.error(err)
		}
		next = base64.RawURLEncoding.EncodeToString(raw)
	}

	hasMore := next != ""
	return api.Cursor[core.Log]{
		PageSize: int(q.PageSize),
		HasMore:  hasMore,
		Previous: previous,
		Next:     next,
		Data:     res,
	}, nil
}

func (s *Store) buildLogsQuery(q *storage.LogsQuery) (*bun.SelectQuery, LogsPaginationToken) {
	t := LogsPaginationToken{}
	sb := s.schema.NewSelect(LogTableName).
		Model((*Log)(nil)).
		Column("id", "type", "hash", "date", "data", "reference")

	if !q.Filters.StartTime.IsZero() {
		sb.Where("date >= ?", q.Filters.StartTime.UTC())
		t.StartTime = q.Filters.StartTime
	}

	if !q.Filters.EndTime.IsZero() {
		sb.Where("date < ?", q.Filters.EndTime.UTC())
		t.EndTime = q.Filters.EndTime
	}

	sb.OrderExpr("id DESC")

	if q.AfterID > 0 {
		sb.Where("id <= ?", q.AfterID)
	}

	// We fetch additional logs to know if there are more before and/or after.
	sb.Limit(int(q.PageSize + 2))
	t.PageSize = q.PageSize

	return sb, t
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
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return logID, nil
}

func (s *Store) GetNextLogID(ctx context.Context) (uint64, error) {
	return s.getNextLogID(ctx, &s.schema)
}

func (s *Store) ReadLogsStartingFromID(ctx context.Context, id uint64) ([]core.Log, error) {
	return s.readLogsStartingFromID(ctx, &s.schema, id)
}

func (s *Store) readLogsStartingFromID(ctx context.Context, exec interface {
	NewSelect(tableName string) *bun.SelectQuery
}, id uint64) ([]core.Log, error) {

	rawLogs := make([]Log, 0)
	err := exec.
		NewSelect(LogTableName).
		Where("id >= ?", id).
		Model(&rawLogs).
		Scan(ctx)
	if err != nil {
		return nil, err
	}
	logs := make([]core.Log, len(rawLogs))
	for index, rawLog := range rawLogs {
		payload, err := core.HydrateLog(rawLog.Type, string(rawLog.Data))
		if err != nil {
			return nil, errors.Wrap(err, "hydrating log")
		}
		logs[index] = core.Log{
			ID:        rawLog.ID,
			Type:      rawLog.Type,
			Hash:      rawLog.Hash,
			Date:      rawLog.Date,
			Data:      payload,
			Reference: rawLog.Reference,
		}
	}

	return logs, nil
}

func (s *Store) UpdateNextLogID(ctx context.Context, id uint64) error {
	_, err := s.schema.
		NewInsert(LogIngestionTableName).
		Model(&LogsIngestion{
			LogId: id,
		}).
		On("CONFLICT (onerow_id) DO UPDATE").
		Set("log_id = EXCLUDED.log_id").
		Exec(ctx)
	return err
}

func (s *Store) ReadLogWithReference(ctx context.Context, reference string) (*core.Log, error) {
	raw := &Log{}
	err := s.schema.
		NewSelect(LogTableName).
		Where("reference = ?", reference).
		Model(raw).
		Limit(1).
		Scan(ctx)
	if err != nil {
		return nil, err
	}
	payload, err := core.HydrateLog(raw.Type, string(raw.Data))
	if err != nil {
		return nil, errors.Wrap(err, "hydrating log")
	}
	return &core.Log{
		ID:        raw.ID,
		Type:      raw.Type,
		Data:      payload,
		Hash:      raw.Hash,
		Date:      raw.Date,
		Reference: raw.Reference,
	}, nil
}

func (s *Store) ReadLastLogWithType(ctx context.Context, logType ...string) (*core.Log, error) {
	raw := &Log{}
	err := s.schema.
		NewSelect(LogTableName).
		Where("type IN (?)", bun.In(logType)).
		OrderExpr("date DESC").
		Model(raw).
		Limit(1).
		Scan(ctx)
	if err != nil {
		return nil, err
	}

	payload, err := core.HydrateLog(raw.Type, string(raw.Data))
	if err != nil {
		return nil, errors.Wrap(err, "hydrating log")
	}

	return &core.Log{
		ID:        raw.ID,
		Type:      raw.Type,
		Data:      payload,
		Hash:      raw.Hash,
		Date:      raw.Date,
		Reference: raw.Reference,
	}, nil
}
