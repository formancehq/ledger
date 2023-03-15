package ledger

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const LogTableName = "log"

type Log struct {
	bun.BaseModel `bun:"log,alias:log"`

	ID   uint64          `bun:"id,unique,type:bigint"`
	Type string          `bun:"type,type:varchar"`
	Hash string          `bun:"hash,type:varchar"`
	Date time.Time       `bun:"date,type:timestamptz"`
	Data json.RawMessage `bun:"data,type:jsonb"`
}

type LogsPaginationToken struct {
	AfterID   uint64    `json:"after"`
	PageSize  uint      `json:"pageSize,omitempty"`
	StartTime time.Time `json:"startTime,omitempty"`
	EndTime   time.Time `json:"endTime,omitempty"`
}

type RawMessage json.RawMessage

func (j RawMessage) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return string(j), nil
}

func (s *Store) batchLogs(ctx context.Context, logs []core.Log) error {
	previousLog, err := s.GetLastLog(ctx)
	if err != nil {
		return errors.Wrap(err, "reading last log")
	}

	txn, err := s.schema.Begin()
	if err != nil {
		return err
	}

	// Beware: COPY query is not supported by bun if the pgx driver is used.
	stmt, err := txn.Prepare(fmt.Sprintf("COPY \"%s\".log (id, type, hash, date, data) FROM STDIN", s.schema.Name()))
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
		logs[i].Hash = core.Hash(previousLog, &l)

		ls[i].ID = id
		ls[i].Type = l.Type
		ls[i].Hash = l.Hash
		ls[i].Date = l.Date
		ls[i].Data = data

		previousLog = &logs[i]
		_, err = stmt.Exec(ls[i].ID, ls[i].Type, ls[i].Hash, ls[i].Date, RawMessage(ls[i].Data))
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

func (s *Store) AppendLogs(ctx context.Context, logs ...core.Log) <-chan error {
	return s.logsBatchWorker.WriteModels(ctx, logs)
}

func (s *Store) GetLastLog(ctx context.Context) (*core.Log, error) {
	sb := s.schema.NewSelect(LogTableName).
		Model((*Log)(nil)).
		Column("id", "type", "hash", "date", "data").
		OrderExpr("id desc").
		Limit(1)

	l := core.Log{}
	data := sql.NullString{}
	row := s.schema.QueryRowContext(ctx, sb.String())
	if err := row.Scan(&l.ID, &l.Type, &l.Hash, &l.Date, &data); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	l.Date = l.Date.UTC()

	var err error
	l.Data, err = core.HydrateLog(l.Type, data.String)
	if err != nil {
		return nil, err
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
		if err := rows.Scan(&l.ID, &l.Type, &l.Hash, &l.Date, &data); err != nil {
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
		Column("id", "type", "hash", "date", "data")

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
