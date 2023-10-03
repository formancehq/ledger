package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/pkg/errors"
)

func (s *Store) appendLog(ctx context.Context, log ...core.Log) error {
	var (
		query string
		args  []interface{}
	)

	switch s.Schema().Flavor() {
	case sqlbuilder.SQLite:
		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto(s.schema.Table("log"))
		ib.Cols("id", "type", "hash", "date", "data")
		for _, l := range log {
			data, err := json.Marshal(l.Data)
			if err != nil {
				panic(err)
			}

			ib.Values(l.ID, l.Type, l.Hash, l.Date, string(data))
		}
		query, args = ib.BuildWithFlavor(s.schema.Flavor())
	case sqlbuilder.PostgreSQL:
		ids := make([]uint64, len(log))
		types := make([]string, len(log))
		hashes := make([]string, len(log))
		dates := make([]time.Time, len(log))
		datas := make([][]byte, len(log))

		for i, l := range log {
			data, err := json.Marshal(l.Data)
			if err != nil {
				panic(err)
			}
			ids[i] = l.ID
			types[i] = l.Type
			hashes[i] = l.Hash
			dates[i] = l.Date
			datas[i] = data
		}

		query = fmt.Sprintf(
			`INSERT INTO "%s".log (id, type, hash, date, data) (SELECT * FROM unnest($1::int[], $2::varchar[], $3::varchar[], $4::timestamptz[], $5::jsonb[]))`,
			s.schema.Name())
		args = []interface{}{
			ids, types, hashes, dates, datas,
		}
	}

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return err
	}

	_, err = executor.ExecContext(ctx, query, args...)
	if err != nil {
		return s.error(err)
	}

	if !s.multipleInstance {
		s.lastLog = &log[len(log)-1]
	}
	return nil
}

func (s *Store) GetLastLog(ctx context.Context) (*core.Log, error) {
	// When having a single instance of the ledger, we can use the cached last log.
	// Otherwise, compute it every single time for now.
	if s.multipleInstance || s.lastLog == nil {
		sb := sqlbuilder.NewSelectBuilder()
		sb.From(s.schema.Table("log"))
		sb.Select("id", "type", "hash", "date", "data")
		sb.OrderBy("id desc")
		sb.Limit(1)

		executor, err := s.executorProvider(ctx)
		if err != nil {
			return nil, err
		}

		l := core.Log{}
		data := sql.NullString{}
		sqlq, _ := sb.BuildWithFlavor(s.schema.Flavor())
		row := executor.QueryRowContext(ctx, sqlq)
		if err := row.Scan(&l.ID, &l.Type, &l.Hash, &l.Date, &data); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		l.Date = l.Date.UTC()

		l.Data, err = core.HydrateLog(l.Type, data.String)
		if err != nil {
			return nil, err
		}
		l.Date = l.Date.UTC()

		if !s.multipleInstance {
			s.lastLog = &l
		}

		return &l, nil
	}
	return s.lastLog, nil
}

func (s *Store) GetLogs(ctx context.Context, q *ledger.LogsQuery) (api.Cursor[core.Log], error) {
	res := []core.Log{}

	if q.PageSize == 0 {
		return api.Cursor[core.Log]{Data: res}, nil
	}

	sb, t := s.buildLogsQuery(q)
	executor, err := s.executorProvider(ctx)
	if err != nil {
		return api.Cursor[core.Log]{}, err
	}

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := executor.QueryContext(ctx, sqlq, args...)
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
		PageSize:           int(q.PageSize),
		HasMore:            hasMore,
		Previous:           previous,
		Next:               next,
		Data:               res,
		PageSizeDeprecated: int(q.PageSize),
		HasMoreDeprecated:  &hasMore,
	}, nil
}

func (s *Store) buildLogsQuery(q *ledger.LogsQuery) (*sqlbuilder.SelectBuilder, LogsPaginationToken) {
	sb := sqlbuilder.NewSelectBuilder()
	t := LogsPaginationToken{}

	sb.Select("id", "type", "hash", "date", "data")
	sb.From(s.schema.Table("log"))

	if !q.Filters.StartTime.IsZero() {
		sb.Where(sb.GE("date", q.Filters.StartTime.UTC()))
		t.StartTime = q.Filters.StartTime
	}
	if !q.Filters.EndTime.IsZero() {
		sb.Where(sb.L("date", q.Filters.EndTime.UTC()))
		t.EndTime = q.Filters.EndTime
	}
	sb.OrderBy("id").Desc()

	if q.AfterID > 0 {
		sb.Where(sb.LE("id", q.AfterID))
	}

	// We fetch additional logs to know if there are more before and/or after.
	sb.Limit(int(q.PageSize + 2))
	t.PageSize = q.PageSize

	return sb, t
}
