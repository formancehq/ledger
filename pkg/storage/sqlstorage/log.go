package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/sharedapi"
	"github.com/formancehq/go-libs/sharedlogging"
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

	sharedlogging.GetLogger(ctx).Debugf("ExecContext: %s %s", query, args)
	_, err = executor.ExecContext(ctx, query, args...)
	if err != nil {
		return s.error(err)
	}
	return nil
}

func (s *Store) LastLog(ctx context.Context) (*core.Log, error) {
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

	return &l, nil
}

func (s *Store) Logs(ctx context.Context, q ledger.LogsQuery) (sharedapi.Cursor[core.Log], error) {
	res := []core.Log{}

	if q.PageSize == 0 {
		return sharedapi.Cursor[core.Log]{Data: res}, nil
	}

	sb, _ := s.buildLogsQuery(q)
	executor, err := s.executorProvider(ctx)
	if err != nil {
		return sharedapi.Cursor[core.Log]{}, err
	}

	sqlq, _ := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := executor.QueryContext(ctx, sqlq)
	if err != nil {
		return sharedapi.Cursor[core.Log]{}, s.error(err)
	}
	defer rows.Close()

	for rows.Next() {
		l := core.Log{}
		data := sql.NullString{}
		if err := rows.Scan(&l.ID, &l.Type, &l.Hash, &l.Date, &data); err != nil {
			return sharedapi.Cursor[core.Log]{}, err
		}
		l.Date = l.Date.UTC()

		l.Data, err = core.HydrateLog(l.Type, data.String)
		if err != nil {
			return sharedapi.Cursor[core.Log]{}, errors.Wrap(err, "hydrating log")
		}
		l.Date = l.Date.UTC()
		res = append(res, l)
	}
	if rows.Err() != nil {
		return sharedapi.Cursor[core.Log]{}, s.error(rows.Err())
	}

	return sharedapi.Cursor[core.Log]{Data: res}, nil
}

func (s *Store) buildLogsQuery(p ledger.LogsQuery) (*sqlbuilder.SelectBuilder, LogsPaginationToken) {
	sb := sqlbuilder.NewSelectBuilder()
	t := LogsPaginationToken{}

	sb.Select("id", "type", "hash", "date", "data")
	sb.From(s.schema.Table("log"))

	if !p.Filters.StartTime.IsZero() {
		sb.Where(sb.GE("timestamp", p.Filters.StartTime.UTC()))
		t.StartTime = p.Filters.StartTime
	}
	if !p.Filters.EndTime.IsZero() {
		sb.Where(sb.L("timestamp", p.Filters.EndTime.UTC()))
		t.EndTime = p.Filters.EndTime
	}

	return sb, t
}
