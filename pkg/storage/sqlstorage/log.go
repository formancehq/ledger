package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/core"
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
	var (
		l    core.Log
		data sql.NullString
	)

	sb := sqlbuilder.NewSelectBuilder()
	sb.From(s.schema.Table("log"))
	sb.Select("id", "type", "hash", "date", "data")
	sb.OrderBy("id desc")
	sb.Limit(1)

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return nil, err
	}

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

func (s *Store) Logs(ctx context.Context) ([]core.Log, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.From(s.schema.Table("log"))
	sb.Select("id", "type", "hash", "date", "data")
	sb.OrderBy("id desc")

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return nil, err
	}

	sqlq, _ := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := executor.QueryContext(ctx, sqlq)
	if err != nil {
		return nil, s.error(err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			panic(err)
		}
	}(rows)

	ret := make([]core.Log, 0)
	for rows.Next() {
		l := core.Log{}
		var (
			data sql.NullString
		)

		err := rows.Scan(&l.ID, &l.Type, &l.Hash, &l.Date, &data)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		l.Date = l.Date.UTC()

		l.Data, err = core.HydrateLog(l.Type, data.String)
		if err != nil {
			return nil, errors.Wrap(err, "hydrating log")
		}
		l.Date = l.Date.UTC()
		ret = append(ret, l)
	}
	if rows.Err() != nil {
		return nil, s.error(rows.Err())
	}

	return ret, nil
}
