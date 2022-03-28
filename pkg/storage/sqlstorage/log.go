package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
	"time"
)

func (s *Store) appendLog(ctx context.Context, exec executor, log ...core.Log) (map[int]error, bool) {
	ret := make(map[int]error)
	hasError := false
	for i, l := range log {

		data, err := json.Marshal(l.Data)
		if err != nil {
			panic(err)
		}

		ib := sqlbuilder.NewInsertBuilder()
		ib.Cols("id", "type", "hash", "date", "data")
		ib.InsertInto(s.schema.Table("log"))
		ib.Values(l.ID, l.Type, l.Hash, l.Date, string(data))

		sql, args := ib.BuildWithFlavor(s.schema.Flavor())
		_, err = exec.ExecContext(ctx, sql, args...)
		if err != nil {
			hasError = true
			ret[i] = s.error(err)
		}
	}
	return ret, hasError
}

func (s *Store) AppendLog(ctx context.Context, logs ...core.Log) (map[int]error, error) {
	tx, err := s.schema.BeginTx(ctx, nil)
	if err != nil {
		return nil, s.error(err)
	}
	defer tx.Rollback()

	ret, hasError := s.appendLog(ctx, tx, logs...)
	if hasError {
		return ret, storage.ErrAborted
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Store) lastLog(ctx context.Context, exec executor) (*core.Log, error) {

	sb := sqlbuilder.NewSelectBuilder()
	sb.From(s.schema.Table("log"))
	sb.Select("id", "type", "hash", "date", "data")
	sb.OrderBy("id desc")
	sb.Limit(1)

	sqlq, _ := sb.BuildWithFlavor(s.schema.Flavor())
	row := exec.QueryRowContext(ctx, sqlq)
	l := core.Log{}
	var (
		ts   sql.NullString
		data sql.NullString
	)
	err := row.Scan(&l.ID, &l.Type, &l.Hash, &ts, &data)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	t, err := time.Parse(time.RFC3339, ts.String)
	if err != nil {
		return nil, err
	}
	l.Date = t
	l.Data, err = core.HydrateLog(l.Type, data.String)
	if err != nil {
		return nil, err
	}
	return &l, nil
}

func (s *Store) LastLog(ctx context.Context) (*core.Log, error) {
	return s.lastLog(ctx, s.schema)
}

func (s *Store) logs(ctx context.Context, exec executor) ([]core.Log, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.From(s.schema.Table("log"))
	sb.Select("id", "type", "hash", "date", "data")
	sb.OrderBy("id desc")

	sqlq, _ := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := exec.QueryContext(ctx, sqlq)
	if err != nil {
		return nil, s.error(err)
	}
	defer rows.Close()

	ret := make([]core.Log, 0)
	for rows.Next() {
		l := core.Log{}
		var (
			ts   sql.NullString
			data sql.NullString
		)

		err := rows.Scan(&l.ID, &l.Type, &l.Hash, &ts, &data)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		fmt.Println(data.String)
		t, err := time.Parse(time.RFC3339, ts.String)
		if err != nil {
			return nil, err
		}
		l.Date = t
		l.Data, err = core.HydrateLog(l.Type, data.String)
		if err != nil {
			return nil, errors.Wrap(err, "hydrating log")
		}
		ret = append(ret, l)
	}
	if rows.Err() != nil {
		return nil, s.error(rows.Err())
	}

	return ret, nil
}

func (s *Store) Logs(ctx context.Context) ([]core.Log, error) {
	return s.logs(ctx, s.schema)
}
