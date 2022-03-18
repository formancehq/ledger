package sqlstorage

import (
	"context"
	"encoding/json"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/sirupsen/logrus"
)

// We have only one mapping for a ledger, so hardcode the id
const mappingId = "0000"

func (s *Store) loadMapping(ctx context.Context, exec executor) (*core.Mapping, error) {

	sb := sqlbuilder.NewSelectBuilder()
	sb.
		Select("mapping").
		From(s.Table("mapping"))

	sqlq, args := sb.BuildWithFlavor(s.flavor)

	rows, err := exec.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return nil, s.error(err)
	}
	if !rows.Next() {
		if rows.Err() != nil {
			return nil, s.error(rows.Err())
		}
		return nil, nil
	}

	var (
		mappingString string
	)

	err = rows.Scan(&mappingString)
	if err != nil {
		return nil, err
	}

	m := &core.Mapping{}
	err = json.Unmarshal([]byte(mappingString), m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (s *Store) LoadMapping(ctx context.Context) (*core.Mapping, error) {
	return s.loadMapping(ctx, s.db)
}

func (s *Store) saveMapping(ctx context.Context, exec executor, mapping core.Mapping) error {

	data, err := json.Marshal(mapping)
	if err != nil {
		return err
	}

	ib := sqlbuilder.NewInsertBuilder()
	ib.InsertInto(s.Table("mapping"))
	ib.Cols("mapping_id", "mapping")
	ib.Values(mappingId, string(data))

	var (
		sqlq string
		args []interface{}
	)
	switch s.flavor {
	case sqlbuilder.Flavor(PostgreSQL):
		sqlq, args = ib.BuildWithFlavor(s.flavor)
		sqlq += " ON CONFLICT (mapping_id) DO UPDATE SET mapping = $2"
	default:
		ib.ReplaceInto(s.Table("mapping"))
		sqlq, args = ib.BuildWithFlavor(s.flavor)
	}

	logrus.Debugln(sqlq, args)

	_, err = exec.ExecContext(ctx, sqlq, args...)
	return s.error(err)
}

func (s *Store) SaveMapping(ctx context.Context, mapping core.Mapping) error {
	return s.saveMapping(ctx, s.db, mapping)
}
