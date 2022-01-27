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

func (s *Store) LoadMapping(ctx context.Context) (*core.Mapping, error) {

	sb := sqlbuilder.NewSelectBuilder()
	sb.
		Select("mapping").
		From(s.table("mapping"))

	sqlq, args := sb.BuildWithFlavor(s.flavor)

	rows, err := s.db.QueryContext(
		ctx,
		sqlq,
		args...,
	)
	if err != nil {
		return nil, s.error(err)
	}
	if !rows.Next() {
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

func (s *Store) SaveMapping(ctx context.Context, mapping core.Mapping) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return s.error(err)
	}

	data, err := json.Marshal(mapping)
	if err != nil {
		return err
	}

	ib := sqlbuilder.NewInsertBuilder()
	ib.InsertInto(s.table("mapping"))
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
		ib.ReplaceInto(s.table("mapping"))
		sqlq, args = ib.BuildWithFlavor(s.flavor)
	}

	logrus.Debugln(sqlq, args)

	_, err = tx.ExecContext(ctx, sqlq, args...)
	if err != nil {
		tx.Rollback()

		return s.error(err)
	}
	return tx.Commit()
}
