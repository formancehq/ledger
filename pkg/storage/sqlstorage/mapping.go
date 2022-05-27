package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
)

// We have only one mapping for a ledger, so hardcode the id
const mappingId = "0000"

func (s *Store) loadMapping(ctx context.Context, exec executor) (*core.Mapping, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("mapping").From(s.schema.Table("mapping"))

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	row := exec.QueryRowContext(ctx, sqlq, args...)

	var mappingString string
	if err := row.Scan(&mappingString); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	m := core.Mapping{}
	if err := json.Unmarshal([]byte(mappingString), &m); err != nil {
		return nil, err
	}

	return &m, nil
}

func (s *Store) LoadMapping(ctx context.Context) (*core.Mapping, error) {
	return s.loadMapping(ctx, s.schema)
}

func (s *Store) saveMapping(ctx context.Context, exec executor, mapping core.Mapping) error {
	data, err := json.Marshal(mapping)
	if err != nil {
		return err
	}

	ib := sqlbuilder.NewInsertBuilder()
	ib.InsertInto(s.schema.Table("mapping"))
	ib.Cols("mapping_id", "mapping")
	ib.Values(mappingId, string(data))

	var (
		sqlq string
		args []interface{}
	)
	switch s.schema.Flavor() {
	case sqlbuilder.Flavor(PostgreSQL):
		sqlq, args = ib.BuildWithFlavor(s.schema.Flavor())
		sqlq += " ON CONFLICT (mapping_id) DO UPDATE SET mapping = $2"
	default:
		ib.ReplaceInto(s.schema.Table("mapping"))
		sqlq, args = ib.BuildWithFlavor(s.schema.Flavor())
	}

	_, err = exec.ExecContext(ctx, sqlq, args...)
	return s.error(err)
}

func (s *Store) SaveMapping(ctx context.Context, mapping core.Mapping) error {
	return s.saveMapping(ctx, s.schema, mapping)
}
