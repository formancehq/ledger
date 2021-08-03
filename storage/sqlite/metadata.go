package sqlite

import (
	"encoding/json"
	"fmt"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/core"
	"github.com/spf13/viper"
)

func (s *SQLiteStore) GetMeta(ty string, id string) (core.Metadata, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select(
		"meta_key",
		"meta_value",
	)
	sb.From("metadata")
	sb.Where(
		sb.And(
			sb.Equal("meta_target_type", ty),
			sb.Equal("meta_target_id", id),
		),
	)

	sqlq, args := sb.BuildWithFlavor(sqlbuilder.SQLite)
	if viper.GetBool("debug") {
		fmt.Println(sqlq, args)
	}

	rows, err := s.db.Query(sqlq, args...)

	if err != nil {
		return nil, err
	}

	meta := core.Metadata{}

	for rows.Next() {
		var meta_key string
		var meta_value string

		err := rows.Scan(
			&meta_key,
			&meta_value,
		)

		if err != nil {
			return nil, err
		}

		var value json.RawMessage

		err = json.Unmarshal([]byte(meta_value), &value)

		if err != nil {
			return nil, err
		}

		meta[meta_key] = value
	}

	return meta, nil
}

func (s *SQLiteStore) SaveMeta(ty string, id string, m core.Metadata) error {
	tx, _ := s.db.Begin()

	for key, value := range m {
		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto("metadata")
		ib.Cols(
			"meta_target_type",
			"meta_target_id",
			"meta_key",
			"meta_value",
		)
		ib.Values(ty, id, key, string(value))

		sqlq, args := ib.BuildWithFlavor(sqlbuilder.SQLite)

		_, err := tx.Exec(sqlq, args...)

		if err != nil {
			tx.Rollback()

			return err
		}
	}

	err := tx.Commit()

	if err != nil {
		return err
	}

	return nil
}
