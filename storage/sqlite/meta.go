package sqlite

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/core"
)

func (s *SQLiteStore) InjectMeta(ty string, id string, fn func(core.Metadata)) {
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
	fmt.Println(sqlq, args)

	rows, err := s.db.Query(sqlq, args...)

	if err != nil {
		log.Println(err)
		return
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
			log.Println(err)

			return
		}

		var value json.RawMessage

		err = json.Unmarshal([]byte(meta_value), &value)

		if err != nil {
			log.Println(err)

			return
		}

		meta[meta_key] = value
	}

	fn(meta)
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
