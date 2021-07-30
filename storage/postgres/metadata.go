package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/core"
)

func (s *PGStore) InjectMeta(ty string, id string, fn func(core.Metadata)) {
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

	sqlq, args := sb.BuildWithFlavor(sqlbuilder.PostgreSQL)
	fmt.Println(sqlq, args)

	rows, err := s.Conn().Query(
		context.TODO(),
		sqlq,
		args...,
	)

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

func (s *PGStore) SaveMeta(ty string, id string, m core.Metadata) error {
	tx, _ := s.Conn().Begin(context.TODO())

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

		sqlq, args := ib.BuildWithFlavor(sqlbuilder.PostgreSQL)

		_, err := tx.Exec(
			context.TODO(),
			sqlq,
			args...,
		)

		if err != nil {
			tx.Rollback(context.TODO())

			return err
		}
	}

	err := tx.Commit(context.TODO())

	if err != nil {
		return err
	}

	return nil
}
