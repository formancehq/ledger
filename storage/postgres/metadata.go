package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/core"
	"github.com/spf13/viper"
)

func (s *PGStore) GetMeta(ty string, id string) (core.Metadata, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select(
		"meta_key",
		"meta_value",
	)
	sb.From(s.table("metadata"))
	sb.Where(
		sb.And(
			sb.Equal("meta_target_type", ty),
			sb.Equal("meta_target_id", id),
		),
	)

	sqlq, args := sb.BuildWithFlavor(sqlbuilder.PostgreSQL)
	if viper.GetBool("debug") {
		fmt.Println(sqlq, args)
	}

	rows, err := s.Conn().Query(
		context.TODO(),
		sqlq,
		args...,
	)

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

func (s *PGStore) SaveMeta(ty string, id string, m core.Metadata) error {
	tx, _ := s.Conn().Begin(context.TODO())

	for key, value := range m {
		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto(s.table("metadata"))
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
