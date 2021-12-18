package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/core"
	"github.com/spf13/viper"
)

func (s *PGStore) GetMeta(ctx context.Context, ty string, id string) (core.Metadata, error) {
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
		ctx,
		sqlq,
		args...,
	)

	if err != nil {
		return nil, err
	}

	meta := core.Metadata{}

	for rows.Next() {
		var metaKey string
		var metaValue string

		err := rows.Scan(
			&metaKey,
			&metaValue,
		)

		if err != nil {
			return nil, err
		}

		var value json.RawMessage

		err = json.Unmarshal([]byte(metaValue), &value)

		if err != nil {
			return nil, err
		}

		meta[metaKey] = value
	}

	return meta, nil
}

func (s *PGStore) SaveMeta(ctx context.Context, id int64, timestamp, targetType, targetID, key, value string) error {
	tx, _ := s.Conn().Begin(ctx)

	ib := sqlbuilder.NewInsertBuilder()
	ib.InsertInto(s.table("metadata"))
	ib.Cols(
		"meta_id",
		"meta_target_type",
		"meta_target_id",
		"meta_key",
		"meta_value",
		"timestamp",
	)
	ib.Values(id, targetType, targetID, key, string(value), timestamp)

	sqlq, args := ib.BuildWithFlavor(sqlbuilder.PostgreSQL)

	_, err := tx.Exec(
		ctx,
		sqlq,
		args...,
	)

	if err != nil {
		tx.Rollback(ctx)

		return err
	}

	err = tx.Commit(ctx)

	if err != nil {
		return err
	}

	return nil
}

func (s *PGStore) LastMetaID(ctx context.Context) (int64, error) {
	count, err := s.CountMeta(ctx)
	if err != nil {
		return 0, err
	}
	return count - 1, nil
}
