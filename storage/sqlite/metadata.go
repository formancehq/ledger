package sqlite

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/core"
	"github.com/sirupsen/logrus"
)

func (s *SQLiteStore) LastMetaID(ctx context.Context) (int64, error) {
	count, err := s.CountMeta(ctx)
	if err != nil {
		return 0, err
	}
	return count - 1, nil
}

func (s *SQLiteStore) GetMeta(ctx context.Context, ty string, id string) (core.Metadata, error) {
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
	logrus.Debugln(sqlq, args)

	rows, err := s.db.QueryContext(ctx, sqlq, args...)

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

func (s *SQLiteStore) SaveMeta(ctx context.Context, id int64, timestamp, targetType, targetID, key, value string) error {
	tx, _ := s.db.Begin()

	ib := sqlbuilder.NewInsertBuilder()
	ib.InsertInto("metadata")
	ib.Cols(
		"meta_id",
		"meta_target_type",
		"meta_target_id",
		"meta_key",
		"meta_value",
		"timestamp",
	)
	ib.Values(
		id,
		targetType,
		targetID,
		key,
		value,
		timestamp,
	)

	sqlq, args := ib.BuildWithFlavor(sqlbuilder.SQLite)
	logrus.Debugln(sqlq, args)

	_, err := tx.ExecContext(ctx, sqlq, args...)

	if err != nil {
		fmt.Println("failed to save metadata", err)
		tx.Rollback()

		return err
	}

	err = tx.Commit()

	if err != nil {
		return err
	}

	return nil
}
