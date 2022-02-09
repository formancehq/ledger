package sqlstorage

import (
	"context"
	"encoding/json"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/sirupsen/logrus"
)

func (s *Store) LastMetaID(ctx context.Context) (int64, error) {
	count, err := s.CountMeta(ctx)
	if err != nil {
		return 0, s.error(err)
	}
	return count - 1, nil
}

func (s *Store) GetMeta(ctx context.Context, ty string, id string) (core.Metadata, error) {
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
	sb.OrderBy("meta_id").Asc()

	sqlq, args := sb.BuildWithFlavor(s.flavor)

	rows, err := s.db.QueryContext(ctx, sqlq, args...)

	if err != nil {
		return nil, s.error(err)
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
			return nil, s.error(err)
		}

		var value json.RawMessage

		err = json.Unmarshal([]byte(metaValue), &value)

		if err != nil {
			return nil, s.error(err)
		}

		meta[metaKey] = value
	}

	return meta, nil
}

func (s *Store) SaveMeta(ctx context.Context, id int64, timestamp, targetType, targetID, key, value string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return s.error(err)
	}

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
	ib.Values(
		id,
		targetType,
		targetID,
		key,
		value,
		timestamp,
	)

	sqlq, args := ib.BuildWithFlavor(s.flavor)
	logrus.Debugln(sqlq, args)

	_, err = tx.ExecContext(ctx, sqlq, args...)
	if err != nil {
		return s.error(err)
	}

	err = tx.Commit()

	if err != nil {
		return err
	}

	return nil
}
