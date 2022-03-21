package sqlstorage

import (
	"context"
	"encoding/json"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/sirupsen/logrus"
)

func (s *Store) lastMetaID(ctx context.Context, exec executor) (int64, error) {
	count, err := s.countMeta(ctx, exec)
	if err != nil {
		return 0, s.error(err)
	}
	return count - 1, nil
}

func (s *Store) LastMetaID(ctx context.Context) (int64, error) {
	return s.lastMetaID(ctx, s.schema)
}

func (s *Store) getMeta(ctx context.Context, exec executor, ty string, id string) (core.Metadata, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("meta_key", "meta_value")
	sb.From(s.schema.Table("metadata"))
	sb.Where(
		sb.And(
			sb.Equal("meta_target_type", ty),
			sb.Equal("meta_target_id", id),
		),
	)
	sb.OrderBy("meta_id").Asc()

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := exec.QueryContext(ctx, sqlq, args...)
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
	if rows.Err() != nil {
		return nil, s.error(rows.Err())
	}

	return meta, nil
}

func (s *Store) GetMeta(ctx context.Context, ty string, id string) (core.Metadata, error) {
	return s.getMeta(ctx, s.schema, ty, id)
}

func (s *Store) saveMeta(ctx context.Context, exec executor, id int64, timestamp, targetType, targetID, key, value string) error {
	ib := sqlbuilder.NewInsertBuilder()
	ib.InsertInto(s.schema.Table("metadata"))
	ib.Cols(
		"meta_id",
		"meta_target_type",
		"meta_target_id",
		"meta_key",
		"meta_value",
		"timestamp",
	)
	ib.Values(id, targetType, targetID, key, value, timestamp)

	sqlq, args := ib.BuildWithFlavor(s.schema.Flavor())
	logrus.Debugln(sqlq, args)

	_, err := exec.ExecContext(ctx, sqlq, args...)
	if err != nil {
		return s.error(err)
	}
	return nil
}

func (s *Store) SaveMeta(ctx context.Context, id int64, timestamp, targetType, targetID, key, value string) error {
	tx, err := s.schema.BeginTx(ctx, nil)
	if err != nil {
		return s.error(err)
	}

	err = s.saveMeta(ctx, tx, id, timestamp, targetType, targetID, key, value)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return s.error(err)
	}
	return nil
}
