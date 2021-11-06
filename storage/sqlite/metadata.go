package sqlite

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
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

func (s *SQLiteStore) SaveMeta(id, timestamp, targetType, targetID, key, value string) error {
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
		string(value),
		timestamp,
	)

	sqlq, args := ib.BuildWithFlavor(sqlbuilder.SQLite)
	if viper.GetBool("debug") {
		fmt.Println(sqlq, args)
	}

	_, err := tx.Exec(sqlq, args...)

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

func (s *SQLiteStore) FindMeta(q query.Query) (query.Cursor, error) {
	q.Limit = int(math.Max(-1, math.Min(float64(q.Limit), 100))) + 1

	c := query.Cursor{}

	in := sqlbuilder.NewSelectBuilder()
	in.Select(
		"meta_id",
		"meta_target_type",
		"meta_target_id",
		"meta_key",
		"meta_value",
	)
	in.From("metadata")
	in.OrderBy("meta_id desc")
	in.Limit(q.Limit)

	sqlq, args := in.BuildWithFlavor(sqlbuilder.SQLite)
	if viper.GetBool("debug") {
		fmt.Println(sqlq, args)
	}

	rows, err := s.db.Query(
		sqlq,
		args...,
	)

	if err != nil {
		return c, err
	}

	foundMetadata := map[int64]core.Metadata{}

	for rows.Next() {

		fmt.Println("rows.Next()")
		md := core.Metadata{}

		var (
			metaID     int64
			targetType string
			targetID   string
			metaKey    string
			metaValue  string
		)

		rows.Scan(
			&metaID,
			&targetType,
			&targetID,
			&metaKey,
			&metaValue,
		)

		var value json.RawMessage

		err = json.Unmarshal([]byte(metaValue), &value)
		if err != nil {
			return c, err
		}

		md[metaKey] = value
		foundMetadata[metaID] = md
	}

	results := make([]core.Metadata, len(foundMetadata))
	for id, md := range foundMetadata {
		results[id] = md
	}

	c.PageSize = q.Limit - 1

	c.HasMore = len(results) == q.Limit
	if c.HasMore {
		results = results[:len(results)-1]
	}
	c.Data = results

	total, _ := s.CountMeta()
	c.Total = int(total)

	return c, nil
}
