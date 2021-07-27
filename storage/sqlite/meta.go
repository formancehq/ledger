package sqlite

import (
	"fmt"
	"log"

	"github.com/huandu/go-sqlbuilder"
)

func (s *SQLiteStore) InjectMeta(ty string, id string, fn func(string, string)) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("meta_key", "meta_value")
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

		fn(meta_key, meta_value)
	}
}
