package bunexplain

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/uptrace/bun"
)

//nolint:unused
type explainHook struct{}

//nolint:unused
func (h *explainHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {}

//nolint:unused
func (h *explainHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	lowerQuery := strings.ToLower(event.Query)
	if strings.HasPrefix(lowerQuery, "explain") ||
		strings.HasPrefix(lowerQuery, "create") ||
		strings.HasPrefix(lowerQuery, "begin") ||
		strings.HasPrefix(lowerQuery, "alter") ||
		strings.HasPrefix(lowerQuery, "rollback") ||
		strings.HasPrefix(lowerQuery, "commit") {
		return ctx
	}

	event.DB.RunInTx(context.Background(), &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		rows, err := tx.Query("explain analyze " + event.Query)
		if err != nil {
			return err
		}
		defer rows.Next()

		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				return err
			}
			fmt.Println(line)
		}

		return tx.Rollback()

	})

	return ctx
}
