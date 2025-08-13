package bunexplain

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/uptrace/bun"
)

type listener func(data string)

//nolint:unused
type explainHook struct {
	listener listener
	json     bool
}

//nolint:unused
func (h *explainHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {}

//nolint:unused
func (h *explainHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {

	lowerQuery := strings.ToLower(event.Query)
	if !strings.HasPrefix(lowerQuery, "select") && !strings.HasPrefix(lowerQuery, "with") {
		return ctx
	}

	if err := event.DB.RunInTx(context.Background(), &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		query := "explain "
		if h.json {
			query += "(format json) "
		} else {
			query += "analyze verbose "
		}
		query += event.Query
		rows, err := tx.Query(query)
		if err != nil {
			return err
		}
		defer func() {
			_ = rows.Close()
		}()

		data := bytes.NewBufferString("")
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				return err
			}
			data.WriteString(line)
			data.WriteString("\r\n")
		}
		if rows.Err() != nil {
			return rows.Err()
		}
		h.listener(data.String())

		return tx.Rollback()

	}); err != nil {
		// Nothing to do, let the original request fail
	}

	return ctx
}

func NewExplainHook(opts ...option) *explainHook {
	ret := &explainHook{}
	for _, opt := range append(defaultOptions, opts...) {
		opt(ret)
	}
	return ret
}

type option func(hook *explainHook)

var defaultOptions = []option{
	WithListener(func(data string) {
		fmt.Println(data)
	}),
}

var WithListener = func(w listener) option {
	return func(hook *explainHook) {
		hook.listener = w
	}
}

var WithJSONFormat = func() option {
	return func(hook *explainHook) {
		hook.json = true
	}
}
