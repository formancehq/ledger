package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/formancehq/go-libs/sharedlogging"
	"github.com/google/uuid"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/opentelemetry"
)

type HandlersByEngine map[string][]MigrationFunc

type Migration struct {
	Number   string
	Name     string
	Handlers HandlersByEngine
}

type Migrations []Migration

func (m Migrations) Len() int {
	return len(m)
}

func (m Migrations) Less(i, j int) bool {
	iNumber, err := strconv.ParseInt(m[i].Number, 10, 64)
	if err != nil {
		panic(err)
	}
	jNumber, err := strconv.ParseInt(m[j].Number, 10, 64)
	if err != nil {
		panic(err)
	}
	return iNumber < jNumber
}

func (m Migrations) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

var _ sort.Interface = &Migrations{}

func Migrate(ctx context.Context, schema Schema, migrations ...Migration) (bool, error) {
	ctx, span := opentelemetry.Start(ctx, "Migrate")
	defer span.End()

	contextKeyID := uuid.NewString()
	id := span.SpanContext().SpanID()
	if id == [8]byte{} {
		sharedlogging.GetLogger(ctx).Debugf("sqlstorage migration SpanID is empty, new id generated %s", contextKeyID)
	} else {
		contextKeyID = fmt.Sprint(id)
	}

	q, args := sqlbuilder.
		CreateTable(schema.Table("migrations")).
		Define(`version varchar, date varchar, UNIQUE("version")`).
		IfNotExists().
		BuildWithFlavor(schema.Flavor())

	_, err := schema.ExecContext(ctx, q, args...)
	if err != nil {
		return false, errorFromFlavor(Flavor(schema.Flavor()), err)
	}

	tx, err := schema.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return false, errorFromFlavor(Flavor(schema.Flavor()), err)
	}
	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	modified := false
	for _, m := range migrations {
		sb := sqlbuilder.NewSelectBuilder()
		sb.Select("version")
		sb.From(schema.Table("migrations"))
		sb.Where(sb.E("version", m.Number))

		// Does not use sql transaction because if the table does not exist, postgres will mark transaction as invalid
		sqlq, args := sb.BuildWithFlavor(schema.Flavor())
		row := schema.QueryRowContext(ctx, sqlq, args...)
		var v string
		if err = row.Scan(&v); err != nil {
			sharedlogging.GetLogger(ctx).Debugf("migration %s (id: %s): %s", m.Number, contextKeyID, err)
		}
		if v != "" {
			sharedlogging.GetLogger(ctx).Debugf("migration %s (id: %s): already up to date", m.Number, contextKeyID)
			continue
		}
		modified = true

		sharedlogging.GetLogger(ctx).Debugf("running migration %s (id: %s)", m.Number, contextKeyID)

		handlersForAnyEngine, ok := m.Handlers["any"]
		if ok {
			for _, h := range handlersForAnyEngine {
				err := h(ctx, schema, tx)
				if err != nil {
					return false, err
				}
			}
		}

		handlersForCurrentEngine, ok := m.Handlers[Flavor(schema.Flavor()).String()]
		if ok {
			for _, h := range handlersForCurrentEngine {
				err := h(ctx, schema, tx)
				if err != nil {
					return false, err
				}
			}
		}

		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto(schema.Table("migrations"))
		ib.Cols("version", "date")
		ib.Values(m.Number, time.Now().UTC().Format(time.RFC3339))
		sqlq, args = ib.BuildWithFlavor(schema.Flavor())
		if _, err = tx.ExecContext(ctx, sqlq, args...); err != nil {
			sharedlogging.GetLogger(ctx).Errorf("failed to insert migration version %s (id: %s): %s",
				contextKeyID, m.Number, err)
			return false, errorFromFlavor(Flavor(schema.Flavor()), err)
		}
	}

	return modified, errorFromFlavor(Flavor(schema.Flavor()), tx.Commit())
}
