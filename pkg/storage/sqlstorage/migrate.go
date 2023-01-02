package sqlstorage

import (
	"context"
	"database/sql"
	"sort"
	"strconv"
	"time"

	"github.com/formancehq/go-libs/logging"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/pkg/errors"
)

func (s *Store) GetMigrationsDone(ctx context.Context) ([]core.MigrationInfo, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("*")
	sb.From(s.schema.Table("migrations"))

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return []core.MigrationInfo{}, s.error(err)
	}

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := executor.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return []core.MigrationInfo{}, s.error(err)
	}
	defer rows.Close()

	res := make([]core.MigrationInfo, 0)
	for rows.Next() {
		var version, date string
		if err := rows.Scan(&version, &date); err != nil {
			return []core.MigrationInfo{}, s.error(err)
		}
		t, err := time.Parse(time.RFC3339, date)
		if err != nil {
			return []core.MigrationInfo{},
				s.error(errors.Wrap(err, "parsing migration date"))
		}
		res = append(res, core.MigrationInfo{
			Version: version,
			Date:    t,
		})
	}
	if rows.Err() != nil {
		return []core.MigrationInfo{}, s.error(err)
	}

	return res, nil
}

func (s *Store) GetMigrationsAvailable() ([]core.MigrationInfo, error) {
	migrations, err := CollectMigrationFiles(MigrationsFS)
	if err != nil {
		return []core.MigrationInfo{}, errors.Wrap(err, "collecting migration files")
	}

	res := make([]core.MigrationInfo, 0)
	for _, m := range migrations {
		res = append(res, core.MigrationInfo{
			Version: m.Version,
			Name:    m.Name,
		})
	}

	return res, nil
}

type HandlersByEngine map[string][]MigrationFunc

type Migration struct {
	core.MigrationInfo `json:"inline"`
	Handlers           HandlersByEngine `json:"-"`
}

type Migrations []Migration

func (m Migrations) Len() int {
	return len(m)
}

func (m Migrations) Less(i, j int) bool {
	iNumber, err := strconv.ParseInt(m[i].Version, 10, 64)
	if err != nil {
		panic(err)
	}
	jNumber, err := strconv.ParseInt(m[j].Version, 10, 64)
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
		sb.Where(sb.E("version", m.Version))

		// Does not use sql transaction because if the table does not exist, postgres will mark transaction as invalid
		sqlq, args := sb.BuildWithFlavor(schema.Flavor())
		row := schema.QueryRowContext(ctx, sqlq, args...)
		var v string
		if err = row.Scan(&v); err != nil {
			logging.GetLogger(ctx).Debugf("migration %s: %s", m.Version, err)
		}
		if v != "" {
			logging.GetLogger(ctx).Debugf("migration %s: already up to date", m.Version)
			continue
		}
		modified = true

		logging.GetLogger(ctx).Debugf("running migration %s", m.Version)

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
		ib.Values(m.Version, time.Now().UTC().Format(time.RFC3339))
		sqlq, args = ib.BuildWithFlavor(schema.Flavor())
		if _, err = tx.ExecContext(ctx, sqlq, args...); err != nil {
			logging.GetLogger(ctx).Errorf("failed to insert migration version %s: %s", m.Version, err)
			return false, errorFromFlavor(Flavor(schema.Flavor()), err)
		}
	}

	return modified, errorFromFlavor(Flavor(schema.Flavor()), tx.Commit())
}
