package migrations

import (
	"context"
	"database/sql"
	"io/fs"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/opentelemetry"
	sqlerrors "github.com/formancehq/ledger/pkg/storage/sqlstorage/errors"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/schema"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const migrationsTableName = "migrations"

type MigrationsTable struct {
	bun.BaseModel `bun:"migrations,alias:migrations"`

	Version string `bun:"version,type:varchar,unique"`
	Date    string `bun:"date,type:varchar"`
}

func createMigrationsTable(ctx context.Context, schema schema.Schema) error {
	_, err := schema.NewCreateTable(migrationsTableName).
		Model((*MigrationsTable)(nil)).
		IfNotExists().
		Exec(ctx)

	return err
}

func Migrate(ctx context.Context, schema schema.Schema, migrations ...Migration) (bool, error) {
	ctx, span := opentelemetry.Start(ctx, "Migrate")
	defer span.End()

	if err := createMigrationsTable(ctx, schema); err != nil {
		return false, sqlerrors.PostgresError(err)
	}

	tx, err := schema.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return false, sqlerrors.PostgresError(err)
	}
	defer func(tx *bun.Tx) {
		_ = tx.Rollback()
	}(&tx)

	modified := false
	for _, m := range migrations {
		sb := schema.NewSelect(migrationsTableName).
			Model((*MigrationsTable)(nil)).
			Column("version").
			Where("version = ?", m.Version)

		// Does not use sql transaction because if the table does not exist, postgres will mark transaction as invalid
		row := schema.QueryRowContext(ctx, sb.String())
		var v string
		if err = row.Scan(&v); err != nil {
			logging.FromContext(ctx).Debugf("migration %s: %s", m.Version, err)
		}
		if v != "" {
			logging.FromContext(ctx).Debugf("migration %s: already up to date", m.Version)
			continue
		}
		modified = true

		logging.FromContext(ctx).Debugf("running migration %s", m.Version)

		handlersForAnyEngine, ok := m.Handlers["any"]
		if ok {
			for _, h := range handlersForAnyEngine {
				err := h(ctx, schema, &tx)
				if err != nil {
					return false, err
				}
			}
		}

		handlersForCurrentEngine, ok := m.Handlers[schema.Flavor()]
		if ok {
			for _, h := range handlersForCurrentEngine {
				err := h(ctx, schema, &tx)
				if err != nil {
					return false, err
				}
			}
		}

		m := MigrationsTable{
			Version: m.Version,
			Date:    time.Now().UTC().Format(time.RFC3339),
		}
		if _, err := schema.NewInsert(migrationsTableName).
			Model(&m).
			Exec(ctx); err != nil {
			logging.FromContext(ctx).Errorf("failed to insert migration version %s: %s", m.Version, err)
			return false, sqlerrors.PostgresError(err)
		}

	}

	return modified, sqlerrors.PostgresError(tx.Commit())
}

func GetMigrations(ctx context.Context, schema schema.Schema) ([]core.MigrationInfo, error) {
	sb := schema.NewSelect(migrationsTableName).
		Model((*MigrationsTable)(nil)).
		Column("version", "date")

	rows, err := schema.QueryContext(ctx, sb.String())
	if err != nil {
		return []core.MigrationInfo{}, err
	}
	defer rows.Close()

	res := make([]core.MigrationInfo, 0)
	for rows.Next() {
		var version, date string
		if err := rows.Scan(&version, &date); err != nil {
			return []core.MigrationInfo{}, err
		}
		t, err := time.Parse(time.RFC3339, date)
		if err != nil {
			return []core.MigrationInfo{},
				errors.Wrap(err, "parsing migration date")
		}
		res = append(res, core.MigrationInfo{
			Version: version,
			Date:    t,
		})
	}
	if rows.Err() != nil {
		return []core.MigrationInfo{}, err
	}

	return res, nil
}

func RegisterGoMigration(fn MigrationFunc) {
	_, filename, _, _ := runtime.Caller(1)
	RegisterGoMigrationFromFilename(filename, fn)
}

func PurgeGoMigrations() {
	RegisteredGoMigrations = []Migration{}
}

func RegisterGoMigrationFromFilename(filename string, fn MigrationFunc) {
	rest, goFile := filepath.Split(filename)
	directory := filepath.Base(rest)

	version, name := extractMigrationInformation(directory)
	engine := strings.Split(goFile, ".")[0]

	RegisteredGoMigrations = append(RegisteredGoMigrations, Migration{
		MigrationInfo: core.MigrationInfo{
			Version: version,
			Name:    name,
		},
		Handlers: map[string][]MigrationFunc{
			engine: {fn},
		},
	})
}

func extractMigrationInformation(filename string) (string, string) {
	parts := strings.SplitN(filename, "-", 2)
	number := parts[0]
	name := parts[1]
	return number, name
}

func CollectMigrationFiles(migrationsFS fs.FS) ([]Migration, error) {
	directories, err := fs.ReadDir(migrationsFS, "migrates")
	if err != nil {
		return nil, err
	}

	migrations := Migrations{}
	for _, directory := range directories {
		directoryName := directory.Name()

		version, name := extractMigrationInformation(directoryName)

		migrationDirectoryName := path.Join("migrates", directoryName)
		units := make(map[string][]MigrationFunc)
		unitsFiles, err := fs.ReadDir(migrationsFS, migrationDirectoryName)
		if err != nil {
			return nil, err
		}

		for _, unit := range unitsFiles {
			parts := strings.SplitN(unit.Name(), ".", 2)
			extension := parts[1]
			engine := parts[0]
			switch extension {
			case "sql":
				content, err := fs.ReadFile(migrationsFS, path.Join(migrationDirectoryName, unit.Name()))
				if err != nil {
					return nil, err
				}

				for _, statement := range strings.Split(string(content), "--statement") {
					statement = strings.TrimSpace(statement)
					if statement != "" {
						units[engine] = append(units[engine], SQLMigrationFunc(statement))
					}
				}

			case "go":
				for _, registeredGoMigration := range RegisteredGoMigrations {
					if registeredGoMigration.Version == version {
						for engine, goMigrationUnits := range registeredGoMigration.Handlers {
							units[engine] = append(units[engine], goMigrationUnits...)
						}
					}
				}
			}
		}

		migrations = append(migrations, Migration{
			MigrationInfo: core.MigrationInfo{
				Version: version,
				Name:    name,
			},
			Handlers: units,
		})
	}

	sort.Sort(migrations)

	return migrations, nil
}

func SQLMigrationFunc(content string) MigrationFunc {
	return func(ctx context.Context, schema schema.Schema, tx *bun.Tx) error {
		plain := strings.ReplaceAll(content, "VAR_LEDGER_NAME", schema.Name())
		r := regexp.MustCompile(`[\n\t\s]+`)
		plain = r.ReplaceAllString(plain, " ")
		_, err := tx.ExecContext(ctx, plain)

		return err
	}
}

var RegisteredGoMigrations []Migration

type MigrationFunc func(ctx context.Context, schema schema.Schema, tx *bun.Tx) error

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
