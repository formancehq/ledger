package sqlstorage

import (
	"context"
	"database/sql"
	"embed"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"
)

//go:embed migrates
var MigrationsFS embed.FS

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

		number, name := extractMigrationInformation(directoryName)

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
				for _, registeredGoMigration := range registeredGoMigrations {
					if registeredGoMigration.Number == number {
						for engine, goMigrationUnits := range registeredGoMigration.Handlers {
							units[engine] = append(units[engine], goMigrationUnits...)
						}
					}
				}
			}
		}

		migrations = append(migrations, Migration{
			Number:   number,
			Name:     name,
			Handlers: units,
		})
	}

	sort.Sort(migrations)

	return migrations, nil
}

func SQLMigrationFunc(content string) MigrationFunc {
	return func(ctx context.Context, schema Schema, tx *sql.Tx) error {
		plain := strings.ReplaceAll(content, "VAR_LEDGER_NAME", schema.Name())
		r := regexp.MustCompile(`[\n\t\s]+`)
		plain = r.ReplaceAllString(plain, " ")
		_, err := tx.ExecContext(ctx, plain)

		return err
	}
}
