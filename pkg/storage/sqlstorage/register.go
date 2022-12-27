package sqlstorage

import (
	"context"
	"database/sql"
	"path/filepath"
	"runtime"
	"strings"
)

var registeredGoMigrations []Migration

type MigrationFunc func(ctx context.Context, schema Schema, tx *sql.Tx) error

func RegisterGoMigration(fn MigrationFunc) {
	_, filename, _, _ := runtime.Caller(1)
	RegisterGoMigrationFromFilename(filename, fn)
}

func RegisterGoMigrationFromFilename(filename string, fn MigrationFunc) {
	rest, goFile := filepath.Split(filename)
	directory := filepath.Base(rest)

	number, name := extractMigrationInformation(directory)
	engine := strings.Split(goFile, ".")[0]

	registeredGoMigrations = append(registeredGoMigrations, Migration{
		Version: number,
		Name:    name,
		Handlers: map[string][]MigrationFunc{
			engine: {fn},
		},
	})
}
