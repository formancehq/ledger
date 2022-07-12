package sqlstorage

import (
	"context"
	"database/sql"
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
	pathParts := strings.Split(filename, "/")
	goFile := pathParts[len(pathParts)-1]
	directory := pathParts[len(pathParts)-2]
	number, name := extractMigrationInformation(directory)
	engine := strings.Split(goFile, ".")[0]

	registeredGoMigrations = append(registeredGoMigrations, Migration{
		Number: number,
		Name:   name,
		Handlers: map[string][]MigrationFunc{
			engine: {fn},
		},
	})
}
