package storage

import (
	"embed"

	_ "github.com/formancehq/orchestration/internal/storage/migrations"
	"github.com/pressly/goose/v3"
	"github.com/uptrace/bun"
)

//go:embed migrations
var embedMigrations embed.FS

func init() {
	goose.SetBaseFS(embedMigrations)
}

func Migrate(db *bun.DB, debug bool) error {
	dialect := db.Dialect().Name().String()
	if dialect == "pg" {
		dialect = "postgres"
	}
	if !debug {
		goose.SetLogger(goose.NopLogger())
	} else {
		goose.SetVerbose(true)
	}
	if err := goose.SetDialect(dialect); err != nil {
		return err
	}

	if err := goose.Up(db.DB, "migrations"); err != nil {
		return err
	}

	return nil
}
