package migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	up := func(tx *sql.Tx) error {
		_, err := tx.Exec(`
			CREATE SCHEMA IF NOT EXISTS connectors;
			CREATE SCHEMA IF NOT EXISTS tasks;
			CREATE SCHEMA IF NOT EXISTS accounts;
			CREATE SCHEMA IF NOT EXISTS payments;
		`)
		if err != nil {
			return err
		}

		return nil
	}

	down := func(tx *sql.Tx) error {
		_, err := tx.Exec(`
			DROP SCHEMA IF EXISTS connectors;
			DROP SCHEMA IF EXISTS tasks;
			DROP SCHEMA IF EXISTS accounts;
			DROP SCHEMA IF EXISTS payments;
		`)
		if err != nil {
			return err
		}

		return nil
	}

	goose.AddMigration(up, down)
}
