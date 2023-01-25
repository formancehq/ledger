package migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	up := func(tx *sql.Tx) error {
		_, err := tx.Exec(`
				CREATE TYPE connector_provider AS ENUM ('BANKING-CIRCLE', 'CURRENCY-CLOUD', 'DUMMY-PAY', 'MODULR', 'STRIPE', 'WISE');;

				CREATE TABLE connectors.connector (
				   id uuid  NOT NULL DEFAULT gen_random_uuid(),
				   created_at timestamp with time zone  NOT NULL DEFAULT NOW() CHECK (created_at<=NOW()),
				   provider connector_provider  NOT NULL UNIQUE,
				   enabled boolean  NOT NULL DEFAULT false,
				   config json NULL,
				   CONSTRAINT connector_pk PRIMARY KEY (id)
				);
		`)
		if err != nil {
			return err
		}

		return nil
	}

	down := func(tx *sql.Tx) error {
		_, err := tx.Exec(`
		DROP TABLE connectors.connector;
		DROP TYPE connector_provider;
		`)
		if err != nil {
			return err
		}

		return nil
	}

	goose.AddMigration(up, down)
}
