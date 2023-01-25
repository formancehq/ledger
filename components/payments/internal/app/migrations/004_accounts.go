package migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	up := func(tx *sql.Tx) error {
		_, err := tx.Exec(`
				CREATE TYPE account_type AS ENUM('SOURCE', 'TARGET', 'UNKNOWN');;

				CREATE TABLE accounts.account (
					id uuid  NOT NULL DEFAULT gen_random_uuid(),
					created_at timestamp with time zone  NOT NULL DEFAULT NOW() CHECK (created_at<=NOW()),
					reference text  NOT NULL UNIQUE,
					provider text  NOT NULL,
					type account_type  NOT NULL,
					CONSTRAINT account_pk PRIMARY KEY (id)
				);
		`)
		if err != nil {
			return err
		}

		return nil
	}

	down := func(tx *sql.Tx) error {
		_, err := tx.Exec(`
		DROP TABLE accounts.account;
		DROP TYPE account_type;
		`)
		if err != nil {
			return err
		}

		return nil
	}

	goose.AddMigration(up, down)
}
