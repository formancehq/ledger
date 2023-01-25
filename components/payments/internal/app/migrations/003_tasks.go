package migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	up := func(tx *sql.Tx) error {
		_, err := tx.Exec(`
				CREATE TYPE task_status AS ENUM ('STOPPED', 'PENDING', 'ACTIVE', 'TERMINATED', 'FAILED');;

				CREATE TABLE tasks.task (
					id uuid  NOT NULL DEFAULT gen_random_uuid(),
					connector_id uuid  NOT NULL,
					created_at timestamp with time zone  NOT NULL DEFAULT NOW() CHECK (created_at<=NOW()),
					updated_at timestamp with time zone  NOT NULL DEFAULT NOW() CHECK (created_at<=updated_at),
					name text  NOT NULL,
					descriptor json  NULL,
					status task_status  NOT NULL,
					error text  NULL,
					state json  NULL,
					CONSTRAINT task_pk PRIMARY KEY (id)
				);

				ALTER TABLE tasks.task ADD CONSTRAINT task_connector
					FOREIGN KEY (connector_id)
					REFERENCES connectors.connector (id)
					ON DELETE CASCADE
					NOT DEFERRABLE
					INITIALLY IMMEDIATE
				;
		`)
		if err != nil {
			return err
		}

		return nil
	}

	down := func(tx *sql.Tx) error {
		_, err := tx.Exec(`
		DROP TABLE tasks.task;
		DROP TYPE task_status;
		`)
		if err != nil {
			return err
		}

		return nil
	}

	goose.AddMigration(up, down)
}
