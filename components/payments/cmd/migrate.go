package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/viper"

	// allow blank import to initiate migrations.
	_ "github.com/formancehq/payments/internal/app/migrations"
	_ "github.com/lib/pq"

	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"
)

func newMigrate() *cobra.Command {
	return &cobra.Command{
		Use:   "migrate",
		Short: "Run migrations",
		RunE:  runMigrate,
	}
}

// Usage: `go run cmd/main.go migrate --postgres-uri {uri} {command}`
/*
Commands:
    up                   Migrate the DB to the most recent version available
    up-by-one            Migrate the DB up by 1
    up-to VERSION        Migrate the DB to a specific VERSION
    down                 Roll back the version by 1
    down-to VERSION      Roll back to a specific VERSION
    redo                 Re-run the latest migration
    reset                Roll back all migrations
    status               Dump the migration status for the current DB
    version              Print the current version of the database
    create NAME [sql|go] Creates new migration file with the current timestamp
    fix                  Apply sequential ordering to migrations
*/

func runMigrate(cmd *cobra.Command, args []string) error {
	postgresURI := viper.GetString(postgresURIFlag)
	if postgresURI == "" {
		postgresURI = cmd.Flag(postgresURIFlag).Value.String()
	}

	if postgresURI == "" {
		return fmt.Errorf("postgres uri is not set")
	}

	database, err := goose.OpenDBWithDriver("postgres", postgresURI)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	defer func() {
		if err = database.Close(); err != nil {
			log.Fatalf("failed to close DB: %v\n", err)
		}
	}()

	if len(args) == 0 {
		return fmt.Errorf("missing migration direction")
	}

	command := args[0]

	if err = goose.Run(command, database, ".", args[1:]...); err != nil {
		log.Printf("migrate %v: %v", command, err)
	}

	return nil
}
