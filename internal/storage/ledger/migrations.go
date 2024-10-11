package ledger

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"text/template"

	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/migrations"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
)

//go:embed migrations
var migrationsDir embed.FS

func getMigrator(ledger ledger.Ledger) *migrations.Migrator {
	migrator := migrations.NewMigrator(
		migrations.WithSchema(ledger.Bucket, false),
		migrations.WithTableName(fmt.Sprintf("migrations_%s", ledger.Name)),
	)
	migrator.RegisterMigrationsFromFileSystem(migrationsDir, "migrations", func(s string) string {
		buf := bytes.NewBufferString("")

		t := template.Must(template.New("migration").Parse(s))
		if err := t.Execute(buf, ledger); err != nil {
			panic(err)
		}

		return buf.String()
	})

	return migrator
}

func Migrate(ctx context.Context, db bun.IDB, ledger ledger.Ledger) error {
	ctx, span := tracing.Start(ctx, "Migrate ledger")
	defer span.End()

	return getMigrator(ledger).Up(ctx, db)
}
