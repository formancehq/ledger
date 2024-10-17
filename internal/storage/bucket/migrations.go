package bucket

import (
	"bytes"
	"context"
	"embed"
	"go.opentelemetry.io/otel/trace"
	"text/template"

	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/uptrace/bun"
)

//go:embed migrations
var migrationsDir embed.FS

func GetMigrator(name string) *migrations.Migrator {

	migrator := migrations.NewMigrator(migrations.WithSchema(name, true))
	migrator.RegisterMigrationsFromFileSystem(migrationsDir, "migrations", func(s string) string {
		buf := bytes.NewBufferString("")

		t := template.Must(template.New("migration").Parse(s))
		if err := t.Execute(buf, map[string]any{
			"Bucket": name,
		}); err != nil {
			panic(err)
		}

		return buf.String()
	})

	return migrator
}

func Migrate(ctx context.Context, tracer trace.Tracer, db bun.IDB, name string) error {
	ctx, span := tracer.Start(ctx, "Migrate bucket")
	defer span.End()

	return GetMigrator(name).Up(ctx, db)
}
